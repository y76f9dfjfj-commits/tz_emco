"""Сценарий B: exactly-once при аварийном падении процессора (SIGKILL) посреди обработки.

Сценарий: поток Simulation + FaultInjector (дубли и опоздания) на config/site.toml
записывается в telemetry.v1 заранее. Процессор убивается SIGKILL после первых закоммиченных
транзакций и перезапускается с тем же INSTANCE_ID. После обработки всего потока (offset группы
равен end offset) выходы decision.v1 и queue.v1, прочитанные read_committed, по каждому ключу
совпадают с эталоном — выходами SiteState, посчитанными в памяти по тому же потоку в том же
порядке и закодированными тем же codec: ни дублей, ни потерь, тот же порядок.
"""

from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path
from typing import Final

import pytest

from tests.integration.kafkakit import (
    LauncherFactory,
    OffsetProbe,
    by_key,
    end_offset,
    produce_all,
    read_committed,
    wait_until,
)
from vqueue.adapters import topics
from vqueue.adapters.codec import decode_telemetry, encode_decision, encode_queue
from vqueue.config import load_site_config
from vqueue.domain.queue import StationQueue
from vqueue.domain.site import SiteState
from vqueue.simulator.engine import Simulation
from vqueue.simulator.faults import FaultInjector
from vqueue.simulator.wire import encode_telemetry

pytestmark = pytest.mark.integration

SITE_TOML: Final = Path(__file__).resolve().parents[2] / "config" / "site.toml"
START_TS: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — начало виртуального времени симуляции."""

SEED: Final = 20_260_915
VIRTUAL_SECONDS: Final = 750
"""12,5 виртуальных минут: 40 машин × 750 с ≈ 30 тыс. сообщений (≈ 60 батчей по 500)."""

KILL_AT_OFFSET: Final = 1_000
"""SIGKILL после того, как закоммичены хотя бы два батча по 500 (≈ 3 % потока)."""


def _stream() -> list[tuple[bytes, bytes]]:
    """Поток telemetry.v1 в порядке выдачи FaultInjector (дубли и опоздания по 5 %)."""
    site = load_site_config(SITE_TOML)
    simulation = Simulation(site, start_ts=START_TS, seed=SEED)
    faults = FaultInjector(seed=SEED, dup_rate=0.05, late_rate=0.05)
    out = []
    for _ in range(VIRTUAL_SECONDS):
        generated = simulation.step()
        out.extend(faults.feed(simulation.ts, generated))
    out.extend(faults.flush())
    return [encode_telemetry(m) for m in out]


def _expected(
    stream: list[tuple[bytes, bytes]],
) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Эталон: выходы SiteState в памяти по тому же потоку (как его декодирует процессор).

    Returns:
        (decision.v1 по unit_uuid, queue.v1 по station_uuid) — значения в порядке публикации.
    """
    state = SiteState(load_site_config(SITE_TOML))
    decisions: dict[str, list[str]] = defaultdict(list)
    queues: dict[str, list[str]] = defaultdict(list)
    for _key, value in stream:
        for output in state.apply(decode_telemetry(value)):
            if isinstance(output, StationQueue):
                k, v = encode_queue(output)
                queues[k.decode()].append(v.decode())
            else:
                k, v = encode_decision(output)
                decisions[k.decode()].append(v.decode())
    return dict(decisions), dict(queues)


def _diff(name: str, actual: dict[str, list[str]], expected: dict[str, list[str]]) -> str:
    """Краткое описание расхождений по ключам для сообщения об ошибке."""
    lines = []
    for key in sorted(set(actual) | set(expected)):
        a, e = actual.get(key, []), expected.get(key, [])
        if a != e:
            first = next(
                (i for i, (x, y) in enumerate(zip(a, e, strict=False)) if x != y),
                min(len(a), len(e)),
            )
            lines.append(
                f"{name}[{key}]: получено {len(a)}, ожидалось {len(e)}, расхождение #{first}:"
                f" {a[first] if first < len(a) else '—'} != {e[first] if first < len(e) else '—'}"
            )
    return "\n".join(lines[:10])


def test_sigkill_mid_processing_restart_outputs_equal_reference_exactly_once(
    launcher: LauncherFactory, fresh_topics: str, run_id: str
) -> None:
    """SIGKILL посреди обработки + перезапуск → выходы по ключам равны эталону SiteState.

    ТЗ «Порядок и дубли»: дубли и опоздавшие сообщения входят в поток;
    транзакционная обработка — ни дублей, ни потерь выходов, порядок по ключу сохранён.
    """
    bootstrap = fresh_topics
    stream = _stream()
    expected_decisions, expected_queues = _expected(stream)
    assert expected_decisions, "эталон без решений — сценарий ничего не проверяет"
    assert len(stream) > 20 * KILL_AT_OFFSET, "поток слишком короток для убийства посреди"

    produce_all(bootstrap, topics.TELEMETRY, stream)
    end = end_offset(bootstrap, topics.TELEMETRY)
    assert end == len(stream)

    procs = launcher(SITE_TOML)
    probe = OffsetProbe(bootstrap, procs.consumer_group, topics.TELEMETRY)
    try:
        first = procs.start()
        wait_until(
            lambda: probe.committed() >= KILL_AT_OFFSET,
            90,
            f"закоммичены первые {KILL_AT_OFFSET} сообщений (два батча)",
            interval_s=0.02,
        )
        first.kill()
        at_kill = probe.committed()
        assert KILL_AT_OFFSET <= at_kill < end, (
            f"SIGKILL не пришёлся на середину: offset {at_kill} из {end}"
        )

        second = procs.start()
        started = time.monotonic()
        wait_until(
            lambda: probe.committed() == end,
            150,
            f"обработка всего потока после перезапуска (end offset {end})",
            interval_s=0.2,
        )
    finally:
        probe.close()
    print(f"SIGKILL на offset {at_kill}/{end}; дообработка {time.monotonic() - started:.1f} с")
    assert second.popen.poll() is None, "перезапущенный процессор завершился"

    decisions = by_key(read_committed(bootstrap, topics.DECISION, f"it-read-dec-{run_id}"))
    queues = by_key(read_committed(bootstrap, topics.QUEUE, f"it-read-queue-{run_id}"))

    assert decisions == expected_decisions, _diff("decision", decisions, expected_decisions)
    assert queues == expected_queues, _diff("queue", queues, expected_queues)
