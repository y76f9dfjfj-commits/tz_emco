"""Сценарий A: пример ТЗ через настоящий Kafka и процессор как отдельный процесс.

ТЗ, раздел «Пример»: площадка S1 и S2 (в 3300 м к югу от S1), T1–T4 закреплены за S1.
Телеметрия: T1 стоит в 15 м от S1 с 11:59:00 (сообщения 11:59:00 и 12:00:00); T2 в 1200 м,
T3 в 3000 м, T4 в 900 м от S1 (2400 м от S2) едут к S1 со скоростью 36 км/ч в 12:00:00.
Ожидание (как в сквозном golden-примере): decision.v1 — отказ no_gain для T2 и рекомендация
T4 S1 → S2 с выигрышем 80 с (JSON ТЗ); последняя queue.v1 для S1 — таблица ТЗ (T1/T2/T3),
для S2 — только T4. Раздел ТЗ «Публикация»: подряд опубликованные очереди станции различаются.
Также: /health/ready → 200 после назначения партиции.
"""

from __future__ import annotations

import json
from itertools import pairwise
from pathlib import Path
from typing import Any, Final

import pytest

from tests.integration.kafkakit import (
    LauncherFactory,
    by_key,
    committed_offset,
    produce_all,
    read_committed,
    wait_until,
)
from tests.sitekit import S1_POINT, north_of
from vqueue.adapters import topics
from vqueue.domain.model import Point

pytestmark = pytest.mark.integration

NOW: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — момент расчёта примера."""

T1_OCCUPIED_AT: Final = 1_789_473_540
"""2026-09-15T11:59:00Z — первое сообщение T1 «стоит в 15 м от S1»."""

S2_POINT: Final = north_of(S1_POINT, -3_300.0)
UNLOAD_POINT: Final = north_of(S1_POINT, -8_000.0)
T1_POSITION: Final = north_of(S1_POINT, 15.0)
T2_POSITION: Final = north_of(S1_POINT, 1_200.0)
T3_POSITION: Final = north_of(S1_POINT, 3_000.0)
T4_POSITION: Final = north_of(S1_POINT, -900.0)

EXPECTED_T4_DECISION: Final[dict[str, Any]] = {
    "unit_uuid": "T4",
    "at": "2026-09-15T12:00:00Z",
    "result": "recommended",
    "from_station": "S1",
    "to_station": "S2",
    "gain_seconds": 80,
}
"""JSON decision.v1 из ТЗ один в один."""

EXPECTED_T2_DECISION: Final[dict[str, Any]] = {
    "unit_uuid": "T2",
    "at": "2026-09-15T12:00:00Z",
    "result": "rejected",
    "reason": "no_gain",
}
"""T2 в 1200 м от S1 тоже в точке решения: выигрыш 50 < 60 → no_gain (golden-пример)."""

EXPECTED_S1_QUEUE: Final[dict[str, Any]] = {
    "station_uuid": "S1",
    "at": "2026-09-15T12:00:00Z",
    "queue": [
        {
            "unit_uuid": "T1",
            "eta": None,
            "service_start": "2026-09-15T11:59:00Z",
            "free_at": "2026-09-15T12:02:50Z",
            "wait_seconds": 0,
        },
        {
            "unit_uuid": "T2",
            "eta": "2026-09-15T12:02:00Z",
            "service_start": "2026-09-15T12:02:50Z",
            "free_at": "2026-09-15T12:06:40Z",
            "wait_seconds": 50,
        },
        {
            "unit_uuid": "T3",
            "eta": "2026-09-15T12:05:00Z",
            "service_start": "2026-09-15T12:06:40Z",
            "free_at": "2026-09-15T12:10:30Z",
            "wait_seconds": 100,
        },
    ],
}
"""Таблица очереди к S1 из ТЗ на 12:00:00."""

EXPECTED_S2_QUEUE: Final[dict[str, Any]] = {
    "station_uuid": "S2",
    "at": "2026-09-15T12:00:00Z",
    "queue": [
        {
            "unit_uuid": "T4",
            "eta": "2026-09-15T12:04:00Z",
            "service_start": "2026-09-15T12:04:00Z",
            "free_at": "2026-09-15T12:07:50Z",
            "wait_seconds": 0,
        }
    ],
}
"""После рекомендации T4 едет к S2: приезд 12:04:00, очереди нет."""


def _point(p: Point) -> str:
    """Точка в TOML; repr float сохраняет значение без потерь."""
    return f"{{ lat = {p.lat!r}, lon = {p.lon!r} }}"


def _write_site_toml(path: Path) -> Path:
    """Пишет TOML площадки примера решения: S1, S2, точка разгрузки, T1–T4 за S1."""
    path.write_text(
        "\n".join(
            [
                "[rules]",
                "service_seconds = 200",
                "maneuver_seconds = 30",
                "speed_kmh = 36.0",
                "zone_radius_m = 50.0",
                "stopped_speed_kmh = 1.0",
                "decision_radius_m = 1500.0",
                "horizon_seconds = 1800",
                "freshness_seconds = 30",
                "min_gain_seconds = 60",
                "",
                "[unload_point]",
                f"lat = {UNLOAD_POINT.lat!r}",
                f"lon = {UNLOAD_POINT.lon!r}",
                "",
                "[[stations]]",
                'station_id = "S1"',
                f"location = {_point(S1_POINT)}",
                "",
                "[[stations]]",
                'station_id = "S2"',
                f"location = {_point(S2_POINT)}",
                "",
                "[assignments]",
                'T1 = "S1"',
                'T2 = "S1"',
                'T3 = "S1"',
                'T4 = "S1"',
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _telemetry(unit: str, ts: int, p: Point, speed_kmh: float) -> tuple[bytes, bytes]:
    """Сообщение telemetry.v1 в формате ТЗ (координаты без округления)."""
    value = {"unit_uuid": unit, "ts": ts, "lat": p.lat, "lon": p.lon, "speed_kmh": speed_kmh}
    return unit.encode(), json.dumps(value).encode()


EXAMPLE_STREAM: Final = [
    _telemetry("T1", T1_OCCUPIED_AT, T1_POSITION, 0.0),
    _telemetry("T1", NOW, T1_POSITION, 0.0),
    _telemetry("T2", NOW, T2_POSITION, 36.0),
    _telemetry("T3", NOW, T3_POSITION, 36.0),
    _telemetry("T4", NOW, T4_POSITION, 36.0),
]
"""Поток примера ТЗ в порядке поступления (как в сквозном golden-тесте)."""


def test_example_through_kafka_publishes_task_decision_and_queues(
    launcher: LauncherFactory, fresh_topics: str, tmp_path: Path, run_id: str
) -> None:
    """Пример ТЗ сквозь Kafka: решение T4 S1 → S2 80, отказ T2 no_gain, очереди S1/S2 по ТЗ.

    Также /health/ready → 200 после назначения партиции telemetry.v1.
    """
    bootstrap = fresh_topics
    procs = launcher(_write_site_toml(tmp_path / "site.toml"))
    proc = procs.start()
    proc.wait_ready(timeout_s=60)

    produce_all(bootstrap, topics.TELEMETRY, EXAMPLE_STREAM)
    wait_until(
        lambda: (
            committed_offset(bootstrap, procs.consumer_group, topics.TELEMETRY)
            == len(EXAMPLE_STREAM)
        ),
        60,
        "обработка всех сообщений примера (offset группы = 5)",
        interval_s=0.3,
    )

    decisions = by_key(read_committed(bootstrap, topics.DECISION, f"it-read-dec-{run_id}"))
    assert {k: [json.loads(v) for v in vs] for k, vs in decisions.items()} == {
        "T2": [EXPECTED_T2_DECISION],
        "T4": [EXPECTED_T4_DECISION],
    }

    queues = by_key(read_committed(bootstrap, topics.QUEUE, f"it-read-queue-{run_id}"))
    assert set(queues) == {"S1", "S2"}
    assert json.loads(queues["S1"][-1]) == EXPECTED_S1_QUEUE
    assert json.loads(queues["S2"][-1]) == EXPECTED_S2_QUEUE
    for station, values in queues.items():
        parsed = [json.loads(v)["queue"] for v in values]
        assert all(a != b for a, b in pairwise(parsed)), (
            f"Подряд одинаковые очереди {station}: {values}"
        )
