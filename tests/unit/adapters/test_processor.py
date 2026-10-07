"""Тесты процессора батча: телеметрия → записи queue.v1, decision.v1 и снимок состояния.

ТЗ «Что нужно сделать», п.1: сервис читает telemetry.v1 и публикует queue.v1 (ключ
station_uuid) и decision.v1 (ключ unit_uuid). выходы в порядке выдачи
агрегата площадки, битое сообщение — WARNING и пропуск, в конце батча ровно одна запись
снимка в топик состояния (ключ site_id), reset откатывает состояние.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence

import pytest

from tests.unit.adapters.example import (
    EXAMPLE_TELEMETRY,
    EXAMPLE_VALUES,
    NOW,
    SITE,
    SITE_ID,
    T1,
    T1_POSITION,
    T4,
    telemetry_json,
)
from vqueue.adapters.codec import decode_snapshot, encode_decision, encode_queue, encode_snapshot
from vqueue.adapters.processor import OutRecord, TelemetryProcessor
from vqueue.adapters.topics import DECISION, QUEUE, STATE, TELEMETRY
from vqueue.domain.model import Telemetry
from vqueue.domain.queue import StationQueue
from vqueue.domain.site import SiteSnapshot, SiteState

FIRST_BATCH = EXAMPLE_VALUES[:2]
"""T1 занимает S1 и продолжает стоять."""
SECOND_BATCH = EXAMPLE_VALUES[2:]
"""T2, T3, T4 в 12:00:00: очереди S1/S2, отказ T2, рекомендация T4."""


def _expected(
    messages: Iterable[Telemetry], snapshot: SiteSnapshot | None = None
) -> tuple[list[OutRecord], SiteSnapshot]:
    """Ожидаемые записи батча, рассчитанные доменом напрямую, и итоговый снимок."""
    state = SiteState(SITE, snapshot)
    records: list[OutRecord] = []
    for msg in messages:
        for out in state.apply(msg):
            if isinstance(out, StationQueue):
                key, value = encode_queue(out)
                records.append(OutRecord(QUEUE, key, value))
            else:
                key, value = encode_decision(out)
                records.append(OutRecord(DECISION, key, value))
    final = state.snapshot()
    records.append(OutRecord(STATE, SITE_ID.encode(), encode_snapshot(final)))
    return records, final


def _state_records(records: Sequence[OutRecord]) -> list[OutRecord]:
    """Записи в топик состояния."""
    return [r for r in records if r.topic == STATE]


def test_topics_names_match_task() -> None:
    """Имена топиков; входной telemetry.v1 и выходные queue.v1, decision.v1 — из ТЗ."""
    assert TELEMETRY == "telemetry.v1"
    assert QUEUE == "queue.v1"
    assert DECISION == "decision.v1"
    assert STATE == "vqueue.state.v1"


def test_out_record_is_frozen_value() -> None:
    """OutRecord — неизменяемое значение со сравнением по полям."""
    record = OutRecord(QUEUE, b"S1", b"{}")

    assert record == OutRecord(QUEUE, b"S1", b"{}")
    with pytest.raises(AttributeError):
        record.topic = DECISION  # type: ignore[misc]


def test_process_example_batch_records_match_domain_outputs() -> None:
    """Пример ТЗ одним батчем: записи и их порядок совпадают с выдачей агрегата SiteState."""
    processor = TelemetryProcessor(SITE, SITE_ID)

    records = processor.process(EXAMPLE_VALUES)

    expected, _ = _expected(EXAMPLE_TELEMETRY)
    assert records == expected


def test_process_example_batch_topics_and_keys() -> None:
    """ТЗ «Данные»: queue.v1 с ключом station_uuid, decision.v1 с ключом unit_uuid."""
    records = TelemetryProcessor(SITE, SITE_ID).process(EXAMPLE_VALUES)

    for record in records:
        payload = json.loads(record.value)
        if record.topic == QUEUE:
            assert record.key == payload["station_uuid"].encode()
        elif record.topic == DECISION:
            assert record.key == payload["unit_uuid"].encode()
        else:
            assert record.topic == STATE
            assert record.key == SITE_ID.encode()
    assert {r.topic for r in records} == {QUEUE, DECISION, STATE}


def test_process_example_batch_t4_decision_matches_task_json() -> None:
    """ТЗ «Данные», decision.v1: решение T4 в выходе процессора — JSON из ТЗ один в один."""
    records = TelemetryProcessor(SITE, SITE_ID).process(EXAMPLE_VALUES)

    t4 = [r for r in records if r.topic == DECISION and r.key == T4.encode()]
    assert len(t4) == 1
    assert json.loads(t4[0].value) == {
        "unit_uuid": "T4",
        "at": "2026-09-15T12:00:00Z",
        "result": "recommended",
        "from_station": "S1",
        "to_station": "S2",
        "gain_seconds": 80,
    }


def test_process_batch_state_record_last_and_single() -> None:
    """В конце батча ровно одна запись снимка, key = site_id."""
    records = TelemetryProcessor(SITE, SITE_ID).process(EXAMPLE_VALUES)

    assert _state_records(records) == [records[-1]]
    assert records[-1].key == SITE_ID.encode()


def test_process_state_record_decodes_to_processor_snapshot() -> None:
    """Запись снимка — encode_snapshot(состояния после батча) и равна processor.snapshot()."""
    processor = TelemetryProcessor(SITE, SITE_ID)

    records = processor.process(EXAMPLE_VALUES)

    _, final = _expected(EXAMPLE_TELEMETRY)
    assert processor.snapshot() == final
    assert decode_snapshot(records[-1].value) == final


def test_process_batch_without_outputs_emits_only_state() -> None:
    """Снимок пишется даже без выходов — дубль сообщения T1 ничего не публикует."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    processor.process(FIRST_BATCH)

    records = processor.process([FIRST_BATCH[-1]])

    assert len(records) == 1
    assert records[0].topic == STATE
    assert records[0].key == SITE_ID.encode()
    assert decode_snapshot(records[0].value) == processor.snapshot()


def test_process_empty_batch_emits_only_state() -> None:
    """Пустой батч — одна запись снимка (исходного состояния)."""
    processor = TelemetryProcessor(SITE, SITE_ID)

    records = processor.process([])

    assert records == [
        OutRecord(STATE, SITE_ID.encode(), encode_snapshot(SiteState(SITE).snapshot()))
    ]


def test_process_two_batches_equal_one_batch_outputs() -> None:
    """Деление потока на батчи не меняет выходов: только добавляется снимок в конце каждого."""
    processor = TelemetryProcessor(SITE, SITE_ID)

    split = processor.process(FIRST_BATCH) + processor.process(SECOND_BATCH)

    whole = TelemetryProcessor(SITE, SITE_ID).process(EXAMPLE_VALUES)
    assert [r for r in split if r.topic != STATE] == [r for r in whole if r.topic != STATE]
    assert len(_state_records(split)) == 2
    assert split[-1] == whole[-1]


def test_process_invalid_message_skipped_rest_processed(caplog: pytest.LogCaptureFixture) -> None:
    """Битое сообщение пропускается с WARNING, остальные обрабатываются."""
    values = [*EXAMPLE_VALUES[:2], b"{broken", *EXAMPLE_VALUES[2:]]
    processor = TelemetryProcessor(SITE, SITE_ID)

    with caplog.at_level(logging.WARNING):
        records = processor.process(values)

    expected, _ = _expected(EXAMPLE_TELEMETRY)
    assert records == expected
    assert any(r.levelno == logging.WARNING for r in caplog.records)


@pytest.mark.parametrize(
    "bad",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"[]", id="not-object"),
        pytest.param(b'{"unit_uuid": "T1"}', id="missing-fields"),
        pytest.param(
            telemetry_json(T1, NOW, T1_POSITION, -1.0),
            id="negative-speed",
        ),
    ],
)
def test_process_only_invalid_messages_emits_only_state(
    bad: bytes, caplog: pytest.LogCaptureFixture
) -> None:
    """Батч только из битых сообщений: состояние не меняется, одна запись снимка, WARNING."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    before = processor.snapshot()

    with caplog.at_level(logging.WARNING):
        records = processor.process([bad])

    assert processor.snapshot() == before
    assert [r.topic for r in records] == [STATE]
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1


def test_process_valid_batch_no_warnings(caplog: pytest.LogCaptureFixture) -> None:
    """Корректный батч не порождает предупреждений."""
    with caplog.at_level(logging.WARNING):
        TelemetryProcessor(SITE, SITE_ID).process(EXAMPLE_VALUES)

    assert not [r for r in caplog.records if r.levelno >= logging.WARNING]


def test_processor_initial_snapshot_continues_from_it() -> None:
    """Процессор, созданный из снимка, продолжает так же, как исходный (рестарт из STATE)."""
    original = TelemetryProcessor(SITE, SITE_ID)
    original.process(FIRST_BATCH)
    restored = TelemetryProcessor(SITE, SITE_ID, original.snapshot())

    assert restored.process(SECOND_BATCH) == original.process(SECOND_BATCH)


def test_processor_snapshot_initially_empty_state() -> None:
    """Без снимка процессор начинает с пустой площадки."""
    assert TelemetryProcessor(SITE, SITE_ID).snapshot() == SiteState(SITE).snapshot()


def test_reset_to_snapshot_replay_same_batch_same_records() -> None:
    """Reset(снимок) откатывает состояние — повтор того же батча даёт те же записи."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    processor.process(FIRST_BATCH)
    before = processor.snapshot()
    first = processor.process(SECOND_BATCH)

    processor.reset(before)

    assert processor.snapshot() == before
    assert processor.process(SECOND_BATCH) == first


def test_reset_none_returns_to_initial_state() -> None:
    """Reset(None) возвращает к пустой площадке: первый батч повторяется один в один."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    first = processor.process(EXAMPLE_VALUES)

    processor.reset(None)

    assert processor.snapshot() == SiteState(SITE).snapshot()
    assert processor.process(EXAMPLE_VALUES) == first


def test_without_reset_replay_is_deduplicated() -> None:
    """Без отката повтор батча — дубли (ТЗ «Порядок и дубли»): выходов нет, только снимок.

    Показывает, что reset действительно нужен для повторной обработки после abort.
    """
    processor = TelemetryProcessor(SITE, SITE_ID)
    processor.process(SECOND_BATCH)

    records = processor.process(SECOND_BATCH)

    assert [r.topic for r in records] == [STATE]


def test_processor_accepts_any_iterable_of_values() -> None:
    """Метод process принимает любой Iterable[bytes], в том числе генератор."""
    records = TelemetryProcessor(SITE, SITE_ID).process(v for v in EXAMPLE_VALUES)

    expected, _ = _expected(EXAMPLE_TELEMETRY)
    assert records == expected
