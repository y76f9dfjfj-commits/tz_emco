"""Тесты транзакционного цикла KafkaRunner и восстановления снимка load_snapshot на фейках.

ТЗ «Что нужно сделать», п.1: сервис читает telemetry.v1 из Kafka и работает на непрерывном
потоке; «Архитектура обработки», п.5–6: транзакционность выходов и восстановление
состояния при рестарте. run_once — consume → mark_poll → begin →
produce всех записей → send_offsets (offset последнего + 1 по партиции) → commit;
retriable-ошибка send_offsets/commit → повтор того же вызова до max_retries; abortable →
abort, откат процессора и seek на начало батча (подряд max_consecutive_aborts — наружу);
фатальная — наружу. Rebalance: on_assign восстанавливает состояние загрузчиком снимка.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Sequence
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException, TopicPartition

from tests.unit.adapters.example import (
    EXAMPLE_TELEMETRY,
    EXAMPLE_VALUES,
    SITE,
    SITE_ID,
)
from tests.unit.adapters.fakes import (
    FakeConsumer,
    FakeMessage,
    FakeProducer,
    FakeStateConsumer,
    Produced,
    TxnState,
    abortable_error,
    eof,
    fatal_error,
    retriable_error,
)
from vqueue.adapters import kafka_runner as runner_module
from vqueue.adapters.codec import encode_snapshot
from vqueue.adapters.health import HealthState
from vqueue.adapters.kafka_runner import KafkaRunner, load_snapshot
from vqueue.adapters.processor import TelemetryProcessor
from vqueue.adapters.topics import STATE, TELEMETRY
from vqueue.domain.site import SiteSnapshot, SiteState

FIRST_OFFSET = 100
"""Офсет первого сообщения журнала: позиции не начинаются с нуля."""


def _log(
    values: Sequence[bytes], partition: int = 0, first: int = FIRST_OFFSET
) -> list[FakeMessage]:
    """Журнал telemetry.v1: значения подряд с офсетами first, first + 1, ..."""
    return [
        FakeMessage(TELEMETRY, partition, first + i, value, b"k") for i, value in enumerate(values)
    ]


def _baseline(*batches: Sequence[bytes]) -> list[Produced]:
    """Записи, которые выдаёт процессор на те же батчи без сбоев."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    out: list[Produced] = []
    for batch in batches:
        out.extend((r.topic, r.key, r.value) for r in processor.process(batch))
    return out


class CountingHealth(HealthState):
    """HealthState со счётчиком mark_poll."""

    def __init__(self) -> None:
        """Создаёт состояние с нулевым счётчиком."""
        super().__init__()
        self.polls = 0

    def mark_poll(self) -> None:
        """Отмечает poll и считает вызовы."""
        self.polls += 1
        super().mark_poll()


def _no_snapshot() -> SiteSnapshot | None:
    """Загрузчик снимка для тестов, где восстановление не важно."""
    return None


def _runner(
    consumer: FakeConsumer,
    producer: FakeProducer,
    health: HealthState | None = None,
    **kwargs: Any,
) -> tuple[KafkaRunner, TelemetryProcessor]:
    """Цикл с реальным процессором примера ТЗ."""
    processor = TelemetryProcessor(SITE, SITE_ID)
    loader = kwargs.pop("load_snapshot", _no_snapshot)
    runner = KafkaRunner(
        consumer,
        producer,
        processor,
        health if health is not None else HealthState(),
        loader,
        **kwargs,
    )
    return runner, processor


# ---------------------------------------------------------------------------
# `run_once`: пустой poll
# ---------------------------------------------------------------------------


def test_run_once_empty_poll_returns_zero_and_marks_poll() -> None:
    """Пустой poll → 0, mark_poll выполнен, транзакции нет."""
    health = HealthState()
    health.mark_assigned(True)
    consumer, producer = FakeConsumer(), FakeProducer()
    runner, _ = _runner(consumer, producer, health)
    assert not health.is_ready(60.0)

    assert runner.run_once() == 0

    assert health.is_ready(60.0)
    assert producer.calls == []


def test_run_once_consume_uses_batch_size_and_timeout() -> None:
    """Consume(batch_size, poll_timeout_s); по умолчанию 500 и 1.0 с."""
    default = FakeConsumer()
    _runner(default, FakeProducer())[0].run_once()
    custom = FakeConsumer()
    _runner(custom, FakeProducer(), batch_size=7, poll_timeout_s=0.25)[0].run_once()

    assert default.consume_calls == [(500, 1.0)]
    assert custom.consume_calls == [(7, 0.25)]


def test_run_once_marks_poll_on_non_empty_batch() -> None:
    """`mark_poll` выполняется и для непустого батча."""
    health = HealthState()
    health.mark_assigned(True)
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), FakeProducer(), health)

    runner.run_once()

    assert health.is_ready(60.0)


# ---------------------------------------------------------------------------
# `run_once`: успешный батч
# ---------------------------------------------------------------------------


def test_run_once_success_transaction_call_order() -> None:
    """Begin → produce каждой записи → send_offsets → commit."""
    consumer, producer = FakeConsumer(_log(EXAMPLE_VALUES)), FakeProducer()
    runner, _ = _runner(consumer, producer)

    runner.run_once()

    expected = _baseline(EXAMPLE_VALUES)
    assert producer.calls == [
        "begin_transaction",
        *["produce"] * len(expected),
        "send_offsets_to_transaction",
        "commit_transaction",
    ]


def test_run_once_success_returns_message_count() -> None:
    """`run_once` возвращает число обработанных сообщений батча."""
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), FakeProducer())

    assert runner.run_once() == len(EXAMPLE_VALUES)


def test_run_once_success_commits_processor_records_in_order() -> None:
    """Все записи процессора (очереди, решения, снимок) зафиксированы в его порядке."""
    producer = FakeProducer()
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), producer)

    runner.run_once()

    assert producer.committed == _baseline(EXAMPLE_VALUES)
    assert producer.committed[-1][:2] == (STATE, SITE_ID.encode())


def test_run_once_success_sends_last_offset_plus_one() -> None:
    """В транзакцию уходит offset последнего сообщения + 1 и метаданные группы."""
    consumer, producer = FakeConsumer(_log(EXAMPLE_VALUES)), FakeProducer()
    runner, _ = _runner(consumer, producer)

    runner.run_once()

    last = FIRST_OFFSET + len(EXAMPLE_VALUES) - 1
    assert producer.sent_offsets == [({(TELEMETRY, 0, last + 1)}, consumer.group_metadata)]


def test_run_once_multi_partition_offsets_per_partition() -> None:
    """Offset последнего + 1 по каждой партиции батча отдельно."""
    p0 = _log(EXAMPLE_VALUES[:3], partition=0, first=10)
    p1 = _log(EXAMPLE_VALUES[3:], partition=1, first=5)
    consumer = FakeConsumer([p0[0], p1[0], p0[1], p1[1], p0[2]])
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    assert runner.run_once() == 5

    assert producer.sent_offsets[0][0] == {(TELEMETRY, 0, 13), (TELEMETRY, 1, 7)}


def test_run_once_values_processed_in_batch_order() -> None:
    """Значения передаются процессору в порядке батча (детерминизм по партиции)."""
    p0 = _log(EXAMPLE_VALUES[:3], partition=0, first=10)
    p1 = _log(EXAMPLE_VALUES[3:], partition=1, first=5)
    order = [p0[0], p1[0], p0[1], p1[1], p0[2]]
    producer = FakeProducer()
    runner, _ = _runner(FakeConsumer(order), producer)

    runner.run_once()

    values = [m.value_ for m in order]
    assert producer.committed == _baseline([v for v in values if v is not None])


def test_run_once_consecutive_batches_continue_state() -> None:
    """Последовательные батчи: состояние процессора переходит между транзакциями."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=2)
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    counts = [runner.run_once() for _ in range(4)]

    assert counts == [2, 2, 1, 0]
    assert producer.committed == _baseline(
        EXAMPLE_VALUES[:2], EXAMPLE_VALUES[2:4], EXAMPLE_VALUES[4:]
    )
    assert [s for s, _ in producer.sent_offsets] == [
        {(TELEMETRY, 0, FIRST_OFFSET + 2)},
        {(TELEMETRY, 0, FIRST_OFFSET + 4)},
        {(TELEMETRY, 0, FIRST_OFFSET + 5)},
    ]


# ---------------------------------------------------------------------------
# `run_once`: ошибки сообщений
# ---------------------------------------------------------------------------


def test_run_once_partition_eof_ignored() -> None:
    """_PARTITION_EOF игнорируется, остальные сообщения обрабатываются."""
    data = _log(EXAMPLE_VALUES)
    consumer = FakeConsumer()
    consumer.injected.append([*data[:2], eof(0, FIRST_OFFSET + 2), *data[2:]])
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    assert runner.run_once() == len(EXAMPLE_VALUES)

    assert producer.committed == _baseline(EXAMPLE_VALUES)
    assert producer.sent_offsets[0][0] == {(TELEMETRY, 0, FIRST_OFFSET + len(EXAMPLE_VALUES))}


def test_run_once_only_eof_returns_zero() -> None:
    """Батч только из EOF — обработанных сообщений нет."""
    consumer = FakeConsumer()
    consumer.injected.append([eof(0, 42)])
    runner, _ = _runner(consumer, FakeProducer())

    assert runner.run_once() == 0


def test_run_once_other_message_error_raises_kafka_exception() -> None:
    """Иная ошибка в сообщении → KafkaException наружу, транзакция не начата."""
    consumer = FakeConsumer()
    error = FakeMessage(error_=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "no topic"))
    consumer.injected.append([*_log(EXAMPLE_VALUES[:1]), error])
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    with pytest.raises(KafkaException):
        runner.run_once()

    assert producer.committed == []
    assert "commit_transaction" not in producer.calls


@pytest.mark.parametrize("missing", ["topic_", "partition_", "offset_"])
def test_run_once_message_without_position_rejected(missing: str) -> None:
    """Сообщение без ошибки, но без топика/партиции/офсета → ValueError, батч не фиксируется."""
    msg = _log(EXAMPLE_VALUES[:1])[0]
    setattr(msg, missing, None)
    consumer = FakeConsumer()
    consumer.injected.append([msg])
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    with pytest.raises(ValueError):
        runner.run_once()

    assert producer.committed == []


# ---------------------------------------------------------------------------
# `run_once`: retriable-ошибка — повтор того же вызова без abort
# ---------------------------------------------------------------------------

TXN_RETRY_CALLS = pytest.mark.parametrize(
    "failing_call", ["send_offsets_to_transaction", "commit_transaction"]
)
"""Вызовы, которые повторяются при retriable-ошибке."""


@TXN_RETRY_CALLS
def test_run_once_retriable_error_retries_same_call_without_abort(failing_call: str) -> None:
    """Retriable в send_offsets/commit → повтор того же вызова, abort и seek не вызываются."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail(failing_call, retriable_error())
    runner, _ = _runner(consumer, producer)

    assert runner.run_once() == len(EXAMPLE_VALUES)

    assert producer.count(failing_call) == 2
    assert producer.count("begin_transaction") == 1
    assert producer.count("abort_transaction") == 0
    assert consumer.seeks == []
    assert producer.committed == _baseline(EXAMPLE_VALUES)
    assert producer.sent_offsets[-1][0] == {(TELEMETRY, 0, FIRST_OFFSET + len(EXAMPLE_VALUES))}


@TXN_RETRY_CALLS
def test_run_once_retriable_up_to_max_retries_then_success_committed(failing_call: str) -> None:
    """Граница: max_retries (по умолчанию 3) неудач подряд — четвёртая попытка фиксирует батч."""
    producer = FakeProducer()
    producer.fail(failing_call, *(retriable_error() for _ in range(3)))
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), producer)

    assert runner.run_once() == len(EXAMPLE_VALUES)

    assert producer.count(failing_call) == 4
    assert producer.count("abort_transaction") == 0
    assert producer.committed == _baseline(EXAMPLE_VALUES)


@TXN_RETRY_CALLS
def test_run_once_retriable_exhausted_raises_without_abort(failing_call: str) -> None:
    """Повторы исчерпаны (1 + max_retries неудач) → исключение наружу, abort не вызывается."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail(failing_call, *(retriable_error() for _ in range(4)))
    runner, _ = _runner(consumer, producer)

    with pytest.raises(KafkaException):
        runner.run_once()

    assert producer.count(failing_call) == 4
    assert producer.count("abort_transaction") == 0
    assert consumer.seeks == []
    assert producer.committed == []


@pytest.mark.parametrize(("failures", "succeeds"), [(1, True), (2, False)])
def test_run_once_custom_max_retries_respected(failures: int, succeeds: bool) -> None:
    """`max_retries=1`: одна retriable-неудача commit переживается, две — нет."""
    producer = FakeProducer()
    producer.fail("commit_transaction", *(retriable_error() for _ in range(failures)))
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), producer, max_retries=1)

    if succeeds:
        assert runner.run_once() == len(EXAMPLE_VALUES)
        assert producer.committed == _baseline(EXAMPLE_VALUES)
    else:
        with pytest.raises(KafkaException):
            runner.run_once()
        assert producer.committed == []
    assert producer.count("commit_transaction") == failures + int(succeeds)


@TXN_RETRY_CALLS
def test_run_once_abortable_during_retry_rolls_back(failing_call: str) -> None:
    """Retriable, затем abortable на повторе → abort, откат процессора, seek, 0."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail(failing_call, retriable_error(), abortable_error())
    runner, processor = _runner(consumer, producer)

    assert runner.run_once() == 0

    assert producer.count("abort_transaction") == 1
    assert processor.snapshot() == SiteState(SITE).snapshot()
    assert set(consumer.seeks) == {(TELEMETRY, 0, FIRST_OFFSET)}
    assert producer.committed == []


def test_run_once_retriable_in_middle_batch_exactly_once_outputs() -> None:
    """Exactly-once: retriable-сбой commit во втором батче не дублирует и не теряет записи."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=2)
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)
    runner.run_once()
    producer.fail("commit_transaction", retriable_error())

    while runner.run_once():
        pass

    assert producer.committed == _baseline(
        EXAMPLE_VALUES[:2], EXAMPLE_VALUES[2:4], EXAMPLE_VALUES[4:]
    )


# ---------------------------------------------------------------------------
# `run_once`: abortable-ошибка — откат и повтор батча (exactly-once по выходам)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "failing_call", ["produce", "send_offsets_to_transaction", "commit_transaction"]
)
def test_run_once_abortable_error_aborts_and_seeks_batch_start(failing_call: str) -> None:
    """Abortable-ошибка → abort, seek на начало батча, возврат 0, ничего не зафиксировано."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail(failing_call, abortable_error())
    runner, _ = _runner(consumer, producer)

    assert runner.run_once() == 0

    assert producer.count("abort_transaction") == 1
    assert producer.state is TxnState.NONE
    assert producer.committed == []
    assert set(consumer.seeks) == {(TELEMETRY, 0, FIRST_OFFSET)}


def test_run_once_abort_rolls_back_processor() -> None:
    """После abort состояние процессора — снимок начала батча."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=2)
    producer = FakeProducer()
    runner, processor = _runner(consumer, producer)
    runner.run_once()
    before = processor.snapshot()
    producer.fail("commit_transaction", abortable_error())

    assert runner.run_once() == 0

    assert processor.snapshot() == before
    assert set(consumer.seeks) == {(TELEMETRY, 0, FIRST_OFFSET + 2)}


def test_run_once_abort_then_replay_exactly_once_outputs() -> None:
    """Exactly-once: после abort и повтора батча зафиксированы те же записи, что без сбоя."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=2)
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)
    runner.run_once()
    producer.fail("commit_transaction", abortable_error())
    assert runner.run_once() == 0

    while runner.run_once():
        pass

    assert producer.committed == _baseline(
        EXAMPLE_VALUES[:2], EXAMPLE_VALUES[2:4], EXAMPLE_VALUES[4:]
    )


def test_run_once_abort_multi_partition_seeks_min_offset_per_partition() -> None:
    """Seek на минимальный офсет батча по каждой партиции."""
    p0 = _log(EXAMPLE_VALUES[:3], partition=0, first=10)
    p1 = _log(EXAMPLE_VALUES[3:], partition=1, first=5)
    consumer = FakeConsumer([p0[0], p1[0], p0[1], p1[1], p0[2]])
    producer = FakeProducer()
    producer.fail("commit_transaction", abortable_error())
    runner, _ = _runner(consumer, producer)

    runner.run_once()

    assert set(consumer.seeks) == {(TELEMETRY, 0, 10), (TELEMETRY, 1, 5)}


def test_run_once_abort_does_not_mark_poll_again() -> None:
    """При откате readiness не продлевается: mark_poll — один раз за consume."""
    health = CountingHealth()
    producer = FakeProducer()
    producer.fail("commit_transaction", abortable_error())
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), producer, health)

    assert runner.run_once() == 0

    assert health.polls == 1


def test_run_once_consecutive_aborts_limit_raises_after_full_rollback() -> None:
    """`max_consecutive_aborts` (5) откатов подряд → исключение наружу, но откат выполнен."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail("commit_transaction", *(abortable_error() for _ in range(5)))
    runner, processor = _runner(consumer, producer)

    assert [runner.run_once() for _ in range(4)] == [0, 0, 0, 0]
    with pytest.raises(KafkaException):
        runner.run_once()

    assert producer.count("abort_transaction") == 5
    assert producer.state is TxnState.NONE
    assert consumer.seeks == [(TELEMETRY, 0, FIRST_OFFSET)] * 5
    assert processor.snapshot() == SiteState(SITE).snapshot()
    assert producer.committed == []


def test_run_once_custom_consecutive_aborts_limit() -> None:
    """`max_consecutive_aborts=2`: первый откат — 0, второй подряд — исключение."""
    producer = FakeProducer()
    producer.fail("commit_transaction", abortable_error(), abortable_error())
    runner, _ = _runner(FakeConsumer(_log(EXAMPLE_VALUES)), producer, max_consecutive_aborts=2)

    assert runner.run_once() == 0
    with pytest.raises(KafkaException):
        runner.run_once()


def test_run_once_successful_commit_resets_consecutive_aborts() -> None:
    """Успешный commit сбрасывает счётчик: 4 отката, успех, 4 отката — без исключения."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=1)
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    results: list[int] = []
    for _ in range(2):
        producer.fail("commit_transaction", *(abortable_error() for _ in range(4)))
        results.extend(runner.run_once() for _ in range(5))

    assert results == [0, 0, 0, 0, 1] * 2
    assert producer.committed == _baseline(EXAMPLE_VALUES[:1], EXAMPLE_VALUES[1:2])


@pytest.mark.parametrize(
    "failing_call", ["produce", "send_offsets_to_transaction", "commit_transaction"]
)
def test_run_once_fatal_error_propagates_without_abort_and_seek(failing_call: str) -> None:
    """Фатальная ошибка пробрасывается без abort и seek."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    producer = FakeProducer()
    producer.fail(failing_call, fatal_error())
    runner, _ = _runner(consumer, producer)

    with pytest.raises(KafkaException):
        runner.run_once()

    assert producer.count("abort_transaction") == 0
    assert consumer.seeks == []
    assert producer.committed == []


# ---------------------------------------------------------------------------
# Run(stop)
# ---------------------------------------------------------------------------


def _run_in_thread(runner: KafkaRunner, stop: threading.Event) -> threading.Thread:
    """Запускает цикл в потоке и ждёт его завершения (не дольше 10 с)."""
    thread = threading.Thread(target=runner.run, args=(stop,), daemon=True)
    thread.start()
    thread.join(timeout=10)
    return thread


def test_run_stops_on_event_and_closes_consumer() -> None:
    """Run крутит run_once до stop, затем закрывает консьюмер."""
    stop = threading.Event()

    def stop_after_third(call: int) -> None:
        if call >= 3:
            stop.set()

    consumer = FakeConsumer(_log(EXAMPLE_VALUES), max_per_call=2, on_consume=stop_after_third)
    producer = FakeProducer()
    runner, _ = _runner(consumer, producer)

    thread = _run_in_thread(runner, stop)

    assert not thread.is_alive()
    assert len(consumer.consume_calls) == 3
    assert consumer.closed
    assert producer.committed == _baseline(
        EXAMPLE_VALUES[:2], EXAMPLE_VALUES[2:4], EXAMPLE_VALUES[4:]
    )


def test_run_stop_already_set_returns_and_closes() -> None:
    """Уже выставленный stop: цикл не читает данные и закрывает консьюмер."""
    stop = threading.Event()
    stop.set()
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    runner, _ = _runner(consumer, FakeProducer())

    thread = _run_in_thread(runner, stop)

    assert not thread.is_alive()
    assert consumer.consume_calls == []
    assert consumer.closed


# ---------------------------------------------------------------------------
# `load_snapshot` — восстановление из compacted-топика состояния
# ---------------------------------------------------------------------------


def _snapshots() -> list[SiteSnapshot]:
    """Разные снимки площадки: после 1, 3 и 5 сообщений примера ТЗ."""
    state = SiteState(SITE)
    out: list[SiteSnapshot] = []
    for i, msg in enumerate(EXAMPLE_TELEMETRY, start=1):
        state.apply(msg)
        if i in (1, 3, 5):
            out.append(state.snapshot())
    return out


def test_load_snapshot_empty_topic_returns_none() -> None:
    """Пустой топик состояния — снимка нет (первый запуск)."""
    assert load_snapshot(FakeStateConsumer(), SITE_ID) is None


def test_load_snapshot_empty_compacted_topic_nonzero_offsets_returns_none() -> None:
    """Пустой топик с ненулевыми watermark (low = high) — снимка нет."""
    assert load_snapshot(FakeStateConsumer(first_offset=57), SITE_ID) is None


def test_load_snapshot_single_value_decoded() -> None:
    """Единственный снимок площадки восстанавливается."""
    snap = _snapshots()[0]
    consumer = FakeStateConsumer([(SITE_ID.encode(), encode_snapshot(snap))])

    assert load_snapshot(consumer, SITE_ID) == snap


def test_load_snapshot_reads_state_topic_partition_zero() -> None:
    """Чтение — из топика состояния, партиция 0."""
    consumer = FakeStateConsumer([(SITE_ID.encode(), encode_snapshot(_snapshots()[0]))])

    load_snapshot(consumer, SITE_ID)

    assert {(t, p) for t, p, _ in consumer.assigned} == {(STATE, 0)}


def test_load_snapshot_several_values_last_wins() -> None:
    """Несколько значений по ключу site_id — берётся последнее (до high watermark)."""
    first, second, third = _snapshots()
    key = SITE_ID.encode()
    consumer = FakeStateConsumer(
        [
            (key, encode_snapshot(first)),
            (key, encode_snapshot(second)),
            (key, encode_snapshot(third)),
        ],
        max_per_call=1,
    )

    assert load_snapshot(consumer, SITE_ID) == third


def test_load_snapshot_compacted_offsets_with_gaps_last_wins() -> None:
    """Compacted-топик: офсеты с пропусками, low > 0 — берётся последнее значение ключа."""
    first, _, third = _snapshots()
    key = SITE_ID.encode()
    consumer = FakeStateConsumer(
        [(key, encode_snapshot(first)), (key, encode_snapshot(third))],
        offsets=[3, 9],
        max_per_call=1,
    )

    assert load_snapshot(consumer, SITE_ID) == third


def test_load_snapshot_foreign_keys_ignored() -> None:
    """Значения чужих площадок игнорируются (даже если не являются снимком)."""
    first, second, _ = _snapshots()
    key = SITE_ID.encode()
    consumer = FakeStateConsumer(
        [
            (key, encode_snapshot(first)),
            (b"site-2", encode_snapshot(second)),
            (key, encode_snapshot(second)),
            (b"site-2", b"garbage"),
            (None, b"garbage"),
        ],
        max_per_call=2,
    )

    assert load_snapshot(consumer, SITE_ID) == second


def test_load_snapshot_only_foreign_keys_returns_none() -> None:
    """В топике только чужие ключи — снимка этой площадки нет."""
    consumer = FakeStateConsumer([(b"site-2", encode_snapshot(_snapshots()[0]))])

    assert load_snapshot(consumer, SITE_ID) is None


def test_load_snapshot_partition_eof_after_data_ignored() -> None:
    """Служебный EOF в конце партиции состояния не мешает восстановлению последнего снимка."""
    first, second, _ = _snapshots()
    key = SITE_ID.encode()
    consumer = FakeStateConsumer([(key, encode_snapshot(first)), (key, encode_snapshot(second))])
    consumer.injected.append(
        [
            FakeMessage(STATE, 0, 0, encode_snapshot(first), key),
            FakeMessage(STATE, 0, 1, encode_snapshot(second), key),
            FakeMessage(STATE, 0, 2, error_=KafkaError(KafkaError._PARTITION_EOF)),
        ]
    )

    assert load_snapshot(consumer, SITE_ID) == second


def test_load_snapshot_other_error_raises_kafka_exception() -> None:
    """Иная ошибка при чтении топика состояния — KafkaException (как в run_once)."""
    consumer = FakeStateConsumer([(SITE_ID.encode(), encode_snapshot(_snapshots()[0]))])
    consumer.injected.append(
        [FakeMessage(STATE, 0, 0, error_=KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART, "x"))]
    )

    with pytest.raises(KafkaException):
        load_snapshot(consumer, SITE_ID)


def test_load_snapshot_tombstone_last_returns_none() -> None:
    """Tombstone (value = null) последним по ключу — снимок удалён, восстановления нет."""
    consumer = FakeStateConsumer(
        [(SITE_ID.encode(), encode_snapshot(_snapshots()[0])), (SITE_ID.encode(), None)]
    )

    assert load_snapshot(consumer, SITE_ID) is None


# ---------------------------------------------------------------------------
# `load_snapshot`: таймауты
# ---------------------------------------------------------------------------


def test_load_snapshot_watermarks_timeout_raises_timeout_error() -> None:
    """`get_watermark_offsets` вернул None (таймаут запроса) → TimeoutError."""
    consumer = FakeStateConsumer(
        [(SITE_ID.encode(), encode_snapshot(_snapshots()[0]))], watermarks_timeout=True
    )

    with pytest.raises(TimeoutError):
        load_snapshot(consumer, SITE_ID, timeout_s=5.0)


def test_load_snapshot_watermarks_requested_with_positive_timeout() -> None:
    """Запрос watermarks ограничен по времени: таймаут > 0 и не больше общего."""
    consumer = FakeStateConsumer([(SITE_ID.encode(), encode_snapshot(_snapshots()[0]))])

    load_snapshot(consumer, SITE_ID, timeout_s=5.0)

    assert consumer.watermark_timeouts
    assert all(0 < t <= 5.0 for t in consumer.watermark_timeouts)


def test_load_snapshot_overall_deadline_raises_timeout_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Общий дедлайн чтения по time.monotonic истёк до high watermark → TimeoutError."""
    clock = [1_000.0]

    def monotonic() -> float:
        return clock[0]

    def tick(_call: int) -> None:
        clock[0] += 10.0

    monkeypatch.setattr(time, "monotonic", monotonic)
    if hasattr(runner_module, "monotonic"):
        monkeypatch.setattr(runner_module, "monotonic", monotonic)
    key = SITE_ID.encode()
    snaps = _snapshots()
    consumer = FakeStateConsumer(
        [(key, encode_snapshot(s)) for s in snaps * 4], max_per_call=1, on_consume=tick
    )

    with pytest.raises(TimeoutError):
        load_snapshot(consumer, SITE_ID, timeout_s=25.0)

    assert consumer.consume_calls < len(snaps) * 4


# ---------------------------------------------------------------------------
# Rebalance: on_assign / on_revoke / on_lost
# ---------------------------------------------------------------------------

ASSIGNED_TP = TopicPartition(TELEMETRY, 0)


def _ready_health() -> HealthState:
    """Здоровье с назначением и свежим poll."""
    health = HealthState()
    health.mark_assigned(True)
    health.mark_poll()
    return health


def test_on_assign_empty_marks_unassigned_without_reset() -> None:
    """Пустое назначение → mark_assigned(False), загрузчик не вызывается, состояние не трогается."""
    calls: list[int] = []

    def loader() -> SiteSnapshot | None:
        calls.append(1)
        return None

    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    health = _ready_health()
    runner, processor = _runner(consumer, FakeProducer(), health, load_snapshot=loader)
    runner.run_once()
    before = processor.snapshot()

    runner.on_assign(consumer, [])

    assert not health.is_ready(60.0)
    assert calls == []
    assert processor.snapshot() == before


def test_on_assign_single_partition_resets_from_loader_and_marks_assigned() -> None:
    """Одна партиция → processor.reset(load_snapshot()) и mark_assigned(True)."""
    snap = _snapshots()[1]
    health = HealthState()
    health.mark_poll()
    consumer = FakeConsumer()
    runner, processor = _runner(consumer, FakeProducer(), health, load_snapshot=lambda: snap)

    runner.on_assign(consumer, [ASSIGNED_TP])

    assert processor.snapshot() == snap
    assert health.is_ready(60.0)


def test_on_assign_after_revoke_state_from_loader_not_memory() -> None:
    """Revoke → assign: состояние берётся из загрузчика (STATE), а не из памяти процесса."""
    loaded = _snapshots()[0]
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    health = _ready_health()
    runner, processor = _runner(consumer, FakeProducer(), health, load_snapshot=lambda: loaded)
    runner.run_once()
    assert processor.snapshot() != loaded

    runner.on_revoke(consumer, [ASSIGNED_TP])
    assert not health.is_ready(60.0)
    runner.on_assign(consumer, [ASSIGNED_TP])

    assert processor.snapshot() == loaded
    assert health.is_ready(60.0)


def test_on_assign_loader_none_resets_to_empty_state() -> None:
    """Загрузчик вернул None (снимка нет) → процессор начинает с пустой площадки."""
    consumer = FakeConsumer(_log(EXAMPLE_VALUES))
    runner, processor = _runner(consumer, FakeProducer())
    runner.run_once()

    runner.on_assign(consumer, [ASSIGNED_TP])

    assert processor.snapshot() == SiteState(SITE).snapshot()


def test_on_assign_two_partitions_raises_value_error() -> None:
    """Модель «площадка = 1 партиция»: назначение двух партиций → ValueError."""
    consumer = FakeConsumer()
    runner, _ = _runner(consumer, FakeProducer())

    with pytest.raises(ValueError):
        runner.on_assign(consumer, [ASSIGNED_TP, TopicPartition(TELEMETRY, 1)])


@pytest.mark.parametrize("callback", ["on_revoke", "on_lost"])
def test_on_revoke_or_lost_marks_unassigned(callback: str) -> None:
    """`on_revoke` и on_lost → mark_assigned(False): сервис не готов."""
    consumer = FakeConsumer()
    health = _ready_health()
    runner, _ = _runner(consumer, FakeProducer(), health)
    assert health.is_ready(60.0)

    getattr(runner, callback)(consumer, [ASSIGNED_TP])

    assert not health.is_ready(60.0)
