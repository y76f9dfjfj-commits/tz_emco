"""Чтение последнего ts из топика для продолжения времени генератора (last_ts_in_topic).

Правило: наибольший ts среди последних RESUME_TAIL сообщений каждой партиции (с учётом
опоздавших — максимум, а не последнее); пустой топик → None; битые сообщения пропускаются с
warning логгера vqueue.simulator.resume; несуществующий топик → TopicUnavailableError;
недоступный брокер → TimeoutError или KafkaException в пределах общего timeout_s.
Каждый тест работает со своим временным топиком из двух партиций (топики стенда не трогает).
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Sequence

import pytest
from confluent_kafka import KafkaException, Producer

from tests.integration.kafkakit import create_topic, delete_topic
from vqueue.simulator.resume import RESUME_TAIL, TopicUnavailableError, last_ts_in_topic

pytestmark = pytest.mark.integration

PARTITIONS = 2


@pytest.fixture
def topic(bootstrap: str, run_id: str) -> Iterator[str]:
    """Временный топик из двух партиций; удаляется после теста."""
    name = f"it-resume-{run_id}"
    create_topic(bootstrap, name, PARTITIONS)
    try:
        yield name
    finally:
        delete_topic(bootstrap, name)


def _ts(ts: int) -> bytes:
    """Сообщение telemetry.v1 с заданным ts."""
    return json.dumps(
        {"unit_uuid": "T-01", "ts": ts, "lat": 49.1, "lon": 142.6, "speed_kmh": 0.0}
    ).encode()


def _produce(bootstrap: str, topic: str, partition: int, values: Sequence[bytes]) -> None:
    """Пишет значения в заданную партицию по порядку и дожидается доставки."""
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True})
    for value in values:
        producer.produce(topic, value=value, key=b"T-01", partition=partition)
    assert producer.flush(30) == 0


def test_last_ts_empty_topic_returns_none(bootstrap: str, topic: str) -> None:
    """Пустой топик → None."""
    assert last_ts_in_topic(bootstrap, topic, timeout_s=15) is None


def test_last_ts_late_message_in_tail_returns_maximum_not_last(bootstrap: str, topic: str) -> None:
    """Опоздавшее сообщение в конце хвоста: результат — максимум по хвосту, а не последний ts."""
    _produce(bootstrap, topic, 0, [_ts(100), _ts(200), _ts(150)])
    assert last_ts_in_topic(bootstrap, topic, timeout_s=15) == 200


def test_last_ts_maximum_across_partitions(bootstrap: str, topic: str) -> None:
    """Максимум берётся по хвостам всех партиций."""
    _produce(bootstrap, topic, 0, [_ts(100), _ts(110)])
    _produce(bootstrap, topic, 1, [_ts(300), _ts(120)])
    assert last_ts_in_topic(bootstrap, topic, timeout_s=15) == 300


def test_last_ts_maximum_exactly_tail_size_from_end_is_counted(bootstrap: str, topic: str) -> None:
    """Граница хвоста: сообщение ровно RESUME_TAIL-е с конца ещё учитывается."""
    _produce(bootstrap, topic, 0, [_ts(10_000), *[_ts(i) for i in range(RESUME_TAIL - 1)]])
    assert last_ts_in_topic(bootstrap, topic, timeout_s=15) == 10_000


def test_last_ts_maximum_before_tail_is_ignored(bootstrap: str, topic: str) -> None:
    """Граница хвоста: сообщение (RESUME_TAIL + 1)-е с конца уже не просматривается."""
    _produce(bootstrap, topic, 0, [_ts(10_000), *[_ts(i) for i in range(RESUME_TAIL)]])
    assert last_ts_in_topic(bootstrap, topic, timeout_s=15) == RESUME_TAIL - 1


def test_last_ts_broken_messages_skipped_with_warning(
    bootstrap: str, topic: str, caplog: pytest.LogCaptureFixture
) -> None:
    """Битые сообщения хвоста пропускаются с warning логгера vqueue.simulator.resume."""
    _produce(bootstrap, topic, 0, [_ts(300), b"not json", b'{"ts": "x"}', b"\xff"])
    with caplog.at_level(logging.WARNING, logger="vqueue.simulator.resume"):
        assert last_ts_in_topic(bootstrap, topic, timeout_s=15) == 300
    warnings = [
        r
        for r in caplog.records
        if r.name == "vqueue.simulator.resume" and r.levelno == logging.WARNING
    ]
    assert warnings


def test_last_ts_only_broken_messages_returns_none(
    bootstrap: str, topic: str, caplog: pytest.LogCaptureFixture
) -> None:
    """В хвосте только битые сообщения → None (как пустой топик)."""
    _produce(bootstrap, topic, 0, [b"not json"])
    with caplog.at_level(logging.WARNING, logger="vqueue.simulator.resume"):
        assert last_ts_in_topic(bootstrap, topic, timeout_s=15) is None


def test_last_ts_missing_topic_raises_topic_unavailable(bootstrap: str, run_id: str) -> None:
    """Несуществующий топик → TopicUnavailableError."""
    with pytest.raises(TopicUnavailableError):
        last_ts_in_topic(bootstrap, f"it-resume-missing-{run_id}", timeout_s=15)


def test_last_ts_unreachable_broker_fails_within_timeout() -> None:
    """Недоступный брокер (127.0.0.1:1) → TimeoutError или KafkaException за timeout_s + допуск."""
    timeout_s = 3.0
    started = time.monotonic()
    with pytest.raises((TimeoutError, KafkaException)):
        last_ts_in_topic("127.0.0.1:1", "telemetry.v1", timeout_s=timeout_s)
    assert time.monotonic() - started <= timeout_s + 2.0
