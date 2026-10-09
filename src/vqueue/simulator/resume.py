"""Продолжение виртуального времени генератора после перезапуска.

При ускорении виртуальное время уходит вперёд настенного, и старт «с сейчас» дал бы
сообщения старше уже учтённых процессором — он отбросил бы их как нарушающие порядок.
Поэтому без явного --start-ts генератор стартует не раньше последнего ts в топике.
"""

from __future__ import annotations

import json
import logging
import math
import time
from collections.abc import Callable
from typing import Final

from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

RESUME_TAIL: Final = 500
"""Сколько последних сообщений каждой партиции просматривать при продолжении времени."""

RESUME_TIMEOUT_S: Final = 30.0
"""Общий предел времени чтения хвоста топика при старте, с (liveness, не бизнес-логика)."""

_POLL_S: Final = 0.5

logger = logging.getLogger("vqueue.simulator.resume")


class TopicUnavailableError(RuntimeError):
    """Метаданные топика не получены: топик не существует или недоступен."""


def start_ts(
    explicit: int | None, output: str, now: int, read_last_ts: Callable[[], int | None]
) -> int:
    """Начало симуляции, секунды epoch.

    Args:
        explicit: Значение --start-ts; если задано, возвращается как есть (брокер не опрашивается).
        output: Куда пишет генератор: "stdout" — продолжать нечего, иначе — Kafka.
        now: Текущее время, секунды epoch.
        read_last_ts: Чтение последнего ts в топике; вызывается только для Kafka.

    Returns:
        explicit; для stdout — now; иначе max(now, последний ts + 1), а при пустом топике — now.
    """
    if explicit is not None:
        return explicit
    if output == "stdout":
        return now
    last_ts = read_last_ts()
    if last_ts is None:
        return now
    return max(now, last_ts + 1)


def last_ts_in_topic(bootstrap: str, topic: str, timeout_s: float = RESUME_TIMEOUT_S) -> int | None:
    """Наибольший ts среди последних RESUME_TAIL сообщений каждой партиции топика.

    Опоздавшие сообщения в хвосте старее соседних, поэтому берётся максимум, а не последний.
    Сообщения, из которых ts не извлечь (не JSON, нет ts, ts не число), пропускаются
    с предупреждением.

    Args:
        bootstrap: Адреса брокеров.
        topic: Топик телеметрии.
        timeout_s: Общий предел времени на все обращения к брокеру, с.

    Returns:
        Наибольший ts или None, если в топике нет сообщений с корректным ts.

    Raises:
        TopicUnavailableError: Топик не существует или его метаданные недоступны.
        TimeoutError: Хвост топика не прочитан за timeout_s.
        KafkaException: Ошибка обращения к брокеру.
    """
    deadline = time.monotonic() + timeout_s

    def remaining() -> float:
        left = deadline - time.monotonic()
        if left <= 0:
            raise TimeoutError(f"Хвост топика {topic} не прочитан за {timeout_s} с")
        return left

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": "vqueue-generator-resume",
            "enable.auto.commit": False,
            "enable.partition.eof": True,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=remaining())
        topic_meta = metadata.topics.get(topic)
        if topic_meta is None or topic_meta.error is not None:
            reason = "нет в метаданных" if topic_meta is None else str(topic_meta.error)
            raise TopicUnavailableError(f"Топик {topic} недоступен на {bootstrap}: {reason}")
        assignment = []
        for partition in topic_meta.partitions:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(topic, partition), timeout=remaining()
            )
            if high > low:
                assignment.append(TopicPartition(topic, partition, max(low, high - RESUME_TAIL)))
        if not assignment:
            return None
        consumer.assign(assignment)
        pending = {tp.partition for tp in assignment}
        last_ts: int | None = None
        while pending:
            msg = consumer.poll(min(remaining(), _POLL_S))
            if msg is None:
                continue
            err = msg.error()
            if err is not None:
                if err.code() == KafkaError._PARTITION_EOF:
                    pending.discard(msg.partition())
                    continue
                raise KafkaException(err)
            ts = _parse_ts(msg.value())
            if ts is None:
                logger.warning(
                    "Пропуск сообщения без корректного ts: %s[%s]@%s",
                    topic,
                    msg.partition(),
                    msg.offset(),
                )
                continue
            last_ts = ts if last_ts is None else max(last_ts, ts)
        return last_ts
    finally:
        consumer.close()


def _parse_ts(value: bytes | None) -> int | None:
    """Поле ts JSON-сообщения или None, если сообщение битое."""
    if value is None:
        return None
    try:
        payload = json.loads(value)
    except ValueError:  # JSONDecodeError и UnicodeDecodeError — подклассы ValueError
        return None
    if not isinstance(payload, dict):
        return None
    ts = payload.get("ts")
    # bool — подкласс int, но как время не имеет смысла.
    if isinstance(ts, bool) or not isinstance(ts, int | float):
        return None
    if isinstance(ts, float) and not math.isfinite(ts):
        return None
    return int(ts)
