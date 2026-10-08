"""Транзакционный цикл процессора: consume → process → produce + offsets в одной транзакции.

Выходы, снимок состояния и офсеты входного топика фиксируются одной транзакцией
(exactly-once в пределах Kafka). Ошибки транзакции разбираются по правилам
confluent_kafka: retriable — повтор того же вызова, txn_requires_abort — откат
транзакции, состояния процессора и позиции консьюмера к началу батча, прочие — наружу.

Позиция чтения входного топика хранится в заголовке снимка (NEXT_OFFSET_HEADER):
новый владелец партиции берёт состояние и позицию из одной записи. Офсеты группы
тоже коммитятся в транзакции — для мониторинга лага и как запасной вариант, когда
снимка или заголовка нет. Только офсетам группы доверять нельзя: librdkafka может
прочитать их уже после фиксации транзакции «зомби», снимок которой не попал
в прочитанное состояние, — и батч выпал бы.

Гонка с «зомби» закрывается с двух сторон. Записи батча доставляются (flush) до
send_offsets_to_transaction: раз брокер принял офсеты со старым поколением группы,
снимок «зомби» уже лежит в топике ниже high watermark, который новый владелец
запросит после rebalance. А load_snapshot дочитывает топик состояния до этого
high watermark, дожидаясь исхода открытых транзакций, а не до LSO.
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Final, Protocol, TypeAlias, TypeVar

from confluent_kafka import OFFSET_BEGINNING, KafkaError, KafkaException, TopicPartition

from vqueue.adapters import topics
from vqueue.adapters.codec import decode_snapshot
from vqueue.adapters.health import HealthState
from vqueue.adapters.processor import OutRecord, TelemetryProcessor
from vqueue.domain.site import SiteSnapshot

logger = logging.getLogger(__name__)

NEXT_OFFSET_HEADER: Final = "telemetry-next-offset"
"""Заголовок снимка: следующий к чтению офсет партиции telemetry (десятичное ASCII-число)."""

_STATE_PARTITION: Final = 0
"""Топик состояния — одна партиция."""

_STATE_BATCH: Final = 500
_STATE_POLL_TIMEOUT_S: Final = 1.0

_T = TypeVar("_T")

# Типы заголовков — как в confluent_kafka (HeadersType): консьюмер возвращает список пар,
# продюсер принимает список (list инвариантен, поэтому тип значения — полный union).
_HeaderValue: TypeAlias = str | bytes | None
ReceivedHeaders: TypeAlias = Mapping[str, _HeaderValue] | Sequence[tuple[str, _HeaderValue]]
ProducedHeaders: TypeAlias = list[tuple[str, _HeaderValue]]


# Порты — минимальное подмножество API confluent_kafka, которое реально используется.
# Параметры позиционные (/): реальные классы и фейки могут называть их по-своему.


class MessagePort(Protocol):
    """Сообщение Kafka (подмножество confluent_kafka.Message)."""

    def error(self) -> KafkaError | None:
        """Ошибка, переданная вместо сообщения, или None."""

    def topic(self) -> str | None:
        """Топик сообщения."""

    def partition(self) -> int | None:
        """Партиция сообщения."""

    def offset(self) -> int | None:
        """Офсет сообщения."""

    def key(self) -> bytes | None:
        """Ключ сообщения."""

    def value(self) -> bytes | None:
        """Значение сообщения."""

    def headers(self) -> ReceivedHeaders | None:
        """Заголовки сообщения или None, если их нет."""


class ConsumerPort(Protocol):
    """Консьюмер входного топика (подмножество confluent_kafka.Consumer)."""

    def consume(self, num_messages: int, timeout: float, /) -> Sequence[MessagePort]:
        """Читает до num_messages сообщений, ожидая не дольше timeout секунд."""

    def seek(self, partition: TopicPartition, /) -> None:
        """Переставляет позицию чтения партиции."""

    def assign(self, partitions: list[TopicPartition], /) -> None:
        """Назначает партиции; в on_assign задаёт стартовые офсеты вместо офсетов группы."""

    def consumer_group_metadata(self) -> object:
        """Метаданные группы для send_offsets_to_transaction."""

    def close(self) -> None:
        """Закрывает консьюмер и покидает группу."""


class ProducerPort(Protocol):
    """Транзакционный продюсер (подмножество confluent_kafka.Producer)."""

    def begin_transaction(self) -> None:
        """Начинает транзакцию."""

    def produce(
        self, topic: str, value: bytes, key: bytes, /, *, headers: ProducedHeaders | None = None
    ) -> None:
        """Ставит сообщение в очередь отправки (заголовки — именованным аргументом)."""

    def send_offsets_to_transaction(
        self, positions: list[TopicPartition], group_metadata: object, /
    ) -> None:
        """Добавляет офсеты консьюмер-группы в текущую транзакцию."""

    def flush(self) -> int:
        """Дожидается доставки поставленных сообщений; возвращает число недоставленных."""

    def commit_transaction(self) -> None:
        """Фиксирует транзакцию."""

    def abort_transaction(self) -> None:
        """Откатывает транзакцию."""


class WatermarkPort(Protocol):
    """Запрос watermark партиции (подмножество confluent_kafka.Consumer)."""

    def get_watermark_offsets(
        self, partition: TopicPartition, /, timeout: float
    ) -> tuple[int, int] | None:
        """Возвращает (low, high) watermark партиции или None по таймауту."""


class StateConsumerPort(Protocol):
    """Консьюмер топика состояния для восстановления (подмножество confluent_kafka.Consumer)."""

    def assign(self, partitions: list[TopicPartition], /) -> None:
        """Назначает партиции вручную, без консьюмер-группы."""

    def consume(self, num_messages: int, timeout: float, /) -> Sequence[MessagePort]:
        """Читает до num_messages сообщений, ожидая не дольше timeout секунд."""

    def position(self, partitions: list[TopicPartition], /) -> list[TopicPartition]:
        """Возвращает следующие к чтению офсеты партиций."""


@dataclass(frozen=True, slots=True)
class RestoredState:
    """Состояние площадки и позиция чтения, восстановленные из одной записи снимка.

    Attributes:
        snapshot: Последний снимок; None — снимка нет (или удалён tombstone).
        next_offset: Следующий к чтению офсет партиции telemetry из заголовка снимка;
            None — позиция берётся из закоммиченного офсета группы.
    """

    snapshot: SiteSnapshot | None
    next_offset: int | None


def _check(msg: MessagePort) -> bool:
    """Проверяет сообщение: True — данные, False — служебное событие, данных нет.

    Конец партиции и нефатальные ошибки консьюмера (librdkafka восстанавливается
    после них сам, позиция чтения не сдвигается) пропускаются.

    Raises:
        KafkaException: Фатальная ошибка консьюмера.
    """
    err = msg.error()
    if err is None:
        return True
    if err.code() == KafkaError._PARTITION_EOF:
        return False
    if err.fatal():
        raise KafkaException(err)
    logger.warning("Нефатальная ошибка консьюмера, пропуск: %s", err)
    return False


def _position(msg: MessagePort) -> tuple[str, int, int]:
    """Возвращает (топик, партиция, офсет) сообщения с данными."""
    topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
    if topic is None or partition is None or offset is None:
        raise ValueError(f"У сообщения нет позиции: {topic!r}/{partition!r}/{offset!r}")
    return topic, partition, offset


def _record_headers(rec: OutRecord, next_offset: int) -> ProducedHeaders | None:
    """Заголовки записи; снимку добавляется следующий офсет telemetry."""
    headers: ProducedHeaders = list(rec.headers)
    if rec.topic == topics.STATE:
        headers.append((NEXT_OFFSET_HEADER, str(next_offset).encode("ascii")))
    return headers or None


def _error(exc: KafkaException) -> KafkaError | None:
    """KafkaError из исключения confluent_kafka (exc.args[0]) или None."""
    err = exc.args[0] if exc.args else None
    return err if isinstance(err, KafkaError) else None


def _requires_abort(exc: KafkaException) -> bool:
    """Транзакция провалена и должна быть откачена (abortable error)."""
    err = _error(exc)
    return err is not None and err.txn_requires_abort()


def _retriable(exc: KafkaException) -> bool:
    """Тот же вызов можно повторить: retriable и не требует abort."""
    err = _error(exc)
    return err is not None and err.retriable() and not err.txn_requires_abort()


class KafkaRunner:
    """Цикл обработки телеметрии с транзакционной публикацией результатов.

    Модель «площадка = одна партиция входного топика»: экземпляр владеет не более
    чем одной партицией, и при каждом её назначении состояние перечитывается
    из топика состояния (до этого партицию мог обрабатывать другой экземпляр).
    """

    def __init__(  # noqa: PLR0913, PLR0917 — зависимости и настройки цикла передаются явно
        self,
        consumer: ConsumerPort,
        producer: ProducerPort,
        processor: TelemetryProcessor,
        health: HealthState,
        load_snapshot: Callable[[], RestoredState],
        batch_size: int = 500,
        poll_timeout_s: float = 1.0,
        max_retries: int = 3,
        max_consecutive_aborts: int = 5,
    ) -> None:
        """Создаёт цикл.

        Args:
            consumer: Консьюмер входного топика (подписан, read_committed, без автокоммита).
            producer: Транзакционный продюсер с выполненным init_transactions().
            processor: Процессор телеметрии площадки.
            health: Состояние готовности; отмечается каждый успешный consume.
            load_snapshot: Чтение последнего зафиксированного снимка площадки
                и позиции чтения из него.
            batch_size: Наибольшее число сообщений в батче (одной транзакции).
            poll_timeout_s: Ожидание сообщений в одном consume, с.
            max_retries: Сколько раз повторять retriable-вызов
                send_offsets_to_transaction / commit_transaction.
            max_consecutive_aborts: После скольких откатов подряд ошибка пробрасывается.
        """
        self._consumer = consumer
        self._producer = producer
        self._processor = processor
        self._health = health
        self._load_snapshot = load_snapshot
        self._batch_size = batch_size
        self._poll_timeout_s = poll_timeout_s
        self._max_retries = max_retries
        self._max_consecutive_aborts = max_consecutive_aborts
        self._consecutive_aborts = 0

    def run_once(self) -> int:
        """Обрабатывает один батч в одной транзакции.

        Returns:
            Число обработанных сообщений; 0 — сообщений не было или транзакция откачена.

        Raises:
            KafkaException: Фатальная ошибка консьюмера, неоткатываемая ошибка
                транзакции, исчерпаны повторы retriable-вызова или достигнут предел
                откатов подряд.
            Exception: Любая иная ошибка внутри транзакции — после попытки abort,
                чтобы незакрытая транзакция не держала LSO выходных топиков.
        """
        msgs = self._consumer.consume(self._batch_size, self._poll_timeout_s)
        self._health.mark_poll()
        data = [m for m in msgs if _check(m)]
        if not data:
            return 0

        # Начало батча (для отката) и следующие офсеты (для коммита) по партициям.
        start: dict[tuple[str, int], int] = {}
        next_offsets: dict[tuple[str, int], int] = {}
        for msg in data:
            topic, partition, offset = _position(msg)
            tp = (topic, partition)
            start[tp] = min(offset, start.get(tp, offset))
            next_offsets[tp] = max(offset + 1, next_offsets.get(tp, offset + 1))
        if len(next_offsets) != 1:
            # Позиция в снимке — одна; on_assign допускает не более одной партиции.
            raise ValueError(f"Батч из нескольких партиций: {sorted(next_offsets)}")
        [next_offset] = next_offsets.values()
        before = self._processor.snapshot()

        try:
            self._producer.begin_transaction()
            # Пустое значение (tombstone) кодек отвергнет как некорректное сообщение.
            records = self._processor.process(m.value() or b"" for m in data)
            for rec in records:
                self._producer.produce(
                    rec.topic, rec.value, rec.key, headers=_record_headers(rec, next_offset)
                )
            # Доставка до офсетов: см. гонку с «зомби» в описании модуля.
            self._producer.flush()
            positions = [TopicPartition(t, p, o) for (t, p), o in next_offsets.items()]
            group_metadata = self._consumer.consumer_group_metadata()
            self._retrying(
                lambda: self._producer.send_offsets_to_transaction(positions, group_metadata)
            )
            self._retrying(self._producer.commit_transaction)
        except KafkaException as exc:
            if not _requires_abort(exc):
                raise
            self._rollback(before, start)
            self._consecutive_aborts += 1
            if self._consecutive_aborts >= self._max_consecutive_aborts:
                logger.error("Откатов транзакции подряд: %d, остановка", self._consecutive_aborts)
                raise
            logger.warning(
                "Транзакция откачена (%d подряд), батч будет обработан повторно: %s",
                self._consecutive_aborts,
                _error(exc),
            )
            return 0
        except Exception:
            self._abort_quietly()
            raise
        self._consecutive_aborts = 0
        return len(data)

    def _retrying(self, call: Callable[[], _T]) -> _T:
        """Выполняет вызов, повторяя его при retriable-ошибке не более max_retries раз."""
        attempt = 0
        while True:
            try:
                return call()
            except KafkaException as exc:
                if attempt >= self._max_retries or not _retriable(exc):
                    raise
                attempt += 1
                logger.warning("Повтор вызова (%d/%d): %s", attempt, self._max_retries, _error(exc))

    def _abort_quietly(self) -> None:
        """Откатывает транзакцию перед пробросом чужой ошибки; сбой abort только логируется."""
        try:
            self._retrying(self._producer.abort_transaction)
        except KafkaException as exc:
            logger.error("Транзакция не откачена: %s", _error(exc))

    def _rollback(self, before: SiteSnapshot, start: dict[tuple[str, int], int]) -> None:
        """Откатывает транзакцию, состояние процессора и позиции чтения к началу батча."""
        self._retrying(self._producer.abort_transaction)
        self._processor.reset(before)
        for (topic, partition), offset in start.items():
            self._consumer.seek(TopicPartition(topic, partition, offset))

    def on_assign(self, consumer: ConsumerPort, partitions: list[TopicPartition]) -> None:
        """Обрабатывает назначение партиций: восстанавливает состояние и позицию из снимка.

        Вызывается librdkafka внутри consume, до выдачи сообщений, — транзакция
        в этот момент не открыта. Позиция из снимка главнее офсета группы: она
        задаётся через consumer.assign с офсетом (штатный способ confluent_kafka
        переопределить стартовую позицию в колбэке назначения). Без позиции в снимке
        assign не вызывается — confluent_kafka назначит партицию сам, с офсета группы.

        Args:
            consumer: Консьюмер, которому назначены партиции.
            partitions: Назначенные партиции входного топика.

        Raises:
            ValueError: Назначено больше одной партиции.
        """
        logger.info("Назначены партиции: %s", [(p.topic, p.partition) for p in partitions])
        if len(partitions) > 1:
            raise ValueError(
                f"Назначено партиций: {len(partitions)}; модель «площадка = 1 партиция» "
                "допускает не более одной на экземпляр"
            )
        if not partitions:
            self._health.mark_assigned(False)
            return
        restored = self._load_snapshot()
        self._processor.reset(restored.snapshot)
        snapshot = restored.snapshot
        logger.info("Состояние восстановлено: now=%s", None if snapshot is None else snapshot.now)
        if restored.next_offset is not None:
            partitions[0].offset = restored.next_offset
            consumer.assign(partitions)
            logger.info("Позиция из снимка: offset=%d", restored.next_offset)
        else:
            logger.info("Позиция — офсет группы")
        self._health.mark_assigned(True)

    def on_revoke(self, consumer: ConsumerPort, partitions: list[TopicPartition]) -> None:
        """Обрабатывает отзыв партиций: процессор перестаёт быть готовым.

        Args:
            consumer: Консьюмер, у которого отозваны партиции (не используется).
            partitions: Отозванные партиции.
        """
        del consumer
        self._health.mark_assigned(False)
        logger.info("Отозваны партиции: %s", [(p.topic, p.partition) for p in partitions])

    def on_lost(self, consumer: ConsumerPort, partitions: list[TopicPartition]) -> None:
        """Обрабатывает потерю партиций — так же, как отзыв."""
        self.on_revoke(consumer, partitions)

    def run(self, stop: threading.Event) -> None:
        """Обрабатывает батчи до сигнала остановки, затем закрывает консьюмер.

        Args:
            stop: Событие остановки; проверяется между батчами.
        """
        try:
            while not stop.is_set():
                self.run_once()
        finally:
            self._consumer.close()


def load_snapshot(
    state_consumer: StateConsumerPort,
    watermarks: WatermarkPort,
    site_id: str,
    timeout_s: float = 60.0,
) -> RestoredState:
    """Читает последний снимок площадки и позицию чтения из топика состояния.

    Топик читается с начала до high watermark. Читающий консьюмер настроен
    с isolation.level=read_committed: снимки откаченных транзакций не видны.
    Watermark запрашивается отдельным клиентом с read_uncommitted — librdkafka
    запрашивает его с уровнем изоляции клиента, и при read_committed вернулся бы LSO,
    то есть граница перед открытой транзакцией «зомби». Чтение до настоящего high
    дожидается исхода такой транзакции (не дольше transaction.timeout.ms продюсера),
    поэтому timeout_s должен его превышать с запасом.
    Таймаут — liveness старта, по time.monotonic(), в бизнес-логику не попадает.

    Args:
        state_consumer: Консьюмер без подписки (read_committed); партиция назначается здесь.
        watermarks: Клиент с isolation.level=read_uncommitted для запроса high watermark.
        site_id: Идентификатор площадки — ключ снимка.
        timeout_s: Общий предел времени запроса watermark и чтения топика, с.

    Returns:
        Последний снимок и позиция из его заголовка NEXT_OFFSET_HEADER. Снимка нет
        (или он удалён tombstone) — RestoredState(None, None); заголовка нет или он
        некорректен — next_offset None (позиция из офсета группы).

    Raises:
        TimeoutError: Watermark не получен или топик не дочитан за timeout_s.
        KafkaException: Фатальная ошибка чтения топика.
        InvalidMessage: Последний снимок не разбирается.
    """
    deadline = time.monotonic() + timeout_s
    tp = TopicPartition(topics.STATE, _STATE_PARTITION)
    offsets = watermarks.get_watermark_offsets(tp, timeout=timeout_s)
    if offsets is None:
        raise TimeoutError(f"Watermark топика {topics.STATE} не получен за {timeout_s} с")
    low, high = offsets
    if high <= low:
        return RestoredState(None, None)
    state_consumer.assign([TopicPartition(topics.STATE, _STATE_PARTITION, OFFSET_BEGINNING)])
    key = site_id.encode()
    latest: MessagePort | None = None
    done = False
    while not done:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError(f"Топик {topics.STATE} не дочитан за {timeout_s} с")
        for msg in state_consumer.consume(_STATE_BATCH, min(_STATE_POLL_TIMEOUT_S, remaining)):
            # EOF при read_committed — это LSO, а не конец: конец определяет только position.
            if _check(msg) and msg.key() == key:
                latest = msg
        # position, а не офсет последнего сообщения: маркеры транзакций занимают офсеты.
        [pos] = state_consumer.position([tp])
        done = pos.offset >= high
    value = None if latest is None else latest.value()
    if latest is None or value is None:
        return RestoredState(None, None)
    return RestoredState(decode_snapshot(value), _next_offset(latest.headers()))


def _next_offset(headers: ReceivedHeaders | None) -> int | None:
    """Разбирает NEXT_OFFSET_HEADER (последний, если повторяется); None — нет или некорректен."""
    if headers is None:
        return None
    pairs = headers.items() if isinstance(headers, Mapping) else headers
    raw = None
    for name, header_value in pairs:
        if name == NEXT_OFFSET_HEADER:
            raw = header_value
    if raw is None:
        return None
    text = raw.decode("ascii", errors="replace") if isinstance(raw, bytes) else raw
    if not (text.isascii() and text.isdigit()):
        logger.warning(
            "Некорректный заголовок %s=%r, позиция — офсет группы", NEXT_OFFSET_HEADER, raw
        )
        return None
    return int(text)
