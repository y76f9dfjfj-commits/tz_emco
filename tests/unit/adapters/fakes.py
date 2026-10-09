"""Фейки портов Kafka для тестов транзакционного цикла без брокера.

Структурно соответствуют MessagePort, ConsumerPort, ProducerPort, StateConsumerPort
и WatermarkPort из vqueue.adapters.kafka_runner. Поведение повторяет семантику confluent_kafka:
- консьюмер читает журнал сообщений по позициям партиций; seek разрешён только по
  назначенным партициям, иначе KafkaException;
- продюсер — автомат транзакции (none / in_transaction / committing / abortable / fatal):
  записи текущей транзакции копятся в буфере, commit переносит их в «зафиксированные»,
  abort отбрасывает; недопустимый в состоянии вызов — KafkaException(_STATE), как у
  реального клиента. Так проверяется exactly-once по выходам.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from enum import StrEnum

from confluent_kafka import OFFSET_BEGINNING, KafkaError, KafkaException, TopicPartition

from vqueue.adapters.kafka_runner import ProducedHeaders
from vqueue.adapters.topics import STATE, TELEMETRY

MessageHeaders = list[tuple[str, bytes | None]]
"""Заголовки полученного сообщения — как их отдаёт confluent_kafka.Message.headers()."""

Position = tuple[str, int, int]
"""(топик, партиция, офсет) — сравнимое представление TopicPartition."""


def positions_of(tps: Sequence[TopicPartition]) -> set[Position]:
    """TopicPartition сравниваются без офсета — переводим в кортежи для проверок."""
    return {(tp.topic, tp.partition, tp.offset) for tp in tps}


def state_error(reason: str) -> KafkaException:
    """Ошибка «недопустимое состояние» (не retriable, не abortable, не fatal)."""
    return KafkaException(KafkaError(KafkaError._STATE, reason))


def retriable_error() -> KafkaException:
    """Повторяемая ошибка: тот же вызов можно повторить."""
    return KafkaException(
        KafkaError(KafkaError._TIMED_OUT, "retriable", retriable=True, txn_requires_abort=False)
    )


def abortable_error() -> KafkaException:
    """Ошибка транзакции, после которой нужен abort."""
    return KafkaException(
        KafkaError(KafkaError._TIMED_OUT, "abortable", retriable=False, txn_requires_abort=True)
    )


def fatal_error() -> KafkaException:
    """Фатальная ошибка продюсера (например, fencing «зомби»)."""
    return KafkaException(KafkaError(KafkaError._FENCED, "fenced", fatal=True))


@dataclass
class FakeMessage:
    """Сообщение Kafka: данные либо ошибка вместо сообщения."""

    topic_: str | None = TELEMETRY
    partition_: int | None = 0
    offset_: int | None = 0
    value_: bytes | None = None
    key_: bytes | None = None
    error_: KafkaError | None = None
    headers_: MessageHeaders | None = None

    def error(self) -> KafkaError | None:
        """Ошибка вместо сообщения или None."""
        return self.error_

    def topic(self) -> str | None:
        """Топик."""
        return self.topic_

    def partition(self) -> int | None:
        """Партиция."""
        return self.partition_

    def offset(self) -> int | None:
        """Офсет."""
        return self.offset_

    def key(self) -> bytes | None:
        """Ключ."""
        return self.key_

    def value(self) -> bytes | None:
        """Значение."""
        return self.value_

    def headers(self) -> MessageHeaders | None:
        """Заголовки или None, если их нет."""
        return self.headers_


def eof(partition: int = 0, offset: int = 0, topic: str = TELEMETRY) -> FakeMessage:
    """Служебное сообщение «достигнут конец партиции»."""
    return FakeMessage(
        topic_=topic,
        partition_=partition,
        offset_=offset,
        error_=KafkaError(KafkaError._PARTITION_EOF),
    )


@dataclass
class FakeConsumer:
    """Консьюмер входного топика поверх журнала сообщений.

    consume отдаёт до num_messages (и не больше max_per_call) сообщений журнала, чей офсет
    не меньше текущей позиции партиции, и сдвигает позиции. Если в injected есть заготовка,
    отдаётся она целиком (для ошибок и EOF). seek переставляет позицию назначенной партиции
    (назначены партиции журнала и явно перечисленные в assigned).
    group_offsets — закоммиченные офсеты группы: с них начинается чтение партиции, если
    позицию не переопределил assign (как в confluent_kafka при назначении партиции).
    assign записывает вызов и ставит позицию партиции = переданному офсету.
    on_consume вызывается перед каждым consume (например, чтобы выставить stop).
    """

    log: list[FakeMessage] = field(default_factory=list)
    max_per_call: int = 1_000_000
    injected: deque[list[FakeMessage]] = field(default_factory=deque)
    on_consume: Callable[[int], None] | None = None
    assigned: set[tuple[str, int]] = field(default_factory=set)
    group_offsets: dict[tuple[str, int], int] = field(default_factory=dict)
    assign_calls: list[list[Position]] = field(default_factory=list)
    group_metadata: object = field(default_factory=object)
    consume_calls: list[tuple[int, float]] = field(default_factory=list)
    seeks: list[Position] = field(default_factory=list)
    closed: bool = False
    _positions: dict[tuple[str, int], int] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Назначает партиции журнала; стартовые позиции — офсеты группы."""
        for m in self.log:
            assert m.topic_ is not None and m.partition_ is not None
            self.assigned.add((m.topic_, m.partition_))
        self._positions.update(self.group_offsets)

    @staticmethod
    def _tp(msg: FakeMessage) -> tuple[str, int]:
        assert msg.topic_ is not None and msg.partition_ is not None
        return msg.topic_, msg.partition_

    def consume(self, num_messages: int, timeout: float, /) -> Sequence[FakeMessage]:
        """Отдаёт следующую порцию сообщений."""
        assert not self.closed, "consume после close"
        self.consume_calls.append((num_messages, timeout))
        if self.on_consume is not None:
            self.on_consume(len(self.consume_calls))
        if self.injected:
            batch = self.injected.popleft()
        else:
            limit = min(num_messages, self.max_per_call)
            batch = [
                m
                for m in self.log
                if m.offset_ is not None and m.offset_ >= self._positions.get(self._tp(m), 0)
            ][:limit]
        for m in batch:
            if m.error_ is None and None not in (m.topic_, m.partition_, m.offset_):
                assert m.offset_ is not None
                key = self._tp(m)
                self._positions[key] = max(self._positions.get(key, 0), m.offset_ + 1)
        return batch

    def seek(self, partition: TopicPartition, /) -> None:
        """Переставляет позицию чтения назначенной партиции."""
        tp = (partition.topic, partition.partition)
        if tp not in self.assigned:
            raise KafkaException(KafkaError(KafkaError._UNKNOWN_PARTITION, f"not assigned {tp}"))
        self.seeks.append((partition.topic, partition.partition, partition.offset))
        self._positions[tp] = partition.offset

    def assign(self, partitions: list[TopicPartition], /) -> None:
        """Назначает партиции; неотрицательный офсет задаёт стартовую позицию."""
        self.assign_calls.append([(p.topic, p.partition, p.offset) for p in partitions])
        for p in partitions:
            self.assigned.add((p.topic, p.partition))
            if p.offset >= 0:
                self._positions[(p.topic, p.partition)] = p.offset

    def consumer_group_metadata(self) -> object:
        """Метаданные группы (непрозрачный объект)."""
        return self.group_metadata

    def close(self) -> None:
        """Закрывает консьюмер."""
        self.closed = True


Produced = tuple[str, bytes, bytes]
"""(топик, ключ, значение) записи продюсера."""


class TxnState(StrEnum):
    """Состояние транзакции продюсера."""

    NONE = "none"
    IN_TRANSACTION = "in_transaction"
    COMMITTING = "committing"
    ABORTABLE = "abortable"
    FATAL = "fatal"


@dataclass
class FakeProducer:
    """Транзакционный продюсер: автомат состояний, журнал вызовов, буфер и фиксация.

    failures — очереди исключений по имени метода: при вызове метода, если его очередь
    не пуста, выбрасывается следующее исключение, а состояние меняется как у confluent_kafka:
    retriable в commit → COMMITTING (разрешён только повтор commit), retriable в
    send_offsets → остаётся IN_TRANSACTION, abortable → ABORTABLE (разрешён только abort),
    fatal → FATAL (запрещено всё).
    """

    calls: list[str] = field(default_factory=list)
    pending: list[Produced] = field(default_factory=list)
    committed: list[Produced] = field(default_factory=list)
    pending_headers: list[ProducedHeaders | None] = field(default_factory=list)
    committed_headers: list[ProducedHeaders | None] = field(default_factory=list)
    sent_offsets: list[tuple[set[Position], object]] = field(default_factory=list)
    failures: dict[str, deque[KafkaException]] = field(default_factory=dict)
    state: TxnState = TxnState.NONE

    def fail(self, method: str, *errors: KafkaException) -> None:
        """Ставит в очередь ошибки для следующих вызовов метода."""
        self.failures.setdefault(method, deque()).extend(errors)

    def _call(self, name: str, allowed: tuple[TxnState, ...]) -> None:
        self.calls.append(name)
        if self.state not in allowed:
            raise state_error(f"{name} недопустим в состоянии {self.state}")
        queue = self.failures.get(name)
        if not queue:
            return
        exc = queue.popleft()
        err = exc.args[0]
        assert isinstance(err, KafkaError)
        if err.fatal():
            self.state = TxnState.FATAL
        elif err.txn_requires_abort():
            self.state = TxnState.ABORTABLE
        elif err.retriable() and name == "commit_transaction":
            self.state = TxnState.COMMITTING
        raise exc

    def count(self, name: str) -> int:
        """Сколько раз вызывался метод."""
        return self.calls.count(name)

    def begin_transaction(self) -> None:
        """Начинает транзакцию."""
        self._call("begin_transaction", (TxnState.NONE,))
        self.state = TxnState.IN_TRANSACTION

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes,
        /,
        *,
        headers: ProducedHeaders | None = None,
    ) -> None:
        """Добавляет запись (и её заголовки) в текущую транзакцию."""
        self._call("produce", (TxnState.IN_TRANSACTION,))
        self.pending.append((topic, key, value))
        self.pending_headers.append(headers)

    def send_offsets_to_transaction(
        self, positions: list[TopicPartition], group_metadata: object, /
    ) -> None:
        """Запоминает офсеты, переданные в транзакцию."""
        self._call("send_offsets_to_transaction", (TxnState.IN_TRANSACTION,))
        self.sent_offsets.append((positions_of(positions), group_metadata))

    def flush(self) -> int:
        """Доставляет поставленные записи (в фейке — сразу); недоставленных нет."""
        self._call("flush", (TxnState.IN_TRANSACTION,))
        return 0

    def commit_transaction(self) -> None:
        """Фиксирует записи транзакции."""
        self._call("commit_transaction", (TxnState.IN_TRANSACTION, TxnState.COMMITTING))
        self.committed.extend(self.pending)
        self.committed_headers.extend(self.pending_headers)
        self.pending.clear()
        self.pending_headers.clear()
        self.state = TxnState.NONE

    def abort_transaction(self) -> None:
        """Отбрасывает записи транзакции (только из IN_TRANSACTION или ABORTABLE)."""
        self._call("abort_transaction", (TxnState.IN_TRANSACTION, TxnState.ABORTABLE))
        self.pending.clear()
        self.pending_headers.clear()
        self.state = TxnState.NONE


@dataclass
class FakeStateConsumer:
    """Консьюмер топика состояния (одна партиция) для восстановления снимка.

    records — пары (ключ, значение) с офсетами first_offset, first_offset + 1, ...
    (или явными offsets для compacted-топика с пропусками). Low watermark — первый офсет,
    high — последний + 1 (для пустого топика low = high = first_offset).
    headers — заголовки записей (параллельно records; None — у всех записей заголовков нет).
    watermarks_timeout — get_watermark_offsets возвращает None (таймаут запроса).
    Заготовки из injected (например, EOF) отдаются первыми, позицию не меняют.
    on_consume вызывается перед каждым consume (например, чтобы сдвинуть часы).
    lso — открытая чужая транзакция: записи с офсетом не меньше lso не видны
    (read_committed), на границе отдаётся EOF, пока consume не вызван lso_resolved_at раз;
    get_watermark_offsets при этом отдаёт настоящий high (как клиент с read_uncommitted).
    """

    records: list[tuple[bytes | None, bytes | None]] = field(default_factory=list)
    first_offset: int = 0
    offsets: list[int] | None = None
    headers: list[MessageHeaders | None] | None = None
    max_per_call: int = 1_000_000
    watermarks_timeout: bool = False
    injected: deque[list[FakeMessage]] = field(default_factory=deque)
    on_consume: Callable[[int], None] | None = None
    assigned: list[Position] = field(default_factory=list)
    watermark_timeouts: list[float] = field(default_factory=list)
    consume_calls: int = 0
    lso: int | None = None
    lso_resolved_at: int = 0
    _pos: int = 0

    def _offsets(self) -> list[int]:
        if self.offsets is not None:
            return self.offsets
        return [self.first_offset + i for i in range(len(self.records))]

    def _watermarks(self) -> tuple[int, int]:
        offs = self._offsets()
        if not offs:
            return self.first_offset, self.first_offset
        return offs[0], offs[-1] + 1

    def get_watermark_offsets(
        self, partition: TopicPartition, /, timeout: float
    ) -> tuple[int, int] | None:
        """(low, high) watermark партиции состояния или None по таймауту."""
        assert (partition.topic, partition.partition) == (STATE, 0)
        self.watermark_timeouts.append(timeout)
        if self.watermarks_timeout:
            return None
        return self._watermarks()

    def assign(self, partitions: list[TopicPartition], /) -> None:
        """Назначает партицию; OFFSET_BEGINNING и прочие логические офсеты — с начала."""
        self.assigned.extend((p.topic, p.partition, p.offset) for p in partitions)
        offset = partitions[0].offset
        low, _ = self._watermarks()
        self._pos = low if offset in (OFFSET_BEGINNING,) or offset < 0 else offset

    def consume(self, num_messages: int, timeout: float, /) -> Sequence[FakeMessage]:
        """Отдаёт следующие записи начиная с позиции."""
        self.consume_calls += 1
        assert self.consume_calls < 10_000, "бесконечное чтение топика состояния"
        assert self.assigned, "consume до assign"
        if self.on_consume is not None:
            self.on_consume(self.consume_calls)
        if self.injected:
            return self.injected.popleft()
        out: list[FakeMessage] = []
        headers = self.headers if self.headers is not None else [None] * len(self.records)
        visible_below = (
            self.lso if self.lso is not None and self.consume_calls < self.lso_resolved_at else None
        )
        for off, (key, value), hdrs in zip(self._offsets(), self.records, headers, strict=True):
            if visible_below is not None and off >= visible_below:
                break
            if off >= self._pos and len(out) < min(num_messages, self.max_per_call):
                out.append(FakeMessage(STATE, 0, off, value, key, headers_=hdrs))
        if out:
            assert out[-1].offset_ is not None
            self._pos = out[-1].offset_ + 1
        if visible_below is not None and self._pos >= visible_below:
            out.append(eof(offset=self._pos, topic=STATE))
        return out

    def position(self, partitions: list[TopicPartition], /) -> list[TopicPartition]:
        """Следующий к чтению офсет."""
        return [TopicPartition(p.topic, p.partition, self._pos) for p in partitions]
