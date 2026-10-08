"""Вспомогательные средства интеграционных тестов: топики, продюсер, чтение выходов, процессор.

Процессор запускается как отдельный процесс `python -m vqueue.processor_main` с настройками
из переменных окружения (.env.example).
"""

from __future__ import annotations

import os
import signal
import socket
import subprocess  # nosec B404 — тест запускает процессор как отдельный процесс.
import sys
import time
import urllib.error
import urllib.request
from collections import defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Final, TextIO, TypeAlias

from confluent_kafka import (
    OFFSET_BEGINNING,
    Consumer,
    KafkaError,
    KafkaException,
    Producer,
    TopicPartition,
)

# NewTopic реэкспортируется confluent_kafka.admin без __all__ — mypy --strict этого не видит.
from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore[attr-defined]

from vqueue.adapters import topics

SRC_DIR: Final = Path(__file__).resolve().parents[2] / "src"
"""Исходники пакета: процессор запускается на рабочем дереве, а не на установленной копии."""

TOPIC_PARTITIONS: Final = {
    topics.TELEMETRY: 1,
    topics.QUEUE: 4,
    topics.DECISION: 4,
    topics.STATE: 1,
}
"""Партиции топиков, как в kafka-init docker-compose."""

TOPIC_CONFIGS: Final = {
    topics.STATE: {
        "cleanup.policy": "compact",
        "segment.ms": "600000",
        "min.cleanable.dirty.ratio": "0.1",
    }
}
"""Особые настройки топиков, как в kafka-init: снимок состояния — compacted.

Короткий segment.ms и низкий min.cleanable.dirty.ratio: компакция не трогает активный
сегмент, без них при рестарте пришлось бы читать все снимки за срок хранения.
"""


def wait_until(
    condition: Callable[[], bool], timeout_s: float, what: str, interval_s: float = 0.1
) -> None:
    """Ждёт выполнения условия не дольше timeout_s; иначе AssertionError с описанием."""
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if condition():
            return
        time.sleep(interval_s)
    if condition():
        return
    raise AssertionError(f"Не дождались за {timeout_s} с: {what}")


def broker_unavailable_reason(bootstrap: str, timeout_s: float = 5.0) -> str | None:
    """Причина недоступности брокера или None, если метаданные получены."""
    admin = AdminClient(
        {"bootstrap.servers": bootstrap, "socket.connection.setup.timeout.ms": 3000}
    )
    try:
        admin.list_topics(timeout=timeout_s)
    except KafkaException as exc:
        return f"брокер Kafka на {bootstrap} недоступен ({exc}); поднимите его: make kafka-up"
    return None


def recreate_topics(bootstrap: str, timeout_s: float = 60.0) -> None:
    """Удаляет и заново создаёт четыре топика процессора с нужными партициями и настройками."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    names = list(TOPIC_PARTITIONS)

    existing = set(admin.list_topics(timeout=10).topics)
    to_delete = [n for n in names if n in existing]
    if to_delete:
        for fut in admin.delete_topics(to_delete, operation_timeout=30).values():
            try:
                fut.result()
            except KafkaException as exc:
                if exc.args[0].code() != KafkaError.UNKNOWN_TOPIC_OR_PART:
                    raise

    def all_deleted() -> bool:
        present = set(admin.list_topics(timeout=10).topics)
        return not present.intersection(names)

    wait_until(all_deleted, timeout_s, "удаление топиков", interval_s=0.2)

    pending = set(names)
    deadline = time.monotonic() + timeout_s
    while pending:
        new = [
            NewTopic(n, num_partitions=TOPIC_PARTITIONS[n], config=TOPIC_CONFIGS.get(n, {}))
            for n in sorted(pending)
        ]
        for name, fut in admin.create_topics(new, operation_timeout=30).items():
            try:
                fut.result()
                pending.discard(name)
            except KafkaException as exc:
                # Топик ещё помечен к удалению — повторим создание.
                if time.monotonic() > deadline:
                    raise
                if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                    raise
        if pending:
            time.sleep(0.5)

    def all_led() -> bool:
        meta = admin.list_topics(timeout=10).topics
        for name, count in TOPIC_PARTITIONS.items():
            t = meta.get(name)
            if t is None or t.error is not None or len(t.partitions) != count:
                return False
            if any(p.leader < 0 for p in t.partitions.values()):
                return False
        return True

    wait_until(all_led, timeout_s, "лидеры партиций новых топиков", interval_s=0.2)


def create_topic(bootstrap: str, name: str, partitions: int) -> None:
    """Создаёт топик и ждёт лидеров всех его партиций."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.create_topics([NewTopic(name, num_partitions=partitions)], operation_timeout=30)[
        name
    ].result()

    def led() -> bool:
        t = admin.list_topics(name, timeout=10).topics.get(name)
        return (
            t is not None
            and t.error is None
            and len(t.partitions) == partitions
            and all(p.leader >= 0 for p in t.partitions.values())
        )

    wait_until(led, 30, f"лидеры партиций {name}", interval_s=0.2)


def delete_topic(bootstrap: str, name: str) -> None:
    """Удаляет топик (без ожидания полного удаления)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    admin.delete_topics([name], operation_timeout=30)[name].result()


def produce_all(bootstrap: str, topic: str, records: Sequence[tuple[bytes, bytes]]) -> None:
    """Отправляет записи (ключ, значение) в топик в заданном порядке и дожидается доставки."""
    producer = Producer(
        {
            "bootstrap.servers": bootstrap,
            "enable.idempotence": True,
            "linger.ms": 20,
            "queue.buffering.max.messages": max(100_000, len(records) + 1),
        }
    )
    errors: list[str] = []

    def report(err: KafkaError | None, _msg: object) -> None:
        if err is not None:
            errors.append(str(err))

    for key, value in records:
        producer.produce(topic, value=value, key=key, on_delivery=report)
        producer.poll(0)
    remaining = producer.flush(60)
    assert remaining == 0, f"Не доставлено {remaining} сообщений в {topic}"
    assert not errors, f"Ошибки доставки в {topic}: {errors[:5]}"


def end_offset(bootstrap: str, topic: str, partition: int = 0) -> int:
    """High watermark партиции топика."""
    consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": "it-watermark"})
    try:
        _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        return int(high)
    finally:
        consumer.close()


class OffsetProbe:
    """Опрос закоммиченного offset группы одним переиспользуемым консьюмером.

    Консьюмер не подписывается и в группу не вступает — только читает её offset.
    """

    def __init__(self, bootstrap: str, group: str, topic: str, partition: int = 0) -> None:
        """Создаёт консьюмер с group.id проверяемой группы; subscribe не вызывается."""
        self._consumer = Consumer(
            {"bootstrap.servers": bootstrap, "group.id": group, "enable.auto.commit": False}
        )
        self._tp = TopicPartition(topic, partition)

    def committed(self) -> int:
        """Закоммиченный offset; 0, если коммитов ещё не было."""
        (tp,) = self._consumer.committed([self._tp], timeout=10)
        return max(int(tp.offset), 0)

    def close(self) -> None:
        """Закрывает консьюмер."""
        self._consumer.close()


def committed_offset(bootstrap: str, group: str, topic: str, partition: int = 0) -> int:
    """Закоммиченный offset группы на партиции; 0, если коммитов ещё не было."""
    probe = OffsetProbe(bootstrap, group, topic, partition)
    try:
        return probe.committed()
    finally:
        probe.close()


def active_members(bootstrap: str, group: str, timeout_s: float = 10.0) -> int:
    """Число активных участников консьюмер-группы (0 — группы нет или она пуста)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    fut = admin.describe_consumer_groups([group], request_timeout=timeout_s)[group]
    try:
        description = fut.result()
    except KafkaException as exc:
        if exc.args[0].code() == KafkaError.GROUP_ID_NOT_FOUND:
            return 0
        raise
    return len(description.members)


def read_committed(
    bootstrap: str, topic: str, group: str, timeout_s: float = 30.0
) -> list[tuple[str, str]]:
    """Читает топик с начала до конца всех партиций (read_committed).

    Returns:
        Записи (ключ, значение) в порядке чтения; порядок внутри партиции (и ключа) сохранён.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
        }
    )
    try:
        meta = consumer.list_topics(topic, timeout=10).topics[topic]
        parts = sorted(meta.partitions)
        consumer.assign([TopicPartition(topic, p, OFFSET_BEGINNING) for p in parts])
        at_eof: set[int] = set()
        out: list[tuple[str, str]] = []
        deadline = time.monotonic() + timeout_s
        while at_eof != set(parts):
            assert time.monotonic() < deadline, f"Не дочитали {topic} за {timeout_s} с"
            for msg in consumer.consume(1000, 0.5):
                err = msg.error()
                part = msg.partition()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF and part is not None:
                        at_eof.add(part)
                        continue
                    raise KafkaException(err)
                if part is not None:
                    at_eof.discard(part)
                key, value = msg.key(), msg.value()
                assert key is not None, f"Сообщение без ключа в {topic}"
                assert value is not None, f"Сообщение без значения в {topic}"
                out.append((key.decode(), value.decode()))
        return out
    finally:
        consumer.close()


def last_headers_by_key(
    bootstrap: str, topic: str, key: str, group: str, timeout_s: float = 30.0
) -> list[tuple[str, bytes | None]] | None:
    """Заголовки последней записи с ключом key (read_committed, одна партиция 0).

    Returns:
        Заголовки последней записи ключа; None, если записей с этим ключом нет.

    Raises:
        AssertionError: Не дочитали топик до конца за timeout_s.
    """
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": group,
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
            "enable.partition.eof": True,
        }
    )
    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING)])
        found: list[tuple[str, bytes | None]] | None = None
        deadline = time.monotonic() + timeout_s
        while True:
            assert time.monotonic() < deadline, f"Не дочитали {topic} за {timeout_s} с"
            for msg in consumer.consume(1000, 0.5):
                err = msg.error()
                if err is not None:
                    if err.code() == KafkaError._PARTITION_EOF:
                        return found
                    raise KafkaException(err)
                msg_key = msg.key()
                if msg_key is not None and msg_key.decode() == key:
                    raw = msg.headers() or []
                    pairs = list(raw.items()) if isinstance(raw, dict) else list(raw)
                    found = [(str(k), _as_bytes(v)) for k, v in pairs]
    finally:
        consumer.close()


def _as_bytes(value: str | bytes | None) -> bytes | None:
    """Значение заголовка в байтах."""
    return value.encode() if isinstance(value, str) else value


def by_key(records: Sequence[tuple[str, str]]) -> dict[str, list[str]]:
    """Группирует значения по ключу, сохраняя порядок."""
    grouped: dict[str, list[str]] = defaultdict(list)
    for key, value in records:
        grouped[key].append(value)
    return dict(grouped)


def free_port() -> int:
    """Свободный TCP-порт на 127.0.0.1 (HEALTH_PORT=0 процессор не принимает)."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def http_status(url: str, timeout_s: float = 1.0) -> int | None:
    """HTTP-код ответа GET или None, если соединиться не удалось."""
    try:
        with urllib.request.urlopen(url, timeout=timeout_s) as resp:  # nosec B310
            return int(resp.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)
    except (urllib.error.URLError, OSError):
        return None


@dataclass
class ProcessorProcess:
    """Запущенный процесс процессора и файл его журнала."""

    popen: subprocess.Popen[bytes]
    log_path: Path
    health_port: int
    log_file: TextIO

    @property
    def ready_url(self) -> str:
        """Адрес готовности /health/ready."""
        return f"http://127.0.0.1:{self.health_port}/health/ready"

    def wait_ready(self, timeout_s: float = 60.0) -> None:
        """Ждёт /health/ready → 200 (партиция назначена)."""

        def ready() -> bool:
            assert self.popen.poll() is None, (
                f"Процессор завершился с кодом {self.popen.returncode}"
            )
            return http_status(self.ready_url) == 200

        wait_until(ready, timeout_s, f"{self.ready_url} → 200", interval_s=0.2)

    def kill(self) -> None:
        """SIGKILL и ожидание завершения."""
        if self.popen.poll() is None:
            self.popen.send_signal(signal.SIGKILL)
        self.popen.wait(timeout=10)

    def stop(self) -> None:
        """Гарантированно останавливает процесс и закрывает журнал."""
        try:
            if self.popen.poll() is None:
                self.popen.kill()
                self.popen.wait(timeout=10)
        finally:
            self.log_file.close()


@dataclass
class ProcessorLauncher:
    """Запускает процессоры с общими настройками теста; останавливает все в конце теста."""

    bootstrap: str
    site_config: Path
    workdir: Path
    site_id: str
    instance_id: str
    consumer_group: str
    started: list[ProcessorProcess] = field(default_factory=list)

    def start(self) -> ProcessorProcess:
        """Запускает `python -m vqueue.processor_main` с env теста и свободным HEALTH_PORT."""
        port = free_port()
        env = dict(os.environ)
        env.update(
            {
                "KAFKA_BOOTSTRAP": self.bootstrap,
                "SITE_CONFIG": str(self.site_config),
                "SITE_ID": self.site_id,
                "INSTANCE_ID": self.instance_id,
                "CONSUMER_GROUP": self.consumer_group,
                "HEALTH_PORT": str(port),
                "LOG_LEVEL": "INFO",
                "PYTHONPATH": os.pathsep.join(
                    p for p in (str(SRC_DIR), env.get("PYTHONPATH", "")) if p
                ),
                "PYTHONUNBUFFERED": "1",
            }
        )
        log_path = self.workdir / f"processor-{len(self.started) + 1}.log"
        log_file = log_path.open("w", encoding="utf-8")
        popen = subprocess.Popen(  # nosec B603 — фиксированная команда без shell.
            [sys.executable, "-m", "vqueue.processor_main"],
            env=env,
            cwd=self.workdir,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        proc = ProcessorProcess(popen, log_path, port, log_file)
        self.started.append(proc)
        return proc

    def stop_all(self) -> None:
        """Останавливает все запущенные процессы и печатает их журналы (видны при падении)."""
        for proc in self.started:
            proc.stop()
        for i, proc in enumerate(self.started, start=1):
            text = proc.log_path.read_text(encoding="utf-8", errors="replace")
            print(f"===== журнал процессора #{i} (код {proc.popen.returncode}) =====")
            print(text)


LauncherFactory: TypeAlias = Callable[[Path], ProcessorLauncher]
"""Создаёт запускатель процессоров теста для заданной TOML-конфигурации площадки."""
