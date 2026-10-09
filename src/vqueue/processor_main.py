"""Точка входа процессора виртуальных очередей: python -m vqueue.processor_main.

Тонкая сборка: настройки из env, логирование, health-check, Kafka-клиенты
и транзакционный цикл. Восстановление состояния и реакция на rebalance — в KafkaRunner.
Сигналы SIGTERM/SIGINT останавливают цикл после текущего батча.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import socket
import threading
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Final

from confluent_kafka import Consumer, Producer

from vqueue.adapters import topics
from vqueue.adapters.health import HealthState, start_health_server
from vqueue.adapters.kafka_runner import KafkaRunner, RestoredState, load_snapshot
from vqueue.adapters.processor import TelemetryProcessor
from vqueue.config import load_site_config

logger = logging.getLogger("vqueue.processor")

_LOG_LEVELS: Final = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_MAX_PORT: Final = 65_535

_TRANSACTION_TIMEOUT_MS: Final = 20_000
"""Предел жизни транзакции: столько новый владелец партиции ждёт исхода транзакции «зомби»."""

_RESTORE_TIMEOUT_S: Final = 60.0
"""Предел восстановления снимка: больше _TRANSACTION_TIMEOUT_MS с запасом на проверку
просроченных транзакций координатором (по умолчанию раз в 10 с)."""


class _JsonFormatter(logging.Formatter):
    """Одна JSON-строка на запись: time, level, logger, message (+ exc) — без неоднозначности."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "time": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc"] = self.formatException(record.exc_info)
        return json.dumps(entry, ensure_ascii=False)


def _env(name: str, default: str) -> str:
    """Значение переменной окружения; пустая строка равносильна отсутствию."""
    return os.environ.get(name) or default


def _parse_port(raw: str) -> int:
    """Разбирает HEALTH_PORT.

    Raises:
        ValueError: Не целое число в диапазоне 1..65535.
    """
    try:
        port = int(raw)
    except ValueError:
        port = 0
    if not 1 <= port <= _MAX_PORT:
        raise ValueError(f"HEALTH_PORT должен быть целым числом 1..{_MAX_PORT}, получено {raw!r}")
    return port


def _parse_log_level(raw: str) -> str:
    """Разбирает LOG_LEVEL (без учёта регистра).

    Raises:
        ValueError: Уровень не из списка допустимых.
    """
    level = raw.strip().upper()
    if level not in _LOG_LEVELS:
        raise ValueError(
            f"LOG_LEVEL должен быть одним из {', '.join(_LOG_LEVELS)}, получено {raw!r}"
        )
    return level


@dataclass(frozen=True, slots=True)
class Settings:
    """Настройки процесса из переменных окружения.

    Attributes:
        kafka_bootstrap: Адреса брокеров Kafka.
        site_config: Путь к TOML-конфигурации площадки.
        site_id: Идентификатор площадки (ключ снимка, часть transactional.id и группы).
        instance_id: Идентификатор экземпляра (часть transactional.id): INSTANCE_ID или имя хоста.
        group_instance_id: group.instance.id для статического членства — только явно
            заданный INSTANCE_ID, иначе None (динамическое членство).
        consumer_group: Консьюмер-группа процессора.
        health_port: Порт HTTP health-check.
        log_level: Уровень логирования.
    """

    kafka_bootstrap: str
    site_config: Path
    site_id: str
    instance_id: str
    group_instance_id: str | None
    consumer_group: str
    health_port: int
    log_level: str

    @classmethod
    def from_env(cls) -> Settings:
        """Читает настройки из env, подставляя безопасные значения по умолчанию.

        Raises:
            ValueError: Некорректный HEALTH_PORT или LOG_LEVEL.
        """
        site_id = _env("SITE_ID", "site-1")
        explicit_instance_id = os.environ.get("INSTANCE_ID") or None
        return cls(
            kafka_bootstrap=_env("KAFKA_BOOTSTRAP", "localhost:9092"),
            site_config=Path(_env("SITE_CONFIG", "config/site.toml")),
            site_id=site_id,
            instance_id=explicit_instance_id or socket.gethostname(),
            group_instance_id=explicit_instance_id,
            consumer_group=_env("CONSUMER_GROUP", f"vqueue-processor-{site_id}"),
            health_port=_parse_port(_env("HEALTH_PORT", "8080")),
            log_level=_parse_log_level(_env("LOG_LEVEL", "INFO")),
        )


def _configure_logging(level: str) -> None:
    """Настраивает корневой логгер: JSON-строки в stderr."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.basicConfig(level=level, handlers=[handler])


def main() -> None:
    """Собирает и запускает процессор до сигнала остановки."""
    settings = Settings.from_env()
    _configure_logging(settings.log_level)
    site = load_site_config(settings.site_config)
    logger.info(
        "Старт процессора: site_id=%s, instance_id=%s, static=%s, станций=%d, bootstrap=%s, "
        "group=%s",
        settings.site_id,
        settings.instance_id,
        settings.group_instance_id is not None,
        len(site.stations),
        settings.kafka_bootstrap,
        settings.consumer_group,
    )

    health = HealthState()
    server = start_health_server(health, settings.health_port)

    # transactional.id уникален для экземпляра; «зомби» после rebalance отсекается
    # по поколению консьюмер-группы (метаданные группы в send_offsets_to_transaction).
    # Стабильный INSTANCE_ID (например, имя пода StatefulSet) дополнительно позволяет
    # init_transactions сразу завершить незакрытую транзакцию прежнего запуска.
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap,
            "transactional.id": f"vqueue-processor-{settings.site_id}-{settings.instance_id}",
            "enable.idempotence": True,
            "transaction.timeout.ms": _TRANSACTION_TIMEOUT_MS,
        }
    )
    producer.init_transactions()

    def restore() -> RestoredState:
        def state_client(isolation: str) -> Consumer:
            return Consumer(
                {
                    "bootstrap.servers": settings.kafka_bootstrap,
                    "group.id": f"{settings.consumer_group}-state",
                    "enable.auto.commit": False,
                    "isolation.level": isolation,
                    "enable.partition.eof": True,
                }
            )

        state_consumer = state_client("read_committed")
        # read_uncommitted: high watermark, а не LSO (см. load_snapshot).
        watermarks = state_client("read_uncommitted")
        try:
            return load_snapshot(
                state_consumer, watermarks, settings.site_id, timeout_s=_RESTORE_TIMEOUT_S
            )
        finally:
            watermarks.close()
            state_consumer.close()

    consumer_config: dict[str, str | int | bool] = {
        "bootstrap.servers": settings.kafka_bootstrap,
        "group.id": settings.consumer_group,
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "auto.offset.reset": "earliest",
    }
    if settings.group_instance_id is not None:
        # Статическое членство — только при явном INSTANCE_ID (имя хоста в контейнере
        # меняется при пересоздании, и «чужой» статический член держал бы партицию). Компромисс:
        # + перезапущенный после падения экземпляр с тем же id сразу получает свою партицию,
        #   без rebalance и ожидания session.timeout.ms (45 с по умолчанию);
        # - SIGTERM не освобождает партицию: статический член при close не покидает группу,
        #   и standby с другим id получит её только по истечении session.timeout.ms;
        # - два живых экземпляра с одним INSTANCE_ID недопустимы: они фенсят друг друга
        #   (и в группе, и по transactional.id), поэтому docker compose --scale processor нельзя.
        consumer_config["group.instance.id"] = settings.group_instance_id
    consumer = Consumer(consumer_config)
    runner = KafkaRunner(
        consumer, producer, TelemetryProcessor(site, settings.site_id), health, restore
    )
    consumer.subscribe(
        [topics.TELEMETRY],
        on_assign=runner.on_assign,
        on_revoke=runner.on_revoke,
        on_lost=runner.on_lost,
    )

    stop = threading.Event()

    def on_signal(signum: int, _: FrameType | None) -> None:
        logger.info("Получен сигнал %s, остановка после текущего батча", signum)
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)

    try:
        runner.run(stop)
    finally:
        # flush не нужен: все записи публикуются в транзакциях, commit_transaction
        # дожидается доставки сам, а записи незавершённой транзакции не нужны.
        server.shutdown()
        server.server_close()
        logger.info("Процессор остановлен")


if __name__ == "__main__":
    main()
