"""Запуск генератора телеметрии: симуляция площадки с записью в Kafka или stdout.

Пример: ``python -m vqueue.simulator.generator_main --speedup 10 --dup-rate 0.01``.
"""

from __future__ import annotations

import argparse
import functools
import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Sequence
from pathlib import Path
from types import FrameType
from typing import Final, Protocol

from confluent_kafka import KafkaError, Message, Producer

from vqueue.config import load_site_config
from vqueue.domain.model import Telemetry
from vqueue.simulator.engine import Simulation
from vqueue.simulator.faults import FaultInjector
from vqueue.simulator.resume import last_ts_in_topic, start_ts
from vqueue.simulator.wire import encode_telemetry

LOG_EVERY_S: Final = 300
"""Период журнала о числе отправленных сообщений, виртуальные секунды."""

FLUSH_TIMEOUT_S: Final = 5.0
"""Сколько ждать доставки отправленного при закрытии, с."""

_PRODUCER_POLL_S: Final = 0.1
_MAX_SLEEP_S: Final = 0.5

logger = logging.getLogger("vqueue.simulator")


class Sink(Protocol):
    """Получатель сериализованных сообщений."""

    @property
    def failed(self) -> int:
        """Число сообщений, доставленных с ошибкой (на текущий момент)."""

    def send(self, key: bytes, value: bytes) -> bool:
        """Отправляет одно сообщение; False — не принято (остановка)."""

    def close(self) -> int:
        """Дожидается доставки отправленного; возвращает число недоставленных."""


class StdoutSink:
    """Запись сообщений в stdout в формате JSON Lines.

    Закрытый читателем канал (например, ``| head``) — штатное завершение:
    stdout перенаправляется в /dev/null, генератор останавливается без ошибки.
    """

    def __init__(self, stop: threading.Event) -> None:
        """Создаёт получатель.

        Args:
            stop: Флаг остановки генератора; выставляется при закрытии канала.
        """
        self._stop = stop
        self._broken = False

    @property
    def failed(self) -> int:
        """Ошибок доставки в stdout не бывает."""
        return 0

    def send(self, key: bytes, value: bytes) -> bool:
        """Печатает значение сообщения отдельной строкой."""
        if self._broken:
            return False
        try:
            sys.stdout.write(value.decode() + "\n")
        except BrokenPipeError:
            self._on_broken_pipe()
            return False
        return True

    def close(self) -> int:
        """Сбрасывает буфер stdout."""
        if not self._broken:
            try:
                sys.stdout.flush()
            except BrokenPipeError:
                self._on_broken_pipe()
        return 0

    def _on_broken_pipe(self) -> None:
        # Остаток буфера уйдёт в /dev/null: иначе интерпретатор упадёт на сбросе stdout при выходе.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        os.close(devnull)
        self._broken = True
        self._stop.set()


class KafkaSink:
    """Запись сообщений в топик Kafka идемпотентным продюсером."""

    def __init__(self, bootstrap: str, topic: str, stop: threading.Event) -> None:
        """Создаёт продюсер.

        Args:
            bootstrap: Адреса брокеров.
            topic: Топик телеметрии.
            stop: Флаг остановки: прерывает ожидание места в буфере продюсера.
        """
        self._topic = topic
        self._stop = stop
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap,
                "enable.idempotence": True,
                "linger.ms": 5,
            }
        )
        self._failed = 0
        self._dropped = 0

    @property
    def failed(self) -> int:
        """Число сообщений, доставленных с ошибкой (по отчётам доставки)."""
        return self._failed

    def send(self, key: bytes, value: bytes) -> bool:
        """Ставит сообщение в очередь продюсера, ожидая места в буфере до остановки."""
        while True:
            try:
                self._producer.produce(self._topic, value=value, key=key, on_delivery=self._report)
            except BufferError:
                if self._stop.is_set():
                    self._dropped += 1
                    return False
                self._producer.poll(_PRODUCER_POLL_S)
                continue
            self._producer.poll(0)
            return True

    def close(self) -> int:
        """Дожидается доставки не дольше FLUSH_TIMEOUT_S.

        Returns:
            Число недоставленных: оставшиеся в очереди продюсера и не принятые при остановке.
        """
        return self._producer.flush(FLUSH_TIMEOUT_S) + self._dropped

    def _report(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._failed += 1


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Генератор телеметрии площадки.")
    parser.add_argument("--site-config", type=Path, default=Path("config/site.toml"))
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092"))
    parser.add_argument("--topic", default="telemetry.v1")
    parser.add_argument(
        "--start-ts",
        type=int,
        default=None,
        help="Начало, секунды epoch (по умолчанию — сейчас, но не раньше последнего ts в топике)",
    )
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument(
        "--speedup", type=float, default=1.0, help="Ускорение времени; 0 — без пауз"
    )
    parser.add_argument(
        "--duration-s", type=int, default=0, help="Длительность, виртуальные секунды; 0 — без конца"
    )
    parser.add_argument("--dup-rate", type=float, default=0.0)
    parser.add_argument("--late-rate", type=float, default=0.0)
    parser.add_argument("--output", choices=("kafka", "stdout"), default="kafka")
    args = parser.parse_args(argv)
    if args.speedup < 0:
        parser.error("--speedup не может быть отрицательным")
    if args.duration_s < 0:
        parser.error("--duration-s не может быть отрицательной")
    return args


def _install_stop_handlers(stop: threading.Event) -> None:
    """Устанавливает обработчики SIGINT/SIGTERM.

    Первый сигнал выставляет флаг остановки (штатное завершение с flush),
    повторный — восстанавливает обработчик по умолчанию и пере-поднимает сигнал
    (немедленный выход).
    """

    def handler(signum: int, _frame: FrameType | None) -> None:
        if stop.is_set():
            signal.signal(signum, signal.SIG_DFL)
            signal.raise_signal(signum)
            return
        stop.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _sleep_until(deadline: float, stop: threading.Event) -> None:
    """Ждёт момента deadline (time.monotonic) отрезками не длиннее 0.5 с, прерываясь по stop."""
    while not stop.is_set():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        stop.wait(min(remaining, _MAX_SLEEP_S))


def run(argv: Sequence[str] | None = None) -> int:
    """Запускает генератор.

    Args:
        argv: Аргументы командной строки; None — из sys.argv.

    Returns:
        Код завершения: 1 — были ошибки доставки или остались недоставленные сообщения, иначе 0.
    """
    args = _parse_args(argv)
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr, format="%(asctime)s %(levelname)s %(message)s"
    )
    site = load_site_config(args.site_config)
    now = int(time.time())
    begin_ts = start_ts(
        args.start_ts,
        args.output,
        now,
        functools.partial(last_ts_in_topic, args.bootstrap, args.topic),
    )
    if begin_ts > now and args.start_ts is None:
        logger.info("Продолжение времени топика %s: старт с ts=%d", args.topic, begin_ts)
    simulation = Simulation(site, start_ts=begin_ts, seed=args.seed)
    faults = FaultInjector(seed=args.seed, dup_rate=args.dup_rate, late_rate=args.late_rate)
    stop = threading.Event()
    sink: Sink = (
        StdoutSink(stop) if args.output == "stdout" else KafkaSink(args.bootstrap, args.topic, stop)
    )
    _install_stop_handlers(stop)

    def emit(messages: Sequence[Telemetry]) -> int:
        sent = 0
        for msg in messages:
            if not sink.send(*encode_telemetry(msg)):
                break
            sent += 1
        return sent

    sent = 0
    started = time.monotonic()
    tick = 0
    try:
        while not stop.is_set() and (args.duration_s == 0 or tick < args.duration_s):
            tick += 1
            if args.speedup > 0:
                # Срок отсчитывается от старта: паузы не накапливают дрейф.
                _sleep_until(started + tick / args.speedup, stop)
                if stop.is_set():
                    break
            generated = simulation.step()
            sent += emit(faults.feed(simulation.ts, generated))
            if tick % LOG_EVERY_S == 0:
                logger.info(
                    "Виртуальное время %d: отправлено %d сообщений, ошибок доставки %d",
                    simulation.ts,
                    sent,
                    sink.failed,
                )
        sent += emit(faults.flush())
    finally:
        undelivered = sink.close()
    failed = sink.failed
    logger.info(
        "Завершено: виртуальное время %d, отправлено %d, ошибок доставки %d, не доставлено %d",
        simulation.ts,
        sent,
        failed,
        undelivered,
    )
    return 1 if failed or undelivered else 0


if __name__ == "__main__":
    sys.exit(run())
