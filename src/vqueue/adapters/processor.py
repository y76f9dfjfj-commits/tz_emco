"""Преобразование батча телеметрии в выходные записи Kafka (без IO)."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from dataclasses import dataclass

from vqueue.adapters import topics
from vqueue.adapters.codec import (
    InvalidMessage,
    decode_telemetry,
    encode_decision,
    encode_queue,
    encode_snapshot,
)
from vqueue.domain.model import SiteConfig
from vqueue.domain.queue import StationQueue
from vqueue.domain.site import SiteSnapshot, SiteState

logger = logging.getLogger(__name__)

_MAX_LOGGED_BYTES = 200
"""Сколько байт некорректного сообщения попадает в лог."""


@dataclass(frozen=True, slots=True)
class OutRecord:
    """Запись к публикации в Kafka.

    Attributes:
        topic: Топик.
        key: Ключ сообщения.
        value: Значение сообщения.
    """

    topic: str
    key: bytes
    value: bytes


class TelemetryProcessor:
    """Применяет телеметрию к состоянию площадки и формирует выходные записи."""

    def __init__(
        self, site: SiteConfig, site_id: str, snapshot: SiteSnapshot | None = None
    ) -> None:
        """Создаёт процессор площадки.

        Args:
            site: Конфигурация площадки.
            site_id: Идентификатор площадки — ключ снимка в топике состояния.
            snapshot: Снимок для восстановления; None — начальное состояние.
        """
        self._site = site
        self._site_key = site_id.encode()
        self._state = SiteState(site, snapshot)

    def process(self, values: Iterable[bytes]) -> list[OutRecord]:
        """Обрабатывает батч значений telemetry.v1.

        Некорректные сообщения пропускаются с предупреждением в логе. В конце батча
        всегда добавляется запись снимка состояния, даже если других выходов нет.

        Args:
            values: Значения сообщений в порядке топика.

        Returns:
            Выходные записи в порядке выдачи доменом, последней — снимок состояния.
        """
        records: list[OutRecord] = []
        for raw in values:
            try:
                msg = decode_telemetry(raw)
            except InvalidMessage as exc:
                logger.warning("Пропущено сообщение %r: %s", raw[:_MAX_LOGGED_BYTES], exc)
                continue
            for out in self._state.apply(msg):
                if isinstance(out, StationQueue):
                    key, value = encode_queue(out)
                    records.append(OutRecord(topics.QUEUE, key, value))
                else:
                    key, value = encode_decision(out)
                    records.append(OutRecord(topics.DECISION, key, value))
        records.append(
            OutRecord(topics.STATE, self._site_key, encode_snapshot(self._state.snapshot()))
        )
        return records

    def snapshot(self) -> SiteSnapshot:
        """Возвращает снимок текущего состояния площадки."""
        return self._state.snapshot()

    def reset(self, snapshot: SiteSnapshot | None) -> None:
        """Заменяет состояние восстановленным из снимка (например, после отката транзакции).

        Args:
            snapshot: Снимок; None — начальное состояние.
        """
        self._state = SiteState(self._site, snapshot)
