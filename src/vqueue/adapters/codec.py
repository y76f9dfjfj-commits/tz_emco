"""Кодек сообщений Kafka: телеметрия, очереди, решения и снимок состояния.

Форматы — ровно как в ТЗ (раздел «Данные»): JSON компактный, поля в порядке ТЗ,
время — ISO 8601 UTC с суффиксом Z и секундной точностью, ключи — UTF-8.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictInt, TypeAdapter, ValidationError

from vqueue.domain.model import Point, Telemetry
from vqueue.domain.queue import StationQueue
from vqueue.domain.recommendation import Decision, Recommendation
from vqueue.domain.site import SiteSnapshot

MAX_TS: Final = 253_402_214_399
"""Наибольший допустимый ts: 9999-12-30T23:59:59Z.

Сутки запаса до конца 9999 года: производные моменты (eta, free_at) лежат в пределах
горизонта и обслуживания очереди и тоже остаются представимыми в ISO 8601. Без границы
сообщение с огромным ts роняло бы процессор при кодировании выхода («ядовитое» сообщение).
"""


class InvalidMessage(ValueError):  # noqa: N818 — «битое сообщение»: имя отражает смысл, а не тип
    """Сообщение не соответствует формату: не JSON, нет поля, неверный тип или значение."""


class _TelemetryIn(BaseModel):
    """Входное сообщение telemetry.v1; лишние поля игнорируются."""

    model_config = ConfigDict(strict=True, extra="ignore", frozen=True)

    unit_uuid: str
    # Строгий int: bool, строки и любые float (включая 1.0) отвергаются.
    ts: StrictInt = Field(ge=0, le=MAX_TS)
    lat: float
    lon: float
    speed_kmh: float = Field(ge=0, allow_inf_nan=False)


_OUT_CONFIG: Final = ConfigDict(frozen=True)
"""Выходные модели неизменяемы: собираются один раз и сразу сериализуются."""


class _QueueEntryOut(BaseModel):
    """Позиция очереди в queue.v1."""

    model_config = _OUT_CONFIG

    unit_uuid: str
    eta: str | None
    service_start: str
    free_at: str
    wait_seconds: int


class _QueueOut(BaseModel):
    """Сообщение queue.v1."""

    model_config = _OUT_CONFIG

    station_uuid: str
    at: str
    queue: tuple[_QueueEntryOut, ...]


class _RecommendedOut(BaseModel):
    """Сообщение decision.v1 с рекомендацией."""

    model_config = _OUT_CONFIG

    unit_uuid: str
    at: str
    result: Literal["recommended"] = "recommended"
    from_station: str
    to_station: str
    gain_seconds: int


class _RejectedOut(BaseModel):
    """Сообщение decision.v1 с отказом."""

    model_config = _OUT_CONFIG

    unit_uuid: str
    at: str
    result: Literal["rejected"] = "rejected"
    reason: str


_SNAPSHOT: Final[TypeAdapter[SiteSnapshot]] = TypeAdapter(SiteSnapshot)


def _describe(exc: ValueError) -> str:
    """Однострочное описание ошибки разбора: поле и причина, без входных данных."""
    if isinstance(exc, ValidationError):
        return "; ".join(
            f"{'.'.join(map(str, e['loc'])) or '<root>'}: {e['msg']}"
            for e in exc.errors(include_url=False)
        )
    return str(exc)


def iso_utc(ts: int) -> str:
    """Форматирует момент времени в ISO 8601 UTC с суффиксом Z.

    Args:
        ts: Секунды epoch.

    Returns:
        Строка вида "2026-09-15T12:00:00Z".
    """
    naive = datetime.fromtimestamp(ts, UTC).replace(tzinfo=None)
    # isoformat, а не strftime: год всегда из четырёх цифр на любой платформе.
    return naive.isoformat(timespec="seconds") + "Z"


def decode_telemetry(raw: bytes) -> Telemetry:
    """Разбирает сообщение telemetry.v1.

    Args:
        raw: Значение сообщения Kafka.

    Returns:
        Доменное сообщение телеметрии.

    Raises:
        InvalidMessage: Не JSON-объект, нет поля, неверный тип или значение вне диапазона.
    """
    try:
        msg = _TelemetryIn.model_validate_json(raw)
        position = Point(msg.lat, msg.lon)
    except ValueError as exc:  # ValidationError — подкласс ValueError
        raise InvalidMessage(f"Некорректная телеметрия: {_describe(exc)}") from exc
    return Telemetry(unit_id=msg.unit_uuid, ts=msg.ts, position=position, speed_kmh=msg.speed_kmh)


def encode_queue(queue: StationQueue) -> tuple[bytes, bytes]:
    """Кодирует очередь станции в сообщение queue.v1.

    Args:
        queue: Очередь станции.

    Returns:
        Пара (ключ station_uuid, значение JSON).
    """
    out = _QueueOut(
        station_uuid=queue.station_id,
        at=iso_utc(queue.at),
        queue=tuple(
            _QueueEntryOut(
                unit_uuid=e.unit_id,
                eta=None if e.eta is None else iso_utc(e.eta),
                service_start=iso_utc(e.service_start),
                free_at=iso_utc(e.free_at),
                wait_seconds=e.wait_seconds,
            )
            for e in queue.entries
        ),
    )
    return queue.station_id.encode(), out.model_dump_json().encode()


def encode_decision(decision: Decision) -> tuple[bytes, bytes]:
    """Кодирует решение в сообщение decision.v1.

    Args:
        decision: Рекомендация или отказ.

    Returns:
        Пара (ключ unit_uuid, значение JSON).
    """
    out: BaseModel
    if isinstance(decision, Recommendation):
        out = _RecommendedOut(
            unit_uuid=decision.unit_id,
            at=iso_utc(decision.at),
            from_station=decision.from_station,
            to_station=decision.to_station,
            gain_seconds=decision.gain_seconds,
        )
    else:
        out = _RejectedOut(
            unit_uuid=decision.unit_id,
            at=iso_utc(decision.at),
            reason=decision.reason.value,
        )
    return decision.unit_id.encode(), out.model_dump_json().encode()


def encode_snapshot(snapshot: SiteSnapshot) -> bytes:
    """Кодирует снимок состояния площадки в JSON.

    Args:
        snapshot: Снимок SiteState.

    Returns:
        Значение сообщения топика состояния.
    """
    data: dict[str, Any] = _SNAPSHOT.dump_python(snapshot, mode="json")
    # served_visits — frozenset: порядок обхода не определён, сортировка делает
    # байты снимка воспроизводимыми.
    for occ in data["occupancies"]:
        occ["served_visits"].sort()
    return json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()


def decode_snapshot(raw: bytes) -> SiteSnapshot:
    """Разбирает снимок состояния площадки.

    Args:
        raw: Значение сообщения топика состояния.

    Returns:
        Снимок SiteState.

    Raises:
        InvalidMessage: Значение не является корректным снимком.
    """
    try:
        return _SNAPSHOT.validate_json(raw)
    except ValueError as exc:  # ValidationError и ошибки инвариантов доменных классов
        raise InvalidMessage(f"Некорректный снимок состояния: {_describe(exc)}") from exc
