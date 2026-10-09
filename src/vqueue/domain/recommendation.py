"""Рекомендация станции машине, подъезжающей к своей станции."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import TypeAlias

from vqueue.domain.geo import distance_m, is_within
from vqueue.domain.model import Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.occupancy import StationOccupancy
from vqueue.domain.queue import StationQueue, estimate_arrival, wait_before
from vqueue.domain.unit_fsm import UnitTrack


class RejectReason(StrEnum):
    """Код отказа в рекомендации."""

    NO_GAIN = "no_gain"
    STALE_TELEMETRY = "stale_telemetry"


@dataclass(frozen=True, slots=True)
class Recommendation:
    """Рекомендация ехать к другой станции.

    Attributes:
        unit_id: Идентификатор машины.
        at: Момент расчёта («сейчас»).
        from_station: Своя станция машины.
        to_station: Рекомендованная станция.
        gain_seconds: Ожидание у своей станции минус ожидание у рекомендованной.
    """

    unit_id: str
    at: int
    from_station: str
    to_station: str
    gain_seconds: int


@dataclass(frozen=True, slots=True)
class Rejection:
    """Отказ в рекомендации.

    Attributes:
        unit_id: Идентификатор машины.
        at: Момент расчёта («сейчас»).
        reason: Код причины отказа.
    """

    unit_id: str
    at: int
    reason: RejectReason


Decision: TypeAlias = Recommendation | Rejection


def is_decision_point(track: UnitTrack, home: Station, rules: Rules) -> bool:
    """Проверяет, находится ли машина в точке принятия решения.

    Машина едет к станции и строго ближе радиуса решения к своей станции.
    Однократность за заезд проверяет вызывающий.

    Args:
        track: Состояние машины.
        home: Своя станция машины.
        rules: Константы расчёта.

    Returns:
        True, если для машины пора считать рекомендацию.
    """
    return track.phase is UnitPhase.TO_STATION and is_within(
        track.position, home.location, rules.decision_radius_m
    )


def recommend(
    track: UnitTrack,
    queues: Mapping[str, StationQueue],
    occupancies: Mapping[str, StationOccupancy],
    site: SiteConfig,
    now: int,
) -> Decision:
    """Выбирает станцию с наименьшим ожиданием для машины.

    Args:
        track: Состояние машины после сообщения, вызвавшего расчёт.
        queues: Очереди всех станций площадки на момент now.
        occupancies: Занятость всех станций площадки.
        site: Конфигурация площадки.
        now: Момент расчёта, секунды epoch.

    Returns:
        Рекомендация либо отказ с кодом причины.

    Raises:
        KeyError: Для станции площадки нет очереди или занятости.
        ValueError: Машина не закреплена за станцией либо своя станция вне
            горизонта (расчёт вызван не в точке решения).
    """
    rules = site.rules
    home_station_id = site.home_station_id(track.unit_id)
    if home_station_id is None:
        raise ValueError(f"Машина {track.unit_id} не закреплена за станцией")
    if not rules.is_fresh(track.last_ts, now):
        return Rejection(track.unit_id, now, RejectReason.STALE_TELEMETRY)

    waits: dict[str, int] = {}
    distances: dict[str, float] = {}
    for station in site.stations:
        sid = station.station_id
        eta = estimate_arrival(track, station, occupancies[sid], site, now)
        if eta is None:  # за горизонтом: станция не участвует
            continue
        waits[sid] = wait_before(queues[sid], eta, track.unit_id)
        distances[sid] = distance_m(track.position, station.location)

    if home_station_id not in waits:
        # В точке решения своя станция ближе радиуса решения и всегда в горизонте:
        # иное — ошибка вызывающего, её нельзя маскировать правдоподобным отказом.
        raise ValueError(
            f"Своя станция {home_station_id} машины {track.unit_id} вне горизонта: "
            "расчёт вызван не в точке решения"
        )

    # Наименьшее ожидание; при равенстве своя, затем ближайшая, затем по id.
    best = min(
        waits,
        key=lambda sid: (waits[sid], sid != home_station_id, distances[sid], sid),
    )
    gain = waits[home_station_id] - waits[best]
    if gain < rules.min_gain_seconds:
        return Rejection(track.unit_id, now, RejectReason.NO_GAIN)
    return Recommendation(track.unit_id, now, home_station_id, best, gain)
