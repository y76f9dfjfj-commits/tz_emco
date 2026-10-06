"""Автомат фаз машины: восстановление состояния по потоку телеметрии."""

from __future__ import annotations

from dataclasses import dataclass

from vqueue.domain.geo import is_within
from vqueue.domain.ingest import is_in_order
from vqueue.domain.model import Point, SiteConfig, Telemetry, UnitPhase


@dataclass(frozen=True, slots=True)
class StationZoneEntry:
    """Непрерывное пребывание машины в радиусе станции.

    Attributes:
        station_id: Идентификатор станции, в радиусе которой находится машина.
        entered_at: ts первого сообщения непрерывного пребывания в радиусе.
    """

    station_id: str
    entered_at: int


@dataclass(frozen=True, slots=True)
class UnitTrack:
    """Состояние машины, восстановленное по телеметрии.

    Attributes:
        unit_id: Идентификатор машины.
        last_ts: Последний учтённый ts.
        position: Последняя позиция.
        phase: Текущая фаза цикла.
        station_id: Станция, на которой машина в фазе AT_STATION; иначе None.
        zone_entry: В радиусе какой станции машина сейчас и с какого момента; иначе None.
        in_unload_zone: Машина сейчас в радиусе точки разгрузки.
    """

    unit_id: str
    last_ts: int
    position: Point
    phase: UnitPhase
    station_id: str | None
    zone_entry: StationZoneEntry | None
    in_unload_zone: bool


def station_in_zone(position: Point, site: SiteConfig) -> str | None:
    """Определяет станцию, в радиусе которой находится точка.

    Args:
        position: Позиция машины.
        site: Конфигурация площадки.

    Returns:
        Идентификатор первой по порядку site.stations станции, до которой
        строго меньше zone_radius_m, или None.
    """
    radius = site.rules.zone_radius_m
    for station in site.stations:
        if is_within(position, station.location, radius):
            return station.station_id
    return None


def _next_phase(
    prev: UnitTrack | None,
    zone: str | None,
    stopped: bool,
    in_unload_zone: bool,
) -> tuple[UnitPhase, str | None]:
    """Вычисляет фазу и станцию обслуживания по правилам ТЗ.

    Returns:
        Пара (фаза, station_id); station_id задан только для AT_STATION.
    """
    if zone is not None and stopped:
        return UnitPhase.AT_STATION, zone
    if prev is None:
        # Первое сообщение без признака «на станции» — состояние по умолчанию.
        return UnitPhase.TO_STATION, None
    if prev.phase is UnitPhase.AT_STATION:
        if zone is not None and zone == prev.station_id:
            # Ещё в радиусе своей станции (манёвр или трогается) — фаза не меняется.
            return UnitPhase.AT_STATION, prev.station_id
        return UnitPhase.TO_UNLOAD, None
    # TO_UNLOAD держится, пока машина не выйдет из радиуса точки разгрузки.
    left_unload = prev.in_unload_zone and not in_unload_zone
    if prev.phase is UnitPhase.TO_UNLOAD and not left_unload:
        return UnitPhase.TO_UNLOAD, None
    return UnitPhase.TO_STATION, None


def advance(prev: UnitTrack | None, msg: Telemetry, site: SiteConfig) -> UnitTrack:
    """Применяет сообщение телеметрии к состоянию машины.

    Фильтрация дублей и порядка — забота вызывающего (см. ingest.is_in_order).

    Args:
        prev: Предыдущее состояние машины или None для первого сообщения.
        msg: Очередное сообщение телеметрии этой машины.
        site: Конфигурация площадки.

    Returns:
        Новое состояние машины.

    Raises:
        ValueError: Сообщение другой машины или msg.ts не больше prev.last_ts.
    """
    if prev is not None:
        if msg.unit_id != prev.unit_id:
            raise ValueError(
                f"Сообщение машины {msg.unit_id!r} применяется к состоянию {prev.unit_id!r}"
            )
        if not is_in_order(prev.last_ts, msg.ts):
            raise ValueError(f"ts сообщения {msg.ts} не больше последнего учтённого {prev.last_ts}")

    rules = site.rules
    zone = station_in_zone(msg.position, site)
    stopped = msg.speed_kmh < rules.stopped_speed_kmh
    in_unload_zone = is_within(msg.position, site.unload_point, rules.zone_radius_m)

    zone_entry: StationZoneEntry | None
    if zone is None:
        zone_entry = None
    elif prev is not None and prev.zone_entry is not None and prev.zone_entry.station_id == zone:
        zone_entry = prev.zone_entry
    else:
        zone_entry = StationZoneEntry(station_id=zone, entered_at=msg.ts)

    phase, station_id = _next_phase(prev, zone, stopped, in_unload_zone)
    return UnitTrack(
        unit_id=msg.unit_id,
        last_ts=msg.ts,
        position=msg.position,
        phase=phase,
        station_id=station_id,
        zone_entry=zone_entry,
        in_unload_zone=in_unload_zone,
    )
