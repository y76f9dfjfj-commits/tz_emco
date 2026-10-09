"""Виртуальная очередь станции: оценка приезда машин и расписание обслуживания."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from vqueue.domain.geo import distance_m, travel_time_s
from vqueue.domain.model import Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.occupancy import StationOccupancy
from vqueue.domain.unit_fsm import UnitTrack


@dataclass(frozen=True, slots=True)
class QueueEntry:
    """Позиция в очереди станции.

    Attributes:
        unit_id: Идентификатор машины.
        eta: Момент приезда; None только для машины, занявшей станцию.
        service_start: Начало обслуживания (для занявшей — момент занятия).
        free_at: Момент, когда станция освободится после этой машины.
        wait_seconds: Ожидание: service_start - eta; 0 для занявшей.
    """

    unit_id: str
    eta: int | None
    service_start: int
    free_at: int
    wait_seconds: int


@dataclass(frozen=True, slots=True)
class StationQueue:
    """Очередь станции на момент расчёта.

    Attributes:
        station_id: Идентификатор станции.
        at: Момент расчёта («сейчас»).
        entries: Позиции очереди по порядку обслуживания.
    """

    station_id: str
    at: int
    entries: tuple[QueueEntry, ...]


def _travel_seconds(track: UnitTrack, station: Station, site: SiteConfig) -> int | None:
    """Время в пути машины до станции по её фазе или None, если она едет не сюда."""
    speed = site.rules.speed_mps
    if track.phase is UnitPhase.TO_STATION:
        return travel_time_s(distance_m(track.position, station.location), speed)
    if track.phase is UnitPhase.TO_UNLOAD:
        # Округляется сумма путей, а не каждое плечо отдельно.
        path = distance_m(track.position, site.unload_point) + distance_m(
            site.unload_point, station.location
        )
        return travel_time_s(path, speed)
    # AT_STATION у другой станции: машина сюда не едет.
    return None


def estimate_arrival(
    track: UnitTrack,
    station: Station,
    occ: StationOccupancy,
    site: SiteConfig,
    now: int,
) -> int | None:
    """Оценивает момент приезда машины к станции.

    Закрепление машины за станцией не проверяется: вызывающий передаёт только
    машины, которые едут к этой станции. Порядок проверок: занявшая станцию,
    свежесть позиции, ожидание в радиусе станции, расчёт для едущей, горизонт.

    Args:
        track: Состояние машины.
        station: Станция, к которой оценивается приезд.
        occ: Занятость этой станции.
        site: Конфигурация площадки.
        now: Момент расчёта, секунды epoch.

    Returns:
        Момент приезда (для ждущей в радиусе — момент входа, может быть < now)
        или None, если машина не попадает в очередь.
    """
    rules = site.rules
    occupant = occ.occupant
    if occ.is_busy(now) and occupant is not None and occupant.unit_id == track.unit_id:
        # Занявшая станцию стоит в очереди отдельно, первой.
        return None
    if not rules.is_fresh(track.last_ts, now):
        return None

    entry = track.zone_entry
    # Машина «к разгрузке», проезжающая радиус станции, не приехала к ней (правило 3
    # занятия — о приехавших): её приезд считается по пути через разгрузку.
    if (
        entry is not None
        and entry.station_id == station.station_id
        and track.phase is not UnitPhase.TO_UNLOAD
    ):
        if occ.has_served(track.unit_id, entry.entered_at):
            # Отстояла своё и ещё не покинула радиус.
            return None
        # Ждёт в радиусе: приезд — момент входа; горизонт не важен (eta <= now).
        return entry.entered_at

    travel = _travel_seconds(track, station, site)
    if travel is None:  # AT_STATION у другой станции
        return None
    # Позиция наблюдалась в last_ts; приезд не раньше «сейчас».
    eta = max(now, track.last_ts + travel)
    # Горизонт: ровно на границе — ещё в очереди.
    return eta if eta <= now + rules.horizon_seconds else None


def schedule(
    arrivals: Iterable[tuple[str, int]],
    free_from: int | None,
    rules: Rules,
) -> tuple[QueueEntry, ...]:
    """Строит расписание обслуживания по моментам приезда.

    Машины обслуживаются в порядке (eta, unit_id); каждая начинает не раньше
    своего приезда и не раньше освобождения станции предыдущей.

    Args:
        arrivals: Пары (unit_id, eta).
        free_from: Когда станция освободится до первой машины; None — свободна.
        rules: Константы расчёта.

    Returns:
        Позиции очереди по порядку обслуживания.
    """
    entries: list[QueueEntry] = []
    free = free_from
    for unit_id, eta in sorted(arrivals, key=lambda a: (a[1], a[0])):
        start = eta if free is None else max(eta, free)
        free = start + rules.occupancy_seconds
        entries.append(
            QueueEntry(
                unit_id=unit_id,
                eta=eta,
                service_start=start,
                free_at=free,
                wait_seconds=start - eta,
            )
        )
    return tuple(entries)


def build_station_queue(
    occ: StationOccupancy,
    tracks: Iterable[UnitTrack],
    site: SiteConfig,
    now: int,
) -> StationQueue:
    """Строит очередь станции на момент расчёта.

    Занявшая станцию машина идёт первой (свежесть на неё не влияет), остальные —
    по расписанию после освобождения станции.

    Args:
        occ: Занятость станции.
        tracks: Машины, едущие к этой станции.
        site: Конфигурация площадки.
        now: Момент расчёта, секунды epoch.

    Returns:
        Очередь станции.

    Raises:
        KeyError: Станции occ.station_id нет в конфигурации.
    """
    station = site.station(occ.station_id)
    occupant = occ.occupant
    head: tuple[QueueEntry, ...] = ()
    if occupant is not None and occ.is_busy(now):
        head = (
            QueueEntry(
                unit_id=occupant.unit_id,
                eta=None,
                service_start=occupant.occupied_at,
                free_at=occupant.free_at,
                wait_seconds=0,
            ),
        )
    arrivals: list[tuple[str, int]] = []
    for track in tracks:
        eta = estimate_arrival(track, station, occ, site, now)
        if eta is not None:
            arrivals.append((track.unit_id, eta))
    # Даже освободившая станцию машина задаёт нижнюю границу начала следующей.
    free_from = occupant.free_at if occupant is not None else None
    return StationQueue(
        station_id=occ.station_id,
        at=now,
        entries=head + schedule(arrivals, free_from, site.rules),
    )


def wait_before(queue: StationQueue, eta: int, unit_id: str) -> int:
    """Оценивает ожидание машины, приезжающей к станции в момент eta.

    Станция освободится по free_at последней из машин, приезжающих раньше:
    занявшая станцию (eta None) всегда раньше, остальные — в порядке schedule,
    по (eta, unit_id). При равном eta раньше обслуживается машина с меньшим
    unit_id — иначе ожидание расходилось бы с расписанием очереди. Запись самой
    машины строгим сравнением не учитывается, а на расписание приезжающих раньше
    она не влияет, поэтому очередь передаётся как есть.

    Args:
        queue: Очередь станции.
        eta: Момент приезда машины к станции.
        unit_id: Идентификатор машины — порядок при равном eta.

    Returns:
        Ожидание в секундах, не меньше нуля; 0, если станция свободна.
    """
    free_moments = [
        entry.free_at
        for entry in queue.entries
        if entry.eta is None or (entry.eta, entry.unit_id) < (eta, unit_id)
    ]
    if not free_moments:
        return 0
    return max(0, max(free_moments) - eta)
