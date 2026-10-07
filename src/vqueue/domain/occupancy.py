"""Занятие и освобождение станции по состоянию машин."""

from __future__ import annotations

from dataclasses import dataclass, replace

from vqueue.domain.model import Rules, UnitPhase
from vqueue.domain.unit_fsm import UnitTrack


@dataclass(frozen=True, slots=True)
class Occupant:
    """Машина, занявшая станцию.

    Attributes:
        unit_id: Идентификатор машины.
        visit_entered_at: zone_entry.entered_at визита, в котором машина заняла
            станцию (ключ визита).
        occupied_at: Момент занятия (ts сообщения).
        free_at: Момент освобождения: occupied_at + occupancy_seconds или раньше
            при выходе машины из радиуса.
    """

    unit_id: str
    visit_entered_at: int
    occupied_at: int
    free_at: int


@dataclass(frozen=True, slots=True)
class StationOccupancy:
    """Состояние занятости одной станции.

    Attributes:
        station_id: Идентификатор станции.
        occupant: Последняя занявшая машина; запись хранится и после освобождения.
        served_visits: Визиты (unit_id, entered_at), в которых машина уже занимала
            станцию и всё ещё остаётся в её радиусе. Визит занимает станцию не более
            одного раза: машина, отстоявшая своё и продолжающая стоять, не занимает
            станцию повторно, даже если между ней и новым занятием станцию занимала
            другая машина.
    """

    station_id: str
    occupant: Occupant | None = None
    served_visits: frozenset[tuple[str, int]] = frozenset()

    def is_busy(self, at: int) -> bool:
        """Проверяет, занята ли станция в момент времени.

        Args:
            at: Момент времени, секунды epoch.

        Returns:
            True, если станция занята; в момент free_at станция уже свободна.
        """
        return self.occupant is not None and at < self.occupant.free_at

    def has_served(self, unit_id: str, visit_entered_at: int) -> bool:
        """Проверяет, занимала ли машина станцию в этом визите (и всё ещё в нём).

        Args:
            unit_id: Идентификатор машины.
            visit_entered_at: Ключ визита — момент входа в радиус станции.

        Returns:
            True, если визит уже обслужен и повторно станцию не займёт.
        """
        return (unit_id, visit_entered_at) in self.served_visits


def _visit_here(occ: StationOccupancy, track: UnitTrack) -> int | None:
    """Возвращает ключ визита, если машина сейчас на этой станции, иначе None."""
    if (
        track.phase is UnitPhase.AT_STATION
        and track.station_id == occ.station_id
        and track.zone_entry is not None
    ):
        return track.zone_entry.entered_at
    return None


def update_occupancy(occ: StationOccupancy, track: UnitTrack, rules: Rules) -> StationOccupancy:
    """Применяет новое состояние машины к занятости станции.

    Сначала проверяется досрочное освобождение (занявшая машина покинула
    станцию или визит), затем — в том же сообщении — занятие свободной станции.
    Сообщения машин, не относящихся к станции, ничего не меняют.

    Функцию нужно вызывать для каждой станции на каждое учтённое сообщение
    машины: досрочное освобождение и забывание покинутых визитов срабатывают
    как раз на сообщениях, где машины на этой станции уже нет.

    Args:
        occ: Текущая занятость станции.
        track: Состояние машины сразу после применения сообщения; t = track.last_ts.
        rules: Константы расчёта.

    Returns:
        Новая занятость станции либо тот же объект occ, если изменений нет.
    """
    t = track.last_ts
    unit = track.unit_id
    visit = _visit_here(occ, track)
    occupant = occ.occupant
    # Визиты этой машины, которые она уже покинула, забываем.
    served = frozenset(
        (u, entered) for u, entered in occ.served_visits if u != unit or entered == visit
    )

    if (
        occupant is not None
        and occupant.unit_id == unit
        and visit != occupant.visit_entered_at
        and t < occupant.free_at
    ):
        # Досрочное освобождение: занявшая машина больше не на станции в своём визите.
        occupant = replace(occupant, free_at=t)
    # Занятие проверяется по уже обновлённой занятости: машина, освободившая станцию
    # старым визитом, может занять её новым визитом в том же сообщении (правило 1).
    busy = occupant is not None and t < occupant.free_at
    if visit is not None and not busy and (unit, visit) not in served:
        occupant = Occupant(
            unit_id=unit,
            visit_entered_at=visit,
            occupied_at=t,
            free_at=t + rules.occupancy_seconds,
        )
        served |= {(unit, visit)}

    if occupant is occ.occupant and served == occ.served_visits:
        return occ
    return replace(occ, occupant=occupant, served_visits=served)
