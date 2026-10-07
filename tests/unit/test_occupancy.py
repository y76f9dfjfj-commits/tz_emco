"""Тесты занятия и освобождения станции.

ТЗ, «Занятие и освобождение станции»:
1. Машина занимает станцию в момент первого сообщения, где она в состоянии «на станции»,
   а станция свободна.
2. Станция освобождается через 230 секунд после занятия либо в момент, когда занявшая её
   машина вышла из радиуса 50 м, — что наступит раньше.

Ключ визита — момент входа в радиус (zone_entry.entered_at): одна машина в одном
непрерывном пребывании в радиусе занимает станцию не более одного раза.
"""

from __future__ import annotations

from typing import Final

import pytest

from tests.sitekit import (
    FAR_POINT,
    S1_ID,
    S1_POINT,
    S2_ID,
    S2_POINT,
    S3_ID,
    UNLOAD_POINT,
    SiteRun,
    make_site,
    north_of,
    tm,
)
from vqueue.domain.model import Point, Rules, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy, update_occupancy
from vqueue.domain.unit_fsm import StationZoneEntry, UnitTrack

SITE: Final = make_site()
RULES: Final = SITE.rules
OCCUPANCY: Final = RULES.occupancy_seconds

T0: Final = 1_789_473_540  # 2026-09-15T11:59:00Z

UNIT_A: Final = "A"
UNIT_B: Final = "B"

NEAR_S1: Final = north_of(S1_POINT, 15.0)
NEAR_S1_OTHER: Final = north_of(S1_POINT, -20.0)
OUTSIDE_S1: Final = north_of(S1_POINT, 60.0)
NEAR_S2: Final = north_of(S2_POINT, 10.0)

STOPPED: Final = 0.0
MOVING: Final = 20.0


def _track(
    unit_id: str,
    ts: int,
    *,
    phase: UnitPhase,
    zone: StationZoneEntry | None = None,
    position: Point = NEAR_S1,
) -> UnitTrack:
    """Состояние машины после advance, собранное вручную.

    Для AT_STATION станция обслуживания — станция зоны zone.
    """
    return UnitTrack(
        unit_id=unit_id,
        last_ts=ts,
        position=position,
        phase=phase,
        station_id=zone.station_id if phase is UnitPhase.AT_STATION and zone else None,
        zone_entry=zone,
        in_unload_zone=False,
    )


def _at_s1(unit_id: str, ts: int, entered_at: int | None = None) -> UnitTrack:
    """Машина «на станции» S1 в визите entered_at (по умолчанию визит начат в ts)."""
    visit = StationZoneEntry(S1_ID, entered_at if entered_at is not None else ts)
    return _track(unit_id, ts, phase=UnitPhase.AT_STATION, zone=visit)


def _left(unit_id: str, ts: int) -> UnitTrack:
    """Машина вышла из радиуса станции — фаза «к разгрузке», вне всех зон."""
    return _track(unit_id, ts, phase=UnitPhase.TO_UNLOAD, position=OUTSIDE_S1)


NO_SERVED: Final[frozenset[tuple[str, int]]] = frozenset()


def _occupied(
    unit_id: str = UNIT_A,
    visit: int = T0,
    occupied_at: int = T0,
    free_at: int = T0 + OCCUPANCY,
    served: frozenset[tuple[str, int]] | None = None,
) -> StationOccupancy:
    """Станция S1 с записью о занявшей её машине.

    По умолчанию состояние согласованное: занявшая машина ещё в своём визите, поэтому
    визит (unit_id, visit) записан в served_visits.
    """
    return StationOccupancy(
        S1_ID,
        Occupant(unit_id, visit, occupied_at, free_at),
        served if served is not None else frozenset({(unit_id, visit)}),
    )


def test_occupancy_seconds_default_is_230_per_task() -> None:
    """ТЗ, «Пример»: станция занята 230 с — 200 обслуживания и 30 манёвра."""
    assert OCCUPANCY == 230


# ---------------------------------------------------------------------------
# is_busy
# ---------------------------------------------------------------------------


def test_is_busy_without_occupant_false() -> None:
    """Станция без занявшей машины свободна в любой момент."""
    occ = StationOccupancy(S1_ID)
    assert occ.occupant is None
    assert not occ.is_busy(0)
    assert not occ.is_busy(T0)


def test_is_busy_before_free_at_true() -> None:
    """Правило 2: до момента освобождения (at < free_at) станция занята."""
    occ = _occupied()
    assert occ.is_busy(T0)
    assert occ.is_busy(T0 + OCCUPANCY - 1)


def test_is_busy_at_free_at_false() -> None:
    """Правило 2: в сам момент free_at станция уже свободна (граница нестрогая)."""
    assert not _occupied().is_busy(T0 + OCCUPANCY)


def test_is_busy_after_free_at_false() -> None:
    """После free_at станция свободна, хотя запись о последней машине сохраняется."""
    occ = _occupied()
    assert not occ.is_busy(T0 + OCCUPANCY + 1)
    assert occ.occupant is not None


# ---------------------------------------------------------------------------
# Занятие (правило 1)
# ---------------------------------------------------------------------------


def test_occupy_first_at_station_message_free_station_occupies_at_ts() -> None:
    """Правило 1: первое сообщение «на станции» при свободной станции — занятие в ts, +230 с."""
    occ = update_occupancy(StationOccupancy(S1_ID), _at_s1(UNIT_A, T0 + 5, entered_at=T0), RULES)
    assert occ == StationOccupancy(
        S1_ID,
        Occupant(UNIT_A, T0, T0 + 5, T0 + 5 + OCCUPANCY),
        frozenset({(UNIT_A, T0)}),
    )


@pytest.mark.parametrize(
    ("service", "maneuver"),
    [(200, 30), (100, 20), (1, 1)],
)
def test_occupy_free_at_uses_rules_occupancy_seconds(service: int, maneuver: int) -> None:
    """Правило 2: срок занятия — обслуживание + манёвр из Rules, а не константа 230."""
    rules = Rules(service_seconds=service, maneuver_seconds=maneuver)
    occ = update_occupancy(StationOccupancy(S1_ID), _at_s1(UNIT_A, T0), rules)
    assert occ.occupant == Occupant(UNIT_A, T0, T0, T0 + service + maneuver)


def test_occupy_released_station_other_unit_occupies() -> None:
    """Правило 1: после освобождения станция занимается следующей машиной в ts её сообщения."""
    prev = _occupied(UNIT_A, free_at=T0 + 40)
    occ = update_occupancy(prev, _at_s1(UNIT_B, T0 + 50, entered_at=T0 + 10), RULES)
    assert occ.occupant == Occupant(UNIT_B, T0 + 10, T0 + 50, T0 + 50 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, T0), (UNIT_B, T0 + 10)}


def test_no_occupy_to_station_phase_inside_radius_unchanged() -> None:
    """Правило 1: машина в радиусе, но «к станции» (едет) — станцию не занимает."""
    prev = StationOccupancy(S1_ID)
    track = _track(UNIT_A, T0, phase=UnitPhase.TO_STATION, zone=StationZoneEntry(S1_ID, T0))
    assert update_occupancy(prev, track, RULES) is prev


def test_no_occupy_to_unload_phase_inside_radius_unchanged() -> None:
    """Правило 1: машина «к разгрузке» в радиусе станции станцию не занимает."""
    prev = StationOccupancy(S1_ID)
    track = _track(UNIT_A, T0, phase=UnitPhase.TO_UNLOAD, zone=StationZoneEntry(S1_ID, T0))
    assert update_occupancy(prev, track, RULES) is prev


def test_no_occupy_busy_by_other_unit_unchanged() -> None:
    """Правило 1: станция занята другой машиной — пришедшая ждёт, занятость не меняется."""
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _at_s1(UNIT_B, T0 + 30), RULES)
    assert occ is prev


def test_no_occupy_one_second_before_free_at_then_occupies_at_free_at() -> None:
    """Правило 1 и 2: за секунду до free_at станция занята, ровно в free_at — свободна."""
    prev = _occupied(UNIT_A)
    waiting_visit = T0 + 10
    still_busy = update_occupancy(
        prev, _at_s1(UNIT_B, T0 + OCCUPANCY - 1, entered_at=waiting_visit), RULES
    )
    assert still_busy is prev

    occ = update_occupancy(
        still_busy, _at_s1(UNIT_B, T0 + OCCUPANCY, entered_at=waiting_visit), RULES
    )
    assert occ.occupant == Occupant(
        UNIT_B, waiting_visit, T0 + OCCUPANCY, T0 + OCCUPANCY + OCCUPANCY
    )


def test_occupy_waiting_unit_after_early_release_at_next_message_ts() -> None:
    """Правила 1 и 2: занявшая уехала досрочно → ожидающая занимает в ts своего сообщения."""
    occ = _occupied(UNIT_A)
    occ = update_occupancy(occ, _at_s1(UNIT_B, T0 + 20, entered_at=T0 + 20), RULES)
    occ = update_occupancy(occ, _left(UNIT_A, T0 + 90), RULES)
    assert occ.occupant == Occupant(UNIT_A, T0, T0, T0 + 90)

    occ = update_occupancy(occ, _at_s1(UNIT_B, T0 + 95, entered_at=T0 + 20), RULES)
    assert occ.occupant == Occupant(UNIT_B, T0 + 20, T0 + 95, T0 + 95 + OCCUPANCY)


def test_occupy_waiting_unit_same_ts_as_release_occupies() -> None:
    """Правило 2: освобождение в t; сообщение ожидающей с тем же t уже застаёт станцию свободной."""
    occ = update_occupancy(_occupied(UNIT_A), _left(UNIT_A, T0 + 90), RULES)
    occ = update_occupancy(occ, _at_s1(UNIT_B, T0 + 90, entered_at=T0 + 20), RULES)
    assert occ.occupant == Occupant(UNIT_B, T0 + 20, T0 + 90, T0 + 90 + OCCUPANCY)


@pytest.mark.parametrize(
    "prev",
    [StationOccupancy(S1_ID), _occupied(UNIT_A), _occupied(UNIT_A, free_at=T0 + 1)],
    ids=["free", "busy-by-A", "released"],
)
def test_other_station_message_unchanged_same_object(prev: StationOccupancy) -> None:
    """Сообщение машины, стоящей на другой станции (S2), занятость S1 не меняет."""
    track = _track(
        UNIT_B,
        T0 + 100,
        phase=UnitPhase.AT_STATION,
        zone=StationZoneEntry(S2_ID, T0 + 100),
        position=NEAR_S2,
    )
    assert update_occupancy(prev, track, RULES) is prev


@pytest.mark.parametrize(
    "phase",
    [UnitPhase.TO_STATION, UnitPhase.TO_UNLOAD],
)
def test_unrelated_unit_away_from_station_unchanged_same_object(phase: UnitPhase) -> None:
    """Сообщение не занимавшей станцию машины вне радиуса ничего не меняет."""
    prev = _occupied(UNIT_A)
    track = _track(UNIT_B, T0 + 10, phase=phase, position=FAR_POINT)
    assert update_occupancy(prev, track, RULES) is prev


def test_occupant_message_while_still_at_station_unchanged_same_object() -> None:
    """Занявшая машина продолжает стоять в том же визите до free_at — занятость не меняется."""
    prev = _occupied(UNIT_A)
    assert update_occupancy(prev, _at_s1(UNIT_A, T0 + 100, entered_at=T0), RULES) is prev


# ---------------------------------------------------------------------------
# Освобождение по времени (правило 2)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dt", [OCCUPANCY, OCCUPANCY + 1, OCCUPANCY * 3])
def test_release_by_time_same_unit_same_visit_not_reoccupied(dt: int) -> None:
    """Правило 2: через 230 с станция свободна, хотя машина ещё стоит; тот же визит не занимает."""
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + dt, entered_at=T0), RULES)
    assert occ is prev
    assert not occ.is_busy(T0 + dt)


def test_release_by_time_same_unit_new_visit_occupies() -> None:
    """Правило 1: та же машина вышла и вернулась (новый визит) — занимает заново."""
    prev = _occupied(UNIT_A, free_at=T0 + 100)
    new_visit = T0 + 500
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 510, entered_at=new_visit), RULES)
    assert occ.occupant == Occupant(UNIT_A, new_visit, T0 + 510, T0 + 510 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, new_visit)}


def test_release_by_time_then_other_unit_occupies() -> None:
    """Правила 1–2: занявшая стоит дольше 230 с; другая машина занимает станцию после free_at."""
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 300, entered_at=T0), RULES)
    occ = update_occupancy(occ, _at_s1(UNIT_B, T0 + 301, entered_at=T0 + 100), RULES)
    assert occ.occupant == Occupant(UNIT_B, T0 + 100, T0 + 301, T0 + 301 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, T0), (UNIT_B, T0 + 100)}


# ---------------------------------------------------------------------------
# Досрочное освобождение (правило 2: выход из радиуса)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dt", [1, 90, OCCUPANCY - 1])
def test_early_release_occupant_left_radius_before_free_at_sets_free_at(dt: int) -> None:
    """Правило 2: занявшая вышла из радиуса в t < free_at → станция свободна с момента t."""
    occ = update_occupancy(_occupied(UNIT_A), _left(UNIT_A, T0 + dt), RULES)
    assert occ == _occupied(UNIT_A, free_at=T0 + dt, served=NO_SERVED)
    assert not occ.is_busy(T0 + dt)
    assert occ.is_busy(T0 + dt - 1)


@pytest.mark.parametrize("dt", [OCCUPANCY, OCCUPANCY + 1, OCCUPANCY + 500])
def test_early_release_occupant_left_at_or_after_free_at_unchanged(dt: int) -> None:
    """Правило 2: выход в момент free_at или позже — free_at не меняется, запись визита снята.

    Состояние изменилось (served_visits), поэтому возвращается новый объект.
    """
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _left(UNIT_A, T0 + dt), RULES)
    assert occ.occupant == prev.occupant
    assert occ.served_visits == NO_SERVED
    assert occ is not prev


def test_early_release_after_previous_early_release_keeps_first_free_at() -> None:
    """Правило 2: после досрочного освобождения последующие сообщения уехавшей его не сдвигают."""
    occ = update_occupancy(_occupied(UNIT_A), _left(UNIT_A, T0 + 50), RULES)
    released = update_occupancy(occ, _left(UNIT_A, T0 + 60), RULES)
    assert released is occ
    assert released == _occupied(UNIT_A, free_at=T0 + 50, served=NO_SERVED)


def test_early_release_occupant_to_station_phase_far_sets_free_at() -> None:
    """Правило 2: занявшая вне радиуса в любой фазе (здесь «к станции») — освобождение в t."""
    track = _track(UNIT_A, T0 + 70, phase=UnitPhase.TO_STATION, position=FAR_POINT)
    occ = update_occupancy(_occupied(UNIT_A), track, RULES)
    assert occ == _occupied(UNIT_A, free_at=T0 + 70, served=NO_SERVED)


def test_early_release_occupant_at_other_station_sets_free_at() -> None:
    """Правило 2: занявшая стоит уже на другой станции (S2) — S1 освобождается в t."""
    track = _track(
        UNIT_A,
        T0 + 70,
        phase=UnitPhase.AT_STATION,
        zone=StationZoneEntry(S2_ID, T0 + 70),
        position=NEAR_S2,
    )
    occ = update_occupancy(_occupied(UNIT_A), track, RULES)
    assert occ == _occupied(UNIT_A, free_at=T0 + 70, served=NO_SERVED)


def test_early_release_occupant_in_other_visit_releases_and_occupies_new_visit() -> None:
    """Правила 1–2: машина на S1 уже в другом визите (выход из радиуса не пришёл).

    Старый визит освобождается в t (выход из радиуса случился не позже t), и тем же
    сообщением новый визит занимает свободную станцию: Occupant(A, V2, t, t + 230).
    """
    occ = update_occupancy(_occupied(UNIT_A), _at_s1(UNIT_A, T0 + 70, entered_at=T0 + 60), RULES)
    assert occ.occupant == Occupant(UNIT_A, T0 + 60, T0 + 70, T0 + 70 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, T0 + 60)}


def test_early_release_occupant_in_other_visit_busy_after_time_release_occupies() -> None:
    """Правила 1–2: то же, но старый визит уже освобождён по времени — новый занимает в t."""
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 400, entered_at=T0 + 390), RULES)
    assert occ.occupant == Occupant(UNIT_A, T0 + 390, T0 + 400, T0 + 400 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, T0 + 390)}


def test_early_release_occupant_new_visit_while_other_unit_busy_only_releases() -> None:
    """Правило 1: в новом визите машина не занимает станцию, занятую другой машиной."""
    prev = _occupied(UNIT_B, visit=T0 + 10, occupied_at=T0 + 20, free_at=T0 + 20 + OCCUPANCY)
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 70, entered_at=T0 + 60), RULES)
    assert occ is prev


def test_early_release_occupant_maneuvering_in_radius_not_released() -> None:
    """Правило 2: занявшая трогается в радиусе (остаётся «на станции») — не освобождение."""
    prev = _occupied(UNIT_A)
    track = _at_s1(UNIT_A, T0 + 30, entered_at=T0)
    assert update_occupancy(prev, track, RULES) is prev


def test_occupy_same_visit_after_other_unit_served_not_reoccupied() -> None:
    """Правило 1: занятие — по первому сообщению «на станции» визита; повторно визит не занимает.

    A заняла станцию и стоит дольше 230 с; B заняла после free_at и уехала. A всё ещё стоит
    в том же визите — её первое сообщение «на станции» уже было, станция остаётся свободной.
    """
    occ = _occupied(UNIT_A)
    occ = update_occupancy(occ, _at_s1(UNIT_B, T0 + OCCUPANCY, entered_at=T0 + 100), RULES)
    assert occ.occupant is not None
    assert occ.occupant.unit_id == UNIT_B
    occ = update_occupancy(occ, _left(UNIT_B, T0 + OCCUPANCY + 1), RULES)
    assert occ.served_visits == {(UNIT_A, T0)}

    after_b = update_occupancy(occ, _at_s1(UNIT_A, T0 + OCCUPANCY + 2, entered_at=T0), RULES)
    assert after_b is occ
    assert not after_b.is_busy(T0 + OCCUPANCY + 2)


def test_early_release_other_unit_leaving_unchanged() -> None:
    """Правило 2: из радиуса выходит не занявшая машина — занятость не меняется."""
    prev = _occupied(UNIT_A)
    assert update_occupancy(prev, _left(UNIT_B, T0 + 30), RULES) is prev


# ---------------------------------------------------------------------------
# Учёт обслуженных визитов (served_visits): визит занимает станцию не более раза
# ---------------------------------------------------------------------------

UNIT_C: Final = "C"


def test_served_visit_removed_when_occupant_leaves_after_time_release() -> None:
    """Занявшая уехала после 230 с: free_at прежний, запись визита снята, объект новый."""
    prev = _occupied(UNIT_A)
    occ = update_occupancy(prev, _left(UNIT_A, T0 + OCCUPANCY + 30), RULES)
    assert occ is not prev
    assert occ == _occupied(UNIT_A, served=NO_SERVED)


def test_served_visit_removed_for_former_occupant_not_last() -> None:
    """Запись визита машины, занимавшей станцию раньше последней, снимается при её отъезде."""
    prev = _occupied(UNIT_B, visit=T0 + 100, served=frozenset({(UNIT_A, T0), (UNIT_B, T0 + 100)}))
    occ = update_occupancy(prev, _left(UNIT_A, T0 + 150), RULES)
    assert occ.occupant == prev.occupant
    assert occ.served_visits == {(UNIT_B, T0 + 100)}


def test_served_visit_stale_record_replaced_by_current_visit() -> None:
    """Устаревшая запись машины (другой визит на той же станции) снимается, новый визит занимает."""
    prev = _occupied(UNIT_B, visit=T0 + 100, free_at=T0 + 120)
    prev = StationOccupancy(S1_ID, prev.occupant, frozenset({(UNIT_A, T0)}))
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 200, entered_at=T0 + 190), RULES)
    assert occ.occupant == Occupant(UNIT_A, T0 + 190, T0 + 200, T0 + 200 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_A, T0 + 190)}


def test_served_visit_stale_record_removed_while_station_busy() -> None:
    """Запись другого визита машины снимается, даже если занять станцию сейчас нельзя."""
    prev = _occupied(UNIT_B, visit=T0 + 100, served=frozenset({(UNIT_A, T0), (UNIT_B, T0 + 100)}))
    occ = update_occupancy(prev, _at_s1(UNIT_A, T0 + 150, entered_at=T0 + 140), RULES)
    assert occ.occupant == prev.occupant
    assert occ.served_visits == {(UNIT_B, T0 + 100)}


SERVED_AC: Final = frozenset({(UNIT_A, T0), (UNIT_C, T0 + 5)})


@pytest.mark.parametrize(
    "track",
    [
        _left(UNIT_B, T0 + 30),
        _track(UNIT_B, T0 + 30, phase=UnitPhase.TO_STATION, position=FAR_POINT),
        _at_s1(UNIT_B, T0 + 30),
        _track(
            UNIT_B,
            T0 + 30,
            phase=UnitPhase.AT_STATION,
            zone=StationZoneEntry(S2_ID, T0 + 30),
            position=NEAR_S2,
        ),
    ],
    ids=["left", "far", "waiting-at-s1", "at-s2"],
)
def test_served_visits_foreign_unit_message_keeps_records_same_object(track: UnitTrack) -> None:
    """Сообщение машины без записей не трогает чужие записи served_visits."""
    prev = _occupied(UNIT_A, served=SERVED_AC)
    assert update_occupancy(prev, track, RULES) is prev


def test_served_visits_new_occupant_keeps_foreign_records() -> None:
    """Новое занятие добавляет визит занявшей, не трогая записи других машин."""
    prev = _occupied(UNIT_A, free_at=T0 + 50, served=SERVED_AC)
    occ = update_occupancy(prev, _at_s1(UNIT_B, T0 + 60, entered_at=T0 + 40), RULES)
    assert occ.occupant == Occupant(UNIT_B, T0 + 40, T0 + 60, T0 + 60 + OCCUPANCY)
    assert occ.served_visits == SERVED_AC | {(UNIT_B, T0 + 40)}


def test_served_visits_early_release_keeps_foreign_records() -> None:
    """Досрочное освобождение снимает только запись уехавшей занявшей."""
    prev = _occupied(UNIT_A, served=SERVED_AC)
    occ = update_occupancy(prev, _left(UNIT_A, T0 + 30), RULES)
    assert occ == _occupied(UNIT_A, free_at=T0 + 30, served=frozenset({(UNIT_C, T0 + 5)}))


def test_has_served_empty_station_false() -> None:
    """has_served: у свободной станции без истории обслуженных визитов нет."""
    assert not StationOccupancy(S1_ID).has_served(UNIT_A, T0)


def test_has_served_after_occupy_true_only_for_that_unit_and_visit() -> None:
    """has_served: занявший визит отмечен; другой визит той же машины и чужая машина — нет."""
    occ = update_occupancy(StationOccupancy(S1_ID), _at_s1(UNIT_A, T0 + 5, entered_at=T0), RULES)
    assert occ.has_served(UNIT_A, T0)
    assert not occ.has_served(UNIT_A, T0 + 5)
    assert not occ.has_served(UNIT_B, T0)


def test_has_served_after_unit_left_false() -> None:
    """has_served: после выхода машины из радиуса её визит больше не отмечен."""
    occ = update_occupancy(_occupied(UNIT_A), _left(UNIT_A, T0 + 30), RULES)
    assert not occ.has_served(UNIT_A, T0)


def test_has_served_matches_served_visits() -> None:
    """has_served согласован с served_visits, включая записи не последней занявшей."""
    occ = _occupied(UNIT_A, served=SERVED_AC)
    assert occ.has_served(UNIT_A, T0)
    assert occ.has_served(UNIT_C, T0 + 5)
    assert not occ.has_served(UNIT_C, T0)


# ---------------------------------------------------------------------------
# Сквозные сценарии: телеметрия → SiteRun (advance + update_occupancy всех станций)
# ---------------------------------------------------------------------------


def _feed(
    run: SiteRun, unit_id: str, ts: int, position: Point, speed: float = STOPPED
) -> UnitTrack:
    """Подаёт сообщение машины; сообщение должно быть учтено (не опоздание и не дубль)."""
    track = run.feed(tm(ts, position, speed, unit_id))
    assert track is not None
    return track


def test_scenario_two_units_a_leaves_early_b_occupies_at_its_next_message() -> None:
    """Правила 1–2: A занимает, B ждёт в радиусе, A уезжает раньше 230 с, B занимает позже."""
    run = SiteRun()
    _feed(run, UNIT_A, T0 - 30, FAR_POINT, MOVING)
    _feed(run, UNIT_A, T0 - 5, NEAR_S1, MOVING)  # вошла в радиус на ходу — визит с T0 - 5
    _feed(run, UNIT_A, T0, NEAR_S1)
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0 - 5, T0, T0 + OCCUPANCY)

    b_track = _feed(run, UNIT_B, T0 + 40, NEAR_S1_OTHER)
    assert b_track.phase is UnitPhase.AT_STATION
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0 - 5, T0, T0 + OCCUPANCY)

    _feed(run, UNIT_A, T0 + 150, NEAR_S1, MOVING)  # трогается в радиусе — ещё «на станции»
    _feed(run, UNIT_B, T0 + 151, NEAR_S1_OTHER)
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0 - 5, T0, T0 + OCCUPANCY)

    a_track = _feed(run, UNIT_A, T0 + 160, OUTSIDE_S1, MOVING)
    assert a_track.phase is UnitPhase.TO_UNLOAD
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0 - 5, T0, T0 + 160)
    assert not run.occupancy[S1_ID].is_busy(T0 + 160)

    _feed(run, UNIT_B, T0 + 161, NEAR_S1_OTHER)
    occ = run.occupancy[S1_ID]
    assert occ.occupant == Occupant(UNIT_B, T0 + 40, T0 + 161, T0 + 161 + OCCUPANCY)
    assert occ.served_visits == {(UNIT_B, T0 + 40)}


def test_scenario_waiting_unit_occupies_after_time_release_while_occupant_stays() -> None:
    """Правила 1–2: A стоит дольше 230 с; B ждёт и занимает первым сообщением после free_at."""
    run = SiteRun()
    a_occupant = Occupant(UNIT_A, T0, T0, T0 + OCCUPANCY)
    _feed(run, UNIT_A, T0, NEAR_S1)
    _feed(run, UNIT_B, T0 + 10, NEAR_S1_OTHER)
    _feed(run, UNIT_B, T0 + OCCUPANCY - 1, NEAR_S1_OTHER)
    assert run.occupancy[S1_ID].occupant == a_occupant

    _feed(run, UNIT_A, T0 + OCCUPANCY + 5, NEAR_S1)  # A всё ещё стоит — повторно не занимает
    assert run.occupancy[S1_ID].occupant == a_occupant

    _feed(run, UNIT_B, T0 + OCCUPANCY + 7, NEAR_S1_OTHER)
    assert run.occupancy[S1_ID].occupant == Occupant(
        UNIT_B, T0 + 10, T0 + OCCUPANCY + 7, T0 + OCCUPANCY + 7 + OCCUPANCY
    )


def test_scenario_unit_moving_through_radius_does_not_occupy() -> None:
    """Правило 1: машина проезжает радиус со скоростью ≥ 1 км/ч — станция остаётся свободной."""
    run = SiteRun()
    _feed(run, UNIT_A, T0, FAR_POINT, MOVING)
    for i, speed in enumerate((MOVING, 1.0, 5.0)):
        track = _feed(run, UNIT_A, T0 + 10 + i, NEAR_S1, speed)
        assert track.phase is UnitPhase.TO_STATION
    _feed(run, UNIT_A, T0 + 20, OUTSIDE_S1, MOVING)
    assert run.occupancy[S1_ID] == StationOccupancy(S1_ID)


def test_scenario_same_unit_returns_after_unload_occupies_again() -> None:
    """Правило 1: полный цикл — станция, разгрузка, возврат — новое занятие в новом визите."""
    run = SiteRun()
    _feed(run, UNIT_A, T0, NEAR_S1)
    _feed(run, UNIT_A, T0 + 200, OUTSIDE_S1, MOVING)
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0, T0, T0 + 200)

    _feed(run, UNIT_A, T0 + 500, UNLOAD_POINT)
    _feed(run, UNIT_A, T0 + 700, FAR_POINT, MOVING)
    _feed(run, UNIT_A, T0 + 900, NEAR_S1, MOVING)
    _feed(run, UNIT_A, T0 + 905, NEAR_S1)
    assert run.occupancy[S1_ID].occupant == Occupant(
        UNIT_A, T0 + 900, T0 + 905, T0 + 905 + OCCUPANCY
    )


def test_scenario_units_at_different_stations_occupy_independently() -> None:
    """Сообщения машины на S2 не меняют занятость S1 и наоборот; S3 остаётся свободной."""
    run = SiteRun()
    _feed(run, UNIT_A, T0, NEAR_S2)
    _feed(run, UNIT_B, T0 + 1, NEAR_S1)
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_B, T0 + 1, T0 + 1, T0 + 1 + OCCUPANCY)
    assert run.occupancy[S2_ID].occupant == Occupant(UNIT_A, T0, T0, T0 + OCCUPANCY)
    assert run.occupancy[S3_ID] == StationOccupancy(S3_ID)


def test_scenario_late_and_duplicate_messages_ignored() -> None:
    """ТЗ, «Порядок и дубли»: дубль и опоздание занявшей не освобождают станцию."""
    run = SiteRun()
    _feed(run, UNIT_A, T0, NEAR_S1)
    assert run.feed(tm(T0, OUTSIDE_S1, MOVING, UNIT_A)) is None
    assert run.feed(tm(T0 - 10, OUTSIDE_S1, MOVING, UNIT_A)) is None
    assert run.occupancy[S1_ID].occupant == Occupant(UNIT_A, T0, T0, T0 + OCCUPANCY)
