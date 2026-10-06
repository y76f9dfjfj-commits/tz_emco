"""Тесты автомата фаз машины по телеметрии.

ТЗ, таблица состояний:
- «на станции» — скорость меньше 1 км/ч и расстояние до станции меньше 50 м;
- «к разгрузке» — вышла из радиуса 50 м станции после обслуживания;
- «к станции» — вышла из радиуса 50 м точки разгрузки; это же состояние по умолчанию
  для машины, чьё первое сообщение не попадает под «на станции».
Правило станции 3: приезд машины в радиус считается наступившим в момент входа в радиус —
поэтому момент входа (zone_entry.entered_at) сохраняется на всё непрерывное пребывание.
"""

from __future__ import annotations

import pytest

from tests.sitekit import (
    FAR_POINT,
    S1_ID,
    S1_POINT,
    S2_ID,
    S2_POINT,
    UNIT_ID,
    UNLOAD_POINT,
    make_site,
    north_of,
    tm,
)
from vqueue.domain.geo import distance_m
from vqueue.domain.model import Point, Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.unit_fsm import StationZoneEntry, UnitTrack, advance, station_in_zone

SITE = make_site()
RADIUS = SITE.rules.zone_radius_m

T0 = 1_789_473_540  # 2026-09-15T11:59:00Z — первое сообщение T1 «на станции» в примере ТЗ.

NEAR_S1 = north_of(S1_POINT, 15.0)
"""Машина в 15 м от S1 — как T1 в примере ТЗ."""
INSIDE_S1_EDGE = north_of(S1_POINT, 49.0)
OUTSIDE_S1 = north_of(S1_POINT, 51.0)
NEAR_UNLOAD = north_of(UNLOAD_POINT, 10.0)
OUTSIDE_UNLOAD = north_of(UNLOAD_POINT, 60.0)

MOVING = 20.0
STOPPED = 0.0


def _track(
    *,
    phase: UnitPhase,
    position: Point = FAR_POINT,
    station_id: str | None = None,
    zone_entry: StationZoneEntry | None = None,
    in_unload_zone: bool = False,
) -> UnitTrack:
    """Состояние машины T1 с last_ts = T0 и заданными полями (старт из нужной фазы)."""
    return UnitTrack(
        unit_id=UNIT_ID,
        last_ts=T0,
        position=position,
        phase=phase,
        station_id=station_id,
        zone_entry=zone_entry,
        in_unload_zone=in_unload_zone,
    )


def _exact_radius_site(center: Point, offset_m: float = 50.0) -> tuple[SiteConfig, Point]:
    """Площадка, у которой радиус зоны ровно равен расстоянию от center до возвращаемой точки."""
    edge = north_of(center, offset_m)
    radius = distance_m(center, edge)
    return make_site(Rules(zone_radius_m=radius)), edge


def test_site_geometry_sanity() -> None:
    """Опорные точки площадки лежат на ожидаемых расстояниях (проверка через distance_m)."""
    assert distance_m(S1_POINT, NEAR_S1) == pytest.approx(15.0, abs=1e-3)
    assert distance_m(S1_POINT, INSIDE_S1_EDGE) < RADIUS
    assert distance_m(S1_POINT, OUTSIDE_S1) > RADIUS
    assert distance_m(UNLOAD_POINT, NEAR_UNLOAD) < RADIUS
    assert distance_m(UNLOAD_POINT, OUTSIDE_UNLOAD) > RADIUS
    for station in SITE.stations:
        assert distance_m(station.location, FAR_POINT) > 1_000
        assert distance_m(station.location, UNLOAD_POINT) > 1_000


# --- station_in_zone --------------------------------------------------------


def test_station_in_zone_at_station_center_returns_station() -> None:
    """Точка станции — в её радиусе."""
    assert station_in_zone(S1_POINT, SITE) == S1_ID


def test_station_in_zone_inside_radius_returns_station() -> None:
    """15 м и 49 м от S1 — меньше 50 м, машина в радиусе S1."""
    assert station_in_zone(NEAR_S1, SITE) == S1_ID
    assert station_in_zone(INSIDE_S1_EDGE, SITE) == S1_ID


def test_station_in_zone_exactly_radius_returns_none() -> None:
    """Ровно на радиусе — не в зоне («меньше 50 м» — строгое неравенство)."""
    site, edge = _exact_radius_site(S1_POINT)
    assert distance_m(S1_POINT, edge) == site.rules.zone_radius_m
    assert station_in_zone(edge, site) is None


def test_station_in_zone_outside_radius_returns_none() -> None:
    """51 м от S1 — вне радиуса."""
    assert station_in_zone(OUTSIDE_S1, SITE) is None


def test_station_in_zone_far_point_returns_none() -> None:
    """Точка в пути — ни в одном радиусе."""
    assert station_in_zone(FAR_POINT, SITE) is None


def test_station_in_zone_unload_point_is_not_station() -> None:
    """Точка разгрузки — не станция."""
    assert station_in_zone(UNLOAD_POINT, SITE) is None


def test_station_in_zone_picks_matching_station() -> None:
    """Возвращается та станция, в радиусе которой машина, а не первая в списке."""
    assert station_in_zone(north_of(S2_POINT, -20.0), SITE) == S2_ID


def test_station_in_zone_overlapping_radii_first_station_in_order() -> None:
    """Пересекающиеся радиусы: выбирается первая по порядку site.stations (контракт)."""
    a = S1_POINT
    b = north_of(S1_POINT, 30.0)
    between = north_of(S1_POINT, 15.0)
    site_ab = SiteConfig(
        stations=(Station("A", a), Station("B", b)), unload_point=UNLOAD_POINT, assignments={}
    )
    site_ba = SiteConfig(
        stations=(Station("B", b), Station("A", a)), unload_point=UNLOAD_POINT, assignments={}
    )
    assert station_in_zone(between, site_ab) == "A"
    assert station_in_zone(between, site_ba) == "B"


# --- первое сообщение -------------------------------------------------------


def test_advance_first_message_stopped_in_radius_at_station() -> None:
    """Пример ТЗ: T1 стоит в 15 м от S1, первое такое сообщение в 11:59:00 — на станции.

    Момент входа в радиус — ts этого сообщения.
    """
    track = advance(None, tm(T0, NEAR_S1, STOPPED), SITE)
    assert track == UnitTrack(
        unit_id=UNIT_ID,
        last_ts=T0,
        position=NEAR_S1,
        phase=UnitPhase.AT_STATION,
        station_id=S1_ID,
        zone_entry=StationZoneEntry(S1_ID, T0),
        in_unload_zone=False,
    )


def test_advance_first_message_outside_radius_to_station() -> None:
    """Первое сообщение не «на станции» — состояние по умолчанию «к станции»."""
    track = advance(None, tm(T0, FAR_POINT, MOVING), SITE)
    assert track == UnitTrack(
        unit_id=UNIT_ID,
        last_ts=T0,
        position=FAR_POINT,
        phase=UnitPhase.TO_STATION,
        station_id=None,
        zone_entry=None,
        in_unload_zone=False,
    )


def test_advance_first_message_stopped_outside_radius_to_station() -> None:
    """Стоит, но вне радиуса станции — «к станции»."""
    track = advance(None, tm(T0, OUTSIDE_S1, STOPPED), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.station_id is None
    assert track.zone_entry is None


def test_advance_first_message_moving_in_radius_to_station_with_zone_entry() -> None:
    """Движется в радиусе станции — «к станции», но момент входа в радиус зафиксирован."""
    track = advance(None, tm(T0, NEAR_S1, MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.station_id is None
    assert track.zone_entry == StationZoneEntry(S1_ID, T0)


def test_advance_first_message_stopped_at_exact_radius_to_station() -> None:
    """Стоит ровно на радиусе станции — не «на станции» (строго меньше 50 м)."""
    site, edge = _exact_radius_site(S1_POINT)
    track = advance(None, tm(T0, edge, STOPPED), site)
    assert track.phase is UnitPhase.TO_STATION
    assert track.zone_entry is None


def test_advance_first_message_in_unload_zone_to_station() -> None:
    """Первое сообщение у точки разгрузки — «к станции», признак зоны разгрузки выставлен."""
    track = advance(None, tm(T0, NEAR_UNLOAD, STOPPED), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.in_unload_zone is True
    assert track.zone_entry is None


def test_advance_first_message_at_other_station_uses_that_station() -> None:
    """Стоит у S2 (не своей станции) — «на станции» S2."""
    pos = north_of(S2_POINT, 10.0)
    track = advance(None, tm(T0, pos, STOPPED), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S2_ID
    assert track.zone_entry == StationZoneEntry(S2_ID, T0)


# --- граница скорости -------------------------------------------------------


def test_advance_speed_exactly_threshold_is_not_stopped() -> None:
    """Скорость ровно 1 км/ч — не «стоит» (порог: строго меньше 1 км/ч)."""
    track = advance(None, tm(T0, NEAR_S1, 1.0), SITE)
    assert track.phase is UnitPhase.TO_STATION


def test_advance_speed_just_below_threshold_is_stopped() -> None:
    """Скорость 0.99 км/ч — «стоит»."""
    track = advance(None, tm(T0, NEAR_S1, 0.99), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S1_ID


def test_advance_speed_threshold_from_rules() -> None:
    """Порог берётся из правил площадки, а не зашит в код."""
    site = make_site(Rules(stopped_speed_kmh=5.0))
    assert advance(None, tm(T0, NEAR_S1, 4.9), site).phase is UnitPhase.AT_STATION
    assert advance(None, tm(T0, NEAR_S1, 5.0), site).phase is UnitPhase.TO_STATION


# --- AT_STATION -------------------------------------------------------------


def _at_s1() -> UnitTrack:
    """Машина на станции S1 с момента T0."""
    return _track(
        phase=UnitPhase.AT_STATION,
        position=NEAR_S1,
        station_id=S1_ID,
        zone_entry=StationZoneEntry(S1_ID, T0),
    )


def test_advance_at_station_maneuver_in_radius_stays_at_station() -> None:
    """Манёвр в радиусе со скоростью ≥ 1 км/ч — машина всё ещё на станции."""
    track = advance(_at_s1(), tm(T0 + 10, INSIDE_S1_EDGE, MOVING), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S1_ID
    assert track.zone_entry == StationZoneEntry(S1_ID, T0)


def test_advance_at_station_speed_threshold_in_radius_stays_at_station() -> None:
    """Ровно 1 км/ч в радиусе своей станции — остаётся на станции."""
    track = advance(_at_s1(), tm(T0 + 1, NEAR_S1, 1.0), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S1_ID


def test_advance_at_station_stopped_again_keeps_entry_moment() -> None:
    """Повторная остановка в радиусе не сдвигает момент входа."""
    track = advance(_at_s1(), tm(T0 + 100, NEAR_S1, STOPPED), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.zone_entry == StationZoneEntry(S1_ID, T0)


def test_advance_at_station_exit_radius_to_unload() -> None:
    """Вышла из радиуса 50 м станции — «к разгрузке», станция и зона сброшены."""
    track = advance(_at_s1(), tm(T0 + 230, OUTSIDE_S1, MOVING), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.station_id is None
    assert track.zone_entry is None
    assert track.in_unload_zone is False


def test_advance_at_station_exit_radius_stopped_to_unload() -> None:
    """Вне радиуса, даже стоя, — уже «к разгрузке»."""
    track = advance(_at_s1(), tm(T0 + 230, OUTSIDE_S1, STOPPED), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD


def test_advance_at_station_exactly_radius_to_unload() -> None:
    """Ровно на радиусе — уже вне зоны, «к разгрузке»."""
    site, edge = _exact_radius_site(S1_POINT)
    track = advance(_at_s1(), tm(T0 + 230, edge, MOVING), site)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.zone_entry is None


def test_advance_at_station_moving_into_other_station_radius_to_unload() -> None:
    """Контракт: с AT_STATION S1 в радиус S2 в движении — S1 покинута, «к разгрузке».

    Момент входа в радиус S2 при этом фиксируется.
    """
    pos = north_of(S2_POINT, 10.0)
    track = advance(_at_s1(), tm(T0 + 300, pos, MOVING), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.station_id is None
    assert track.zone_entry == StationZoneEntry(S2_ID, T0 + 300)


def test_advance_at_station_stopped_at_other_station_at_that_station() -> None:
    """Контракт: «в зоне и стоит» → «на станции» той станции, где стоит, из любой фазы."""
    pos = north_of(S2_POINT, 10.0)
    track = advance(_at_s1(), tm(T0 + 300, pos, STOPPED), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S2_ID
    assert track.zone_entry == StationZoneEntry(S2_ID, T0 + 300)


# --- TO_UNLOAD --------------------------------------------------------------


def _to_unload(in_unload_zone: bool = False, position: Point = FAR_POINT) -> UnitTrack:
    """Машина «к разгрузке»."""
    return _track(phase=UnitPhase.TO_UNLOAD, position=position, in_unload_zone=in_unload_zone)


def test_advance_to_unload_enters_unload_radius_stays_to_unload() -> None:
    """Вход в радиус точки разгрузки — всё ещё «к разгрузке», признак зоны выставлен."""
    track = advance(_to_unload(), tm(T0 + 1, NEAR_UNLOAD, MOVING), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.in_unload_zone is True
    assert track.station_id is None
    assert track.zone_entry is None


def test_advance_to_unload_stopped_in_unload_radius_stays_to_unload() -> None:
    """Стоит на разгрузке — «к разгрузке»."""
    prev = _to_unload(in_unload_zone=True, position=NEAR_UNLOAD)
    track = advance(prev, tm(T0 + 1, UNLOAD_POINT, STOPPED), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.in_unload_zone is True


def test_advance_to_unload_exits_unload_radius_to_station() -> None:
    """Вышла из радиуса 50 м точки разгрузки — «к станции»."""
    prev = _to_unload(in_unload_zone=True, position=NEAR_UNLOAD)
    track = advance(prev, tm(T0 + 1, OUTSIDE_UNLOAD, MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.in_unload_zone is False
    assert track.station_id is None


def test_advance_to_unload_exactly_unload_radius_is_outside() -> None:
    """Ровно на радиусе точки разгрузки — вне зоны: выход из неё, «к станции»."""
    site, edge = _exact_radius_site(UNLOAD_POINT)
    prev = _to_unload(in_unload_zone=True, position=NEAR_UNLOAD)
    track = advance(prev, tm(T0 + 1, edge, MOVING), site)
    assert track.in_unload_zone is False
    assert track.phase is UnitPhase.TO_STATION


def test_advance_to_unload_never_enters_unload_zone_stays_to_unload() -> None:
    """Без захода в радиус разгрузки машина остаётся «к разгрузке»."""
    track = _to_unload()
    for i, pos in enumerate((FAR_POINT, OUTSIDE_UNLOAD, north_of(FAR_POINT, 100.0)), start=1):
        track = advance(track, tm(T0 + i, pos, MOVING), SITE)
        assert track.phase is UnitPhase.TO_UNLOAD
        assert track.in_unload_zone is False


def test_advance_to_unload_passing_station_radius_stays_to_unload() -> None:
    """Проезд через радиус станции без остановки не меняет «к разгрузке»; вход фиксируется."""
    track = advance(_to_unload(), tm(T0 + 1, NEAR_S1, MOVING), SITE)
    assert track.phase is UnitPhase.TO_UNLOAD
    assert track.station_id is None
    assert track.zone_entry == StationZoneEntry(S1_ID, T0 + 1)


def test_advance_to_unload_stopped_in_station_radius_at_station() -> None:
    """Контракт: остановка в радиусе станции — «на станции» из любой фазы."""
    track = advance(_to_unload(), tm(T0 + 1, NEAR_S1, STOPPED), SITE)
    assert track.phase is UnitPhase.AT_STATION
    assert track.station_id == S1_ID


# --- TO_STATION -------------------------------------------------------------


def _to_station(position: Point = FAR_POINT) -> UnitTrack:
    """Машина «к станции», вне всех радиусов."""
    return _track(phase=UnitPhase.TO_STATION, position=position)


def test_advance_to_station_enters_radius_moving_records_entry() -> None:
    """Въезд в радиус без остановки — «к станции», момент входа зафиксирован."""
    track = advance(_to_station(), tm(T0 + 5, INSIDE_S1_EDGE, MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.station_id is None
    assert track.zone_entry == StationZoneEntry(S1_ID, T0 + 5)


def test_advance_to_station_stops_later_keeps_entry_moment() -> None:
    """Правило станции 3: остановка позже — «на станции», момент входа — момент въезда."""
    entered = advance(_to_station(), tm(T0 + 5, INSIDE_S1_EDGE, MOVING), SITE)
    inside = advance(entered, tm(T0 + 8, NEAR_S1, MOVING), SITE)
    assert inside.zone_entry == StationZoneEntry(S1_ID, T0 + 5)
    stopped = advance(inside, tm(T0 + 12, NEAR_S1, STOPPED), SITE)
    assert stopped.phase is UnitPhase.AT_STATION
    assert stopped.station_id == S1_ID
    assert stopped.zone_entry == StationZoneEntry(S1_ID, T0 + 5)


def test_advance_to_station_passes_through_radius_stays_to_station() -> None:
    """Проезд насквозь без остановки — «к станции», зона после выхода сброшена."""
    track = advance(_to_station(), tm(T0 + 1, INSIDE_S1_EDGE, MOVING), SITE)
    track = advance(track, tm(T0 + 2, S1_POINT, MOVING), SITE)
    track = advance(track, tm(T0 + 3, north_of(S1_POINT, -60.0), MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.station_id is None
    assert track.zone_entry is None


def test_advance_to_station_reentry_records_new_entry_moment() -> None:
    """После выхода из радиуса повторный въезд фиксирует новый момент входа."""
    track = advance(_to_station(), tm(T0 + 1, NEAR_S1, MOVING), SITE)
    track = advance(track, tm(T0 + 2, OUTSIDE_S1, MOVING), SITE)
    track = advance(track, tm(T0 + 3, NEAR_S1, MOVING), SITE)
    assert track.zone_entry == StationZoneEntry(S1_ID, T0 + 3)


def test_advance_to_station_in_unload_zone_stays_to_station() -> None:
    """«К станции» через радиус разгрузки — фаза не меняется, признак зоны выставляется."""
    track = advance(_to_station(), tm(T0 + 1, NEAR_UNLOAD, MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.in_unload_zone is True
    track = advance(track, tm(T0 + 2, OUTSIDE_UNLOAD, MOVING), SITE)
    assert track.phase is UnitPhase.TO_STATION
    assert track.in_unload_zone is False


# --- полный цикл ------------------------------------------------------------


def test_advance_full_cycle() -> None:
    """Цикл: к станции → на станции → к разгрузке → разгрузка → к станции → на станции."""
    steps: list[tuple[Point, float, UnitPhase, str | None, bool]] = [
        (FAR_POINT, MOVING, UnitPhase.TO_STATION, None, False),
        (INSIDE_S1_EDGE, MOVING, UnitPhase.TO_STATION, None, False),
        (NEAR_S1, STOPPED, UnitPhase.AT_STATION, S1_ID, False),
        (NEAR_S1, STOPPED, UnitPhase.AT_STATION, S1_ID, False),
        (INSIDE_S1_EDGE, MOVING, UnitPhase.AT_STATION, S1_ID, False),
        (OUTSIDE_S1, MOVING, UnitPhase.TO_UNLOAD, None, False),
        (FAR_POINT, MOVING, UnitPhase.TO_UNLOAD, None, False),
        (NEAR_UNLOAD, MOVING, UnitPhase.TO_UNLOAD, None, True),
        (UNLOAD_POINT, STOPPED, UnitPhase.TO_UNLOAD, None, True),
        (OUTSIDE_UNLOAD, MOVING, UnitPhase.TO_STATION, None, False),
        (FAR_POINT, MOVING, UnitPhase.TO_STATION, None, False),
        (NEAR_S1, MOVING, UnitPhase.TO_STATION, None, False),
        (NEAR_S1, STOPPED, UnitPhase.AT_STATION, S1_ID, False),
    ]
    track: UnitTrack | None = None
    for i, (pos, speed, phase, station_id, in_unload) in enumerate(steps):
        track = advance(track, tm(T0 + i * 10, pos, speed), SITE)
        assert track.phase is phase, f"шаг {i}"
        assert track.station_id == station_id, f"шаг {i}"
        assert track.in_unload_zone is in_unload, f"шаг {i}"
    assert track is not None
    # Второй заезд: момент входа — въезд в радиус на шаге 11, а не момент остановки.
    assert track.zone_entry == StationZoneEntry(S1_ID, T0 + 110)


# --- служебные поля и ошибки -----------------------------------------------


def test_advance_updates_last_ts_and_position() -> None:
    """last_ts и position берутся из учтённого сообщения."""
    prev = _to_station()
    new_pos = north_of(FAR_POINT, 10.0)
    track = advance(prev, tm(T0 + 7, new_pos, MOVING), SITE)
    assert track.last_ts == T0 + 7
    assert track.position == new_pos
    assert track.unit_id == UNIT_ID


def test_advance_does_not_modify_prev() -> None:
    """Предыдущее состояние не меняется (результат — новый объект)."""
    prev = _at_s1()
    snapshot = _at_s1()
    advance(prev, tm(T0 + 230, OUTSIDE_S1, MOVING), SITE)
    assert prev == snapshot


def test_advance_equal_ts_raises() -> None:
    """Сообщение с ts, равным последнему учтённому, — ValueError (фильтр — у вызывающего)."""
    with pytest.raises(ValueError):
        advance(_to_station(), tm(T0, NEAR_S1, MOVING), SITE)


def test_advance_older_ts_raises() -> None:
    """Сообщение с ts меньше последнего учтённого — ValueError."""
    with pytest.raises(ValueError):
        advance(_to_station(), tm(T0 - 1, NEAR_S1, MOVING), SITE)


def test_advance_other_unit_raises() -> None:
    """Сообщение другой машины к состоянию T1 — ValueError."""
    with pytest.raises(ValueError):
        advance(_to_station(), tm(T0 + 1, NEAR_S1, MOVING, unit_id="T2"), SITE)
