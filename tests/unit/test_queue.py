"""Тесты виртуальной очереди станции.

ТЗ, «Правила расчёта → Очередь»:
1. Время приезда «к станции» — расстояние до станции / расчётная скорость; «к разгрузке» —
   путь до точки разгрузки плюс путь от неё до станции / расчётная скорость.
2. Машины выстраиваются по времени приезда; каждая занимает станцию на 230 с с момента
   приезда либо с момента освобождения станции предыдущей, если он позже. Ожидание —
   начало обслуживания минус приезд.
3. Занявшая станцию машина стоит в очереди первой; её free_at — момент занятия + 230 с.
4. Машина, чей приезд позже горизонта 30 минут, в очередь не попадает.
5. Машина с позицией старше 30 с относительно «сейчас» исключается; занявшую это не касается.

Правило 3 занятия: машина в радиусе станции, пока станция занята другой, ждёт; её приезд —
момент входа в радиус.

ТЗ, «Рекомендация», п.1 (wait_before): ожидание машины — момент освобождения станции минус
её приезд, но не меньше нуля; момент освобождения — free_at последней машины очереди, которая
приедет раньше нашей (занявшая станцию — всегда раньше); если таких нет — станция свободна.
"""

from __future__ import annotations

import itertools
from collections.abc import Iterator
from dataclasses import replace
from typing import Final

import pytest

from tests.sitekit import (
    S1_ID,
    S1_POINT,
    S2_ID,
    S2_POINT,
    UNIT_ID,
    UNLOAD_POINT,
    make_site,
    north_of,
)
from vqueue.domain.model import Point, Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy
from vqueue.domain.queue import (
    QueueEntry,
    StationQueue,
    build_station_queue,
    estimate_arrival,
    schedule,
    wait_before,
)
from vqueue.domain.unit_fsm import StationZoneEntry, UnitTrack

SITE: Final = make_site()
RULES: Final = SITE.rules
OCC: Final = RULES.occupancy_seconds
S1: Final = SITE.station(S1_ID)

NOW: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — «сейчас»."""

T1: Final = UNIT_ID
T2: Final = "T2"
T3: Final = "T3"
T4: Final = "T4"


def _track(
    unit_id: str,
    position: Point,
    *,
    last_ts: int = NOW,
    phase: UnitPhase = UnitPhase.TO_STATION,
    zone_entry: StationZoneEntry | None = None,
) -> UnitTrack:
    """Трек машины, заданный напрямую (без прогона телеметрии).

    В фазе AT_STATION станция машины — станция её zone_entry; вне точки разгрузки.
    """
    return UnitTrack(
        unit_id=unit_id,
        last_ts=last_ts,
        position=position,
        phase=phase,
        station_id=(
            zone_entry.station_id
            if phase is UnitPhase.AT_STATION and zone_entry is not None
            else None
        ),
        zone_entry=zone_entry,
        in_unload_zone=False,
    )


def _waiting_at_s1(unit_id: str, entered_at: int, *, last_ts: int = NOW) -> UnitTrack:
    """Машина стоит в радиусе S1 (в 20 м) с момента entered_at."""
    return _track(
        unit_id,
        north_of(S1_POINT, 20.0),
        last_ts=last_ts,
        phase=UnitPhase.AT_STATION,
        zone_entry=StationZoneEntry(S1_ID, entered_at),
    )


def _busy_s1(unit_id: str, occupied_at: int, *, free_at: int | None = None) -> StationOccupancy:
    """S1 занята машиной unit_id с occupied_at (визит начался в тот же момент)."""
    return StationOccupancy(
        S1_ID,
        Occupant(
            unit_id=unit_id,
            visit_entered_at=occupied_at,
            occupied_at=occupied_at,
            free_at=occupied_at + OCC if free_at is None else free_at,
        ),
        served_visits=frozenset({(unit_id, occupied_at)}),
    )


FREE_S1: Final = StationOccupancy(S1_ID)


# ---------------------------------------------------------------------------
# schedule (п.2)
# ---------------------------------------------------------------------------


def test_schedule_empty_input_returns_empty() -> None:
    """П.2: нет приезжающих — пустое расписание."""
    assert schedule([], None, RULES) == ()
    assert schedule([], NOW, RULES) == ()


def test_schedule_single_unit_free_station_starts_at_eta_without_wait() -> None:
    """П.2: станция свободна — обслуживание с момента приезда, ожидание 0."""
    assert schedule([(T2, NOW + 120)], None, RULES) == (
        QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),
    )


def test_schedule_free_from_before_eta_starts_at_eta() -> None:
    """П.2: станция освобождается раньше приезда — начало = приезд, ожидание 0."""
    assert schedule([(T2, NOW + 120)], NOW + 50, RULES) == (
        QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),
    )


def test_schedule_free_from_equal_eta_starts_at_eta_without_wait() -> None:
    """П.2, граница: освобождение ровно в момент приезда — ожидание 0."""
    assert schedule([(T2, NOW + 120)], NOW + 120, RULES) == (
        QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),
    )


def test_schedule_free_from_after_eta_starts_at_free_from_and_waits() -> None:
    """П.2: станция освободится позже приезда — начало = освобождение, ожидание = разница."""
    assert schedule([(T2, NOW + 120)], NOW + 170, RULES) == (
        QueueEntry(T2, NOW + 120, NOW + 170, NOW + 170 + OCC, 50),
    )


def test_schedule_overlapping_chain_waits_for_previous() -> None:
    """П.2: вторая приезжает, пока первая обслуживается, — начинает с free_at первой."""
    assert schedule([(T2, 100), (T3, 150)], None, RULES) == (
        QueueEntry(T2, 100, 100, 100 + OCC, 0),
        QueueEntry(T3, 150, 100 + OCC, 100 + 2 * OCC, 100 + OCC - 150),
    )


def test_schedule_gap_chain_arrival_after_release_waits_zero() -> None:
    """П.2: вторая приезжает после освобождения станции — начинает по приезду, ожидание 0."""
    assert schedule([(T2, 100), (T3, 100 + OCC + 70)], None, RULES) == (
        QueueEntry(T2, 100, 100, 100 + OCC, 0),
        QueueEntry(T3, 100 + OCC + 70, 100 + OCC + 70, 100 + 2 * OCC + 70, 0),
    )


def test_schedule_arrival_exactly_at_release_waits_zero() -> None:
    """П.2, граница: приезд ровно в момент освобождения предыдущей — ожидание 0."""
    entries = schedule([(T2, 100), (T3, 100 + OCC)], None, RULES)
    assert entries[1] == QueueEntry(T3, 100 + OCC, 100 + OCC, 100 + 2 * OCC, 0)


def test_schedule_three_units_chain_accumulates_wait() -> None:
    """П.2: цепочка из трёх с перекрытием — каждая ждёт освобождения предыдущей."""
    entries = schedule([(T2, 0), (T3, 10), (T4, 20)], None, RULES)
    assert [(e.service_start, e.free_at, e.wait_seconds) for e in entries] == [
        (0, OCC, 0),
        (OCC, 2 * OCC, OCC - 10),
        (2 * OCC, 3 * OCC, 2 * OCC - 20),
    ]


def test_schedule_sorted_by_eta_regardless_of_input_order() -> None:
    """П.2: машины выстраиваются по времени приезда, а не по порядку входа."""
    entries = schedule([(T4, 300), (T2, 100), (T3, 200)], None, RULES)
    assert [e.unit_id for e in entries] == [T2, T3, T4]
    assert [e.eta for e in entries] == [100, 200, 300]


def test_schedule_equal_eta_ordered_by_unit_id() -> None:
    """П.2, детерминизм: при равном приезде порядок — по unit_id."""
    entries = schedule([("B", 100), ("C", 100), ("A", 100)], None, RULES)
    assert [e.unit_id for e in entries] == ["A", "B", "C"]
    assert [e.wait_seconds for e in entries] == [0, OCC, 2 * OCC]


def test_schedule_result_independent_of_input_permutation() -> None:
    """П.2, детерминизм: любая перестановка входа даёт одно и то же расписание."""
    arrivals = [("B", 100), ("A", 100), ("C", 50), ("D", 900)]
    expected = schedule(arrivals, NOW, RULES)
    for perm in itertools.permutations(arrivals):
        assert schedule(perm, NOW, RULES) == expected


def test_schedule_accepts_one_shot_iterator() -> None:
    """Аргумент arrivals — любой Iterable, в том числе одноразовый генератор."""

    def gen() -> Iterator[tuple[str, int]]:
        yield (T3, 200)
        yield (T2, 100)

    assert [e.unit_id for e in schedule(gen(), None, RULES)] == [T2, T3]


def test_schedule_uses_occupancy_from_rules() -> None:
    """П.2: длительность занятия — обслуживание + манёвр из Rules, а не константа 230."""
    rules = Rules(service_seconds=100, maneuver_seconds=20)
    assert schedule([(T2, 0), (T3, 50)], None, rules) == (
        QueueEntry(T2, 0, 0, 120, 0),
        QueueEntry(T3, 50, 120, 240, 70),
    )


# ---------------------------------------------------------------------------
# estimate_arrival: едущие (п.1, п.4, п.5)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("meters", "travel"), [(1200.0, 120), (3000.0, 300)])
def test_estimate_to_station_distance_over_speed_examples(meters: float, travel: int) -> None:
    """П.1, пример ТЗ: 1200 м → 120 с, 3000 м → 300 с при 10 м/с."""
    track = _track(T2, north_of(S1_POINT, meters))
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + travel


def test_estimate_to_station_counts_from_last_ts() -> None:
    """П.1: путь отсчитывается от момента наблюдения позиции (last_ts), а не от «сейчас»."""
    track = _track(T2, north_of(S1_POINT, 1200.0), last_ts=NOW - 10)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 110


def test_estimate_to_station_not_earlier_than_now() -> None:
    """Решение из README: позиция 20 с назад в 100 м (приезд «в прошлом») → eta = now."""
    track = _track(T2, north_of(S1_POINT, 100.0), last_ts=NOW - 20)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW


def test_estimate_to_station_uses_rules_speed() -> None:
    """П.1: скорость — расчётная из Rules (18 км/ч = 5 м/с → 1200 м за 240 с)."""
    site = make_site(Rules(speed_kmh=18.0))
    track = _track(T2, north_of(S1_POINT, 1200.0))
    assert estimate_arrival(track, site.station(S1_ID), FREE_S1, site, NOW) == NOW + 240


def test_estimate_to_unload_goes_via_unload_point() -> None:
    """П.1: «к разгрузке» — путь до разгрузки плюс от неё до станции, а не напрямую.

    Машина в 500 м севернее S1, разгрузка в 3000 м южнее: 3500 + 3000 = 6500 м → 650 с
    (напрямую было бы 50 с).
    """
    track = _track(T1, north_of(S1_POINT, 500.0), phase=UnitPhase.TO_UNLOAD)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 650


def test_estimate_to_unload_rounds_sum_of_legs_not_each_leg() -> None:
    """П.1: округляется сумма путей.

    Станция в 3003 м от разгрузки (300,3 с), машина в 4003 м от разгрузки (400,3 с):
    округление суммы 700,6 → 701; сумма округлений отрезков дала бы 700, округление вверх — 702.
    """
    unload = north_of(S1_POINT, -3003.0)
    site = SiteConfig(
        stations=(Station(S1_ID, S1_POINT),),
        unload_point=unload,
        assignments={T1: S1_ID},
        rules=RULES,
    )
    track = _track(T1, north_of(S1_POINT, 1000.0), phase=UnitPhase.TO_UNLOAD)
    assert estimate_arrival(track, site.station(S1_ID), FREE_S1, site, NOW) == NOW + 701


def test_estimate_to_unload_in_unload_zone_still_via_unload() -> None:
    """П.1: машина у самой точки разгрузки «к разгрузке» — путь от разгрузки до станции."""
    track = replace(
        _track(T1, north_of(UNLOAD_POINT, 10.0), phase=UnitPhase.TO_UNLOAD), in_unload_zone=True
    )
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 301


def test_estimate_fresh_exactly_30s_stays_in_queue() -> None:
    """П.5, граница: позиция ровно 30 с назад — свежая."""
    track = _track(T2, north_of(S1_POINT, 1200.0), last_ts=NOW - 30)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 90


def test_estimate_stale_31s_excluded() -> None:
    """П.5, граница: позиция 31 с назад — несвежая, машина исключается."""
    track = _track(T2, north_of(S1_POINT, 1200.0), last_ts=NOW - 31)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) is None


def test_estimate_freshness_uses_rules_threshold() -> None:
    """П.5: порог свежести берётся из Rules."""
    site = make_site(Rules(freshness_seconds=5))
    track = _track(T2, north_of(S1_POINT, 1200.0), last_ts=NOW - 6)
    assert estimate_arrival(track, site.station(S1_ID), FREE_S1, site, NOW) is None


def test_estimate_exactly_at_horizon_stays_in_queue() -> None:
    """П.4, граница: приезд ровно через 30 минут — в очереди."""
    track = _track(T2, north_of(S1_POINT, 18_000.0))
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 1800


def test_estimate_beyond_horizon_by_one_second_excluded() -> None:
    """П.4, граница: приезд через 1801 с — позже горизонта, не в очереди."""
    track = _track(T2, north_of(S1_POINT, 18_010.0))
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) is None


def test_estimate_horizon_from_now_with_old_position_exactly_at_horizon_kept() -> None:
    """П.4, граница: позиция 10 с назад, приезд ровно now + 1800 — в очереди (горизонт от now)."""
    track = _track(T2, north_of(S1_POINT, 18_100.0), last_ts=NOW - 10)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 1800


def test_estimate_horizon_from_now_with_old_position_one_second_beyond_excluded() -> None:
    """П.4, граница: позиция 10 с назад, приезд now + 1801 — позже горизонта от now."""
    track = _track(T2, north_of(S1_POINT, 18_110.0), last_ts=NOW - 10)
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) is None


def test_estimate_to_unload_beyond_horizon_excluded() -> None:
    """П.4: горизонт применяется и к пути через разгрузку."""
    site = make_site(Rules(horizon_seconds=600))
    track = _track(T1, north_of(S1_POINT, 500.0), phase=UnitPhase.TO_UNLOAD)
    assert estimate_arrival(track, site.station(S1_ID), FREE_S1, site, NOW) is None


def test_estimate_to_station_through_other_station_radius_uses_distance() -> None:
    """П.1: проезд через радиус чужой станции S2 — приезд к S1 по расстоянию (2000 м → 200 с)."""
    track = _track(T2, S2_POINT, zone_entry=StationZoneEntry(S2_ID, NOW - 3))
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) == NOW + 200


def test_estimate_at_other_station_excluded() -> None:
    """Решение из README: машина «на станции» у другой станции сюда не едет — не в очереди."""
    track = _track(
        T2,
        S2_POINT,
        phase=UnitPhase.AT_STATION,
        zone_entry=StationZoneEntry(S2_ID, NOW - 50),
    )
    assert estimate_arrival(track, S1, FREE_S1, SITE, NOW) is None


# ---------------------------------------------------------------------------
# estimate_arrival: в радиусе станции (правило 3 занятия, п.3, п.5)
# ---------------------------------------------------------------------------


def test_estimate_waiting_in_radius_eta_is_zone_entry_time() -> None:
    """Правило 3 занятия: ждёт в радиусе, станция занята другой — приезд = вход в радиус."""
    occ = _busy_s1(T1, NOW - 60)
    track = _waiting_at_s1(T2, NOW - 40)
    assert estimate_arrival(track, S1, occ, SITE, NOW) == NOW - 40


def test_estimate_waiting_in_radius_stale_excluded() -> None:
    """П.5: ждущая в радиусе, но с позицией старше 30 с — исключается."""
    occ = _busy_s1(T1, NOW - 60)
    track = _waiting_at_s1(T2, NOW - 50, last_ts=NOW - 31)
    assert estimate_arrival(track, S1, occ, SITE, NOW) is None


def test_estimate_waiting_in_radius_fresh_at_30s_kept() -> None:
    """П.5, граница: ждущая в радиусе с позицией ровно 30 с назад — в очереди."""
    occ = _busy_s1(T1, NOW - 60)
    track = _waiting_at_s1(T2, NOW - 50, last_ts=NOW - 30)
    assert estimate_arrival(track, S1, occ, SITE, NOW) == NOW - 50


def test_estimate_moving_in_radius_eta_is_zone_entry_time() -> None:
    """Правило 3 занятия: «к станции» уже в радиусе этой станции на ходу — приезд = вход."""
    occ = _busy_s1(T1, NOW - 60)
    track = _track(T2, north_of(S1_POINT, 30.0), zone_entry=StationZoneEntry(S1_ID, NOW - 5))
    assert estimate_arrival(track, S1, occ, SITE, NOW) == NOW - 5


def test_estimate_to_unload_passing_radius_counts_via_unload_not_entry() -> None:
    """П.1: «к разгрузке» в радиусе станции (после чужого/своего визита) не ждёт её.

    Машина в 30 м от S1 с zone_entry S1, станция занята другой: приезд — через разгрузку
    (3030 + 3000 м → 603 с), а не момент входа в радиус.
    """
    occ = _busy_s1(T1, NOW - 60)
    track = _track(
        T2,
        north_of(S1_POINT, 30.0),
        phase=UnitPhase.TO_UNLOAD,
        zone_entry=StationZoneEntry(S1_ID, NOW - 5),
    )
    assert estimate_arrival(track, S1, occ, SITE, NOW) == NOW + 603


def test_estimate_occupant_excluded() -> None:
    """П.3: занявшая станцию стоит в очереди отдельно — estimate_arrival для неё None."""
    occ = _busy_s1(T1, NOW - 60)
    track = _waiting_at_s1(T1, NOW - 60)
    assert estimate_arrival(track, S1, occ, SITE, NOW) is None


def test_estimate_occupant_excluded_even_if_moving() -> None:
    """П.3: занявшая (станция ещё занята по времени) не получает приезд, даже в другой фазе."""
    occ = _busy_s1(T1, NOW - 60)
    track = _track(T1, north_of(S1_POINT, 1200.0))
    assert estimate_arrival(track, S1, occ, SITE, NOW) is None


def test_estimate_served_still_standing_after_release_excluded() -> None:
    """Занятие, правило 2 + ключ визита: отстояла 230 с и стоит в том же визите — не в очереди."""
    occupied_at = NOW - OCC - 10
    occ = _busy_s1(T1, occupied_at)
    assert not occ.is_busy(NOW)
    track = _waiting_at_s1(T1, occupied_at)
    assert estimate_arrival(track, S1, occ, SITE, NOW) is None


def test_estimate_new_visit_after_service_is_queued() -> None:
    """Ключ визита: обслуженный визит — старый; новый вход в радиус снова ставит в очередь."""
    old_visit = NOW - OCC - 100
    busy = _busy_s1(T2, NOW - 30)
    occ = StationOccupancy(
        S1_ID, busy.occupant, served_visits=frozenset({(T1, old_visit), (T2, NOW - 30)})
    )
    track = _waiting_at_s1(T1, NOW - 10)
    assert estimate_arrival(track, S1, occ, SITE, NOW) == NOW - 10


# ---------------------------------------------------------------------------
# build_station_queue (п.2, п.3, п.5)
# ---------------------------------------------------------------------------


def test_build_empty_station_queue() -> None:
    """Нет машин, станция свободна — пустая очередь со station_id и at = now."""
    assert build_station_queue(FREE_S1, [], SITE, NOW) == StationQueue(S1_ID, NOW, ())


def test_build_only_occupant_is_first_with_null_eta() -> None:
    """П.3: занявшая — первая, eta None, начало = момент занятия, free_at из занятости."""
    occ = _busy_s1(T1, NOW - 60)
    queue = build_station_queue(occ, [_waiting_at_s1(T1, NOW - 60)], SITE, NOW)
    assert queue == StationQueue(S1_ID, NOW, (QueueEntry(T1, None, NOW - 60, NOW - 60 + OCC, 0),))


def test_build_occupant_free_at_taken_from_occupancy() -> None:
    """П.3: free_at занявшей берётся из занятости как есть (а не пересчитывается)."""
    occ = _busy_s1(T1, NOW - 60, free_at=NOW + 7)
    queue = build_station_queue(occ, [_waiting_at_s1(T1, NOW - 60)], SITE, NOW)
    assert queue.entries == (QueueEntry(T1, None, NOW - 60, NOW + 7, 0),)


def test_build_stale_occupant_still_first() -> None:
    """П.5: правило свежести не касается занявшей — она первая до расчётного освобождения."""
    occ = _busy_s1(T1, NOW - 100)
    track = _waiting_at_s1(T1, NOW - 100, last_ts=NOW - 90)
    queue = build_station_queue(occ, [track], SITE, NOW)
    assert queue.entries == (QueueEntry(T1, None, NOW - 100, NOW - 100 + OCC, 0),)


def test_build_occupant_present_without_its_track() -> None:
    """П.3: занявшая берётся из занятости, даже если её трека нет среди переданных."""
    occ = _busy_s1(T1, NOW - 60)
    queue = build_station_queue(occ, [], SITE, NOW)
    assert queue.entries == (QueueEntry(T1, None, NOW - 60, NOW - 60 + OCC, 0),)


def test_build_queue_after_occupant_waits_for_its_free_at() -> None:
    """П.2–3: едущая после занявшей начинает с её free_at."""
    occ = _busy_s1(T1, NOW - 60)
    tracks = [_waiting_at_s1(T1, NOW - 60), _track(T2, north_of(S1_POINT, 1200.0))]
    queue = build_station_queue(occ, tracks, SITE, NOW)
    assert queue.entries == (
        QueueEntry(T1, None, NOW - 60, NOW + 170, 0),
        QueueEntry(T2, NOW + 120, NOW + 170, NOW + 170 + OCC, 50),
    )


def test_build_excludes_stale_and_beyond_horizon_units() -> None:
    """П.4–5: несвежая и приезжающая позже горизонта в очередь не попадают."""
    occ = _busy_s1(T1, NOW - 60)
    tracks = [
        _waiting_at_s1(T1, NOW - 60),
        _track(T2, north_of(S1_POINT, 1200.0)),
        _track(T3, north_of(S1_POINT, 1500.0), last_ts=NOW - 31),
        _track(T4, north_of(S1_POINT, 18_010.0)),
    ]
    queue = build_station_queue(occ, tracks, SITE, NOW)
    assert [e.unit_id for e in queue.entries] == [T1, T2]


def test_build_waiting_in_radius_ordered_by_entry_before_moving() -> None:
    """Правило 3 занятия + п.2: ждущая в радиусе (приезд в прошлом) впереди едущей."""
    occ = _busy_s1(T1, NOW - 60)
    tracks = [
        _track(T3, north_of(S1_POINT, 100.0)),
        _waiting_at_s1(T2, NOW - 40),
        _waiting_at_s1(T1, NOW - 60),
    ]
    queue = build_station_queue(occ, tracks, SITE, NOW)
    assert queue.entries == (
        QueueEntry(T1, None, NOW - 60, NOW + 170, 0),
        QueueEntry(T2, NOW - 40, NOW + 170, NOW + 170 + OCC, 210),
        QueueEntry(T3, NOW + 10, NOW + 170 + OCC, NOW + 170 + 2 * OCC, 160 + OCC),
    )


def test_build_released_by_time_no_occupant_entry_waiter_starts_at_old_free_at() -> None:
    """П.3 + п.2: станция освободилась по времени, никто ещё не занял.

    Занявшей в очереди нет (она отстояла и стоит в том же визите), а ждавшая в радиусе
    начинает не раньше прежнего free_at.
    """
    occupied_at = NOW - OCC - 10
    occ = _busy_s1(T1, occupied_at)
    tracks = [_waiting_at_s1(T1, occupied_at), _waiting_at_s1(T2, NOW - 100, last_ts=NOW - 11)]
    queue = build_station_queue(occ, tracks, SITE, NOW)
    assert queue == StationQueue(
        S1_ID, NOW, (QueueEntry(T2, NOW - 100, NOW - 10, NOW - 10 + OCC, 90),)
    )


def test_build_released_by_time_late_arrival_starts_at_eta() -> None:
    """П.2: станция освободилась по времени — едущая начинает по приезду, ожидание 0."""
    occ = _busy_s1(T1, NOW - OCC - 10)
    queue = build_station_queue(occ, [_track(T2, north_of(S1_POINT, 1200.0))], SITE, NOW)
    assert queue.entries == (QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),)


def test_build_released_early_by_exit_no_occupant_entry() -> None:
    """Занятие, правило 2: занявшая вышла из радиуса (досрочно освобождена) — не первая.

    T1 уехала «к разгрузке» в 60 м от S1: её приезд — через разгрузку (3060 + 3000 м → 606 с),
    она стоит в очереди как обычная едущая; T2 едет из 1200 м и начинает по приезду.
    """
    occ = StationOccupancy(
        S1_ID,
        Occupant(unit_id=T1, visit_entered_at=NOW - 100, occupied_at=NOW - 100, free_at=NOW - 20),
    )
    tracks = [
        _track(T1, north_of(S1_POINT, 60.0), last_ts=NOW, phase=UnitPhase.TO_UNLOAD),
        _track(T2, north_of(S1_POINT, 1200.0)),
    ]
    queue = build_station_queue(occ, tracks, SITE, NOW)
    assert queue.entries == (
        QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),
        QueueEntry(T1, NOW + 606, NOW + 606, NOW + 606 + OCC, 0),
    )


def test_build_no_occupant_moving_in_radius_starts_at_entry() -> None:
    """Правило 3 занятия + п.2: станция никем не занята, машина въехала в радиус на ходу.

    Приезд — момент входа в радиус (в прошлом), обслуживание с него же, ожидание 0.
    """
    track = _track(T2, north_of(S1_POINT, 30.0), zone_entry=StationZoneEntry(S1_ID, NOW - 5))
    queue = build_station_queue(FREE_S1, [track], SITE, NOW)
    assert queue.entries == (QueueEntry(T2, NOW - 5, NOW - 5, NOW - 5 + OCC, 0),)


def test_build_queue_station_id_and_at() -> None:
    """Очередь помечена станцией занятости и моментом расчёта at = now."""
    occ = StationOccupancy(S2_ID)
    queue = build_station_queue(occ, [_track(T2, north_of(S2_POINT, 1000.0))], SITE, NOW + 5)
    assert queue.station_id == S2_ID
    assert queue.at == NOW + 5
    assert queue.entries == (QueueEntry(T2, NOW + 100, NOW + 100, NOW + 100 + OCC, 0),)


def test_build_queue_independent_of_tracks_order() -> None:
    """П.2, детерминизм: порядок переданных треков не влияет на очередь."""
    occ = _busy_s1(T1, NOW - 60)
    tracks = [
        _waiting_at_s1(T1, NOW - 60),
        _track(T2, north_of(S1_POINT, 1200.0)),
        _track(T3, north_of(S1_POINT, 1200.0)),
        _track(T4, north_of(S1_POINT, 300.0)),
    ]
    expected = build_station_queue(occ, tracks, SITE, NOW)
    for perm in itertools.permutations(tracks):
        assert build_station_queue(occ, perm, SITE, NOW) == expected
    assert [e.unit_id for e in expected.entries] == [T1, T4, T2, T3]


# ---------------------------------------------------------------------------
# wait_before (ТЗ, «Рекомендация», п.1)
# ---------------------------------------------------------------------------


def _occupant_entry(unit_id: str, free_at: int) -> QueueEntry:
    """Запись занявшей станцию машины: eta None, обслуживание с free_at - 230."""
    return QueueEntry(unit_id, None, free_at - OCC, free_at, 0)


CANDIDATE: Final = "T0"
"""Машина, для которой считается ожидание: id меньше всех в очереди — при равном eta она первая."""


def _s1_queue(*entries: QueueEntry) -> StationQueue:
    """Очередь S1 на NOW из заданных записей."""
    return StationQueue(S1_ID, NOW, entries)


def test_wait_before_empty_queue_is_zero() -> None:
    """Рекомендация п.1: в очереди никого — станция свободна, ожидание 0."""
    assert wait_before(_s1_queue(), NOW + 90, CANDIDATE) == 0


def test_wait_before_empty_queue_eta_in_past_is_zero() -> None:
    """Рекомендация п.1: пустая очередь и приезд «в прошлом» (ждёт в радиусе) — ожидание 0."""
    assert wait_before(_s1_queue(), NOW - 100, CANDIDATE) == 0


def test_wait_before_only_occupant_waits_until_its_free_at() -> None:
    """Рекомендация п.1: только занявшая (eta None) освободит в NOW+200 → ждать 200 - 90."""
    assert wait_before(_s1_queue(_occupant_entry(T1, NOW + 200)), NOW + 90, CANDIDATE) == 110


def test_wait_before_occupant_released_before_arrival_is_zero() -> None:
    """Рекомендация п.1: занявшая освободит раньше приезда — ожидание 0, не отрицательное."""
    assert wait_before(_s1_queue(_occupant_entry(T1, NOW + 50)), NOW + 90, CANDIDATE) == 0


def test_wait_before_occupant_released_exactly_at_arrival_is_zero() -> None:
    """Рекомендация п.1: освобождение ровно в момент приезда — ожидание 0."""
    assert wait_before(_s1_queue(_occupant_entry(T1, NOW + 90)), NOW + 90, CANDIDATE) == 0


def test_wait_before_occupant_counts_even_for_arrival_in_past() -> None:
    """Рекомендация п.1: занявшая «приехала раньше» любого приезда, даже eta < now."""
    assert wait_before(_s1_queue(_occupant_entry(T1, NOW + 100)), NOW - 20, CANDIDATE) == 120


def test_wait_before_entry_with_equal_eta_and_greater_id_not_counted() -> None:
    """Рекомендация п.1: тот же eta и больший unit_id — обслуживается позже, не учитывается."""
    queue = _s1_queue(QueueEntry(T2, NOW + 90, NOW + 90, NOW + 90 + OCC, 0))
    assert wait_before(queue, NOW + 90, CANDIDATE) == 0


def test_wait_before_own_entry_not_counted() -> None:
    """Рекомендация п.1: запись самой машины в её очереди на её ожидание не влияет."""
    queue = _s1_queue(QueueEntry(T2, NOW + 90, NOW + 90, NOW + 90 + OCC, 0))
    assert wait_before(queue, NOW + 90, T2) == 0


def test_wait_before_entry_with_equal_eta_and_smaller_id_counted() -> None:
    """Равный eta: schedule обслуживает по (eta, unit_id) — машина с меньшим id впереди.

    T4 приезжает одновременно с T2, но в расписании стоит после неё: ждёт всё её обслуживание.
    """
    queue = _s1_queue(QueueEntry(T2, NOW + 90, NOW + 90, NOW + 90 + OCC, 0))
    assert wait_before(queue, NOW + 90, T4) == OCC


def test_wait_before_matches_schedule_for_equal_etas() -> None:
    """Ожидание по wait_before совпадает с wait_seconds в расписании при одинаковых eta."""
    entries = schedule([(T2, NOW + 90), (T3, NOW + 90), (T4, NOW + 90)], None, SITE.rules)
    queue = _s1_queue(*entries)
    for entry in entries:
        assert entry.eta is not None
        assert wait_before(queue, entry.eta, entry.unit_id) == entry.wait_seconds


def test_wait_before_entry_one_second_earlier_counted() -> None:
    """Рекомендация п.1: запись с eta на 1 с раньше — учитывается: ждать до её free_at."""
    queue = _s1_queue(QueueEntry(T2, NOW + 89, NOW + 89, NOW + 89 + OCC, 0))
    assert wait_before(queue, NOW + 90, CANDIDATE) == OCC - 1


def test_wait_before_later_entries_not_counted() -> None:
    """Рекомендация п.1: машины, приезжающие позже, на ожидание не влияют."""
    queue = _s1_queue(
        _occupant_entry(T1, NOW + 100),
        QueueEntry(T2, NOW + 120, NOW + 120, NOW + 120 + OCC, 0),
    )
    assert wait_before(queue, NOW + 110, CANDIDATE) == 0
    assert wait_before(queue, NOW + 90, CANDIDATE) == 10


def test_wait_before_takes_release_of_last_earlier_entry() -> None:
    """Рекомендация п.1: освобождение — free_at последней из приезжающих раньше (максимум).

    Очередь: T1 занята до NOW+50, T2 приезд NOW+30 → обслуживание NOW+50..NOW+280,
    T3 приезд NOW+60 → NOW+280..NOW+510, T4 приезд NOW+600. Машина с приездом NOW+100
    ждёт после T3: 510 - 100 = 410; с приездом NOW+61 — тоже после T3: 510 - 61.
    """
    queue = _s1_queue(
        _occupant_entry(T1, NOW + 50),
        QueueEntry(T2, NOW + 30, NOW + 50, NOW + 50 + OCC, 20),
        QueueEntry(T3, NOW + 60, NOW + 50 + OCC, NOW + 50 + 2 * OCC, OCC - 10),
        QueueEntry(T4, NOW + 600, NOW + 600, NOW + 600 + OCC, 0),
    )
    assert wait_before(queue, NOW + 100, CANDIDATE) == 50 + 2 * OCC - 100
    assert wait_before(queue, NOW + 61, CANDIDATE) == 50 + 2 * OCC - 61
    assert wait_before(queue, NOW + 60, CANDIDATE) == 50 + OCC - 60
    assert wait_before(queue, NOW + 40, CANDIDATE) == 50 + OCC - 40
    assert wait_before(queue, NOW + 30, CANDIDATE) == 50 - 30
    assert wait_before(queue, NOW + 600, CANDIDATE) == 0
    assert wait_before(queue, NOW + 601, CANDIDATE) == 0 + OCC - 1


def test_wait_before_never_negative() -> None:
    """Рекомендация п.1: «но не меньше нуля» — поздний приезд к давно освободившейся станции."""
    queue = _s1_queue(
        _occupant_entry(T1, NOW + 10),
        QueueEntry(T2, NOW + 20, NOW + 20, NOW + 20 + OCC, 0),
    )
    assert wait_before(queue, NOW + 1_000, CANDIDATE) == 0


def test_wait_before_own_entry_in_built_queue_does_not_change_wait() -> None:
    """Рекомендация п.1: своя запись в очереди не меняет ожидание (= её wait_seconds).

    Согласованный случай: занявшая ещё занимает станцию и стоит в очереди первой. Очередь
    строится вместе с самой машиной T4; ожидание по wait_before совпадает с wait_seconds её
    записи и с ожиданием по очереди без неё.
    """
    occ = _busy_s1(T1, NOW - 60)
    t4 = _track(T4, north_of(S1_POINT, 900.0))
    others = [_waiting_at_s1(T1, NOW - 60), _track(T2, north_of(S1_POINT, 1200.0))]
    with_t4 = build_station_queue(occ, [*others, t4], SITE, NOW)
    without_t4 = build_station_queue(occ, others, SITE, NOW)
    eta = estimate_arrival(t4, S1, occ, SITE, NOW)
    assert eta == NOW + 90

    own = next(e for e in with_t4.entries if e.unit_id == T4)
    assert wait_before(with_t4, eta, T4) == wait_before(without_t4, eta, T4) == own.wait_seconds
    assert own.wait_seconds == NOW - 60 + OCC - (NOW + 90)


def test_wait_before_past_release_not_counted_for_waiter_in_radius() -> None:
    """Рекомендация п.1: прошлое освобождение станции в ожидании не учитывается.

    S1 освободилась по времени в NOW-10 (занявшая отстояла, в очереди её нет). T2 ждёт в
    радиусе с NOW-100: в очереди её wait_seconds = 90 (начало с прежнего free_at), но
    wait_before считает только записи очереди — раньше T2 никто не приедет → ожидание 0.
    """
    occupied_at = NOW - OCC - 10
    occ = _busy_s1(T1, occupied_at)
    t2 = _waiting_at_s1(T2, NOW - 100, last_ts=NOW - 11)
    queue = build_station_queue(occ, [_waiting_at_s1(T1, occupied_at), t2], SITE, NOW)
    eta = estimate_arrival(t2, S1, occ, SITE, NOW)
    assert eta == NOW - 100
    assert queue.entries[0].wait_seconds == 90
    assert wait_before(queue, eta, T2) == 0
