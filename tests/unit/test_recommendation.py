"""Тесты рекомендации станции.

ТЗ, «Правила расчёта → Рекомендация»:
- считается, когда машина в состоянии «к станции» оказывается ближе 1500 м к своей станции
  («впервые за заезд» — забота агрегата, здесь проверяется только точка решения);
1. ожидание у станции — момент освобождения минус приезд машины, не меньше нуля;
2. выбирается станция с наименьшим ожиданием; при равенстве — своя, затем ближайшая
   (затем — по идентификатору станции, для детерминизма);
3. выигрыш — ожидание у своей минус ожидание у выбранной; меньше 60 с — отказ no_gain.

«Коды отказа»: no_gain — выигрыш меньше порога; stale_telemetry — сообщение, вызвавшее
расчёт, старше «сейчас» больше чем на 30 с. Станция, приезд к которой за горизонтом 30 минут,
в выборе не участвует.

Геометрия по умолчанию (sitekit): S1 в начале координат, S2 в 2 км к северу, S3 в 4 км к
северу, разгрузка в 3 км к югу. Машина T4 в 900 м к югу от S1: до S1 900 м (90 с),
до S2 2900 м (290 с), до S3 4900 м (490 с).
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import replace
from typing import Final

import pytest

from tests.sitekit import (
    S1_ID,
    S1_POINT,
    S2_ID,
    S3_ID,
    UNLOAD_POINT,
    make_site,
    north_of,
)
from vqueue.domain.geo import EARTH_RADIUS_M, distance_m
from vqueue.domain.model import Point, Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy
from vqueue.domain.queue import QueueEntry, StationQueue
from vqueue.domain.recommendation import (
    Decision,
    Recommendation,
    Rejection,
    RejectReason,
    is_decision_point,
    recommend,
)
from vqueue.domain.unit_fsm import StationZoneEntry, UnitTrack

T4: Final = "T4"


def _assign_t4(site: SiteConfig, home: str = S1_ID) -> SiteConfig:
    """Та же площадка, но машина T4 закреплена за станцией home (своя станция T4)."""
    return replace(site, assignments={T4: home})


SITE: Final = _assign_t4(make_site())
"""Площадка sitekit, T4 закреплена за S1."""
RULES: Final = SITE.rules
OCC: Final = RULES.occupancy_seconds
S1: Final = SITE.station(S1_ID)

NOW: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — «сейчас»."""

T4_POSITION: Final = north_of(S1_POINT, -900.0)
"""T4 в 900 м к югу от S1."""

ETA_S1: Final = NOW + 90
ETA_S2: Final = NOW + 290
ETA_S3: Final = NOW + 490


def east_of(origin: Point, meters: float) -> Point:
    """Точка на заданном расстоянии к востоку (отрицательное — к западу) по параллели.

    Сдвиги на ±meters от одной точки дают в точности равные расстояния до неё.
    """
    dlon = math.degrees(meters / (EARTH_RADIUS_M * math.cos(math.radians(origin.lat))))
    return Point(origin.lat, origin.lon + dlon)


def _track(
    position: Point = T4_POSITION,
    *,
    unit_id: str = T4,
    last_ts: int = NOW,
    phase: UnitPhase = UnitPhase.TO_STATION,
) -> UnitTrack:
    """Трек машины вне радиусов станций, заданный напрямую."""
    return UnitTrack(
        unit_id=unit_id,
        last_ts=last_ts,
        position=position,
        phase=phase,
        station_id=None,
        zone_entry=None,
        in_unload_zone=False,
    )


def _occupant(unit_id: str, free_at: int) -> QueueEntry:
    """Запись занявшей станцию машины, освобождающей её в free_at."""
    return QueueEntry(unit_id, None, free_at - OCC, free_at, 0)


def _arrival(unit_id: str, eta: int, start: int | None = None) -> QueueEntry:
    """Запись едущей машины: приезд eta, начало обслуживания start (по умолчанию eta)."""
    begin = eta if start is None else start
    return QueueEntry(unit_id, eta, begin, begin + OCC, begin - eta)


def _queues(
    site: SiteConfig, entries: Mapping[str, tuple[QueueEntry, ...]] | None = None
) -> dict[str, StationQueue]:
    """Очереди всех станций площадки на NOW из записей по станциям; не указанные — пустые."""
    given = entries if entries is not None else {}
    return {
        s.station_id: StationQueue(s.station_id, NOW, given.get(s.station_id, ()))
        for s in site.stations
    }


def _free(site: SiteConfig) -> dict[str, StationOccupancy]:
    """Занятость всех станций площадки — все свободны."""
    return {s.station_id: StationOccupancy(s.station_id) for s in site.stations}


def _recommend(
    queues: Mapping[str, StationQueue],
    *,
    track: UnitTrack | None = None,
    site: SiteConfig = SITE,
    now: int = NOW,
) -> Decision:
    """Вызов recommend для T4 (по умолчанию) со свободной занятостью всех станций."""
    return recommend(
        track if track is not None else _track(),
        queues,
        _free(site),
        site,
        now,
    )


def _site(*stations: Station, rules: Rules | None = None, home: str = S1_ID) -> SiteConfig:
    """Площадка из заданных станций; разгрузка — как в sitekit, T4 закреплена за home."""
    return SiteConfig(
        stations=stations,
        unload_point=UNLOAD_POINT,
        assignments={T4: home},
        rules=rules if rules is not None else Rules(),
    )


def test_geometry_of_default_site() -> None:
    """Опорная геометрия тестов: T4 в 900/2900/4900 м от S1/S2/S3."""
    assert round(distance_m(T4_POSITION, S1.location), 3) == 900.0
    assert round(distance_m(T4_POSITION, SITE.station(S2_ID).location), 3) == 2900.0
    assert round(distance_m(T4_POSITION, SITE.station(S3_ID).location), 3) == 4900.0


# ---------------------------------------------------------------------------
# Точка решения: «к станции» и ближе 1500 м к своей станции
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("meters", [0.0, 60.0, 900.0, 1499.0])
def test_decision_point_to_station_closer_than_1500_true(meters: float) -> None:
    """Рекомендация: «к станции» ближе 1500 м к своей — точка решения."""
    assert is_decision_point(_track(north_of(S1_POINT, -meters)), S1, RULES)


@pytest.mark.parametrize("meters", [1501.0, 3000.0])
def test_decision_point_to_station_farther_than_1500_false(meters: float) -> None:
    """Рекомендация: дальше 1500 м от своей станции — решения нет."""
    assert not is_decision_point(_track(north_of(S1_POINT, -meters)), S1, RULES)


def test_decision_point_exactly_at_radius_false() -> None:
    """Рекомендация: «ближе 1500 м» строго — ровно на радиусе решения не считается.

    Радиус задаётся ровно равным вычисленному расстоянию, чтобы исключить погрешность.
    """
    position = north_of(S1_POINT, -1500.0)
    exact = distance_m(position, S1.location)
    assert round(exact, 6) == 1500.0
    on_border = Rules(decision_radius_m=exact)
    just_beyond = Rules(decision_radius_m=exact + 1e-6)

    assert not is_decision_point(_track(position), S1, on_border)
    assert is_decision_point(_track(position), S1, just_beyond)


@pytest.mark.parametrize("phase", [UnitPhase.AT_STATION, UnitPhase.TO_UNLOAD])
def test_decision_point_not_to_station_phase_false(phase: UnitPhase) -> None:
    """Рекомендация: только в состоянии «к станции»; на станции и к разгрузке — нет."""
    assert not is_decision_point(_track(north_of(S1_POINT, -300.0), phase=phase), S1, RULES)


def test_decision_point_radius_taken_from_rules() -> None:
    """Рекомендация: радиус решения берётся из Rules (1000 м: 800 м — да, 1200 м — нет)."""
    rules = Rules(decision_radius_m=1000.0)
    assert is_decision_point(_track(north_of(S1_POINT, -800.0)), S1, rules)
    assert not is_decision_point(_track(north_of(S1_POINT, -1200.0)), S1, rules)


def test_decision_point_measured_to_given_home_station() -> None:
    """Рекомендация: расстояние меряется до переданной своей станции, а не до ближайшей."""
    near_s2 = north_of(SITE.station(S2_ID).location, -300.0)
    assert not is_decision_point(_track(near_s2), S1, RULES)
    assert is_decision_point(_track(near_s2), SITE.station(S2_ID), RULES)


# ---------------------------------------------------------------------------
# Отказ stale_telemetry
# ---------------------------------------------------------------------------


def _gain_queues() -> dict[str, StationQueue]:
    """Очереди с большим выигрышем: S1 занята до NOW+500, остальные свободны."""
    return _queues(SITE, {S1_ID: (_occupant("T1", NOW + 500),)})


def test_stale_31s_rejected_stale_telemetry() -> None:
    """Коды отказа: сообщение старше «сейчас» на 31 с → stale_telemetry (даже при выигрыше)."""
    decision = _recommend(_gain_queues(), track=_track(last_ts=NOW - 31))
    assert decision == Rejection(T4, NOW, RejectReason.STALE_TELEMETRY)


def test_stale_exactly_30s_still_calculated() -> None:
    """Коды отказа: «больше чем на 30 с» — ровно 30 с ещё свежее, расчёт идёт.

    Приезд к S1 считается от last_ts: NOW-30+90 = NOW+60, ожидание 500-60 = 440;
    к S2 — NOW-30+290, станция свободна → выигрыш 440.
    """
    decision = _recommend(_gain_queues(), track=_track(last_ts=NOW - 30))
    assert decision == Recommendation(T4, NOW, S1_ID, S2_ID, 440)


def test_stale_takes_precedence_over_no_gain() -> None:
    """Коды отказа: устаревшее сообщение без выигрыша — всё равно stale_telemetry."""
    decision = _recommend(_queues(SITE), track=_track(last_ts=NOW - 100))
    assert decision == Rejection(T4, NOW, RejectReason.STALE_TELEMETRY)


def test_stale_threshold_taken_from_rules() -> None:
    """Коды отказа: порог свежести берётся из Rules (10 с: 11 с — отказ, 10 с — расчёт)."""
    site = _assign_t4(make_site(Rules(freshness_seconds=10)))
    queues = _queues(site, {S1_ID: (_occupant("T1", NOW + 500),)})
    stale = _recommend(queues, track=_track(last_ts=NOW - 11), site=site)
    fresh = _recommend(queues, track=_track(last_ts=NOW - 10), site=site)
    assert stale == Rejection(T4, NOW, RejectReason.STALE_TELEMETRY)
    assert isinstance(fresh, Recommendation)


# ---------------------------------------------------------------------------
# Отказ no_gain и порог выигрыша
# ---------------------------------------------------------------------------


def test_no_gain_all_stations_free_home_wins_tie() -> None:
    """П.2–3: все станции свободны — ожидания равны 0, при равенстве своя → no_gain."""
    assert _recommend(_queues(SITE)) == Rejection(T4, NOW, RejectReason.NO_GAIN)


def test_no_gain_home_has_smallest_wait() -> None:
    """П.3: у своей станции наименьшее ожидание → no_gain."""
    queues = _queues(
        SITE,
        {
            S2_ID: (_occupant("T1", ETA_S2 + 100),),
            S3_ID: (_occupant("T2", ETA_S3 + 100),),
        },
    )
    assert _recommend(queues) == Rejection(T4, NOW, RejectReason.NO_GAIN)


def test_no_gain_equal_positive_waits_home_wins_tie() -> None:
    """П.2: ожидание своей равно ожиданию чужой (по 100 с) — выбирается своя, no_gain."""
    queues = _queues(
        SITE,
        {
            S1_ID: (_occupant("T1", ETA_S1 + 100),),
            S2_ID: (_occupant("T2", ETA_S2 + 100),),
            S3_ID: (_occupant("T3", ETA_S3 + 100),),
        },
    )
    assert _recommend(queues) == Rejection(T4, NOW, RejectReason.NO_GAIN)


def test_no_gain_gain_59_rejected() -> None:
    """П.3: выигрыш 59 с — меньше 60 → no_gain."""
    queues = _queues(SITE, {S1_ID: (_occupant("T1", ETA_S1 + 59),)})
    assert _recommend(queues) == Rejection(T4, NOW, RejectReason.NO_GAIN)


def test_gain_exactly_60_recommended() -> None:
    """П.3: «меньше 60 с» — отказ; ровно 60 с — рекомендация."""
    queues = _queues(SITE, {S1_ID: (_occupant("T1", ETA_S1 + 60),)})
    assert _recommend(queues) == Recommendation(T4, NOW, S1_ID, S2_ID, 60)


def test_gain_threshold_taken_from_rules() -> None:
    """П.3: порог выигрыша берётся из Rules (100 с: 99 — отказ, 100 — рекомендация)."""
    site = _assign_t4(make_site(Rules(min_gain_seconds=100)))
    below = _queues(site, {S1_ID: (_occupant("T1", ETA_S1 + 99),)})
    at = _queues(site, {S1_ID: (_occupant("T1", ETA_S1 + 100),)})
    assert _recommend(below, site=site) == Rejection(T4, NOW, RejectReason.NO_GAIN)
    assert _recommend(at, site=site) == Recommendation(T4, NOW, S1_ID, S2_ID, 100)


def test_gain_counted_against_best_not_nearest() -> None:
    """П.2–3: выбирается наименьшее ожидание, а не ближайшая станция.

    S1 ждать 300 с, S2 (ближе) — 200 с, S3 (дальше) — 0 с → S3, выигрыш 300.
    """
    queues = _queues(
        SITE,
        {
            S1_ID: (_occupant("T1", ETA_S1 + 300),),
            S2_ID: (_occupant("T2", ETA_S2 + 200),),
        },
    )
    assert _recommend(queues) == Recommendation(T4, NOW, S1_ID, S3_ID, 300)


def test_gain_is_home_wait_minus_best_wait() -> None:
    """П.3: выигрыш = ожидание у своей минус ожидание у выбранной (300 - 70 = 230)."""
    queues = _queues(
        SITE,
        {
            S1_ID: (_occupant("T1", ETA_S1 + 300),),
            S2_ID: (_occupant("T2", ETA_S2 + 70),),
            S3_ID: (_occupant("T3", ETA_S3 + 150),),
        },
    )
    assert _recommend(queues) == Recommendation(T4, NOW, S1_ID, S2_ID, 230)


def test_wait_at_other_station_counts_only_earlier_arrivals() -> None:
    """П.1: у чужой станции учитываются только машины, приезжающие раньше нашей.

    S2: T5 приедет раньше (освободит ETA_S2+40), T6 — позже (освободит ETA_S2+500)
    → ожидание у S2 40 с; S3 свободна, но S1 ждать 100 → S3 с выигрышем 100.
    Без S3 (площадка S1, S2) — S2 с выигрышем 60.
    """
    s2_entries = (
        _arrival("T5", ETA_S2 + 40 - OCC),
        _arrival("T6", ETA_S2 + 1, ETA_S2 + 40),
    )
    home = (_occupant("T1", ETA_S1 + 100),)
    queues = _queues(SITE, {S1_ID: home, S2_ID: s2_entries})
    assert _recommend(queues) == Recommendation(T4, NOW, S1_ID, S3_ID, 100)

    two = _site(S1, SITE.station(S2_ID))
    queues2 = _queues(two, {S1_ID: home, S2_ID: s2_entries})
    assert _recommend(queues2, site=two) == Recommendation(T4, NOW, S1_ID, S2_ID, 60)


# ---------------------------------------------------------------------------
# Выбор при равенстве ожиданий: ближайшая, затем по station_id
# ---------------------------------------------------------------------------


def test_tie_between_foreign_stations_nearest_wins() -> None:
    """П.2: у S2 и S3 ожидание 0 — выбирается ближайшая S2 (2900 м против 4900 м)."""
    queues = _queues(SITE, {S1_ID: (_occupant("T1", ETA_S1 + 100),)})
    assert _recommend(queues) == Recommendation(T4, NOW, S1_ID, S2_ID, 100)


def test_tie_nearest_wins_regardless_of_id_and_config_order() -> None:
    """П.2: ближайшая выигрывает, даже если её id «больше» и она позже в конфигурации."""
    far = Station("SA", north_of(T4_POSITION, 3000.0))
    near = Station("SZ", north_of(T4_POSITION, -2000.0))
    site = _site(S1, far, near)
    queues = _queues(site, {S1_ID: (_occupant("T1", ETA_S1 + 100),)})
    assert _recommend(queues, site=site) == Recommendation(T4, NOW, S1_ID, "SZ", 100)


def test_tie_equal_wait_and_distance_by_station_id() -> None:
    """П.2 + детерминизм: равны ожидания и расстояния — выбор по station_id (меньший).

    SB к востоку и SA к западу от T4 на 2000 м: расстояния в точности равны.
    """
    sb = Station("SB", east_of(T4_POSITION, 2000.0))
    sa = Station("SA", east_of(T4_POSITION, -2000.0))
    assert distance_m(T4_POSITION, sa.location) == distance_m(T4_POSITION, sb.location)
    home = (_occupant("T1", ETA_S1 + 100),)

    for site in (_site(S1, sb, sa), _site(sa, S1, sb), _site(sb, sa, S1)):
        queues = _queues(site, {S1_ID: home})
        assert _recommend(queues, site=site) == Recommendation(T4, NOW, S1_ID, "SA", 100)


# ---------------------------------------------------------------------------
# Горизонт: станция с приездом позже 30 минут не участвует
# ---------------------------------------------------------------------------


def test_station_beyond_horizon_not_considered_even_if_free() -> None:
    """Очередь п.4: приезд к станции позже горизонта — станция не участвует, даже свободная.

    SFAR в 18 010 м от T4 (1801 с > 1800 с), очереди нет; S1 ждать 300 → no_gain.
    """
    far = Station("SFAR", north_of(T4_POSITION, -18_010.0))
    site = _site(S1, far)
    queues = _queues(site, {S1_ID: (_occupant("T1", ETA_S1 + 300),)})
    assert _recommend(queues, site=site) == Rejection(T4, NOW, RejectReason.NO_GAIN)


def test_station_beyond_horizon_skipped_next_best_chosen() -> None:
    """Очередь п.4: за горизонтом свободная SFAR пропускается, выбирается S2 (ждать 50)."""
    far = Station("SFAR", north_of(T4_POSITION, -18_010.0))
    site = _site(S1, SITE.station(S2_ID), far)
    queues = _queues(
        site,
        {
            S1_ID: (_occupant("T1", ETA_S1 + 300),),
            S2_ID: (_occupant("T2", ETA_S2 + 50),),
        },
    )
    assert _recommend(queues, site=site) == Recommendation(T4, NOW, S1_ID, S2_ID, 250)


def test_station_exactly_at_horizon_considered() -> None:
    """Очередь п.4: приезд ровно через 1800 с — ещё в горизонте, станция участвует."""
    edge = Station("SEDGE", north_of(T4_POSITION, -18_000.0))
    site = _site(S1, edge)
    queues = _queues(site, {S1_ID: (_occupant("T1", ETA_S1 + 300),)})
    assert _recommend(queues, site=site) == Recommendation(T4, NOW, S1_ID, "SEDGE", 300)


# ---------------------------------------------------------------------------
# Поля решения
# ---------------------------------------------------------------------------


def test_recommendation_fields() -> None:
    """decision.v1: unit, at = now, from_station = своя, to_station, gain_seconds."""
    queues = _queues(SITE, {S1_ID: (_occupant("T1", ETA_S1 + 120),)})
    decision = _recommend(queues)
    assert isinstance(decision, Recommendation)
    assert decision.unit_id == T4
    assert decision.at == NOW
    assert decision.from_station == S1_ID
    assert decision.to_station == S2_ID
    assert decision.gain_seconds == 120


def test_decision_at_is_now_not_last_ts() -> None:
    """decision.v1: at — «сейчас», а не ts сообщения (last_ts на 20 с раньше)."""
    queues = _queues(SITE, {S1_ID: (_occupant("T1", NOW + 500),)})
    decision = _recommend(queues, track=_track(last_ts=NOW - 20))
    assert isinstance(decision, Recommendation)
    assert decision.at == NOW


def test_rejection_fields_at_is_now() -> None:
    """decision.v1 (отказ): unit, at = now, reason; at — «сейчас», а не ts сообщения."""
    decision = _recommend(_queues(SITE), track=_track(last_ts=NOW - 5))
    assert isinstance(decision, Rejection)
    assert decision.unit_id == T4
    assert decision.at == NOW
    assert decision.reason is RejectReason.NO_GAIN


def test_reject_reason_codes_match_task() -> None:
    """Коды отказа ТЗ: no_gain и stale_telemetry (строковые значения для decision.v1)."""
    assert RejectReason("no_gain") is RejectReason.NO_GAIN
    assert RejectReason("stale_telemetry") is RejectReason.STALE_TELEMETRY
    assert {r.value for r in RejectReason} == {"no_gain", "stale_telemetry"}


def test_from_station_is_assigned_home_station() -> None:
    """П.3: from_station — своя станция по закреплению (T4 за S2), выигрыш считается от неё.

    T4 в 900 м к югу от S2: до S2 90 с, до S1 110 с, до S3 290 с. S2 ждать 200,
    S3 занята до NOW+2000 (ждать 1710), S1 свободна → S1, выигрыш 200.
    """
    s2 = SITE.station(S2_ID)
    track = _track(north_of(s2.location, -900.0))
    queues = _queues(
        SITE,
        {
            S2_ID: (_occupant("T1", NOW + 90 + 200),),
            S3_ID: (_occupant("T2", NOW + 2_000),),
        },
    )
    site = _assign_t4(SITE, S2_ID)
    decision = _recommend(queues, track=track, site=site)
    assert decision == Recommendation(T4, NOW, S2_ID, S1_ID, 200)


# ---------------------------------------------------------------------------
# Своя очередь может содержать саму машину
# ---------------------------------------------------------------------------


def test_own_queue_with_or_without_unit_gives_same_decision() -> None:
    """П.1: своя запись в очереди своей станции не влияет на её ожидание.

    S1: T1 занята до NOW+200; T4 приедет NOW+90, начнёт NOW+200 (ждать 110); T5 приедет
    позже. Решение одинаково с записью T4 и без неё: S2, выигрыш 110.
    """
    occupant = _occupant("T1", NOW + 200)
    own = _arrival(T4, ETA_S1, NOW + 200)
    later = _arrival("T5", NOW + 300, NOW + 200 + OCC)
    with_unit = _queues(SITE, {S1_ID: (occupant, own, later)})
    without_unit = _queues(SITE, {S1_ID: (occupant, _arrival("T5", NOW + 300))})

    expected = Recommendation(T4, NOW, S1_ID, S2_ID, 110)
    assert _recommend(with_unit) == expected
    assert _recommend(without_unit) == expected


def test_own_queue_with_unit_no_gain_same_as_without() -> None:
    """П.1: и при отказе своя запись не меняет результат (S1 ждать 30 → no_gain)."""
    occupant = _occupant("T1", ETA_S1 + 30)
    own = _arrival(T4, ETA_S1, ETA_S1 + 30)
    with_unit = _queues(SITE, {S1_ID: (occupant, own)})
    without_unit = _queues(SITE, {S1_ID: (occupant,)})

    expected = Rejection(T4, NOW, RejectReason.NO_GAIN)
    assert _recommend(with_unit) == expected
    assert _recommend(without_unit) == expected


def test_unit_waiting_in_home_radius_arrival_is_zone_entry() -> None:
    """П.1 + правило 3 занятия: ждущая в радиусе своей станции машина «приехала» при входе.

    T4 в 20 м от S1 (вошла в радиус в NOW-10), S1 занята T1 до NOW+150 — приезд к S1
    NOW-10, ожидание 160; S2 свободна → S2, выигрыш 160. Занятость передаётся в recommend.
    """
    track = UnitTrack(
        unit_id=T4,
        last_ts=NOW,
        position=north_of(S1_POINT, 20.0),
        phase=UnitPhase.TO_STATION,
        station_id=None,
        zone_entry=StationZoneEntry(S1_ID, NOW - 10),
        in_unload_zone=False,
    )
    occupancies = _free(SITE)
    occupancies[S1_ID] = StationOccupancy(
        S1_ID,
        Occupant("T1", NOW + 150 - OCC, NOW + 150 - OCC, NOW + 150),
        served_visits=frozenset({("T1", NOW + 150 - OCC)}),
    )
    queues = _queues(SITE, {S1_ID: (_occupant("T1", NOW + 150),)})
    decision = recommend(track, queues, occupancies, SITE, NOW)
    assert decision == Recommendation(T4, NOW, S1_ID, S2_ID, 160)


def test_unit_in_radius_of_other_station_arrival_there_is_zone_entry() -> None:
    """П.1 + правило 3 занятия: машина «к станции» в радиусе ЧУЖОЙ S2 по пути к своей S1.

    T4 в 20 м к югу от S2 (вошла в радиус в NOW-40): до своей S1 1980 м — приезд NOW+198,
    S1 занята до NOW+600 → ждать 402. S2 занята до NOW+100: приезд к S2 — момент входа
    NOW-40, ждать 140 (а не 100, как было бы при приезде «сейчас»). До S3 2020 м — приезд
    NOW+202, занята до NOW+400 → ждать 198. Выбор S2, выигрыш 402 - 140 = 262.
    """
    s2 = SITE.station(S2_ID)
    track = UnitTrack(
        unit_id=T4,
        last_ts=NOW,
        position=north_of(s2.location, -20.0),
        phase=UnitPhase.TO_STATION,
        station_id=None,
        zone_entry=StationZoneEntry(S2_ID, NOW - 40),
        in_unload_zone=False,
    )
    queues = _queues(
        SITE,
        {
            S1_ID: (_occupant("T1", NOW + 600),),
            S2_ID: (_occupant("T2", NOW + 100),),
            S3_ID: (_occupant("T3", NOW + 400),),
        },
    )
    assert _recommend(queues, track=track) == Recommendation(T4, NOW, S1_ID, S2_ID, 262)


# ---------------------------------------------------------------------------
# Ошибки вызывающего: машина не закреплена, своя станция вне горизонта
# ---------------------------------------------------------------------------


def test_unassigned_unit_raises_value_error() -> None:
    """Своя станция берётся из закрепления; незакреплённая машина — ошибка вызывающего."""
    with pytest.raises(ValueError):
        _recommend(_queues(SITE), track=_track(unit_id="T9"))


def test_unassigned_unit_raises_even_if_stale() -> None:
    """Порядок проверок: незакреплённость проверяется раньше свежести."""
    with pytest.raises(ValueError):
        _recommend(_queues(SITE), track=_track(unit_id="T9", last_ts=NOW - 100))


def test_home_beyond_horizon_raises_value_error() -> None:
    """Своя станция вне горизонта (18 010 м, 1801 с) — расчёт не в точке решения, ошибка.

    Не отказ no_gain: своя станция обязана участвовать в выборе.
    """
    track = _track(north_of(S1_POINT, -18_010.0))
    with pytest.raises(ValueError):
        _recommend(_queues(SITE), track=track)


def test_home_beyond_horizon_but_stale_rejected_stale_telemetry() -> None:
    """Порядок проверок: несвежий трек отклоняется stale_telemetry раньше проверки горизонта."""
    track = _track(north_of(S1_POINT, -18_010.0), last_ts=NOW - 31)
    decision = _recommend(_queues(SITE), track=track)
    assert decision == Rejection(T4, NOW, RejectReason.STALE_TELEMETRY)
