"""Свойства рекомендации станции на случайных площадках, очередях и позициях.

ТЗ, «Рекомендация», п.1–3 и «Коды отказа»: рекомендация только с выигрышем не меньше порога
и к чужой станции; выигрыш — ожидание у своей минус у выбранной; выбранная — с наименьшим
ожиданием среди станций в горизонте; результат детерминирован.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

from hypothesis import given
from hypothesis import strategies as st

from vqueue.domain.geo import EARTH_RADIUS_M
from vqueue.domain.model import Point, Rules, SiteConfig, Station, UnitPhase
from vqueue.domain.occupancy import StationOccupancy
from vqueue.domain.queue import QueueEntry, StationQueue, estimate_arrival, schedule, wait_before
from vqueue.domain.recommendation import (
    Recommendation,
    Rejection,
    RejectReason,
    recommend,
)
from vqueue.domain.unit_fsm import UnitTrack

_NOW = 1_789_473_600
_ORIGIN = Point(49.14, 142.65)
_HOME = "S0"
_UNIT = "T0"
_RULES = Rules()


def _shift(origin: Point, north_m: float, east_m: float) -> Point:
    """Точка, сдвинутая на north_m к северу и east_m к востоку (локально плоско)."""
    dlat = math.degrees(north_m / EARTH_RADIUS_M)
    dlon = math.degrees(east_m / (EARTH_RADIUS_M * math.cos(math.radians(origin.lat))))
    return Point(origin.lat + dlat, origin.lon + dlon)


@dataclass(frozen=True)
class _Case:
    """Случайный сценарий: площадка, машина у своей станции S0, очереди всех станций."""

    site: SiteConfig
    track: UnitTrack
    queues: dict[str, StationQueue]
    occupancies: dict[str, StationOccupancy]


_offsets = st.tuples(
    st.floats(min_value=-20_000.0, max_value=20_000.0, allow_nan=False),
    st.floats(min_value=-20_000.0, max_value=20_000.0, allow_nan=False),
)
_etas = st.integers(min_value=_NOW - 120, max_value=_NOW + 2_400)


@st.composite
def _queue(draw: st.DrawFn, station_id: str) -> StationQueue:
    """Корректная очередь станции: необязательная занявшая и расписание приезжающих."""
    occ = _RULES.occupancy_seconds
    free_from = draw(st.none() | st.integers(min_value=_NOW + 1, max_value=_NOW + occ))
    etas = draw(st.lists(_etas, max_size=6))
    arrivals = [(f"{station_id}-U{i}", eta) for i, eta in enumerate(etas)]
    entries: tuple[QueueEntry, ...] = schedule(arrivals, free_from, _RULES)
    if free_from is not None:
        head = QueueEntry(f"{station_id}-OCC", None, free_from - occ, free_from, 0)
        entries = (head, *entries)
    return StationQueue(station_id, _NOW, entries)


@st.composite
def _cases(draw: st.DrawFn) -> _Case:
    """Площадка из своей S0 и 1–4 чужих станций, машина «к станции» ближе 1500 м к S0."""
    others = draw(st.lists(_offsets, min_size=1, max_size=4))
    stations = [Station(_HOME, _ORIGIN)]
    stations += [Station(f"S{i + 1}", _shift(_ORIGIN, n, e)) for i, (n, e) in enumerate(others)]
    stations = draw(st.permutations(stations))
    site = SiteConfig(
        stations=tuple(stations),
        unload_point=_shift(_ORIGIN, -30_000.0, 0.0),
        assignments={_UNIT: _HOME},
        rules=_RULES,
    )
    radius = draw(st.floats(min_value=0.0, max_value=1_499.0, allow_nan=False))
    angle = draw(st.floats(min_value=0.0, max_value=2 * math.pi, allow_nan=False))
    track = UnitTrack(
        unit_id=_UNIT,
        last_ts=draw(st.integers(min_value=_NOW - 40, max_value=_NOW)),
        position=_shift(_ORIGIN, radius * math.cos(angle), radius * math.sin(angle)),
        phase=UnitPhase.TO_STATION,
        station_id=None,
        zone_entry=None,
        in_unload_zone=False,
    )
    queues = {s.station_id: draw(_queue(s.station_id)) for s in site.stations}
    occupancies = {s.station_id: StationOccupancy(s.station_id) for s in site.stations}
    return _Case(site, track, queues, occupancies)


def _waits(case: _Case) -> dict[str, int]:
    """Ожидания машины у станций в горизонте через те же estimate_arrival и wait_before.

    Оракул не независим от п.1: свойства проверяют сборку решения (выбор станции, выигрыш,
    порог, отказы), а сам расчёт ожидания (п.1) покрыт unit-тестами wait_before.
    """
    waits: dict[str, int] = {}
    for station in case.site.stations:
        sid = station.station_id
        eta = estimate_arrival(case.track, station, case.occupancies[sid], case.site, _NOW)
        if eta is not None:
            waits[sid] = wait_before(case.queues[sid], eta, case.track.unit_id)
    return waits


def _run(case: _Case) -> Recommendation | Rejection:
    """Вызов recommend для сценария на момент _NOW."""
    return recommend(case.track, case.queues, case.occupancies, case.site, _NOW)


def _is_fresh(case: _Case) -> bool:
    """Сообщение не старше «сейчас» больше чем на порог свежести."""
    return _NOW - case.track.last_ts <= case.site.rules.freshness_seconds


@given(_cases())
def test_recommendation_has_min_gain_and_other_station(case: _Case) -> None:
    """П.3: рекомендация ⇒ выигрыш ≥ 60, to_station ≠ from_station = своя, at = now."""
    decision = _run(case)
    if isinstance(decision, Recommendation):
        assert decision.gain_seconds >= case.site.rules.min_gain_seconds
        assert decision.to_station != decision.from_station
        assert decision.from_station == _HOME
        assert decision.unit_id == _UNIT
        assert decision.at == _NOW


@given(_cases())
def test_stale_iff_older_than_freshness(case: _Case) -> None:
    """Коды отказа: stale_telemetry тогда и только тогда, когда сообщение старше 30 с."""
    decision = _run(case)
    is_stale = isinstance(decision, Rejection) and decision.reason is RejectReason.STALE_TELEMETRY
    assert is_stale == (not _is_fresh(case))
    assert decision.at == _NOW


@given(_cases())
def test_gain_is_home_wait_minus_chosen_wait(case: _Case) -> None:
    """П.3: gain_seconds = ожидание у своей − ожидание у выбранной (сборка решения)."""
    decision = _run(case)
    if isinstance(decision, Recommendation):
        waits = _waits(case)
        assert decision.to_station in waits
        assert decision.gain_seconds == waits[_HOME] - waits[decision.to_station]


@given(_cases())
def test_chosen_station_has_minimal_wait(case: _Case) -> None:
    """П.2: выбранная станция — с наименьшим ожиданием среди участвующих (в горизонте)."""
    decision = _run(case)
    if isinstance(decision, Recommendation):
        waits = _waits(case)
        assert waits[decision.to_station] == min(waits.values())


@given(_cases())
def test_no_gain_iff_best_gain_below_threshold(case: _Case) -> None:
    """П.3: для свежего сообщения no_gain ⇔ своя − минимум ожиданий < 60."""
    if not _is_fresh(case):
        return
    decision = _run(case)
    waits = _waits(case)
    best_gain = waits[_HOME] - min(waits.values())
    rejected = isinstance(decision, Rejection)
    assert rejected == (best_gain < case.site.rules.min_gain_seconds)
    if isinstance(decision, Rejection):
        assert decision.reason is RejectReason.NO_GAIN


@given(_cases())
def test_decision_is_deterministic(case: _Case) -> None:
    """Детерминизм: повторный вызов и иной порядок ключей в отображениях — то же решение."""
    first = _run(case)
    queues_rev = dict(reversed(list(case.queues.items())))
    occ_rev = dict(reversed(list(case.occupancies.items())))
    assert _run(case) == first
    assert recommend(case.track, queues_rev, occ_rev, case.site, _NOW) == first
