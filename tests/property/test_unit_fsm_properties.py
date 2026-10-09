"""Property-тесты инвариантов состояния машины (UnitTrack) после каждого шага advance.

ТЗ, таблица состояний: «на станции» — скорость меньше 1 км/ч и расстояние до станции
меньше 50 м; «к разгрузке» — вышла из радиуса станции; «к станции» — вышла из радиуса
точки разгрузки. Правило станций 3: приезд — момент входа в радиус станции.
"""

from __future__ import annotations

import math

from hypothesis import given
from hypothesis import strategies as st

from tests.sitekit import FAR_POINT, S1_POINT, S2_POINT, S3_POINT, UNLOAD_POINT, make_site, tm
from vqueue.domain.geo import EARTH_RADIUS_M, is_within
from vqueue.domain.model import Point, Telemetry, UnitPhase
from vqueue.domain.unit_fsm import UnitTrack, advance, station_in_zone

SITE = make_site()
RADIUS = SITE.rules.zone_radius_m

ANCHORS = (S1_POINT, S2_POINT, S3_POINT, UNLOAD_POINT, FAR_POINT)
"""Опорные точки: станции, точка разгрузки и точка в пути."""

T_START = 1_789_473_000

# Угол в градусах на 1 м вдоль меридиана и (приближённо) вдоль параллели.
_DEG_PER_M = 180.0 / (math.pi * EARTH_RADIUS_M)


@st.composite
def positions(draw: st.DrawFn) -> Point:
    """Опорная точка + сдвиг 0–120 м по северу и востоку: часто внутри радиусов и у их краёв."""
    anchor = draw(st.sampled_from(ANCHORS))
    north = draw(st.floats(min_value=-120.0, max_value=120.0, allow_nan=False))
    east = draw(st.floats(min_value=-120.0, max_value=120.0, allow_nan=False))
    return Point(anchor.lat + north * _DEG_PER_M, anchor.lon + east * _DEG_PER_M * 1.53)


speeds = st.one_of(
    st.sampled_from((0.0, 0.99, 1.0, 1.01)),
    st.floats(min_value=0.0, max_value=40.0, allow_nan=False),
)


@st.composite
def ordered_stream(draw: st.DrawFn) -> list[Telemetry]:
    """Поток сообщений одной машины со строго возрастающими ts (шаг 1–30 с)."""
    n = draw(st.integers(min_value=1, max_value=40))
    result: list[Telemetry] = []
    ts = T_START
    for _ in range(n):
        ts += draw(st.integers(min_value=1, max_value=30))
        result.append(tm(ts, draw(positions()), draw(speeds)))
    return result


def _check_invariants(track: UnitTrack) -> None:
    """Проверяет инварианты UnitTrack относительно площадки SITE."""
    assert (track.station_id is not None) == (track.phase is UnitPhase.AT_STATION)
    if track.phase is UnitPhase.AT_STATION:
        assert track.zone_entry is not None
        assert track.zone_entry.station_id == track.station_id
    zone = station_in_zone(track.position, SITE)
    if zone is None:
        assert track.zone_entry is None
    else:
        assert track.zone_entry is not None
        assert track.zone_entry.station_id == zone
        assert track.zone_entry.entered_at <= track.last_ts
    assert track.in_unload_zone == is_within(track.position, SITE.unload_point, RADIUS)


@given(ordered_stream())
def test_unit_track_invariants_hold_after_every_step(stream: list[Telemetry]) -> None:
    """После каждого шага advance выполняются инварианты фазы, зоны станции и зоны разгрузки."""
    track: UnitTrack | None = None
    for msg in stream:
        track = advance(track, msg, SITE)
        assert track.last_ts == msg.ts
        assert track.position == msg.position
        _check_invariants(track)


@given(ordered_stream())
def test_stopped_in_station_radius_implies_at_that_station(stream: list[Telemetry]) -> None:
    """«Стоит» (< 1 км/ч) в радиусе станции — всегда «на станции» именно этой станции."""
    track: UnitTrack | None = None
    for msg in stream:
        track = advance(track, msg, SITE)
        zone = station_in_zone(msg.position, SITE)
        if zone is not None and msg.speed_kmh < SITE.rules.stopped_speed_kmh:
            assert track.phase is UnitPhase.AT_STATION
            assert track.station_id == zone
