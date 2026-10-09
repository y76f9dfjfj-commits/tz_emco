"""Property-тесты геометрии на координатах площадки (lat 49.0–49.3, lon 142.5–142.8).

ТЗ: расстояние «по прямой между координатами», время в пути = расстояние / скорость.
"""

from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from vqueue.domain.geo import distance_m, travel_time_s
from vqueue.domain.model import Point

points = st.builds(
    Point,
    lat=st.floats(min_value=49.0, max_value=49.3, allow_nan=False, allow_infinity=False),
    lon=st.floats(min_value=142.5, max_value=142.8, allow_nan=False, allow_infinity=False),
)
distances = st.floats(min_value=0.0, max_value=100_000.0, allow_nan=False, allow_infinity=False)
speeds = st.floats(min_value=0.1, max_value=50.0, allow_nan=False, allow_infinity=False)


@given(points, points)
def test_distance_symmetric(a: Point, b: Point) -> None:
    """d(a, b) = d(b, a)."""
    assert abs(distance_m(a, b) - distance_m(b, a)) <= 1e-6


@given(points, points)
def test_distance_non_negative(a: Point, b: Point) -> None:
    """Расстояние неотрицательно."""
    assert distance_m(a, b) >= 0.0


@given(points)
def test_distance_to_self_is_zero(a: Point) -> None:
    """d(a, a) = 0."""
    assert distance_m(a, a) == 0.0


@given(points, points, points)
def test_distance_triangle_inequality(a: Point, b: Point, c: Point) -> None:
    """d(a, c) ≤ d(a, b) + d(b, c) с допуском 1e-6 м."""
    assert distance_m(a, c) <= distance_m(a, b) + distance_m(b, c) + 1e-6


@given(distances, distances, speeds)
def test_travel_time_monotonic_in_distance(d1: float, d2: float, speed: float) -> None:
    """Большее расстояние не даёт меньшего времени в пути."""
    low, high = sorted((d1, d2))
    assert travel_time_s(low, speed) <= travel_time_s(high, speed)


@given(distances, speeds)
def test_travel_time_within_half_second_of_exact(distance: float, speed: float) -> None:
    """Округление отклоняется от точного значения не больше чем на полсекунды."""
    result = travel_time_s(distance, speed)
    assert result >= 0
    assert abs(result - distance / speed) <= 0.5 + 1e-9
