"""Тесты геометрии: расстояние по прямой, время в пути, попадание в радиус.

ТЗ: «Расстояние считается по прямой между координатами», расчётная скорость 10 м/с,
радиусы — строгие («меньше 50 м», «ближе 1500 м»).
"""

from __future__ import annotations

import math

import pytest

from vqueue.domain.geo import EARTH_RADIUS_M, distance_m, is_within, travel_time_s
from vqueue.domain.model import Point, Rules

S1 = Point(49.14, 142.65)


def _north_of(origin: Point, meters: float) -> Point:
    """Точка на заданном расстоянии строго к северу (по меридиану)."""
    return Point(origin.lat + math.degrees(meters / EARTH_RADIUS_M), origin.lon)


# --- distance_m ------------------------------------------------------------


def test_distance_same_point_is_zero() -> None:
    """Расстояние от точки до самой себя — 0."""
    assert distance_m(S1, S1) == 0.0


def test_distance_is_symmetric() -> None:
    """Расстояние не зависит от порядка точек."""
    other = Point(49.16, 142.70)
    assert distance_m(S1, other) == pytest.approx(distance_m(other, S1), abs=1e-9)


def test_distance_one_degree_latitude() -> None:
    """1° по меридиану = π·R/180 ≈ 111 194.93 м."""
    expected = math.pi * EARTH_RADIUS_M / 180
    assert distance_m(Point(0.0, 0.0), Point(1.0, 0.0)) == pytest.approx(expected, abs=0.01)


def test_distance_one_degree_latitude_at_site() -> None:
    """Длина градуса меридиана не зависит от широты (сфера)."""
    expected = math.pi * EARTH_RADIUS_M / 180
    assert distance_m(Point(49.0, 142.6), Point(50.0, 142.6)) == pytest.approx(expected, abs=0.01)


def test_distance_along_parallel_scaled_by_cos_lat() -> None:
    """Малое смещение по долготе на широте φ сжато в cos φ раз."""
    lat = 49.14
    d_lon = 0.01
    expected = math.radians(d_lon) * EARTH_RADIUS_M * math.cos(math.radians(lat))
    assert distance_m(Point(lat, 142.65), Point(lat, 142.65 + d_lon)) == pytest.approx(
        expected, rel=1e-6
    )


@pytest.mark.parametrize("meters", [0.5, 15.0, 49.9, 50.0, 900.0, 1200.0, 1500.0, 3000.0])
def test_distance_small_meridian_offsets(meters: float) -> None:
    """Малые расстояния (масштаб радиусов ТЗ) считаются с точностью до миллиметра."""
    assert distance_m(S1, _north_of(S1, meters)) == pytest.approx(meters, abs=1e-3)


# --- travel_time_s ---------------------------------------------------------


@pytest.mark.parametrize(
    ("distance", "expected"),
    [(1200.0, 120), (3000.0, 300), (900.0, 90), (2400.0, 240)],
)
def test_travel_time_task_examples(distance: float, expected: int) -> None:
    """Примеры ТЗ при 36 км/ч: 1200 м → 120 с, 3000 м → 300 с, 900 м → 90 с, 2400 м → 240 с."""
    result = travel_time_s(distance, Rules().speed_mps)
    assert result == expected
    assert isinstance(result, int)


def test_travel_time_zero_distance_is_zero() -> None:
    """Машина в точке назначения приезжает мгновенно."""
    assert travel_time_s(0.0, 10.0) == 0


@pytest.mark.parametrize(
    ("distance", "expected"),
    [
        (1205.0, 121),  # 120.5 → вверх (не банковское округление к 120)
        (1215.0, 122),  # 121.5 → вверх
        (5.0, 1),  # 0.5 → вверх
        (1204.9, 120),  # 120.49 → вниз
        (1204.0, 120),  # 120.4 → вниз
        (1206.0, 121),  # 120.6 → вверх
    ],
)
def test_travel_time_rounds_half_up(distance: float, expected: int) -> None:
    """Округление к ближайшей секунде, половина — вверх."""
    assert travel_time_s(distance, 10.0) == expected


def test_travel_time_uses_given_speed() -> None:
    """Время пропорционально расстоянию и обратно пропорционально скорости."""
    assert travel_time_s(1000.0, 20.0) == 50


def test_travel_time_negative_distance_raises_value_error() -> None:
    """Отрицательное расстояние недопустимо."""
    with pytest.raises(ValueError):
        travel_time_s(-1.0, 10.0)


@pytest.mark.parametrize("speed", [0.0, -10.0])
def test_travel_time_non_positive_speed_raises_value_error(speed: float) -> None:
    """Скорость должна быть положительной."""
    with pytest.raises(ValueError):
        travel_time_s(100.0, speed)


# --- is_within -------------------------------------------------------------


def test_is_within_same_point_true() -> None:
    """Точка в собственном радиусе."""
    assert is_within(S1, S1, 50.0)


def test_is_within_inside_radius_true() -> None:
    """15 м от станции — в радиусе 50 м (пример T1 из ТЗ)."""
    assert is_within(_north_of(S1, 15.0), S1, 50.0)


def test_is_within_outside_radius_false() -> None:
    """60 м от станции — вне радиуса 50 м."""
    assert not is_within(_north_of(S1, 60.0), S1, 50.0)


def test_is_within_exact_boundary_false() -> None:
    """Ровно на границе — не внутри: условие строгое («меньше 50 м»)."""
    point = _north_of(S1, 50.0)
    assert not is_within(point, S1, distance_m(point, S1))


def test_is_within_just_beyond_boundary_true() -> None:
    """Радиус чуть больше расстояния — точка внутри."""
    point = _north_of(S1, 50.0)
    assert is_within(point, S1, distance_m(point, S1) + 1e-6)


def test_is_within_decision_radius_strict() -> None:
    """Радиус решения 1500 м тоже строгий («ближе 1500 м»)."""
    point = _north_of(S1, 1500.0)
    radius = distance_m(point, S1)
    assert not is_within(point, S1, radius)
    assert is_within(point, S1, radius + 1e-6)
    assert is_within(_north_of(S1, 1499.0), S1, 1500.0)
    assert not is_within(_north_of(S1, 1501.0), S1, 1500.0)


def test_is_within_is_symmetric() -> None:
    """Порядок точек не важен."""
    point = _north_of(S1, 40.0)
    assert is_within(point, S1, 50.0) == is_within(S1, point, 50.0)
