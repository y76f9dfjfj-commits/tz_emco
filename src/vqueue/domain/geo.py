"""Геометрия: расстояние «по прямой», время в пути и попадание в радиус."""

from __future__ import annotations

import math
from typing import Final

from vqueue.domain.model import Point

EARTH_RADIUS_M: Final = 6_371_000.0
"""Средний радиус Земли, м."""


def distance_m(a: Point, b: Point) -> float:
    """Расстояние между точками по прямой (формула гаверсинусов).

    Args:
        a: Первая точка.
        b: Вторая точка.

    Returns:
        Расстояние в метрах.
    """
    lat1, lat2 = math.radians(a.lat), math.radians(b.lat)
    dlat = lat2 - lat1
    dlon = math.radians(b.lon - a.lon)
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    # min() защищает asin от погрешности округления чуть выше 1.
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(h)))


def travel_time_s(distance: float, speed_mps: float) -> int:
    """Время в пути в целых секундах, половина округляется вверх.

    Args:
        distance: Расстояние, м (не отрицательное).
        speed_mps: Скорость, м/с (положительная).

    Returns:
        Время в пути, с.

    Raises:
        ValueError: Если расстояние отрицательно или скорость не положительна.
    """
    if distance < 0:
        raise ValueError(f"Расстояние не может быть отрицательным: {distance!r}")
    if speed_mps <= 0:
        raise ValueError(f"Скорость должна быть > 0: {speed_mps!r}")
    return int(distance / speed_mps + 0.5)


def is_within(a: Point, b: Point, radius_m: float) -> bool:
    """Проверяет, что точки ближе заданного радиуса (строго меньше).

    Args:
        a: Первая точка.
        b: Вторая точка.
        radius_m: Радиус, м.

    Returns:
        True, если расстояние строго меньше радиуса.
    """
    return distance_m(a, b) < radius_m
