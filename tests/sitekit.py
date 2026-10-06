"""Небольшая тестовая площадка и построение точек на заданном расстоянии.

Три станции и точка разгрузки разнесены на километры, чтобы радиусы 50 м не пересекались.
Точки строятся сдвигом по широте (вдоль меридиана): 1 м ≈ 1/111194.93 градуса.
"""

from __future__ import annotations

import math
from typing import Final

from vqueue.domain.geo import EARTH_RADIUS_M
from vqueue.domain.model import Point, Rules, SiteConfig, Station, Telemetry

S1_ID: Final = "S1"
S2_ID: Final = "S2"
S3_ID: Final = "S3"
UNIT_ID: Final = "T1"

S1_POINT: Final = Point(49.14, 142.65)


def north_of(origin: Point, meters: float) -> Point:
    """Точка на заданном расстоянии к северу (отрицательное — к югу) по меридиану."""
    return Point(origin.lat + math.degrees(meters / EARTH_RADIUS_M), origin.lon)


S2_POINT: Final = north_of(S1_POINT, 2_000.0)
S3_POINT: Final = north_of(S1_POINT, 4_000.0)
UNLOAD_POINT: Final = north_of(S1_POINT, -3_000.0)
"""Точка разгрузки в 3 км к югу от S1 (5 и 7 км от S2 и S3)."""

FAR_POINT: Final = north_of(S1_POINT, -1_500.0)
"""Точка «в пути»: вне радиусов всех станций и точки разгрузки."""


def make_site(rules: Rules | None = None) -> SiteConfig:
    """Площадка: станции S1, S2, S3 и точка разгрузки; машина T1 закреплена за S1."""
    return SiteConfig(
        stations=(
            Station(S1_ID, S1_POINT),
            Station(S2_ID, S2_POINT),
            Station(S3_ID, S3_POINT),
        ),
        unload_point=UNLOAD_POINT,
        assignments={UNIT_ID: S1_ID},
        rules=rules if rules is not None else Rules(),
    )


def tm(ts: int, position: Point, speed_kmh: float = 0.0, unit_id: str = UNIT_ID) -> Telemetry:
    """Сообщение телеметрии машины (по умолчанию T1, стоит)."""
    return Telemetry(unit_id=unit_id, ts=ts, position=position, speed_kmh=speed_kmh)
