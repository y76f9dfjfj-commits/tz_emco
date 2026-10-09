"""Площадка и поток телеметрии примера ТЗ для тестов адаптеров.

Площадка совпадает с golden-тестом раздела «Пример»: S1 и S2 (в 3300 м к югу от S1),
точка разгрузки в 8 км к югу; T1–T4 закреплены за S1. Поток: T1 стоит в 15 м от S1
(11:59:00 и 12:00:00), T2 в 1200 м и T3 в 3000 м едут к S1, T4 в 900 м от S1 и 2400 м
от S2 — в 12:00:00. Итог: очереди S1 и S2, отказ no_gain для T2, рекомендация T4 → S2.
"""

from __future__ import annotations

import json
from typing import Final

from tests.sitekit import S1_ID, S1_POINT, north_of
from vqueue.domain.model import Point, SiteConfig, Station, Telemetry

SITE_ID: Final = "site-1"
"""Идентификатор площадки — ключ записи снимка в топике состояния."""

S2_ID: Final = "S2"
T1: Final = "T1"
T2: Final = "T2"
T3: Final = "T3"
T4: Final = "T4"

NOW: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — момент расчёта примера."""

T1_OCCUPIED_AT: Final = 1_789_473_540
"""2026-09-15T11:59:00Z — T1 заняла S1."""

T1_POSITION: Final = north_of(S1_POINT, 15.0)
T2_POSITION: Final = north_of(S1_POINT, 1_200.0)
T3_POSITION: Final = north_of(S1_POINT, 3_000.0)
T4_POSITION: Final = north_of(S1_POINT, -900.0)

SITE: Final = SiteConfig(
    stations=(Station(S1_ID, S1_POINT), Station(S2_ID, north_of(S1_POINT, -3_300.0))),
    unload_point=north_of(S1_POINT, -8_000.0),
    assignments={T1: S1_ID, T2: S1_ID, T3: S1_ID, T4: S1_ID},
)


def telemetry_json(unit_id: str, ts: int, position: Point, speed_kmh: float) -> bytes:
    """Сообщение telemetry.v1 в формате ТЗ («Данные»)."""
    return json.dumps(
        {
            "unit_uuid": unit_id,
            "ts": ts,
            "lat": position.lat,
            "lon": position.lon,
            "speed_kmh": speed_kmh,
        }
    ).encode("utf-8")


EXAMPLE_TELEMETRY: Final = (
    Telemetry(T1, T1_OCCUPIED_AT, T1_POSITION, 0.0),
    Telemetry(T1, NOW, T1_POSITION, 0.0),
    Telemetry(T2, NOW, T2_POSITION, 36.0),
    Telemetry(T3, NOW, T3_POSITION, 36.0),
    Telemetry(T4, NOW, T4_POSITION, 36.0),
)
"""Поток примера ТЗ в доменном виде."""

EXAMPLE_VALUES: Final = tuple(
    telemetry_json(m.unit_id, m.ts, m.position, m.speed_kmh) for m in EXAMPLE_TELEMETRY
)
"""Тот же поток в виде значений сообщений telemetry.v1."""
