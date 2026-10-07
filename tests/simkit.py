"""Инструменты проверки генератора телеметрии: малая площадка, прогон и разбор траекторий.

Малая площадка: станции в 1–1.5 км от точки разгрузки, чтобы цикл машины занимал минуты.
На S1 закреплено 4 машины — при цикле ~400 с и занятии 230 с на машину очередь неизбежна
(ТЗ, «Генератор»: «соблюдают очередь»).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Final

from vqueue.domain.geo import EARTH_RADIUS_M, distance_m
from vqueue.domain.model import Point, SiteConfig, Station, Telemetry
from vqueue.simulator.engine import Simulation

START_TS: Final = 1_789_473_600
"""Начало прогона: 2026-09-15T12:00:00Z."""

MOVING_KMH: Final = 36.0
STOPPED_KMH: Final = 0.0


def offset(origin: Point, north_m: float, east_m: float) -> Point:
    """Точка, сдвинутая от origin на north_m к северу и east_m к востоку (локально)."""
    lat = origin.lat + math.degrees(north_m / EARTH_RADIUS_M)
    lon = origin.lon + math.degrees(east_m / (EARTH_RADIUS_M * math.cos(math.radians(origin.lat))))
    return Point(lat, lon)


UNLOAD: Final = Point(49.14, 142.66)

SMALL_SITE: Final = SiteConfig(
    stations=(
        Station("S1", offset(UNLOAD, 1_000.0, 0.0)),
        Station("S2", offset(UNLOAD, 0.0, 1_500.0)),
        Station("S3", offset(UNLOAD, -1_200.0, -600.0)),
    ),
    unload_point=UNLOAD,
    assignments={
        "A1": "S1",
        "A2": "S1",
        "A3": "S1",
        "A4": "S1",
        "B1": "S2",
        "B2": "S2",
        "C1": "S3",
    },
)


def run(sim: Simulation, steps: int) -> list[list[Telemetry]]:
    """Делает steps шагов симуляции и возвращает выдачу каждого шага."""
    return [sim.step() for _ in range(steps)]


def by_unit(batches: list[list[Telemetry]]) -> dict[str, list[Telemetry]]:
    """Раскладывает выдачу шагов по машинам с сохранением порядка."""
    result: dict[str, list[Telemetry]] = {}
    for batch in batches:
        for msg in batch:
            result.setdefault(msg.unit_id, []).append(msg)
    return result


def local_xy(p: Point, origin: Point) -> tuple[float, float]:
    """Локальные плоские координаты точки относительно origin, м (восток, север)."""
    x = math.radians(p.lon - origin.lon) * EARTH_RADIUS_M * math.cos(math.radians(origin.lat))
    y = math.radians(p.lat - origin.lat) * EARTH_RADIUS_M
    return x, y


def distance_to_segment_m(p: Point, a: Point, b: Point) -> float:
    """Расстояние от точки до отрезка ab в локальной плоской проекции, м."""
    px, py = local_xy(p, a)
    bx, by = local_xy(b, a)
    length2 = bx * bx + by * by
    t = 0.0 if length2 == 0 else max(0.0, min(1.0, (px * bx + py * by) / length2))
    return math.hypot(px - t * bx, py - t * by)


@dataclass(frozen=True, slots=True)
class Visit:
    """Пребывание машины в радиусе своей станции (подряд идущие сообщения в радиусе).

    Attributes:
        first_ts: ts первого сообщения в радиусе.
        last_ts: ts последнего сообщения в радиусе.
        stopped: Число сообщений со скоростью 0 за визит.
        last_stopped_ts: ts последнего сообщения со скоростью 0 (момент отъезда) или None.
        complete: Визит целиком внутри окна наблюдения (не обрезан началом или концом).
        from_window_start: Визит идёт с первого сообщения окна (момент входа неизвестен).
    """

    first_ts: int
    last_ts: int
    stopped: int
    last_stopped_ts: int | None
    complete: bool
    from_window_start: bool


def visits(msgs: list[Telemetry], station: Point, radius_m: float) -> list[Visit]:
    """Визиты машины в радиус станции по её сообщениям (упорядочены по ts)."""
    result: list[Visit] = []
    current: list[Telemetry] = []
    starts_at_window = False

    def close(at_end: bool) -> None:
        stopped = [m for m in current if m.speed_kmh == STOPPED_KMH]
        result.append(
            Visit(
                first_ts=current[0].ts,
                last_ts=current[-1].ts,
                stopped=len(stopped),
                last_stopped_ts=stopped[-1].ts if stopped else None,
                complete=not (starts_at_window or at_end),
                from_window_start=starts_at_window,
            )
        )

    for i, msg in enumerate(msgs):
        inside = distance_m(msg.position, station) < radius_m
        if inside:
            if not current:
                starts_at_window = i == 0
            current.append(msg)
        elif current:
            close(at_end=False)
            current = []
    if current:
        close(at_end=True)
    return result
