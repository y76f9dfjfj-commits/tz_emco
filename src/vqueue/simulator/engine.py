"""Детерминированная симуляция площадки: движение машин, обслуживание и очередь на станциях.

Модель без IO и системных часов: виртуальное время задаётся параметром и продвигается
шагами по одной секунде, случайность — только из генератора с заданным seed.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from enum import Enum, auto
from typing import Final

from vqueue.domain.geo import distance_m
from vqueue.domain.model import Point, SiteConfig, Telemetry

STOP_OFFSET_SHARE: Final = 0.2
"""Точка остановки у станции: доля радиуса станции от её центра в сторону разгрузки.

При радиусе 50 м — 10 м до центра со стороны подъезда, заведомо внутри радиуса.
"""


class _Phase(Enum):
    """Фаза машины в симуляции."""

    TO_STATION = auto()
    """Едет от точки разгрузки к точке остановки у своей станции."""
    WAITING = auto()
    """Стоит в точке остановки и ждёт освобождения станции."""
    SERVING = auto()
    """Обслуживается на станции (стоит в точке остановки)."""
    TO_UNLOAD = auto()
    """Едет от станции к точке разгрузки."""


@dataclass(frozen=True, slots=True)
class _Route:
    """Маршрут машины между точкой разгрузки и точкой остановки у своей станции.

    Attributes:
        station_id: Своя станция машины.
        unload: Точка разгрузки.
        stop: Точка остановки у станции.
        length_m: Длина маршрута по прямой, м.
    """

    station_id: str
    unload: Point
    stop: Point
    length_m: float

    def point_at(self, travelled_m: float, *, to_station: bool) -> Point:
        """Позиция на маршруте после проезда заданного пути.

        Линейная интерполяция широты и долготы — на километровых расстояниях
        отклонение от геодезической пренебрежимо мало.

        Args:
            travelled_m: Пройденный от начала отрезка путь, м.
            to_station: Направление: от разгрузки к станции (True) или обратно.

        Returns:
            Координаты машины.
        """
        start, end = (self.unload, self.stop) if to_station else (self.stop, self.unload)
        return _interpolate(start, end, travelled_m, self.length_m)


@dataclass(slots=True)
class _Unit:
    """Изменяемое состояние машины в симуляции.

    Attributes:
        unit_id: Идентификатор машины.
        route: Маршрут машины.
        phase: Текущая фаза.
        travelled_m: Пройденный путь по текущему отрезку (для фаз в пути), м.
        arrived_at: Момент приезда в точку остановки (для WAITING), с.
        service_start: Начало обслуживания (для SERVING), с.
    """

    unit_id: str
    route: _Route
    phase: _Phase
    travelled_m: float = 0.0
    arrived_at: int = 0
    service_start: int = 0


def _interpolate(start: Point, end: Point, travelled_m: float, length_m: float) -> Point:
    """Точка на отрезке после проезда travelled_m из length_m метров.

    Нулевая длина возможна при валидной конфигурации: станция совпадает с точкой
    разгрузки или ближе к ней, чем точка остановки (SiteConfig этого не запрещает).
    """
    share = 1.0 if length_m <= 0 else min(1.0, travelled_m / length_m)
    return Point(
        start.lat + (end.lat - start.lat) * share,
        start.lon + (end.lon - start.lon) * share,
    )


class Simulation:
    """Симуляция площадки с виртуальными часами.

    Каждая машина ездит только к своей станции (рекомендации не исполняются):
    по прямой от точки разгрузки к точке остановки у станции, ждёт своей очереди,
    стоит service_seconds, едет обратно; разгрузка мгновенная. Станция обслуживает
    одну машину; следующая начинает обслуживание не раньше своего приезда и не
    раньше чем через occupancy_seconds после начала предыдущего обслуживания.
    Порядок обслуживания — по моменту приезда, при равенстве — по unit_id.
    """

    def __init__(self, site: SiteConfig, start_ts: int, seed: int) -> None:
        """Создаёт симуляцию с начальным состоянием, определяемым seed.

        Машины разбрасываются по маршруту «к станции → к разгрузке» в случайных
        точках цикла, чтобы реже приезжали на станции одновременно.

        Args:
            site: Конфигурация площадки.
            start_ts: Начальное виртуальное время, секунды epoch.
            seed: Начальное значение генератора случайных чисел.
        """
        self._rules = site.rules
        self._ts = start_ts
        rng = random.Random(seed)  # nosec B311 — симуляция, не криптография.
        routes = {s.station_id: self._route(site, s.station_id) for s in site.stations}
        self._units: list[_Unit] = []
        for unit_id in sorted(site.assignments):
            route = routes[site.assignments[unit_id]]
            offset = rng.uniform(0.0, 2.0 * route.length_m)
            to_station = offset < route.length_m
            self._units.append(
                _Unit(
                    unit_id=unit_id,
                    route=route,
                    phase=_Phase.TO_STATION if to_station else _Phase.TO_UNLOAD,
                    travelled_m=offset if to_station else offset - route.length_m,
                )
            )
        # Начало последнего обслуживания на каждой станции; None — ещё не обслуживала.
        self._last_start: dict[str, int | None] = {s.station_id: None for s in site.stations}

    @property
    def ts(self) -> int:
        """Текущее виртуальное время, секунды epoch."""
        return self._ts

    def step(self) -> list[Telemetry]:
        """Продвигает мир на одну секунду.

        Returns:
            По одному сообщению на машину (в порядке unit_id) с ts нового времени.
        """
        self._ts += 1
        now = self._ts
        rules = self._rules
        for unit in self._units:
            self._move(unit, now)
        self._admit(now)
        return [
            Telemetry(
                unit_id=unit.unit_id,
                ts=now,
                position=self._position(unit),
                speed_kmh=rules.speed_kmh if self._is_moving(unit) else 0.0,
            )
            for unit in self._units
        ]

    def _move(self, unit: _Unit, now: int) -> None:
        """Продвигает одну машину на секунду: окончание обслуживания и движение."""
        rules = self._rules
        if unit.phase is _Phase.SERVING and now - unit.service_start >= rules.service_seconds:
            # Отъезд начинается в ту же секунду: машина уже в пути.
            unit.phase = _Phase.TO_UNLOAD
            unit.travelled_m = 0.0
        if not self._is_moving(unit):
            return
        unit.travelled_m += rules.speed_mps
        if unit.travelled_m < unit.route.length_m:
            return
        if unit.phase is _Phase.TO_STATION:
            unit.phase = _Phase.WAITING
            unit.arrived_at = now
        else:
            # Разгрузка мгновенная: в этой секунде машина в точке разгрузки, дальше — к станции.
            unit.phase = _Phase.TO_STATION
            unit.travelled_m = 0.0

    def _admit(self, now: int) -> None:
        """Ставит на обслуживание первых ждущих на свободных станциях.

        Станция свободна, если с начала последнего обслуживания прошло не меньше
        occupancy_seconds. Отдельная проверка «станция кого-то обслуживает» не нужна:
        Rules гарантирует maneuver_seconds > 0, поэтому occupancy_seconds > service_seconds
        и обслуживание к этому моменту уже закончено.
        """
        occupancy = self._rules.occupancy_seconds
        waiting = sorted(
            (u for u in self._units if u.phase is _Phase.WAITING),
            key=lambda u: (u.arrived_at, u.unit_id),
        )
        for unit in waiting:
            station_id = unit.route.station_id
            last = self._last_start[station_id]
            if last is not None and now - last < occupancy:
                continue
            unit.phase = _Phase.SERVING
            unit.service_start = now
            self._last_start[station_id] = now

    @staticmethod
    def _is_moving(unit: _Unit) -> bool:
        """Машина в пути (а не стоит у станции)."""
        return unit.phase in (_Phase.TO_STATION, _Phase.TO_UNLOAD)

    @staticmethod
    def _position(unit: _Unit) -> Point:
        """Текущие координаты машины."""
        route = unit.route
        if unit.phase is _Phase.TO_STATION:
            return route.point_at(unit.travelled_m, to_station=True)
        if unit.phase is _Phase.TO_UNLOAD:
            return route.point_at(unit.travelled_m, to_station=False)
        return route.stop

    @staticmethod
    def _route(site: SiteConfig, station_id: str) -> _Route:
        """Строит маршрут от точки разгрузки до точки остановки у станции."""
        center = site.station(station_id).location
        unload = site.unload_point
        full = distance_m(unload, center)
        offset = min(site.rules.zone_radius_m * STOP_OFFSET_SHARE, full)
        stop = _interpolate(center, unload, offset, full)
        return _Route(station_id=station_id, unload=unload, stop=stop, length_m=full - offset)
