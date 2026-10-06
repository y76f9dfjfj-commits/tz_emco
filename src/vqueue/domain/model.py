"""Базовые доменные модели: площадка, правила расчёта, телеметрия."""

from __future__ import annotations

import math
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from enum import StrEnum
from types import MappingProxyType
from typing import Final

MAX_LAT: Final = 90.0
MAX_LON: Final = 180.0


@dataclass(frozen=True, slots=True)
class Point:
    """Точка в координатах WGS84.

    Attributes:
        lat: Широта, градусы.
        lon: Долгота, градусы.
    """

    lat: float
    lon: float

    def __post_init__(self) -> None:
        """Проверяет, что координаты конечны и лежат в допустимых диапазонах.

        Raises:
            ValueError: Широта вне [-90, 90] или долгота вне [-180, 180].
        """
        if not (math.isfinite(self.lat) and -MAX_LAT <= self.lat <= MAX_LAT):
            raise ValueError(f"Широта вне диапазона [-90, 90]: {self.lat!r}")
        if not (math.isfinite(self.lon) and -MAX_LON <= self.lon <= MAX_LON):
            raise ValueError(f"Долгота вне диапазона [-180, 180]: {self.lon!r}")


@dataclass(frozen=True, slots=True)
class Station:
    """Станция обслуживания.

    Attributes:
        station_id: Идентификатор станции (station_uuid).
        location: Координаты станции.
    """

    station_id: str
    location: Point


@dataclass(frozen=True, slots=True)
class Rules:
    """Константы расчёта из ТЗ; значения по умолчанию совпадают с ТЗ.

    Attributes:
        service_seconds: Время обслуживания одной машины, с.
        maneuver_seconds: Манёвр между машинами, с.
        speed_kmh: Расчётная скорость движения, км/ч.
        zone_radius_m: Радиус станции и точки разгрузки, м.
        stopped_speed_kmh: Порог «машина стоит» (скорость строго меньше), км/ч.
        decision_radius_m: Радиус принятия решения до своей станции, м.
        horizon_seconds: Горизонт очереди, с.
        freshness_seconds: Позиция считается свежей, если не старше, с.
        min_gain_seconds: Минимальный выигрыш для рекомендации, с.
    """

    service_seconds: int = 200
    maneuver_seconds: int = 30
    speed_kmh: float = 36.0
    zone_radius_m: float = 50.0
    stopped_speed_kmh: float = 1.0
    decision_radius_m: float = 1500.0
    horizon_seconds: int = 1800
    freshness_seconds: int = 30
    min_gain_seconds: int = 60

    def __post_init__(self) -> None:
        """Проверяет, что все константы — конечные положительные числа.

        Raises:
            ValueError: Константа не число (в т.ч. bool), не конечна или не больше нуля.
        """
        for f in fields(self):
            value = getattr(self, f.name)
            is_number = isinstance(value, int | float) and not isinstance(value, bool)
            if not (is_number and math.isfinite(value) and value > 0):
                raise ValueError(
                    f"Rules.{f.name} должно быть конечным числом > 0, получено {value!r}"
                )

    @property
    def occupancy_seconds(self) -> int:
        """Время занятия станции одной машиной: обслуживание плюс манёвр, с."""
        return self.service_seconds + self.maneuver_seconds

    @property
    def speed_mps(self) -> float:
        """Расчётная скорость в м/с."""
        return self.speed_kmh * 1000.0 / 3600.0


@dataclass(frozen=True, slots=True)
class SiteConfig:
    """Конфигурация площадки: станции, точка разгрузки, закрепления и правила.

    Attributes:
        stations: Станции площадки (не менее одной, идентификаторы уникальны).
        unload_point: Координаты точки разгрузки.
        assignments: Закрепление машин: unit_uuid -> station_uuid своей станции.
        rules: Константы расчёта.
    """

    stations: tuple[Station, ...]
    unload_point: Point
    # hash=False: read-only представление закреплений нехешируемо, а для равенства не нужно.
    assignments: Mapping[str, str] = field(hash=False)
    rules: Rules = field(default_factory=Rules)

    def __post_init__(self) -> None:
        """Проверяет инварианты и замораживает закрепления.

        Raises:
            ValueError: Нет станций, есть повторяющиеся идентификаторы станций
                или закрепление ссылается на несуществующую станцию.
        """
        if not self.stations:
            raise ValueError("На площадке должна быть хотя бы одна станция")
        ids = [s.station_id for s in self.stations]
        duplicates = sorted({i for i in ids if ids.count(i) > 1})
        if duplicates:
            raise ValueError(f"Повторяющиеся идентификаторы станций: {duplicates}")
        known = set(ids)
        unknown = sorted(
            f"{unit}->{station}"
            for unit, station in self.assignments.items()
            if station not in known
        )
        if unknown:
            raise ValueError(f"Закрепления ссылаются на несуществующие станции: {unknown}")
        # Копия в read-only представление: внешний dict не может изменить конфигурацию.
        object.__setattr__(self, "assignments", MappingProxyType(dict(self.assignments)))

    def station(self, station_id: str) -> Station:
        """Возвращает станцию по идентификатору.

        Args:
            station_id: Идентификатор станции.

        Returns:
            Станция с указанным идентификатором.

        Raises:
            KeyError: Если станции нет в конфигурации.
        """
        for s in self.stations:
            if s.station_id == station_id:
                return s
        raise KeyError(station_id)

    def home_station_id(self, unit_id: str) -> str | None:
        """Возвращает идентификатор своей станции машины.

        Args:
            unit_id: Идентификатор машины.

        Returns:
            Идентификатор станции или None, если машина не закреплена.
        """
        return self.assignments.get(unit_id)


class UnitPhase(StrEnum):
    """Фаза цикла машины, определяемая по телеметрии."""

    TO_STATION = "to_station"
    """Едет к станции; также состояние по умолчанию."""
    AT_STATION = "at_station"
    """Находится на станции."""
    TO_UNLOAD = "to_unload"
    """Едет к точке разгрузки после обслуживания."""


@dataclass(frozen=True, slots=True)
class Telemetry:
    """Одно сообщение телеметрии машины.

    Attributes:
        unit_id: Идентификатор машины (unit_uuid).
        ts: Время события, секунды epoch по часам машины.
        position: Координаты машины.
        speed_kmh: Фактическая скорость, км/ч (нужна только для признака «стоит»).
    """

    unit_id: str
    ts: int
    position: Point
    speed_kmh: float
