"""Тесты базовых моделей: правила площадки, конфигурация, фазы машины, телеметрия.

Константы и инварианты — по разделу ТЗ «Константы в конфигурации сервиса».
"""

from __future__ import annotations

import dataclasses
import math
from typing import Any

import pytest

from vqueue.domain.model import (
    Point,
    Rules,
    SiteConfig,
    Station,
    Telemetry,
    UnitPhase,
)

RULE_FIELDS = (
    "service_seconds",
    "maneuver_seconds",
    "speed_kmh",
    "zone_radius_m",
    "stopped_speed_kmh",
    "decision_radius_m",
    "horizon_seconds",
    "freshness_seconds",
    "min_gain_seconds",
)


def _site(
    stations: tuple[Station, ...] | None = None,
    assignments: dict[str, str] | None = None,
) -> SiteConfig:
    """Собирает минимальную валидную конфигурацию площадки."""
    if stations is None:
        stations = (
            Station("S1", Point(49.14, 142.65)),
            Station("S2", Point(49.16, 142.67)),
        )
    if assignments is None:
        assignments = {"T-01": "S1", "T-02": "S2"}
    return SiteConfig(
        stations=stations,
        unload_point=Point(49.13, 142.66),
        assignments=assignments,
    )


# --- Rules -----------------------------------------------------------------


def test_rules_defaults_match_task_constants() -> None:
    """Значения по умолчанию совпадают с таблицей констант ТЗ (единственный перечень)."""
    rules = Rules()
    assert rules.service_seconds == 200
    assert rules.maneuver_seconds == 30
    assert rules.speed_kmh == 36.0
    assert rules.zone_radius_m == 50.0
    assert rules.stopped_speed_kmh == 1.0
    assert rules.decision_radius_m == 1500.0
    assert rules.horizon_seconds == 1800
    assert rules.freshness_seconds == 30
    assert rules.min_gain_seconds == 60


def test_rules_occupancy_seconds_default_is_230() -> None:
    """Машина занимает станцию на 230 с: 200 обслуживания + 30 манёвра."""
    assert Rules().occupancy_seconds == 230


def test_rules_occupancy_seconds_is_service_plus_maneuver() -> None:
    """Время занятия станции складывается из обслуживания и манёвра."""
    assert Rules(service_seconds=100, maneuver_seconds=15).occupancy_seconds == 115


def test_rules_speed_mps_default_is_10() -> None:
    """Расчётная скорость 36 км/ч равна 10 м/с."""
    assert Rules().speed_mps == pytest.approx(10.0)


def test_rules_speed_mps_converts_kmh() -> None:
    """Перевод км/ч в м/с: делим на 3.6."""
    assert Rules(speed_kmh=72.0).speed_mps == pytest.approx(20.0)


@pytest.mark.parametrize("field", RULE_FIELDS)
@pytest.mark.parametrize("value", [0, -1, True, math.inf, math.nan], ids=repr)
def test_rules_invalid_value_raises_value_error(field: str, value: Any) -> None:
    """Константа правил должна быть конечным положительным числом, не bool."""
    kwargs: dict[str, Any] = {field: value}
    with pytest.raises(ValueError):
        Rules(**kwargs)


# --- Point / неизменяемость ------------------------------------------------


@pytest.mark.parametrize(
    ("lat", "lon"),
    [
        (90.1, 0.0),
        (-90.1, 0.0),
        (0.0, 180.1),
        (0.0, -180.1),
        (math.nan, 0.0),
        (0.0, math.nan),
        (math.inf, 0.0),
        (0.0, -math.inf),
    ],
    ids=repr,
)
def test_point_invalid_coordinates_raise_value_error(lat: float, lon: float) -> None:
    """Координаты вне [-90, 90] × [-180, 180] или не конечные недопустимы."""
    with pytest.raises(ValueError):
        Point(lat, lon)


@pytest.mark.parametrize(
    ("lat", "lon"), [(90.0, 180.0), (-90.0, -180.0), (90.0, -180.0), (-90.0, 180.0)]
)
def test_point_boundary_coordinates_are_valid(lat: float, lon: float) -> None:
    """Границы ±90 по широте и ±180 по долготе допустимы."""
    point = Point(lat, lon)
    assert (point.lat, point.lon) == (lat, lon)


@pytest.mark.parametrize(
    ("obj", "field", "value"),
    [
        (Point(49.1, 142.6), "lat", 0.0),
        (Station("S1", Point(49.1, 142.6)), "station_id", "S2"),
        (Rules(), "service_seconds", 100),
        (Telemetry("T-12", 1757930400, Point(49.1288, 142.6601), 21.4), "ts", 0),
    ],
    ids=["Point", "Station", "Rules", "Telemetry"],
)
def test_value_objects_are_frozen(obj: object, field: str, value: object) -> None:
    """Объекты-значения неизменяемы."""
    with pytest.raises(dataclasses.FrozenInstanceError):
        setattr(obj, field, value)


# --- UnitPhase -------------------------------------------------------------


def test_unit_phase_has_three_members() -> None:
    """Ровно три состояния машины из ТЗ."""
    assert set(UnitPhase) == {UnitPhase.TO_STATION, UnitPhase.AT_STATION, UnitPhase.TO_UNLOAD}


# --- SiteConfig ------------------------------------------------------------


def test_site_config_default_rules_match_task() -> None:
    """Без явных правил конфигурация использует константы ТЗ."""
    assert _site().rules == Rules()


def test_site_config_station_returns_station_by_id() -> None:
    """Станция находится по идентификатору."""
    assert _site().station("S2") == Station("S2", Point(49.16, 142.67))


def test_site_config_station_unknown_raises_key_error() -> None:
    """Запрос неизвестной станции — KeyError."""
    with pytest.raises(KeyError):
        _site().station("S9")


def test_site_config_home_station_id_for_assigned_unit() -> None:
    """Закреплённая машина получает свою станцию."""
    site = _site()
    assert site.home_station_id("T-01") == "S1"
    assert site.home_station_id("T-02") == "S2"


def test_site_config_home_station_id_unknown_unit_returns_none() -> None:
    """Для машины без закрепления своей станции нет."""
    assert _site().home_station_id("T-99") is None


def test_site_config_without_stations_raises_value_error() -> None:
    """Площадка без станций недопустима."""
    with pytest.raises(ValueError):
        _site(stations=(), assignments={})


def test_site_config_duplicate_station_ids_raise_value_error() -> None:
    """Идентификаторы станций уникальны."""
    stations = (
        Station("S1", Point(49.14, 142.65)),
        Station("S1", Point(49.16, 142.67)),
    )
    with pytest.raises(ValueError):
        _site(stations=stations, assignments={"T-01": "S1"})


def test_site_config_assignment_to_unknown_station_raises_value_error() -> None:
    """Машину нельзя закрепить за несуществующей станцией."""
    with pytest.raises(ValueError):
        _site(assignments={"T-01": "S1", "T-02": "S9"})


def test_site_config_without_assignments_is_valid() -> None:
    """Пустое закрепление не нарушает инвариантов: все машины без своей станции."""
    assert _site(assignments={}).home_station_id("T-01") is None


def test_site_config_copies_assignments() -> None:
    """Закрепление не меняется (ТЗ): мутация исходного dict не влияет на конфиг."""
    source = {"T-01": "S1", "T-02": "S2"}
    site = _site(assignments=source)
    source["T-01"] = "S2"
    source["T-03"] = "S1"
    del source["T-02"]
    assert dict(site.assignments) == {"T-01": "S1", "T-02": "S2"}
    assert site.home_station_id("T-01") == "S1"
    assert site.home_station_id("T-03") is None


def test_site_config_is_hashable() -> None:
    """Конфигурация хешируема; равные конфигурации имеют равный хеш."""
    assert hash(_site()) == hash(_site())
