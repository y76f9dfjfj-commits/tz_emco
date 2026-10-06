"""Тесты загрузки конфигурации площадки из TOML.

ТЗ, «Как устроена площадка»: 4 станции, 1 точка разгрузки, 40 машин, каждая закреплена
за одной станцией; константы — по таблице «Константы в конфигурации сервиса».
"""

from __future__ import annotations

import itertools
import tomllib
from collections import Counter
from pathlib import Path

import pydantic
import pytest

from vqueue.config import load_site_config
from vqueue.domain.geo import distance_m
from vqueue.domain.model import Point, Rules, SiteConfig, Station

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SITE_TOML = PROJECT_ROOT / "config" / "site.toml"

RULES_TOML = """\
[rules]
service_seconds = {service_seconds}
maneuver_seconds = 30
speed_kmh = 36.0
zone_radius_m = 50.0
stopped_speed_kmh = 1.0
decision_radius_m = 1500.0
horizon_seconds = 1800
freshness_seconds = 30
min_gain_seconds = 60
"""

BODY_TOML = """\
[unload_point]
lat = 49.13
lon = 142.66

[[stations]]
station_id = "S1"
location = { lat = 49.14, lon = 142.65 }

[[stations]]
station_id = "S2"
location = { lat = 49.16, lon = 142.67 }

[assignments]
"T-01" = "S1"
"T-02" = "S2"
"""


def _write(tmp_path: Path, text: str) -> Path:
    """Пишет TOML во временный файл и возвращает путь."""
    path = tmp_path / "site.toml"
    path.write_text(text, encoding="utf-8")
    return path


def _full(service_seconds: int = 200) -> str:
    """Полный валидный TOML площадки."""
    return RULES_TOML.format(service_seconds=service_seconds) + "\n" + BODY_TOML


@pytest.fixture(scope="module")
def site() -> SiteConfig:
    """Рабочая конфигурация площадки config/site.toml."""
    return load_site_config(SITE_TOML)


# --- Загрузка из временного файла ------------------------------------------


def test_load_valid_toml_builds_site_config(tmp_path: Path) -> None:
    """Валидный TOML превращается в SiteConfig со всеми полями."""
    loaded = load_site_config(_write(tmp_path, _full()))
    assert loaded.stations == (
        Station("S1", Point(49.14, 142.65)),
        Station("S2", Point(49.16, 142.67)),
    )
    assert loaded.unload_point == Point(49.13, 142.66)
    assert dict(loaded.assignments) == {"T-01": "S1", "T-02": "S2"}
    assert loaded.rules == Rules()


def test_load_custom_rule_value_is_applied(tmp_path: Path) -> None:
    """Значение правила берётся из файла, а не из умолчаний."""
    loaded = load_site_config(_write(tmp_path, _full(service_seconds=180)))
    assert loaded.rules.service_seconds == 180
    assert loaded.rules.occupancy_seconds == 210


def test_load_without_rules_section_uses_defaults(tmp_path: Path) -> None:
    """Секция [rules] необязательна: без неё действуют константы ТЗ."""
    loaded = load_site_config(_write(tmp_path, BODY_TOML))
    assert loaded.rules == Rules()


def test_load_broken_toml_raises_decode_error(tmp_path: Path) -> None:
    """Синтаксически битый TOML — TOMLDecodeError."""
    with pytest.raises(tomllib.TOMLDecodeError):
        load_site_config(_write(tmp_path, "[rules\nservice_seconds = "))


@pytest.mark.parametrize("value", [0, -200])
def test_load_non_positive_constant_raises_validation_error(tmp_path: Path, value: int) -> None:
    """Неположительная константа правил — ошибка валидации."""
    with pytest.raises(pydantic.ValidationError):
        load_site_config(_write(tmp_path, _full(service_seconds=value)))


def test_load_invariant_violation_raises_validation_error(tmp_path: Path) -> None:
    """Нарушение инварианта SiteConfig (закрепление за несуществующей станцией)."""
    text = _full().replace('"T-02" = "S2"', '"T-02" = "S9"')
    with pytest.raises(pydantic.ValidationError):
        load_site_config(_write(tmp_path, text))


def test_load_typo_in_rules_key_raises_validation_error(tmp_path: Path) -> None:
    """Опечатка в ключе правил не проглатывается молча (extra="forbid")."""
    text = "[rules]\nservice_second = 180\n\n" + BODY_TOML
    with pytest.raises(pydantic.ValidationError):
        load_site_config(_write(tmp_path, text))


def test_load_unknown_top_level_section_raises_validation_error(tmp_path: Path) -> None:
    """Неизвестная секция верхнего уровня — ошибка (extra="forbid")."""
    text = _full() + "\n[unload]\nlat = 49.13\nlon = 142.66\n"
    with pytest.raises(pydantic.ValidationError):
        load_site_config(_write(tmp_path, text))


# --- Рабочая конфигурация config/site.toml ---------------------------------


def test_site_toml_has_four_stations(site: SiteConfig) -> None:
    """На площадке 4 станции с уникальными идентификаторами."""
    ids = [s.station_id for s in site.stations]
    assert len(ids) == 4
    assert len(set(ids)) == 4


def test_site_toml_has_forty_units_ten_per_station(site: SiteConfig) -> None:
    """40 машин, каждая закреплена за станцией, по 10 на станцию."""
    assert len(site.assignments) == 40
    assert Counter(site.assignments.values()) == Counter({s.station_id: 10 for s in site.stations})


def test_site_toml_rules_match_task_constants(site: SiteConfig) -> None:
    """Константы рабочей конфигурации равны умолчаниям (= таблице ТЗ, см. test_model)."""
    assert site.rules == Rules()


def test_site_toml_station_zones_do_not_overlap(site: SiteConfig) -> None:
    """Станции попарно дальше 2·R зоны.

    Иначе машина может одновременно оказаться «на станции» у двух станций
    и состояние `на станции` становится неоднозначным.
    """
    limit = 2 * site.rules.zone_radius_m
    for a, b in itertools.combinations(site.stations, 2):
        assert distance_m(a.location, b.location) > limit, (a.station_id, b.station_id)


def test_site_toml_unload_point_outside_station_zones(site: SiteConfig) -> None:
    """Точка разгрузки вне зоны каждой станции.

    Иначе стоящая на разгрузке машина распознаётся как `на станции`
    и ложно занимает станцию.
    """
    for station in site.stations:
        assert distance_m(station.location, site.unload_point) >= site.rules.zone_radius_m, (
            station.station_id
        )


def test_site_toml_stations_beyond_decision_radius_from_unload(site: SiteConfig) -> None:
    """Каждая станция дальше радиуса решения от точки разгрузки.

    Рекомендация считается, когда машина `к станции` впервые оказывается ближе 1500 м
    к своей станции; если станция ближе к разгрузке, решение срабатывало бы сразу
    после разгрузки, и не было бы момента «подъезда» из ТЗ.
    """
    for station in site.stations:
        assert distance_m(station.location, site.unload_point) > site.rules.decision_radius_m, (
            station.station_id
        )
