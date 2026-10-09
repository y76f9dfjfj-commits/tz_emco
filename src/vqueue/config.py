"""Загрузка конфигурации площадки из TOML (граница: валидация через pydantic)."""

from __future__ import annotations

import tomllib
from pathlib import Path

from pydantic import TypeAdapter

from vqueue.domain.model import SiteConfig

_ADAPTER: TypeAdapter[SiteConfig] = TypeAdapter(SiteConfig)


def load_site_config(path: Path) -> SiteConfig:
    """Читает и валидирует конфигурацию площадки.

    Args:
        path: Путь к TOML-файлу конфигурации.

    Returns:
        Провалидированная конфигурация площадки.

    Raises:
        tomllib.TOMLDecodeError: Файл не является корректным TOML.
        pydantic.ValidationError: Нарушен формат или инварианты конфигурации.
    """
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    # extra="forbid": опечатка в ключе не должна молча подменяться значением по умолчанию.
    return _ADAPTER.validate_python(data, extra="forbid")
