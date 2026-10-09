"""Сериализация телеметрии генератора в формат топика telemetry.v1.

Генератор — отдельное «внешнее» приложение, поэтому формат сообщения описан здесь,
независимо от кодека процессора.
"""

from __future__ import annotations

import json
from typing import Final

from vqueue.domain.model import Telemetry

COORD_DIGITS: Final = 7
"""Знаков после запятой в координатах (~1 см)."""


def encode_telemetry(t: Telemetry) -> tuple[bytes, bytes]:
    """Сериализует сообщение в формат telemetry.v1.

    Значение — компактный JSON ровно с ключами unit_uuid, ts, lat, lon, speed_kmh
    в этом порядке; координаты округлены до COORD_DIGITS знаков.

    Args:
        t: Сообщение телеметрии.

    Returns:
        Пара (ключ — unit_uuid, значение — JSON) в UTF-8.
    """
    value = {
        "unit_uuid": t.unit_id,
        "ts": int(t.ts),
        "lat": round(t.position.lat, COORD_DIGITS),
        "lon": round(t.position.lon, COORD_DIGITS),
        "speed_kmh": t.speed_kmh,
    }
    encoded = json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return t.unit_id.encode("utf-8"), encoded.encode("utf-8")
