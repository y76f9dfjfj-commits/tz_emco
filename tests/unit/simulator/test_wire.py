"""Тесты формата сообщений генератора в telemetry.v1 (ТЗ, «Данные»).

ТЗ: топик telemetry.v1, ключ — unit_uuid, значение —
{"unit_uuid": "T-12", "ts": 1757930400, "lat": 49.1288, "lon": 142.6601, "speed_kmh": 21.4}.
Сообщение генератора должно разбираться кодеком процессора.
"""

from __future__ import annotations

import json
import math

from hypothesis import given
from hypothesis import strategies as st

from vqueue.adapters.codec import decode_telemetry
from vqueue.domain.model import Point, Telemetry
from vqueue.simulator.wire import encode_telemetry

_TASK_EXAMPLE = Telemetry("T-12", 1_757_930_400, Point(49.1288, 142.6601), 21.4)


def test_encode_task_example_matches_task_json_exactly() -> None:
    """Пример из ТЗ кодируется в тот же JSON: ключи по порядку, компактно, ts целое."""
    key, value = encode_telemetry(_TASK_EXAMPLE)
    assert key == b"T-12"
    assert value == (
        b'{"unit_uuid":"T-12","ts":1757930400,"lat":49.1288,"lon":142.6601,"speed_kmh":21.4}'
    )


def test_encode_value_has_exactly_task_keys_in_order() -> None:
    """В значении ровно ключи unit_uuid, ts, lat, lon, speed_kmh в этом порядке."""
    _, value = encode_telemetry(Telemetry("U", 1, Point(1.0, 2.0), 0.0))
    assert list(json.loads(value)) == ["unit_uuid", "ts", "lat", "lon", "speed_kmh"]


def test_encode_value_is_compact_json() -> None:
    """JSON без пробелов между элементами."""
    _, value = encode_telemetry(Telemetry("U 1", 5, Point(-10.5, -170.25), 36.0))
    text = value.decode("utf-8")
    assert ", " not in text
    assert ": " not in text
    assert text == json.dumps(json.loads(text), separators=(",", ":"), ensure_ascii=False) or (
        text == json.dumps(json.loads(text), separators=(",", ":"))
    )


def test_encode_ts_is_integer_and_speed_number() -> None:
    """Ts — целое JSON-число, скорость — число (36.0 в пути, 0.0 на месте)."""
    _, value = encode_telemetry(Telemetry("U", 1_789_473_600, Point(1.0, 2.0), 36.0))
    data = json.loads(value)
    assert type(data["ts"]) is int
    assert data["ts"] == 1_789_473_600
    assert data["speed_kmh"] == 36.0


def test_encode_key_is_unit_uuid_in_utf8() -> None:
    """Ключ — unit_uuid в UTF-8, в том числе не ASCII."""
    unit = "Машина-7"
    key, value = encode_telemetry(Telemetry(unit, 1, Point(1.0, 2.0), 0.0))
    assert key == unit.encode("utf-8")
    assert json.loads(value.decode("utf-8"))["unit_uuid"] == unit


def test_encode_rounds_coordinates_to_seven_digits() -> None:
    """lat/lon округляются до 7 знаков после запятой."""
    _, value = encode_telemetry(Telemetry("U", 1, Point(49.123456789, 142.987654321), 0.0))
    data = json.loads(value)
    assert data["lat"] == 49.1234568
    assert data["lon"] == 142.9876543


def test_task_example_round_trips_through_processor_codec() -> None:
    """Сообщение генератора разбирается процессором в то же доменное сообщение."""
    _, value = encode_telemetry(_TASK_EXAMPLE)
    assert decode_telemetry(value) == _TASK_EXAMPLE


@given(
    unit=st.text(min_size=1, max_size=20),
    ts=st.integers(min_value=0, max_value=4_102_444_800),
    lat=st.floats(min_value=-90.0, max_value=90.0),
    lon=st.floats(min_value=-180.0, max_value=180.0),
    speed=st.sampled_from([0.0, 36.0]),
)
def test_any_message_round_trips_through_processor_codec(
    unit: str, ts: int, lat: float, lon: float, speed: float
) -> None:
    """Любое сообщение генератора разбирается процессором; координаты — с точностью 1e-7."""
    msg = Telemetry(unit, ts, Point(lat, lon), speed)
    key, value = encode_telemetry(msg)
    decoded = decode_telemetry(value)
    assert key == unit.encode("utf-8")
    assert decoded.unit_id == unit
    assert decoded.ts == ts
    assert decoded.speed_kmh == speed
    assert math.isclose(decoded.position.lat, lat, abs_tol=5.1e-8)
    assert math.isclose(decoded.position.lon, lon, abs_tol=5.1e-8)
