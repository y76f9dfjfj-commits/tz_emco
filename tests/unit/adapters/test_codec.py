"""Тесты кодека: telemetry.v1 на входе, queue.v1 / decision.v1 и снимок состояния на выходе.

ТЗ, раздел «Данные»: формат входного сообщения и выходных JSON; время — ISO 8601 UTC.
JSON компактный, порядок полей как в ТЗ, ключи UTF-8, любая ошибка
разбора входа или снимка — InvalidMessage.
"""

from __future__ import annotations

import json
from typing import Any, Final

import pytest

from tests.unit.adapters.example import (
    EXAMPLE_TELEMETRY,
    NOW,
    S2_ID,
    SITE,
    T1,
    T1_OCCUPIED_AT,
    T2,
    T4,
)
from vqueue.adapters.codec import (
    MAX_TS,
    InvalidMessage,
    decode_snapshot,
    decode_telemetry,
    encode_decision,
    encode_queue,
    encode_snapshot,
    iso_utc,
)
from vqueue.domain.model import Point, Telemetry
from vqueue.domain.queue import QueueEntry, StationQueue
from vqueue.domain.recommendation import Recommendation, Rejection, RejectReason
from vqueue.domain.site import SiteSnapshot, SiteState

TASK_TELEMETRY: Final = {
    "unit_uuid": "T-12",
    "ts": 1757930400,
    "lat": 49.1288,
    "lon": 142.6601,
    "speed_kmh": 21.4,
}
"""Пример входного сообщения telemetry.v1 из ТЗ («Данные»)."""

T1_FREE_AT: Final = 1_789_473_770
"""12:02:50 — освобождение S1 после T1."""
T2_ETA: Final = 1_789_473_720
"""12:02:00 — приезд T2."""
T2_FREE_AT: Final = 1_789_474_000
"""12:06:40 — освобождение S1 после T2."""


def _raw(obj: Any) -> bytes:
    """Сериализует объект в байты JSON."""
    return json.dumps(obj).encode("utf-8")


def _compact(obj: dict[str, Any]) -> bytes:
    """Компактный JSON (без пробелов) с порядком ключей как в объекте."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _assert_json_equal(actual: bytes, expected: dict[str, Any]) -> None:
    """Сравнивает JSON по значению, порядку ключей верхнего уровня и компактной записи."""
    decoded = json.loads(actual)
    assert decoded == expected
    assert list(decoded) == list(expected)
    assert actual == _compact(expected)


# ---------------------------------------------------------------------------
# `decode_telemetry` — вход telemetry.v1 (ТЗ, «Данные»)
# ---------------------------------------------------------------------------


def test_decode_telemetry_task_example_decoded() -> None:
    """ТЗ «Данные»: пример сообщения T-12 разбирается в доменную телеметрию."""
    msg = decode_telemetry(_raw(TASK_TELEMETRY))

    assert msg == Telemetry(
        unit_id="T-12", ts=1757930400, position=Point(49.1288, 142.6601), speed_kmh=21.4
    )
    assert type(msg.ts) is int


def test_decode_telemetry_extra_fields_ignored() -> None:
    """Лишние поля сообщения игнорируются."""
    raw = _raw({**TASK_TELEMETRY, "heading": 90, "driver": {"name": "x"}})

    assert decode_telemetry(raw) == decode_telemetry(_raw(TASK_TELEMETRY))


def test_decode_telemetry_integer_coordinates_and_speed_accepted() -> None:
    """Координаты и скорость — числа JSON; целые значения допустимы."""
    raw = _raw({**TASK_TELEMETRY, "lat": 49, "lon": 142, "speed_kmh": 0})

    msg = decode_telemetry(raw)

    assert msg.position == Point(49.0, 142.0)
    assert msg.speed_kmh == 0.0


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("speed_kmh", 0.0, id="speed-zero"),
        pytest.param("lat", 90.0, id="lat-max"),
        pytest.param("lat", -90.0, id="lat-min"),
        pytest.param("lon", 180.0, id="lon-max"),
        pytest.param("lon", -180.0, id="lon-min"),
    ],
)
def test_decode_telemetry_boundary_values_accepted(field: str, value: float) -> None:
    """Границы включительно: speed_kmh >= 0, широта [-90, 90], долгота [-180, 180]."""
    msg = decode_telemetry(_raw({**TASK_TELEMETRY, field: value}))

    actual = {"speed_kmh": msg.speed_kmh, "lat": msg.position.lat, "lon": msg.position.lon}
    assert actual[field] == value


def test_decode_telemetry_non_ascii_unit_id_decoded() -> None:
    """Идентификатор машины в UTF-8 (кириллица) сохраняется как есть."""
    msg = decode_telemetry(_raw({**TASK_TELEMETRY, "unit_uuid": "Т-1"}))

    assert msg.unit_id == "Т-1"


def test_invalid_message_is_value_error() -> None:
    """InvalidMessage — подкласс ValueError."""
    assert issubclass(InvalidMessage, ValueError)


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"{", id="truncated"),
        pytest.param(b"not json", id="text"),
        pytest.param(b"\xff\xfe\x00", id="not-utf8"),
    ],
)
def test_decode_telemetry_not_json_raises_invalid_message(raw: bytes) -> None:
    """Не JSON → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_telemetry(raw)


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param([], id="list"),
        pytest.param([TASK_TELEMETRY], id="list-of-object"),
        pytest.param("T-12", id="string"),
        pytest.param(42, id="number"),
        pytest.param(None, id="null"),
    ],
)
def test_decode_telemetry_not_object_raises_invalid_message(payload: Any) -> None:
    """JSON, но не объект → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_telemetry(_raw(payload))


@pytest.mark.parametrize("field", sorted(TASK_TELEMETRY))
def test_decode_telemetry_missing_field_raises_invalid_message(field: str) -> None:
    """Отсутствует обязательное поле → InvalidMessage."""
    payload = {k: v for k, v in TASK_TELEMETRY.items() if k != field}

    with pytest.raises(InvalidMessage):
        decode_telemetry(_raw(payload))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("ts", "1757930400", id="ts-string"),
        pytest.param("ts", True, id="ts-bool-true"),
        pytest.param("ts", False, id="ts-bool-false"),
        pytest.param("ts", 1757930400.5, id="ts-fractional"),
        pytest.param("ts", None, id="ts-null"),
        pytest.param("unit_uuid", 12, id="unit-number"),
        pytest.param("unit_uuid", None, id="unit-null"),
        pytest.param("lat", None, id="lat-null"),
        pytest.param("lat", [49.1], id="lat-list"),
        pytest.param("speed_kmh", None, id="speed-null"),
    ],
)
def test_decode_telemetry_wrong_type_raises_invalid_message(field: str, value: Any) -> None:
    """Неверный тип поля (ts — строгое целое, bool запрещён) → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_telemetry(_raw({**TASK_TELEMETRY, field: value}))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("speed_kmh", -0.1, id="speed-negative"),
        pytest.param("speed_kmh", -1, id="speed-negative-int"),
        pytest.param("lat", 91.0, id="lat-91"),
        pytest.param("lat", 90.000001, id="lat-just-above"),
        pytest.param("lat", -90.5, id="lat-below"),
        pytest.param("lon", 180.5, id="lon-above"),
        pytest.param("lon", -181.0, id="lon-below"),
    ],
)
def test_decode_telemetry_out_of_range_raises_invalid_message(field: str, value: float) -> None:
    """Отрицательная скорость, координаты вне диапазона Point → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_telemetry(_raw({**TASK_TELEMETRY, field: value}))


@pytest.mark.parametrize("field", ["lat", "lon"])
def test_decode_telemetry_nan_coordinate_raises_invalid_message(field: str) -> None:
    """NaN координаты отвергает доменный Point (ValueError) → InvalidMessage."""
    raw = _raw(TASK_TELEMETRY).replace(
        f'"{field}": {TASK_TELEMETRY[field]}'.encode(), f'"{field}": NaN'.encode()
    )
    assert b"NaN" in raw

    with pytest.raises(InvalidMessage):
        decode_telemetry(raw)


# ---------------------------------------------------------------------------
# `iso_utc` — время ISO 8601 UTC с суффиксом Z (ТЗ, «Данные»)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("ts", "expected"),
    [
        pytest.param(1_789_473_600, "2026-09-15T12:00:00Z", id="task-now"),
        pytest.param(T1_OCCUPIED_AT, "2026-09-15T11:59:00Z", id="task-t1-occupied"),
        pytest.param(1_757_930_400, "2025-09-15T10:00:00Z", id="task-telemetry-ts"),
        pytest.param(0, "1970-01-01T00:00:00Z", id="epoch"),
        pytest.param(951_782_400, "2000-02-29T00:00:00Z", id="leap-day"),
    ],
)
def test_iso_utc_epoch_seconds_formatted_with_z(ts: int, expected: str) -> None:
    """Секунды epoch → ISO 8601 UTC, секундная точность, суффикс Z."""
    assert iso_utc(ts) == expected


# ---------------------------------------------------------------------------
# `encode_queue` — queue.v1, ключ station_uuid (ТЗ, «Данные»)
# ---------------------------------------------------------------------------

TASK_QUEUE: Final = StationQueue(
    station_id="S1",
    at=NOW,
    entries=(
        QueueEntry(T1, None, T1_OCCUPIED_AT, T1_FREE_AT, 0),
        QueueEntry(T2, T2_ETA, T1_FREE_AT, T2_FREE_AT, 50),
    ),
)
"""Очередь S1 из примера JSON queue.v1 в ТЗ (T1 и T2)."""

TASK_QUEUE_JSON: Final = {
    "station_uuid": "S1",
    "at": "2026-09-15T12:00:00Z",
    "queue": [
        {
            "unit_uuid": "T1",
            "eta": None,
            "service_start": "2026-09-15T11:59:00Z",
            "free_at": "2026-09-15T12:02:50Z",
            "wait_seconds": 0,
        },
        {
            "unit_uuid": "T2",
            "eta": "2026-09-15T12:02:00Z",
            "service_start": "2026-09-15T12:02:50Z",
            "free_at": "2026-09-15T12:06:40Z",
            "wait_seconds": 50,
        },
    ],
}
"""JSON queue.v1 из ТЗ один в один."""


def test_encode_queue_task_example_matches_task_json() -> None:
    """ТЗ «Данные», queue.v1: очередь S1 (T1, T2) совпадает с JSON из ТЗ, ключ — station_uuid."""
    key, value = encode_queue(TASK_QUEUE)

    assert key == b"S1"
    _assert_json_equal(value, TASK_QUEUE_JSON)
    for entry in json.loads(value)["queue"]:
        assert list(entry) == ["unit_uuid", "eta", "service_start", "free_at", "wait_seconds"]


def test_encode_queue_occupant_eta_null() -> None:
    """ТЗ: eta = null для машины, которая уже на станции; у едущей — строка ISO."""
    _, value = encode_queue(TASK_QUEUE)

    queue = json.loads(value)["queue"]
    assert queue[0]["eta"] is None
    assert b'"eta":null' in value
    assert queue[1]["eta"] == "2026-09-15T12:02:00Z"


def test_encode_queue_wait_seconds_is_integer() -> None:
    """ТЗ: wait_seconds — целое число секунд, не строка и не дробное."""
    _, value = encode_queue(TASK_QUEUE)

    waits = [e["wait_seconds"] for e in json.loads(value)["queue"]]
    assert waits == [0, 50]
    assert all(type(w) is int for w in waits)


def test_encode_queue_empty_queue_encoded_as_empty_list() -> None:
    """Пустая очередь (станция освободилась) публикуется со списком queue = []."""
    key, value = encode_queue(StationQueue(S2_ID, NOW, ()))

    assert key == b"S2"
    _assert_json_equal(value, {"station_uuid": "S2", "at": "2026-09-15T12:00:00Z", "queue": []})


def test_encode_queue_non_ascii_station_key_utf8() -> None:
    """Ключи сообщений — UTF-8."""
    key, value = encode_queue(StationQueue("Станция-1", NOW, ()))

    assert key == "Станция-1".encode()
    assert json.loads(value)["station_uuid"] == "Станция-1"


# ---------------------------------------------------------------------------
# `encode_decision` — decision.v1, ключ unit_uuid (ТЗ, «Данные»)
# ---------------------------------------------------------------------------


def test_encode_decision_recommended_matches_task_json() -> None:
    """ТЗ «Данные», decision.v1: рекомендация T4 S1 → S2, выигрыш 80 — JSON из ТЗ."""
    key, value = encode_decision(Recommendation(T4, NOW, "S1", "S2", 80))

    assert key == b"T4"
    _assert_json_equal(
        value,
        {
            "unit_uuid": "T4",
            "at": "2026-09-15T12:00:00Z",
            "result": "recommended",
            "from_station": "S1",
            "to_station": "S2",
            "gain_seconds": 80,
        },
    )


def test_encode_decision_rejected_no_gain_matches_task_json() -> None:
    """ТЗ «Данные», decision.v1: отказ T4 с причиной no_gain — JSON из ТЗ."""
    key, value = encode_decision(Rejection(T4, NOW, RejectReason.NO_GAIN))

    assert key == b"T4"
    _assert_json_equal(
        value,
        {
            "unit_uuid": "T4",
            "at": "2026-09-15T12:00:00Z",
            "result": "rejected",
            "reason": "no_gain",
        },
    )


def test_encode_decision_rejected_stale_telemetry_reason_code() -> None:
    """ТЗ «Коды отказа»: stale_telemetry кодируется строкой кода причины."""
    key, value = encode_decision(Rejection("T7", NOW, RejectReason.STALE_TELEMETRY))

    assert key == b"T7"
    _assert_json_equal(
        value,
        {
            "unit_uuid": "T7",
            "at": "2026-09-15T12:00:00Z",
            "result": "rejected",
            "reason": "stale_telemetry",
        },
    )


def test_encode_decision_non_ascii_unit_key_utf8() -> None:
    """Ключ decision.v1 — unit_uuid в UTF-8."""
    key, _ = encode_decision(Rejection("Т-1", NOW, RejectReason.NO_GAIN))

    assert key == "Т-1".encode()


# ---------------------------------------------------------------------------
# `encode_snapshot` / decode_snapshot — снимок SiteState для топика состояния
# ---------------------------------------------------------------------------


def _example_snapshot() -> SiteSnapshot:
    """Снимок площадки после потока примера ТЗ: есть занятость, редирект, решения, очереди."""
    state = SiteState(SITE)
    for msg in EXAMPLE_TELEMETRY:
        state.apply(msg)
    return state.snapshot()


def test_snapshot_example_is_non_trivial() -> None:
    """Снимок примера содержит все части состояния — round-trip проверяет их все."""
    snap = _example_snapshot()

    assert snap.now == NOW
    assert len(snap.tracks) == 4
    assert snap.redirects == ((T4, S2_ID),)
    assert set(snap.decided) == {T2, T4}
    assert any(occ.occupant is not None for occ in snap.occupancies)
    assert any(entries for _, entries in snap.published)


def test_snapshot_round_trip_after_task_example_equal() -> None:
    """`decode_snapshot(encode_snapshot(s)) == s` для состояния после примера ТЗ."""
    snap = _example_snapshot()

    raw = encode_snapshot(snap)

    assert isinstance(raw, bytes)
    assert decode_snapshot(raw) == snap


def test_snapshot_round_trip_initial_state_equal() -> None:
    """Снимок пустой площадки (now = None, нет машин) переживает round-trip."""
    snap = SiteState(SITE).snapshot()

    assert snap.now is None
    assert decode_snapshot(encode_snapshot(snap)) == snap


def test_snapshot_round_trip_intermediate_states_equal() -> None:
    """Round-trip верен после каждого сообщения потока примера, не только в конце."""
    state = SiteState(SITE)
    for msg in EXAMPLE_TELEMETRY:
        state.apply(msg)
        snap = state.snapshot()
        assert decode_snapshot(encode_snapshot(snap)) == snap


def test_snapshot_encoding_deterministic() -> None:
    """Равные снимки кодируются в одинаковые байты (идемпотентная запись в compacted-топик)."""
    assert encode_snapshot(_example_snapshot()) == encode_snapshot(_example_snapshot())


def test_snapshot_restored_state_continues_identically() -> None:
    """Состояние, восстановленное из закодированного снимка, обрабатывает поток так же."""
    original = SiteState(SITE)
    for msg in EXAMPLE_TELEMETRY[:3]:
        original.apply(msg)
    restored = SiteState(SITE, decode_snapshot(encode_snapshot(original.snapshot())))

    for msg in EXAMPLE_TELEMETRY[3:]:
        assert restored.apply(msg) == original.apply(msg)
    assert restored.snapshot() == original.snapshot()


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(b"", id="empty"),
        pytest.param(b"not json", id="text"),
        pytest.param(b"\xff", id="not-utf8"),
        pytest.param(b"[]", id="list"),
        pytest.param(b"null", id="null"),
        pytest.param(b"{}", id="empty-object"),
        pytest.param(b'{"now": "x"}', id="wrong-type"),
    ],
)
def test_decode_snapshot_broken_raises_invalid_message(raw: bytes) -> None:
    """Битый снимок → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_snapshot(raw)


def test_decode_snapshot_truncated_valid_snapshot_raises_invalid_message() -> None:
    """Обрезанный корректный снимок → InvalidMessage."""
    raw = encode_snapshot(_example_snapshot())

    with pytest.raises(InvalidMessage):
        decode_snapshot(raw[: len(raw) // 2])


# ---------------------------------------------------------------------------
# Границы ts и конечность скорости
# ---------------------------------------------------------------------------


def test_max_ts_formattable_in_iso_utc() -> None:
    """MAX_TS ещё представим в ISO 8601 с четырёхзначным годом (выход не переполнится)."""
    assert iso_utc(MAX_TS).startswith("9999-12-")
    assert iso_utc(MAX_TS).endswith("T23:59:59Z")


@pytest.mark.parametrize("ts", [0, MAX_TS])
def test_decode_telemetry_ts_bounds_inclusive_accepted(ts: int) -> None:
    """Границы ts включительно: 0 и MAX_TS принимаются."""
    assert decode_telemetry(_raw({**TASK_TELEMETRY, "ts": ts})).ts == ts


@pytest.mark.parametrize("ts", [-1, MAX_TS + 1])
def test_decode_telemetry_ts_out_of_bounds_raises_invalid_message(ts: int) -> None:
    """Значение ts < 0 или > MAX_TS → InvalidMessage."""
    with pytest.raises(InvalidMessage):
        decode_telemetry(_raw({**TASK_TELEMETRY, "ts": ts}))


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_decode_telemetry_non_finite_speed_raises_invalid_message(literal: str) -> None:
    """`speed_kmh` должна быть конечной: NaN и бесконечности → InvalidMessage."""
    raw = _raw(TASK_TELEMETRY).replace(b'"speed_kmh": 21.4', f'"speed_kmh": {literal}'.encode())
    assert literal.encode() in raw

    with pytest.raises(InvalidMessage):
        decode_telemetry(raw)
