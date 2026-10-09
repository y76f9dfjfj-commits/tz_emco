"""Модульные тесты продолжения виртуального времени генератора (vqueue.simulator.resume).

Правило (README, раздел о генераторе): «Время» ТЗ — расчёт ведётся во времени событий, поэтому
перезапущенный генератор не должен отправлять ts меньше уже записанных в топик. start_ts:
явный --start-ts — как есть (брокер не опрашивается); вывод в stdout — now (брокер не
опрашивается); иначе max(now, последний ts + 1), при пустом топике — now. _parse_ts: ts из
JSON-объекта сообщения; битое сообщение (не UTF-8, не JSON, не объект, нет ts, ts — строка,
bool, NaN/inf) → None; конечный float → int.
"""

from __future__ import annotations

import pytest

from vqueue.simulator.resume import _parse_ts, start_ts

NOW = 1_789_473_600
"""2026-09-15T12:00:00Z — «текущее время» тестов."""


def _never() -> int | None:
    """read_last_ts, который не должен вызываться."""
    raise AssertionError("read_last_ts не должен вызываться")


class _Reader:
    """read_last_ts с заданным ответом и счётчиком вызовов."""

    def __init__(self, last: int | None) -> None:
        self.last = last
        self.calls = 0

    def __call__(self) -> int | None:
        self.calls += 1
        return self.last


# --- start_ts ---------------------------------------------------------------


@pytest.mark.parametrize("output", ["stdout", "kafka"])
def test_start_ts_explicit_returns_it_without_reading_topic(output: str) -> None:
    """Явный --start-ts возвращается как есть, топик не читается (и для stdout, и для Kafka)."""
    assert start_ts(NOW - 10_000, output, NOW, _never) == NOW - 10_000


def test_start_ts_explicit_zero_is_explicit_not_missing() -> None:
    """explicit=0 — заданное значение (не «не задано»): возвращается 0, топик не читается."""
    assert start_ts(0, "kafka", NOW, _never) == 0


def test_start_ts_explicit_in_future_returned_as_is() -> None:
    """Явное значение позже now не подменяется."""
    assert start_ts(NOW + 500, "kafka", NOW, _never) == NOW + 500


def test_start_ts_stdout_returns_now_without_reading_topic() -> None:
    """Вывод в stdout: продолжать нечего — now, топик не читается."""
    assert start_ts(None, "stdout", NOW, _never) == NOW


def test_start_ts_kafka_empty_topic_returns_now() -> None:
    """Kafka, пустой топик (None) → now; топик читается ровно один раз."""
    reader = _Reader(None)
    assert start_ts(None, "kafka", NOW, reader) == NOW
    assert reader.calls == 1


def test_start_ts_kafka_last_in_past_returns_now() -> None:
    """Последний ts далеко в прошлом → now (max)."""
    reader = _Reader(NOW - 100)
    assert start_ts(None, "kafka", NOW, reader) == NOW
    assert reader.calls == 1


def test_start_ts_kafka_last_equals_now_minus_1_returns_now() -> None:
    """Граница: last + 1 == now → now."""
    assert start_ts(None, "kafka", NOW, _Reader(NOW - 1)) == NOW


def test_start_ts_kafka_last_equals_now_returns_now_plus_1() -> None:
    """Граница: last == now → now + 1 (ts не повторяется)."""
    assert start_ts(None, "kafka", NOW, _Reader(NOW)) == NOW + 1


def test_start_ts_kafka_last_in_future_returns_last_plus_1() -> None:
    """Ускоренный прогон ушёл вперёд реального времени → продолжаем с last + 1."""
    assert start_ts(None, "kafka", NOW, _Reader(NOW + 3_600)) == NOW + 3_601


def test_start_ts_kafka_last_zero_is_value_not_empty() -> None:
    """last=0 — значение, а не «пусто»: при now=0 результат max(0, 0 + 1) = 1."""
    assert start_ts(None, "kafka", 0, _Reader(0)) == 1


# --- _parse_ts --------------------------------------------------------------


def test_parse_ts_valid_message_returns_ts() -> None:
    """Корректное сообщение telemetry.v1 → его ts."""
    raw = b'{"unit_uuid":"T-01","ts":1789473600,"lat":49.1,"lon":142.6,"speed_kmh":0.0}'
    assert _parse_ts(raw) == 1_789_473_600


def test_parse_ts_only_ts_field_is_enough() -> None:
    """Для продолжения времени нужен только ts."""
    assert _parse_ts(b'{"ts": 5}') == 5


def test_parse_ts_integral_float_returns_int() -> None:
    """Поле ts как float с целым значением → int того же значения."""
    result = _parse_ts(b'{"ts": 1789473600.0}')
    assert result == 1_789_473_600
    assert type(result) is int


@pytest.mark.parametrize(
    "raw",
    [
        pytest.param(None, id="no-value"),
        pytest.param(b"", id="empty"),
        pytest.param(b"not json", id="not-json"),
        pytest.param(b'{"ts": 1', id="truncated-json"),
        pytest.param(b"\xff\xfe\x00", id="not-utf8"),
        pytest.param(b"[1, 2]", id="array"),
        pytest.param(b"1789473600", id="bare-number"),
        pytest.param(b'"1789473600"', id="bare-string"),
        pytest.param(b"null", id="null"),
        pytest.param(b'{"unit_uuid": "T-01"}', id="no-ts"),
        pytest.param(b'{"ts": null}', id="ts-null"),
        pytest.param(b'{"ts": "1789473600"}', id="ts-string"),
        pytest.param(b'{"ts": true}', id="ts-true"),
        pytest.param(b'{"ts": false}', id="ts-false"),
        pytest.param(b'{"ts": NaN}', id="ts-nan"),
        pytest.param(b'{"ts": Infinity}', id="ts-inf"),
        pytest.param(b'{"ts": -Infinity}', id="ts-minus-inf"),
        pytest.param(b'{"ts": [1]}', id="ts-list"),
        pytest.param(b'{"ts": {"v": 1}}', id="ts-object"),
    ],
)
def test_parse_ts_broken_message_returns_none(raw: bytes | None) -> None:
    """Битое сообщение (не UTF-8/JSON/объект, нет ts, ts не число или не конечен) → None."""
    assert _parse_ts(raw) is None
