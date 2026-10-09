"""Тесты сбоев доставки генератора: дубли и опоздания (ТЗ, «Данные»).

ТЗ: «В потоке есть дубли и сообщения не по порядку с опозданием до 60 секунд»; генератор
«умеет добавлять дубли и опоздания». ts сообщений не меняются: опоздание — позднее
получение сообщения со старым ts.
"""

from __future__ import annotations

import math
from collections import Counter
from typing import Final

import pytest

from tests.simkit import UNLOAD
from vqueue.domain.model import Telemetry
from vqueue.simulator.faults import FaultInjector

_T0: Final = 1_789_473_600
_UNITS: Final = ("T1", "T2", "T3")


def _batch(now: int) -> list[Telemetry]:
    """Сообщения, сгенерированные в момент now (ts = now), по одному на машину."""
    return [Telemetry(u, now, UNLOAD, 36.0) for u in _UNITS]


def _drive(inj: FaultInjector, seconds: int) -> tuple[list[Telemetry], list[tuple[int, Telemetry]]]:
    """Подаёт по секунде; возвращает вход и выдачу (момент отправки, сообщение) с flush."""
    sent: list[Telemetry] = []
    out: list[tuple[int, Telemetry]] = []
    for i in range(seconds):
        now = _T0 + i
        batch = _batch(now)
        sent.extend(batch)
        out.extend((now, m) for m in inj.feed(now, batch))
    out.extend((_T0 + seconds, m) for m in inj.flush())
    return sent, out


def test_zero_rates_pass_stream_unchanged() -> None:
    """dup_rate = late_rate = 0: выдача совпадает с входом и порядком, отложенного нет."""
    inj = FaultInjector(seed=1)
    for i in range(100):
        batch = _batch(_T0 + i)
        assert inj.feed(_T0 + i, batch) == batch
    assert inj.flush() == []


def test_dup_rate_one_sends_every_message_twice() -> None:
    """dup_rate = 1: каждое сообщение отправлено ровно дважды (с учётом flush)."""
    sent, out = _drive(FaultInjector(seed=2, dup_rate=1.0), 100)
    assert Counter(m for _, m in out) == Counter(sent + sent)


def test_late_rate_one_delays_every_message_within_bounds() -> None:
    """late_rate = 1: всё отложено на 1..max_delay_s, ts не меняется."""
    max_delay = 10
    inj = FaultInjector(seed=3, late_rate=1.0, max_delay_s=max_delay)
    seconds = 200
    sent, out = _drive(inj, seconds)
    assert Counter(m for _, m in out) == Counter(sent)
    for sent_at, m in out:
        if sent_at < _T0 + seconds:  # без flush: момент отправки известен точно
            assert 1 <= sent_at - m.ts <= max_delay


def test_late_rate_one_nothing_sent_at_generation_time() -> None:
    """late_rate = 1: в момент генерации сообщение не отправляется."""
    inj = FaultInjector(seed=4, late_rate=1.0, max_delay_s=5)
    for i in range(50):
        now = _T0 + i
        assert all(m.ts < now for m in inj.feed(now, _batch(now)))


def test_late_delays_spread_over_range() -> None:
    """Задержки распределены по 1..max_delay_s, а не фиксированы."""
    max_delay = 5
    seconds = 300
    _, out = _drive(FaultInjector(seed=5, late_rate=1.0, max_delay_s=max_delay), seconds)
    delays = {at - m.ts for at, m in out if at < _T0 + seconds}
    assert delays == set(range(1, max_delay + 1))


def test_max_delay_one_delays_exactly_one_second() -> None:
    """Граница max_delay_s = 1: каждое сообщение приходит ровно через 1 с."""
    seconds = 50
    _, out = _drive(FaultInjector(seed=6, late_rate=1.0, max_delay_s=1), seconds)
    assert {at - m.ts for at, m in out if at < _T0 + seconds} == {1}


@pytest.mark.parametrize(("dup", "late"), [(0.3, 0.3), (1.0, 1.0), (0.5, 0.0), (0.0, 0.5)])
def test_any_rates_delay_not_above_max_and_never_early(dup: float, late: float) -> None:
    """Любая выдача (в т.ч. дубль) — не раньше ts и не позже ts + max_delay_s (ТЗ: до 60 с)."""
    max_delay = 60
    seconds = 300
    _, out = _drive(FaultInjector(seed=7, dup_rate=dup, late_rate=late), seconds)
    for at, m in out:
        if at < _T0 + seconds:
            assert 0 <= at - m.ts <= max_delay


@pytest.mark.parametrize(("dup", "late"), [(0.3, 0.3), (1.0, 1.0), (0.0, 0.7)])
def test_nothing_lost_extras_are_only_duplicates(dup: float, late: float) -> None:
    """Ничего не теряется: выдача ⊇ входа, лишнее — только копии входных сообщений."""
    sent, out = _drive(FaultInjector(seed=8, dup_rate=dup, late_rate=late), 200)
    got = Counter(m for _, m in out)
    expected = Counter(sent)
    assert not expected - got
    assert set(got) == set(expected)
    # Не более одного дубля на сообщение.
    assert all(got[m] <= 2 * expected[m] for m in got)


def test_dup_rate_zero_has_no_duplicates() -> None:
    """dup_rate = 0 при опозданиях: каждое сообщение ровно один раз."""
    sent, out = _drive(FaultInjector(seed=9, late_rate=0.5), 200)
    assert Counter(m for _, m in out) == Counter(sent)


def test_flush_returns_all_pending_and_then_nothing() -> None:
    """Метод flush отдаёт всё отложенное; повторный flush пуст."""
    inj = FaultInjector(seed=10, late_rate=1.0, max_delay_s=60)
    batch = _batch(_T0)
    assert inj.feed(_T0, batch) == []
    assert Counter(inj.flush()) == Counter(batch)
    assert inj.flush() == []


def test_pending_released_when_due() -> None:
    """Отложенное отправляется, когда время дошло до срока, даже без новых сообщений."""
    inj = FaultInjector(seed=11, late_rate=1.0, max_delay_s=3)
    batch = _batch(_T0)
    inj.feed(_T0, batch)
    released: list[Telemetry] = []
    for i in range(1, 4):
        released.extend(inj.feed(_T0 + i, []))
    assert Counter(released) == Counter(batch)
    assert inj.flush() == []


def test_due_output_ordered_by_due_time_then_arrival() -> None:
    """При пропуске времени выдача упорядочена по сроку, при равенстве — по поступлению."""
    inj = FaultInjector(seed=12, late_rate=1.0, max_delay_s=1)
    first = _batch(_T0)
    second = _batch(_T0 + 1)
    assert inj.feed(_T0, first) == []
    assert inj.feed(_T0 + 1, second) == first
    assert inj.feed(_T0 + 2, []) == second


def test_injector_deterministic_by_seed() -> None:
    """Одинаковый seed и вход — одинаковая выдача; другой seed — другая."""
    _, a = _drive(FaultInjector(seed=13, dup_rate=0.3, late_rate=0.3), 100)
    _, b = _drive(FaultInjector(seed=13, dup_rate=0.3, late_rate=0.3), 100)
    _, c = _drive(FaultInjector(seed=14, dup_rate=0.3, late_rate=0.3), 100)
    assert a == b
    assert a != c


@pytest.mark.parametrize("rate", [-0.01, 1.01, math.nan, math.inf, -math.inf])
def test_dup_rate_out_of_range_raises(rate: float) -> None:
    """dup_rate вне [0, 1] или не число — ValueError."""
    with pytest.raises(ValueError):
        FaultInjector(seed=0, dup_rate=rate)


@pytest.mark.parametrize("rate", [-0.01, 1.01, math.nan, math.inf, -math.inf])
def test_late_rate_out_of_range_raises(rate: float) -> None:
    """late_rate вне [0, 1] или не число — ValueError."""
    with pytest.raises(ValueError):
        FaultInjector(seed=0, late_rate=rate)


@pytest.mark.parametrize("delay", [0, -1])
def test_max_delay_below_one_raises(delay: int) -> None:
    """max_delay_s < 1 — ValueError."""
    with pytest.raises(ValueError):
        FaultInjector(seed=0, max_delay_s=delay)


@pytest.mark.parametrize(("dup", "late", "delay"), [(0.0, 0.0, 1), (1.0, 1.0, 1), (0.0, 1.0, 60)])
def test_boundary_parameters_accepted(dup: float, late: float, delay: int) -> None:
    """Границы 0 и 1 для rates и max_delay_s = 1 допустимы."""
    FaultInjector(seed=0, dup_rate=dup, late_rate=late, max_delay_s=delay)
