"""Тесты фильтра порядка и дублей телеметрии.

ТЗ, «Порядок и дубли»: сообщение с ts, не превышающим последний учтённый ts той же машины,
игнорируется — это покрывает и дубли, и нарушение порядка. Опоздавшее сообщение с более
новым ts учитывается.
"""

from __future__ import annotations

from vqueue.domain.ingest import is_in_order


def test_is_in_order_first_message_no_last_ts_accepted() -> None:
    """Первое сообщение машины (учтённого ts ещё нет) учитывается."""
    assert is_in_order(None, 1_757_930_400) is True


def test_is_in_order_first_message_zero_ts_accepted() -> None:
    """Первое сообщение учитывается при любом ts, в т.ч. 0 (None не путается с 0)."""
    assert is_in_order(None, 0) is True


def test_is_in_order_newer_ts_accepted() -> None:
    """Сообщение с ts больше последнего учтённого — сообщение учитывается."""
    assert is_in_order(1_000, 1_001) is True


def test_is_in_order_equal_ts_duplicate_ignored() -> None:
    """Сообщение с ts, равным последнему учтённому (дубль), — сообщение игнорируется."""
    assert is_in_order(1_000, 1_000) is False


def test_is_in_order_older_ts_late_ignored() -> None:
    """Сообщение с ts меньше последнего учтённого (опоздание) — сообщение игнорируется."""
    assert is_in_order(1_000, 999) is False


def test_is_in_order_late_by_60s_ignored() -> None:
    """Опоздание на 60 с (максимум по ТЗ) со старым ts — игнорируется."""
    assert is_in_order(1_060, 1_000) is False


def test_is_in_order_late_arrival_but_newer_ts_accepted() -> None:
    """Опоздавшее сообщение с ts новее учтённого учитывается.

    Сообщение 1004 задержалось в пути; к его приходу учтено только 1003 — 1004 новее и
    учитывается, хотя пришло с опозданием.
    """
    last_ts = 1_003
    assert is_in_order(last_ts, 1_004) is True


def test_is_in_order_zero_last_ts_compares_numerically() -> None:
    """last_ts = 0 — обычное значение: ts 0 — дубль, ts 1 — новее."""
    assert is_in_order(0, 0) is False
    assert is_in_order(0, 1) is True
