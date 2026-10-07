"""Property-тесты расписания станции.

ТЗ, «Очередь», п.2: машины выстраиваются по времени приезда; каждая занимает станцию
на время занятия (обслуживание + манёвр) с момента приезда либо с момента освобождения
станции предыдущей, если он позже; ожидание — начало обслуживания минус приезд.
Первая начинает не раньше освобождения станции (free_from).
"""

from __future__ import annotations

from itertools import pairwise

from hypothesis import given
from hypothesis import strategies as st

from vqueue.domain.model import Rules
from vqueue.domain.queue import schedule

_T0 = 1_789_473_600

unit_ids = st.text(alphabet="ABCT0123456789", min_size=1, max_size=4)
etas = st.integers(min_value=_T0 - 600, max_value=_T0 + 1800)
arrivals_lists = st.dictionaries(unit_ids, etas, max_size=12).map(lambda d: list(d.items()))
free_froms = st.none() | etas
rules_st = st.builds(
    Rules,
    service_seconds=st.integers(min_value=1, max_value=400),
    maneuver_seconds=st.integers(min_value=1, max_value=60),
)


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_keeps_every_unit_once_with_its_eta(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: в расписании каждая машина ровно один раз и с тем же временем приезда."""
    entries = schedule(arrivals, free_from, rules)
    assert sorted((e.unit_id, e.eta) for e in entries) == sorted(arrivals)


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_ordered_by_eta_then_unit_id(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: порядок — по приезду, при равенстве по unit_id."""
    entries = schedule(arrivals, free_from, rules)
    keys = [(e.eta, e.unit_id) for e in entries]
    assert keys == sorted(keys)


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_start_not_before_eta_and_wait_is_difference(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: начало не раньше приезда; ожидание = начало - приезд, не отрицательно."""
    for e in schedule(arrivals, free_from, rules):
        assert e.eta is not None
        assert e.service_start >= e.eta
        assert e.wait_seconds == e.service_start - e.eta
        assert e.wait_seconds >= 0


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_each_interval_lasts_occupancy(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: каждая занимает станцию ровно на обслуживание + манёвр."""
    for e in schedule(arrivals, free_from, rules):
        assert e.free_at - e.service_start == rules.occupancy_seconds


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_intervals_do_not_overlap_and_go_in_order(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: интервалы [service_start, free_at) не перекрываются и идут по порядку."""
    entries = schedule(arrivals, free_from, rules)
    for prev, cur in pairwise(entries):
        assert cur.service_start >= prev.free_at


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_start_is_max_of_eta_and_previous_release(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2: начало = max(приезд, освобождение предыдущей) — станция не простаивает зря."""
    entries = schedule(arrivals, free_from, rules)
    for prev, cur in pairwise(entries):
        assert cur.eta is not None
        assert cur.service_start == max(cur.eta, prev.free_at)


@given(arrivals_lists, free_froms, rules_st)
def test_schedule_first_starts_not_before_free_from(
    arrivals: list[tuple[str, int]], free_from: int | None, rules: Rules
) -> None:
    """П.2–3: первая начинает в max(приезд, free_from); без free_from — по приезду."""
    entries = schedule(arrivals, free_from, rules)
    if not entries:
        return
    first = entries[0]
    assert first.eta is not None
    expected = first.eta if free_from is None else max(first.eta, free_from)
    assert first.service_start == expected


@given(
    arrivals_lists.flatmap(lambda a: st.tuples(st.just(a), st.permutations(a))),
    free_froms,
    rules_st,
)
def test_schedule_independent_of_input_permutation(
    pair: tuple[list[tuple[str, int]], list[tuple[str, int]]],
    free_from: int | None,
    rules: Rules,
) -> None:
    """П.2, детерминизм: перестановка входа не меняет расписание."""
    original, shuffled = pair
    assert schedule(original, free_from, rules) == schedule(shuffled, free_from, rules)
