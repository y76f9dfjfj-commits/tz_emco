"""Свойства генератора телеметрии на случайных seed и start_ts (ТЗ, генератор и «Данные»).

Симуляция: инварианты движения (скорость 0/36, шаг не больше 10 м, только по отрезку
«станция — разгрузка», стоянка только в радиусе своей станции) и детерминизм.
Сквозной поток Simulation + FaultInjector → SiteState: дубли не меняют результат
(«Порядок и дубли»), опоздания не ломают обработку и не теряют последнюю позицию.
"""

from __future__ import annotations

import itertools
from typing import Final

from hypothesis import given, settings
from hypothesis import strategies as st

from tests.simkit import (
    MOVING_KMH,
    SMALL_SITE,
    START_TS,
    STOPPED_KMH,
    by_unit,
    distance_to_segment_m,
    run,
)
from vqueue.domain.geo import distance_m
from vqueue.domain.model import Telemetry
from vqueue.domain.site import SiteOutput, SiteState
from vqueue.simulator.engine import Simulation
from vqueue.simulator.faults import FaultInjector

_STEPS: Final = 400
_seeds = st.integers(min_value=0, max_value=2**32 - 1)
_starts = st.integers(min_value=START_TS - 10**6, max_value=START_TS + 10**6)
_rates = st.floats(min_value=0.0, max_value=1.0)


@settings(max_examples=15, deadline=None)
@given(seed=_seeds, start=_starts)
def test_simulation_motion_invariants(seed: int, start: int) -> None:
    """При любом seed: скорость 0/36, шаг ≤ 10 м, путь по отрезку, стоянка в радиусе станции."""
    site = SMALL_SITE
    rules = site.rules
    batches = run(Simulation(site, start, seed), _STEPS)
    assert [b[0].ts for b in batches] == list(range(start + 1, start + _STEPS + 1))
    for unit, msgs in by_unit(batches).items():
        assert len(msgs) == _STEPS
        home = site.station(site.assignments[unit]).location
        for prev, cur in itertools.pairwise(msgs):
            assert distance_m(prev.position, cur.position) <= rules.speed_mps + 0.01
        for m in msgs:
            assert m.speed_kmh in (STOPPED_KMH, MOVING_KMH)
            in_radius = distance_m(m.position, home) < rules.zone_radius_m
            if m.speed_kmh == STOPPED_KMH:
                assert in_radius
            assert in_radius or distance_to_segment_m(m.position, home, site.unload_point) <= 1.0


@settings(max_examples=10, deadline=None)
@given(seed=_seeds, start=_starts)
def test_simulation_deterministic(seed: int, start: int) -> None:
    """Одинаковые (site, start_ts, seed) — одинаковые последовательности step()."""
    a = run(Simulation(SMALL_SITE, start, seed), 50)
    b = run(Simulation(SMALL_SITE, start, seed), 50)
    assert a == b


def _deliver(seed: int, sim_seed: int, dup: float, late: float) -> list[Telemetry]:
    """Поток после сбоев доставки в порядке отправки (с финальным flush)."""
    sim = Simulation(SMALL_SITE, START_TS, sim_seed)
    inj = FaultInjector(seed, dup_rate=dup, late_rate=late, max_delay_s=60)
    out: list[Telemetry] = []
    for _ in range(_STEPS):
        batch = sim.step()
        out.extend(inj.feed(sim.ts, batch))
    out.extend(inj.flush())
    return out


def _process(stream: list[Telemetry]) -> tuple[SiteState, list[SiteOutput]]:
    state = SiteState(SMALL_SITE)
    outputs: list[SiteOutput] = []
    for msg in stream:
        outputs.extend(state.apply(msg))
    return state, outputs


@settings(max_examples=8, deadline=None)
@given(seed=_seeds, sim_seed=_seeds, dup=_rates)
def test_duplicates_do_not_change_site_state(seed: int, sim_seed: int, dup: float) -> None:
    """Дубли без опозданий: выдача и снимок SiteState те же, что без дублей."""
    clean = _deliver(seed, sim_seed, 0.0, 0.0)
    noisy = _deliver(seed, sim_seed, dup, 0.0)
    clean_state, clean_out = _process(clean)
    noisy_state, noisy_out = _process(noisy)
    assert noisy_out == clean_out
    assert noisy_state.snapshot() == clean_state.snapshot()


@settings(max_examples=8, deadline=None)
@given(seed=_seeds, sim_seed=_seeds, dup=_rates, late=_rates)
def test_late_and_duplicates_keep_last_position(
    seed: int, sim_seed: int, dup: float, late: float
) -> None:
    """Дубли и опоздания до 60 с не ломают обработку; итоговая позиция машин — последняя."""
    clean_state, _ = _process(_deliver(seed, sim_seed, 0.0, 0.0))
    noisy_state, _ = _process(_deliver(seed, sim_seed, dup, late))
    assert noisy_state.now == START_TS + _STEPS
    clean_tracks = {t.unit_id: t for t in clean_state.snapshot().tracks}
    for t in noisy_state.snapshot().tracks:
        assert t.last_ts == START_TS + _STEPS
        assert t.position == clean_tracks[t.unit_id].position
