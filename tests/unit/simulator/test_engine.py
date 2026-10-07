"""Тесты симуляции площадки (ТЗ, «Что нужно сделать» п.2 — генератор телеметрии).

Генератор: «машины ездят по прямой между точкой разгрузки и своей станцией с постоянной
скоростью, стоят на станции 200 секунд, соблюдают очередь». Симуляция проверяется как
чёрный ящик по выдаваемой телеметрии; домен (geo, unit_fsm, occupancy) — инструмент проверки.
"""

from __future__ import annotations

import itertools
from functools import cache
from pathlib import Path
from typing import Final, cast

from tests.simkit import (
    MOVING_KMH,
    SMALL_SITE,
    START_TS,
    STOPPED_KMH,
    UNLOAD,
    Visit,
    by_unit,
    distance_to_segment_m,
    offset,
    run,
    visits,
)
from vqueue.config import load_site_config
from vqueue.domain.geo import distance_m
from vqueue.domain.model import SiteConfig, Station, Telemetry, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy, update_occupancy
from vqueue.domain.unit_fsm import UnitTrack, advance
from vqueue.simulator.engine import Simulation

_SEED: Final = 7
_STEPS: Final = 1_500
"""25 минут малой площадки: несколько циклов каждой машины и очередь на S1."""

_SEGMENT_TOLERANCE_M: Final = 1.0
_STEP_TOLERANCE_M: Final = 0.01
_CONFIG_PATH: Final = Path(__file__).resolve().parents[3] / "config" / "site.toml"


@cache
def _small_run() -> tuple[tuple[Telemetry, ...], ...]:
    """Один прогон малой площадки, общий для тестов модуля (выдача по шагам)."""
    sim = Simulation(SMALL_SITE, START_TS, _SEED)
    return tuple(tuple(batch) for batch in run(sim, _STEPS))


def _small_by_unit() -> dict[str, list[Telemetry]]:
    return by_unit([list(b) for b in _small_run()])


def test_simulation_initial_ts_equals_start_ts() -> None:
    """До первого шага виртуальное время равно start_ts."""
    assert Simulation(SMALL_SITE, START_TS, _SEED).ts == START_TS


def test_step_advances_ts_by_one_second() -> None:
    """Каждый step продвигает время ровно на 1 с."""
    sim = Simulation(SMALL_SITE, START_TS, _SEED)
    for i in range(1, 50):
        sim.step()
        assert sim.ts == START_TS + i


def test_step_emits_one_message_per_assigned_unit_with_new_ts() -> None:
    """Шаг выдаёт ровно одно сообщение на каждую закреплённую машину, ts = новое время."""
    expected = sorted(SMALL_SITE.assignments)
    for i, batch in enumerate(_small_run(), start=1):
        assert sorted(m.unit_id for m in batch) == expected
        assert {m.ts for m in batch} == {START_TS + i}


def test_simulation_same_inputs_produce_same_stream() -> None:
    """Одинаковые (site, start_ts, seed) дают одинаковые последовательности step()."""
    a = run(Simulation(SMALL_SITE, START_TS, 42), 300)
    b = run(Simulation(SMALL_SITE, START_TS, 42), 300)
    assert a == b


def test_simulation_different_seed_produces_different_stream() -> None:
    """Другой seed даёт другое начальное состояние и другой поток."""
    a = run(Simulation(SMALL_SITE, START_TS, 1), 5)
    b = run(Simulation(SMALL_SITE, START_TS, 2), 5)
    assert a != b


def test_simulation_start_ts_shifts_stream_time_only_by_offset() -> None:
    """Сообщения датируются от start_ts: первый шаг даёт ts = start_ts + 1."""
    batch = Simulation(SMALL_SITE, START_TS + 1_000, 3).step()
    assert {m.ts for m in batch} == {START_TS + 1_001}


def test_speed_is_only_zero_or_design_speed() -> None:
    """Скорость в сообщении — 36.0 в пути и 0.0 на месте, других значений нет."""
    speeds = {m.speed_kmh for batch in _small_run() for m in batch}
    assert speeds <= {STOPPED_KMH, MOVING_KMH}
    assert speeds == {STOPPED_KMH, MOVING_KMH}


def test_displacement_per_step_not_above_design_speed() -> None:
    """За 1 с машина смещается не больше чем на speed_mps (10 м) — постоянная скорость."""
    limit = SMALL_SITE.rules.speed_mps + _STEP_TOLERANCE_M
    for msgs in _small_by_unit().values():
        for prev, cur in itertools.pairwise(msgs):
            assert distance_m(prev.position, cur.position) <= limit


def test_moving_steps_mostly_cover_design_speed() -> None:
    """В пути машина едет с постоянной скоростью: подавляющая часть шагов — ровно 10 м."""
    speed = SMALL_SITE.rules.speed_mps
    for msgs in _small_by_unit().values():
        moving = [
            distance_m(prev.position, cur.position)
            for prev, cur in itertools.pairwise(msgs)
            if prev.speed_kmh == MOVING_KMH and cur.speed_kmh == MOVING_KMH
        ]
        assert moving
        full = [d for d in moving if abs(d - speed) <= 0.05]
        assert len(full) >= 0.9 * len(moving)


def test_stopped_unit_stays_in_place() -> None:
    """Два подряд сообщения со скоростью 0 — без смещения (стоящая машина не ползёт)."""
    for msgs in _small_by_unit().values():
        for prev, cur in itertools.pairwise(msgs):
            if prev.speed_kmh == STOPPED_KMH and cur.speed_kmh == STOPPED_KMH:
                assert distance_m(prev.position, cur.position) <= _STEP_TOLERANCE_M


def test_unit_moves_only_on_segment_between_unload_and_own_station() -> None:
    """Машина находится на отрезке «своя станция — точка разгрузки» (езда по прямой)."""
    site = SMALL_SITE
    radius = site.rules.zone_radius_m
    for unit, msgs in _small_by_unit().items():
        home = site.station(site.assignments[unit]).location
        for m in msgs:
            off = distance_to_segment_m(m.position, home, site.unload_point)
            # Ждущая машина может стоять в стороне от отрезка, но в радиусе станции.
            assert off <= _SEGMENT_TOLERANCE_M or distance_m(m.position, home) < radius


def test_stopped_unit_is_inside_own_station_radius() -> None:
    """Стоящая машина (скорость 0) всегда строго внутри радиуса 50 м своей станции."""
    site = SMALL_SITE
    for unit, msgs in _small_by_unit().items():
        home = site.station(site.assignments[unit]).location
        for m in msgs:
            if m.speed_kmh == STOPPED_KMH:
                assert distance_m(m.position, home) < site.rules.zone_radius_m


def test_unit_reaches_unload_point_radius_in_each_cycle() -> None:
    """Машина доезжает до точки разгрузки (входит в её радиус) — цикл через разгрузку."""
    site = SMALL_SITE
    for msgs in _small_by_unit().values():
        closest = min(distance_m(m.position, site.unload_point) for m in msgs)
        assert closest < site.rules.zone_radius_m


def test_completed_visit_stands_at_least_service_seconds() -> None:
    """Полный визит на станцию — не меньше 200 с стоянки; без ожидания — ровно 200 с."""
    site = SMALL_SITE
    stops = [
        v.stopped
        for unit, msgs in _small_by_unit().items()
        for v in visits(
            msgs, site.station(site.assignments[unit]).location, site.rules.zone_radius_m
        )
        if v.complete
    ]
    assert stops
    assert min(stops) == site.rules.service_seconds


def _station_visits(
    site: SiteConfig, streams: dict[str, list[Telemetry]]
) -> dict[str, list[tuple[str, Visit]]]:
    """Завершённые отъездом визиты по станциям: (unit_id, визит)."""
    result: dict[str, list[tuple[str, Visit]]] = {s.station_id: [] for s in site.stations}
    for unit, msgs in streams.items():
        sid = site.assignments[unit]
        for v in visits(msgs, site.station(sid).location, site.rules.zone_radius_m):
            if v.last_stopped_ts is not None and v.last_ts != msgs[-1].ts:
                result[sid].append((unit, v))
    return result


def test_waiting_unit_departs_exactly_occupancy_after_previous() -> None:
    """Ждавшая машина (стояла дольше 200 с) уезжает ровно через 230 с после предыдущей.

    ТЗ: занятие станции = 200 с обслуживания + 30 с манёвра; ждущая начинает обслуживание
    в момент освобождения станции предыдущей машиной.
    """
    rules = SMALL_SITE.rules
    checked = 0
    for items in _station_visits(SMALL_SITE, _small_by_unit()).values():
        ordered = sorted(items, key=lambda uv: cast(int, uv[1].last_stopped_ts))
        for (_, prev), (_, cur) in itertools.pairwise(ordered):
            if cur.complete and cur.stopped > rules.service_seconds:
                checked += 1
                assert cur.last_stopped_ts == cast(int, prev.last_stopped_ts) + (
                    rules.occupancy_seconds
                )
    assert checked > 0


def test_departure_order_follows_arrival_order() -> None:
    """Порядок отъездов со станции — порядок входа в радиус (при равенстве — по unit_id)."""
    checked = 0
    for items in _station_visits(SMALL_SITE, _small_by_unit()).values():
        for (ua, a), (ub, b) in itertools.combinations(items, 2):
            if a.from_window_start and b.from_window_start:
                continue  # момент входа обеих машин до окна наблюдения неизвестен
            checked += 1
            by_arrival = (a.first_ts, ua) < (b.first_ts, ub)
            by_departure = cast(int, a.last_stopped_ts) < cast(int, b.last_stopped_ts)
            assert by_arrival == by_departure
    assert checked > 0


_SAME_POINT_SITE: Final = SiteConfig(
    stations=(Station("S0", UNLOAD), Station("S1", offset(UNLOAD, 1_000.0, 0.0))),
    unload_point=UNLOAD,
    assignments={"Z1": "S0", "Z2": "S0", "A1": "S1"},
)


def test_station_at_unload_point_simulates_without_errors() -> None:
    """Граница: станция совпадает с точкой разгрузки (маршрут 0 м) — шаги без ошибок.

    Машины этой станции остаются в её радиусе, стоят и обслуживаются; сообщения валидны.
    """
    site = _SAME_POINT_SITE
    rules = site.rules
    sim = Simulation(site, START_TS, 5)
    batches = run(sim, 1_000)
    assert sim.ts == START_TS + 1_000
    for i, batch in enumerate(batches, start=1):
        assert sorted(m.unit_id for m in batch) == sorted(site.assignments)
        assert {m.ts for m in batch} == {START_TS + i}
    streams = by_unit(batches)
    for unit in ("Z1", "Z2"):
        msgs = streams[unit]
        assert all(m.speed_kmh in (STOPPED_KMH, MOVING_KMH) for m in msgs)
        assert all(distance_m(m.position, UNLOAD) < rules.zone_radius_m for m in msgs)
        assert any(m.speed_kmh == STOPPED_KMH for m in msgs)
    occupants = _occupants(site, batches)
    assert {o.unit_id for o in occupants["S0"]} == {"Z1", "Z2"}


def _departures(site: SiteConfig, streams: dict[str, list[Telemetry]]) -> dict[str, list[int]]:
    """Моменты отъезда (последнее сообщение со скоростью 0 визита) по станциям."""
    result: dict[str, list[int]] = {s.station_id: [] for s in site.stations}
    for unit, msgs in streams.items():
        sid = site.assignments[unit]
        home = site.station(sid).location
        for v in visits(msgs, home, site.rules.zone_radius_m):
            # Визит, обрезанный концом окна, ещё не завершён отъездом.
            if v.last_stopped_ts is not None and v.last_ts != msgs[-1].ts:
                result[sid].append(v.last_stopped_ts)
    return {sid: sorted(ts) for sid, ts in result.items()}


def test_service_starts_of_one_station_are_spaced_by_occupancy_seconds() -> None:
    """Обслуживания на станции не перекрываются: начала соседних — не чаще чем через 230 с.

    Начало обслуживания выводится из телеметрии: отъезд минус 200 с, поэтому интервал между
    отъездами равен интервалу между началами обслуживания.
    """
    occupancy = SMALL_SITE.rules.occupancy_seconds
    deps = _departures(SMALL_SITE, _small_by_unit())
    assert len(deps["S1"]) >= 3
    for times in deps.values():
        for prev, cur in itertools.pairwise(times):
            assert cur - prev >= occupancy


def test_queue_forms_at_overloaded_station() -> None:
    """На S1 (4 машины, цикл ~400 с) машины ждут: есть визит длиннее 200 с стоянки."""
    site = SMALL_SITE
    home = site.station("S1").location
    streams = _small_by_unit()
    longest = max(
        v.stopped
        for unit in ("A1", "A2", "A3", "A4")
        for v in visits(streams[unit], home, site.rules.zone_radius_m)
    )
    assert longest > site.rules.service_seconds


def _occupants(site: SiteConfig, batches: list[list[Telemetry]]) -> dict[str, list[Occupant]]:
    """Прогон телеметрии через домен: все занятия по станциям в порядке появления."""
    tracks: dict[str, UnitTrack] = {}
    occ = {s.station_id: StationOccupancy(s.station_id) for s in site.stations}
    seen: dict[str, list[Occupant]] = {s.station_id: [] for s in site.stations}
    for batch in batches:
        for msg in batch:
            track = advance(tracks.get(msg.unit_id), msg, site)
            tracks[msg.unit_id] = track
            for sid, current in occ.items():
                occ[sid] = update_occupancy(current, track, site.rules)
                o = occ[sid].occupant
                items = seen[sid]
                if o is None or (items and items[-1] == o):
                    continue
                same = items and items[-1].unit_id == o.unit_id
                if same and items[-1].occupied_at == o.occupied_at:
                    # То же занятие, уточнён free_at (выход из радиуса).
                    items[-1] = o
                else:
                    items.append(o)
    return seen


def test_domain_occupations_do_not_overlap() -> None:
    """По расчёту домена занятия станции не перекрываются и каждая станция обслуживает."""
    occupants = _occupants(SMALL_SITE, [list(b) for b in _small_run()])
    for sid, items in occupants.items():
        assert items, sid
        for prev, cur in itertools.pairwise(items):
            assert cur.occupied_at >= prev.free_at


def test_domain_occupation_keeps_station_for_service_seconds() -> None:
    """Домен видит занятие не короче 200 с: машина не уезжает раньше конца обслуживания."""
    rules = SMALL_SITE.rules
    occupants = _occupants(SMALL_SITE, [list(b) for b in _small_run()])
    end = START_TS + _STEPS
    for items in occupants.values():
        for o in items:
            if o.occupied_at + rules.occupancy_seconds < end and o.occupied_at > START_TS + 1:
                assert o.free_at - o.occupied_at >= rules.service_seconds


_ALLOWED: Final = {
    (UnitPhase.TO_STATION, UnitPhase.AT_STATION),
    (UnitPhase.AT_STATION, UnitPhase.TO_UNLOAD),
    (UnitPhase.TO_UNLOAD, UnitPhase.TO_STATION),
}


def test_full_cycle_on_working_site_within_two_hours() -> None:
    """Рабочая площадка (config/site.toml), 2 ч: у каждой машины полный цикл ТЗ.

    Фазы по unit_fsm меняются только по циклу «к станции → на станции → к разгрузке →
    к станции», и каждая машина проходит все три перехода.
    """
    site = load_site_config(_CONFIG_PATH)
    sim = Simulation(site, START_TS, 1)
    tracks: dict[str, UnitTrack] = {}
    transitions: dict[str, set[tuple[UnitPhase, UnitPhase]]] = {u: set() for u in site.assignments}
    for _ in range(7_200):
        for msg in sim.step():
            prev = tracks.get(msg.unit_id)
            track = advance(prev, msg, site)
            tracks[msg.unit_id] = track
            if prev is not None and prev.phase is not track.phase:
                transitions[msg.unit_id].add((prev.phase, track.phase))
    assert sim.ts == START_TS + 7_200
    for unit, seen in transitions.items():
        assert seen <= _ALLOWED, unit
        assert seen == _ALLOWED, unit
