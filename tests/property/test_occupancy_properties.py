"""Property-тесты занятия и освобождения станции на потоках телеметрии нескольких машин.

ТЗ, «Занятие и освобождение станции»:
1. Машина занимает станцию в момент первого сообщения «на станции», когда станция свободна.
2. Станция освобождается через 230 с после занятия либо в момент выхода занявшей машины
   из радиуса 50 м — что раньше.

Поток 2–3 машин у станций (позиции в радиусе, у его границы и вне его, скорости около
порога 1 км/ч, сообщения разных машин не по порядку) прогоняется через SiteRun —
advance и update_occupancy для каждой станции; после каждого сообщения проверяются
инварианты по ТЗ.
"""

from __future__ import annotations

import math
from typing import Final

from hypothesis import given
from hypothesis import strategies as st

from tests.sitekit import FAR_POINT, S1_POINT, S2_POINT, UNLOAD_POINT, SiteRun, tm
from vqueue.domain.geo import EARTH_RADIUS_M
from vqueue.domain.model import Point, Telemetry, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy
from vqueue.domain.unit_fsm import UnitTrack

T_START: Final = 1_789_473_000
UNITS: Final = ("A", "B", "C")

_DEG_PER_M: Final = 180.0 / (math.pi * EARTH_RADIUS_M)


@st.composite
def positions(draw: st.DrawFn) -> Point:
    """Чаще всего — около S1 (0–80 м, в т.ч. у границы 50 м); реже — разгрузка, S2, в пути."""
    anchor = draw(st.sampled_from((S1_POINT, S1_POINT, S1_POINT, UNLOAD_POINT, S2_POINT)))
    if draw(st.integers(min_value=0, max_value=9)) == 0:
        return FAR_POINT
    north = draw(
        st.one_of(
            st.sampled_from((0.0, 15.0, -30.0, 49.0, 49.9, 50.1, 51.0, -60.0)),
            st.floats(min_value=-80.0, max_value=80.0, allow_nan=False),
        )
    )
    return Point(anchor.lat + north * _DEG_PER_M, anchor.lon)


speeds = st.one_of(
    st.sampled_from((0.0, 0.5, 0.99, 1.0, 1.01, 20.0)),
    st.floats(min_value=0.0, max_value=5.0, allow_nan=False),
)


@st.composite
def multi_unit_stream(draw: st.DrawFn) -> list[Telemetry]:
    """Перемешанный поток 2–3 машин; глобальный шаг времени от −60 до +60 с.

    Отрицательный шаг даёт опоздания между машинами; опоздание одной и той же машины
    отбрасывается прогоном (ТЗ, «Порядок и дубли»).
    """
    units = UNITS[: draw(st.integers(min_value=2, max_value=3))]
    n = draw(st.integers(min_value=1, max_value=60))
    ts = T_START
    result: list[Telemetry] = []
    for _ in range(n):
        ts += draw(st.integers(min_value=-60, max_value=60))
        unit = draw(st.sampled_from(units))
        result.append(tm(ts, draw(positions()), draw(speeds), unit))
    return result


def _visit_at(track: UnitTrack, station_id: str) -> int | None:
    """Ключ визита, если машина «на станции» station_id; иначе None."""
    if track.phase is UnitPhase.AT_STATION and track.station_id == station_id:
        assert track.zone_entry is not None
        return track.zone_entry.entered_at
    return None


def _is_new_occupation(old: Occupant | None, new: Occupant | None) -> bool:
    """Запись о занятии сменилась (а не только сдвинулся её free_at)."""
    if new is None:
        return False
    if old is None:
        return True
    return (old.unit_id, old.visit_entered_at, old.occupied_at) != (
        new.unit_id,
        new.visit_entered_at,
        new.occupied_at,
    )


def _check_station_step(
    prev: StationOccupancy,
    occ: StationOccupancy,
    track: UnitTrack,
    run: SiteRun,
    history: list[Occupant],
) -> None:
    """Проверяет инварианты ТЗ для одной станции после одного учтённого сообщения.

    history — занятия этой станции в порядке появления (с актуальным free_at последнего).
    """
    t = track.last_ts
    rules = run.site.rules
    station_id = occ.station_id
    old, new = prev.occupant, occ.occupant

    if new is not None:
        # Правило 2: срок занятия не больше 230 с и положителен; занятие — не раньше въезда.
        assert 0 < new.free_at - new.occupied_at <= rules.occupancy_seconds
        assert new.occupied_at >= new.visit_entered_at

    if _is_new_occupation(old, new):
        assert new is not None
        # Правило 1: занимает машина этого сообщения, «на станции» этой станции, в момент t.
        visit = _visit_at(track, station_id)
        assert visit is not None
        assert new == Occupant(track.unit_id, visit, t, t + rules.occupancy_seconds)
        assert occ.has_served(track.unit_id, visit)
        # Визит занимает станцию не более одного раза.
        assert all((o.unit_id, o.visit_entered_at) != (new.unit_id, visit) for o in history)
        if history:
            # Занятия не перекрываются и идут по времени (правило 2 — освобождение в t,
            # если прежняя занявшая уже не в своём визите).
            last = history[-1]
            assert new.occupied_at >= last.occupied_at
            if last.unit_id != track.unit_id:
                assert last.free_at <= new.occupied_at
        history.append(new)
    else:
        assert (old is None) == (new is None)
        if old is not None and new is not None and new.free_at != old.free_at:
            # Правило 2: досрочное освобождение — только сообщением занявшей, в момент t.
            assert track.unit_id == old.unit_id
            assert new.free_at == t < old.free_at
            assert _visit_at(track, station_id) != old.visit_entered_at
        if new is not None:
            history[-1] = new

    # served_visits: каждая запись — машина, чей последний трек «на станции» здесь в этом визите.
    for unit_id, visit_key in occ.served_visits:
        assert _visit_at(run.tracks[unit_id], station_id) == visit_key
        assert occ.has_served(unit_id, visit_key)
    if occ.occupant == prev.occupant and occ.served_visits == prev.served_visits:
        assert occ is prev


@given(multi_unit_stream())
def test_occupancy_invariants_hold_on_multi_unit_streams(stream: list[Telemetry]) -> None:
    """Правила 1–2 на произвольных потоках телеметрии нескольких машин.

    Срок занятия в (0, 230], занятие только «на станции», визит занимает не более раза,
    занятия не перекрываются, served_visits согласованы с треками.
    """
    run = SiteRun()
    histories: dict[str, list[Occupant]] = {sid: [] for sid in run.occupancy}
    for msg in stream:
        before = dict(run.occupancy)
        track = run.feed(msg)
        if track is None:
            assert run.occupancy == before
            continue
        for station_id, occ in run.occupancy.items():
            _check_station_step(before[station_id], occ, track, run, histories[station_id])
