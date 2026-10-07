"""Свойства агрегата площадки SiteState на случайных потоках телеметрии.

Потоки нескольких машин (в т.ч. незакреплённой) с дублями и опозданиями до 60 с (ТЗ,
«Данные»). Проверяются: детерминизм (ТЗ «Время» — результат не зависит от часов), игнор
дублей («Порядок и дубли»), восстановление из снимка, отсутствие одинаковых подряд
публикаций («Публикация»), одно решение за заезд («Рекомендация» п.5), отсутствие машины
в двух очередях сразу («Рекомендация» п.4), «сейчас» = наибольший учтённый ts.
"""

from __future__ import annotations

from typing import Final

from hypothesis import given
from hypothesis import strategies as st

from tests.sitekit import S1_POINT, SiteRun, north_of, tm
from vqueue.domain.model import SiteConfig, Station, Telemetry, UnitPhase
from vqueue.domain.queue import QueueEntry, StationQueue
from vqueue.domain.recommendation import Recommendation, Rejection
from vqueue.domain.site import SiteOutput, SiteState

_T0: Final = 1_789_473_600
_MAX_LATENESS: Final = 60

_SITE: Final = SiteConfig(
    stations=(Station("S1", S1_POINT), Station("S2", north_of(S1_POINT, -3_300.0))),
    unload_point=north_of(S1_POINT, -8_000.0),
    assignments={"T1": "S1", "T2": "S1", "T3": "S1", "T4": "S2"},
)
_UNITS: Final = ("T1", "T2", "T3", "T4", "X")
"""X не закреплена."""

_OFFSETS: Final = (
    15.0,  # в радиусе S1
    -20.0,  # в радиусе S1
    -100.0,  # сразу за радиусом S1
    -900.0,  # внутри радиуса решения S1
    -1_400.0,
    -1_600.0,  # за радиусом решения S1
    -2_000.0,  # 1300 м от S2: внутри радиуса решения S2
    -3_285.0,  # в радиусе S2
    -3_400.0,  # сразу за радиусом S2
    -5_000.0,
    -7_000.0,
    -8_000.0,  # точка разгрузки
    -25_000.0,  # за горизонтом
)
_POSITIONS: Final = tuple(north_of(S1_POINT, m) for m in _OFFSETS)


@st.composite
def _streams(draw: st.DrawFn) -> list[Telemetry]:
    """Поток: события по возрастанию ts, доставленные с задержкой до 60 с, плюс дубли."""
    size = draw(st.integers(min_value=0, max_value=30))
    deliveries: list[tuple[int, int, Telemetry]] = []
    ts = _T0
    for i in range(size):
        ts += draw(st.integers(min_value=0, max_value=25))
        msg = tm(
            ts,
            draw(st.sampled_from(_POSITIONS)),
            draw(st.sampled_from((0.0, 36.0))),
            unit_id=draw(st.sampled_from(_UNITS)),
        )
        deliveries.append((ts + draw(st.integers(0, _MAX_LATENESS)), i, msg))
        if draw(st.booleans()):
            deliveries.append((ts + draw(st.integers(0, _MAX_LATENESS)), i, msg))
    deliveries.sort(key=lambda d: (d[0], d[1]))
    return [msg for _, _, msg in deliveries]


def _run(state: SiteState, stream: list[Telemetry]) -> list[list[SiteOutput]]:
    """Выходы на каждое сообщение потока."""
    return [state.apply(msg) for msg in stream]


def _flat(outputs: list[list[SiteOutput]]) -> list[SiteOutput]:
    """Все выходы подряд."""
    return [o for step in outputs for o in step]


@given(_streams())
def test_site_same_stream_same_outputs(stream: list[Telemetry]) -> None:
    """ТЗ «Время»: два агрегата на одном потоке дают одинаковые выходы и состояние."""
    a, b = SiteState(_SITE), SiteState(_SITE)

    assert _run(a, stream) == _run(b, stream)
    assert a.snapshot() == b.snapshot()


@given(_streams(), st.data())
def test_site_inserted_duplicates_give_no_outputs_and_keep_rest(
    stream: list[Telemetry], data: st.DataObject
) -> None:
    """ТЗ «Порядок и дубли»: точный дубль уже поданного сообщения даёт [] и ничего не меняет.

    Выходы на исходные сообщения потока с дублями те же, что без них.
    """
    expected = _run(SiteState(_SITE), stream)

    state = SiteState(_SITE)
    for i, msg in enumerate(stream):
        assert state.apply(msg) == expected[i]
        if data.draw(st.booleans()):
            dup = stream[data.draw(st.integers(min_value=0, max_value=i))]
            before = state.snapshot()
            assert state.apply(dup) == []
            assert state.snapshot() == before


@given(_streams(), st.data())
def test_site_snapshot_restore_any_point_same_outputs(
    stream: list[Telemetry], data: st.DataObject
) -> None:
    """Восстановление из снимка в произвольной точке потока не меняет последующие выходы."""
    split = data.draw(st.integers(min_value=0, max_value=len(stream)))
    original = SiteState(_SITE)
    _run(original, stream[:split])
    snap = original.snapshot()
    restored = SiteState(_SITE, snap)

    assert restored.snapshot() == snap
    assert _run(restored, stream[split:]) == _run(original, stream[split:])
    assert restored.snapshot() == original.snapshot()


@given(_streams())
def test_site_consecutive_published_queues_of_station_differ(stream: list[Telemetry]) -> None:
    """ТЗ «Публикация»: одинаковые подряд очереди станции не публикуются.

    Первая публикация станции отличается от пустой очереди (пустая на старте не публикуется).
    """
    published: dict[str, tuple[QueueEntry, ...]] = {s.station_id: () for s in _SITE.stations}
    for out in _flat(_run(SiteState(_SITE), stream)):
        if isinstance(out, StationQueue):
            assert out.entries != published[out.station_id]
            published[out.station_id] = out.entries


@given(_streams())
def test_site_at_most_one_decision_per_unit_between_station_visits(
    stream: list[Telemetry],
) -> None:
    """ТЗ «Рекомендация» п.5: не более одного решения на машину между её посещениями станции.

    Посещение — фаза «на станции» по автомату фаз (параллельный прогон через домен).
    Решения выдаются только закреплённым машинам, по сообщению этой же машины, первым выходом.
    """
    state = SiteState(_SITE)
    run = SiteRun(_SITE)
    since_visit: dict[str, int] = {}
    for msg in stream:
        out = state.apply(msg)
        track = run.feed(msg) if _SITE.home_station_id(msg.unit_id) is not None else None
        decisions = [o for o in out if isinstance(o, Recommendation | Rejection)]
        assert len(decisions) <= 1
        if decisions:
            assert out[0] is decisions[0]
            assert decisions[0].unit_id == msg.unit_id
            assert track is not None
            since_visit[msg.unit_id] = since_visit.get(msg.unit_id, 0) + 1
            assert since_visit[msg.unit_id] == 1
        if track is not None and track.phase is UnitPhase.AT_STATION:
            since_visit[msg.unit_id] = 0


@given(_streams())
def test_site_unit_in_at_most_one_published_queue(stream: list[Telemetry]) -> None:
    """ТЗ «Рекомендация» п.4: машина учитывается в очереди только одной станции.

    В каждой опубликованной очереди машина не более одного раза; среди последних
    опубликованных очередей всех станций машина встречается не более одного раза.
    """
    latest: dict[str, tuple[QueueEntry, ...]] = {}
    state = SiteState(_SITE)
    for msg in stream:
        for out in state.apply(msg):
            if isinstance(out, StationQueue):
                ids = [e.unit_id for e in out.entries]
                assert len(ids) == len(set(ids))
                latest[out.station_id] = out.entries
        everywhere = [e.unit_id for entries in latest.values() for e in entries]
        assert len(everywhere) == len(set(everywhere))


@given(_streams())
def test_site_now_is_max_counted_ts_and_outputs_at_now(stream: list[Telemetry]) -> None:
    """ТЗ «Время», «Порядок и дубли»: «сейчас» — наибольший ts учтённых сообщений.

    Учитываются сообщения закреплённых машин с ts больше последнего учтённого ts машины;
    у каждого выхода at равно «сейчас»; на неучтённое сообщение выходов нет.
    """
    state = SiteState(_SITE)
    last_ts: dict[str, int] = {}
    now: int | None = None
    for msg in stream:
        out = state.apply(msg)
        counted = _SITE.home_station_id(msg.unit_id) is not None and (
            msg.unit_id not in last_ts or msg.ts > last_ts[msg.unit_id]
        )
        if counted:
            last_ts[msg.unit_id] = msg.ts
            now = msg.ts if now is None else max(now, msg.ts)
        else:
            assert out == []
        assert state.now == now
        assert all(o.at == now for o in out)
