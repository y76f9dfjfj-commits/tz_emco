"""Golden-тесты по разделу ТЗ «Пример»: станция S1, момент расчёта 12:00:00 (2026-09-15).

Одна машина занимает станцию на 230 секунд: 200 обслуживания и 30 манёвра.

Вход — только телеметрия:
- T1 стоит в 15 м от S1, скорость 0, первое такое сообщение в 11:59:00 → заняла станцию
  в 11:59:00;
- T2 в 1200 м от S1, едет к станции → приезд через 120 с, в 12:02:00;
- T3 в 3000 м от S1, едет к станции → приезд через 300 с, в 12:05:00.

Очередь к S1:
1. T1 — приезд «—», начало обслуживания 11:59:00, освободит 12:02:50, ожидание 0 с;
2. T2 — приезд 12:02:00, начало 12:02:50, освободит 12:06:40, ожидание 50 с;
3. T3 — приезд 12:05:00, начало 12:06:40, освободит 12:10:30, ожидание 100 с.

Решение: T4 едет к своей S1 и пересекает радиус 1500 м (до S1 900 м, до S2 2400 м,
у S2 очереди нет). Ожидание у S1 — 80 с (приезд 12:01:30, освобождение 12:02:50 после T1),
у S2 — 0 с (приезд 12:04:00) → направить T4 к S2, выигрыш 80 с.

Файл разбит на разделы по этапам расчёта: занятие станции (T1), очередь, решение,
сквозной прогон всего примера через агрегат площадки SiteState.
Для раздела «Решение» построена отдельная площадка (S1 и S2 в 3300 м к югу от S1), где
T4 в 900 м от S1 и 2400 м от S2; T1–T3 и очередь к S1 на ней те же, что в таблице ТЗ.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

import pytest

from tests.sitekit import FAR_POINT, S1_ID, S1_POINT, UNIT_ID, SiteRun, make_site, north_of, tm
from vqueue.domain.geo import distance_m
from vqueue.domain.model import SiteConfig, Station, Telemetry, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy
from vqueue.domain.queue import (
    QueueEntry,
    StationQueue,
    build_station_queue,
    estimate_arrival,
    wait_before,
)
from vqueue.domain.recommendation import (
    Recommendation,
    Rejection,
    RejectReason,
    is_decision_point,
    recommend,
)
from vqueue.domain.site import SiteOutput, SiteState
from vqueue.domain.unit_fsm import UnitTrack

# ---------------------------------------------------------------------------
# Общие данные примера
# ---------------------------------------------------------------------------

SITE: Final = make_site()

T1: Final = UNIT_ID
"""В sitekit машина T1 закреплена за S1 — как в примере."""

NOW: Final = 1_789_473_600
"""2026-09-15T12:00:00Z — момент расчёта примера."""

T1_OCCUPIED_AT: Final = 1_789_473_540
"""2026-09-15T11:59:00Z — первое сообщение T1 «стоит в 15 м от S1»."""

T1_FREE_AT: Final = 1_789_473_770
"""2026-09-15T12:02:50Z — 11:59:00 + 230 с."""

T1_POSITION: Final = north_of(S1_POINT, 15.0)
"""T1 стоит в 15 м от S1."""


def _iso(ts: int) -> str:
    """Время в формате ISO 8601 UTC, как в выходных сообщениях ТЗ."""
    return datetime.fromtimestamp(ts, UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _replay(messages: list[Telemetry]) -> tuple[UnitTrack, StationOccupancy]:
    """Прогоняет телеметрию через площадку; возвращает трек T1 и занятость S1."""
    run = SiteRun(SITE)
    run.feed_all(messages)
    return run.tracks[T1], run.occupancy[S1_ID]


# ---------------------------------------------------------------------------
# Занятие станции (ТЗ, «Занятие и освобождение станции», правила 1 и 2)
# ---------------------------------------------------------------------------


def test_example_constants_match_task_times() -> None:
    """Опорные ts примера соответствуют 11:59:00 и 12:02:50 UTC, T1 — в 15 м от S1."""
    assert _iso(T1_OCCUPIED_AT) == "2026-09-15T11:59:00Z"
    assert _iso(T1_FREE_AT) == "2026-09-15T12:02:50Z"
    assert round(distance_m(S1_POINT, T1_POSITION), 3) == 15.0


def test_example_t1_first_stopped_message_occupies_s1_until_12_02_50() -> None:
    """Пример ТЗ: T1 стоит в 15 м от S1, первое сообщение 11:59:00 → занята 11:59:00–12:02:50."""
    track, occ = _replay([tm(T1_OCCUPIED_AT, T1_POSITION, 0.0)])

    assert track.phase is UnitPhase.AT_STATION
    assert occ == StationOccupancy(
        S1_ID,
        Occupant(
            unit_id=T1,
            visit_entered_at=T1_OCCUPIED_AT,
            occupied_at=T1_OCCUPIED_AT,
            free_at=T1_FREE_AT,
        ),
        served_visits=frozenset({(T1, T1_OCCUPIED_AT)}),
    )
    assert occ.occupant is not None
    assert _iso(occ.occupant.occupied_at) == "2026-09-15T11:59:00Z"
    assert _iso(occ.occupant.free_at) == "2026-09-15T12:02:50Z"


def test_example_t1_after_approach_occupies_at_first_stopped_message() -> None:
    """Пример ТЗ с подъездом: T1 вошла в радиус на ходу, встала в 11:59:00 → занятие в 11:59:00.

    «Первое такое сообщение» — первое, где машина стоит в радиусе; вход в радиус на ходу
    станцию не занимает, но задаёт ключ визита.
    """
    track, occ = _replay(
        [
            tm(T1_OCCUPIED_AT - 60, FAR_POINT, 30.0),
            tm(T1_OCCUPIED_AT - 10, north_of(S1_POINT, 40.0), 5.0),
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
        ]
    )

    assert track.phase is UnitPhase.AT_STATION
    assert occ.occupant == Occupant(
        unit_id=T1,
        visit_entered_at=T1_OCCUPIED_AT - 10,
        occupied_at=T1_OCCUPIED_AT,
        free_at=T1_FREE_AT,
    )


def test_example_s1_busy_at_calculation_moment_12_00_00() -> None:
    """Пример ТЗ: в момент расчёта 12:00:00 станция S1 занята T1, в 12:02:50 — свободна."""
    _, occ = _replay(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(NOW, T1_POSITION, 0.0),
        ]
    )

    assert _iso(NOW) == "2026-09-15T12:00:00Z"
    assert occ.is_busy(NOW)
    assert occ.is_busy(T1_FREE_AT - 1)
    assert not occ.is_busy(T1_FREE_AT)


# ---------------------------------------------------------------------------
# Очередь (ТЗ, «Очередь», правила 1–3)
# ---------------------------------------------------------------------------

T2: Final = "T2"
T3: Final = "T3"


def _example_queue() -> StationQueue:
    """Прогоняет телеметрию примера и строит очередь к S1 на 12:00:00.

    T1 стоит в 15 м от S1 с 11:59:00 (и в 12:00:00); T2 и T3 едут к S1 из 1200 м и 3000 м,
    их позиции — в 12:00:00.
    """
    run = SiteRun(SITE)
    run.feed_all(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(NOW, north_of(S1_POINT, 1_200.0), 36.0, unit_id=T2),
            tm(NOW, north_of(S1_POINT, 3_000.0), 36.0, unit_id=T3),
            tm(NOW, T1_POSITION, 0.0),
        ]
    )
    assert run.tracks[T2].phase is UnitPhase.TO_STATION
    assert run.tracks[T3].phase is UnitPhase.TO_STATION
    return build_station_queue(run.occupancy[S1_ID], run.tracks.values(), SITE, NOW)


def test_example_queue_s1_matches_task_table() -> None:
    """Пример ТЗ, таблица «Очередь к S1»: T1 (занявшая), T2 ждёт 50 с, T3 ждёт 100 с."""
    queue = _example_queue()

    assert queue == StationQueue(
        S1_ID,
        NOW,
        (
            QueueEntry(T1, None, T1_OCCUPIED_AT, T1_FREE_AT, 0),
            QueueEntry(T2, NOW + 120, T1_FREE_AT, T1_FREE_AT + 230, 50),
            QueueEntry(T3, NOW + 300, T1_FREE_AT + 230, T1_FREE_AT + 460, 100),
        ),
    )


def test_example_queue_s1_times_in_iso() -> None:
    """Пример ТЗ: времена очереди к S1 совпадают с таблицей ТЗ в ISO 8601 UTC."""
    queue = _example_queue()

    rows = [
        (
            e.unit_id,
            None if e.eta is None else _iso(e.eta),
            _iso(e.service_start),
            _iso(e.free_at),
            e.wait_seconds,
        )
        for e in queue.entries
    ]
    assert _iso(queue.at) == "2026-09-15T12:00:00Z"
    assert rows == [
        ("T1", None, "2026-09-15T11:59:00Z", "2026-09-15T12:02:50Z", 0),
        ("T2", "2026-09-15T12:02:00Z", "2026-09-15T12:02:50Z", "2026-09-15T12:06:40Z", 50),
        ("T3", "2026-09-15T12:05:00Z", "2026-09-15T12:06:40Z", "2026-09-15T12:10:30Z", 100),
    ]


def test_example_queue_independent_of_tracks_order() -> None:
    """Пример ТЗ, п.2: очередь к S1 не зависит от порядка, в котором переданы треки."""
    run = SiteRun(SITE)
    run.feed_all(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(NOW, north_of(S1_POINT, 3_000.0), 36.0, unit_id=T3),
            tm(NOW, north_of(S1_POINT, 1_200.0), 36.0, unit_id=T2),
            tm(NOW, T1_POSITION, 0.0),
        ]
    )
    tracks = list(run.tracks.values())
    forward = build_station_queue(run.occupancy[S1_ID], tracks, SITE, NOW)
    backward = build_station_queue(run.occupancy[S1_ID], reversed(tracks), SITE, NOW)

    assert forward == backward == _example_queue()


def test_example_queue_stale_occupant_stays_first() -> None:
    """Пример ТЗ, п.5: T1 молчит с 11:59:00 (позиция старше 30 с) — всё равно первая.

    Правило свежести не касается машины, занявшей станцию: очередь та же, что в таблице ТЗ.
    """
    run = SiteRun(SITE)
    run.feed_all(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(NOW, north_of(S1_POINT, 1_200.0), 36.0, unit_id=T2),
            tm(NOW, north_of(S1_POINT, 3_000.0), 36.0, unit_id=T3),
        ]
    )
    assert NOW - run.tracks[T1].last_ts > SITE.rules.freshness_seconds

    queue = build_station_queue(run.occupancy[S1_ID], run.tracks.values(), SITE, NOW)

    assert queue == _example_queue()


# ---------------------------------------------------------------------------
# Решение для T4 (ТЗ, «Рекомендация», «Коды отказа»)
# ---------------------------------------------------------------------------

T4: Final = "T4"

DECISION_S2_ID: Final = "S2"
DECISION_S2_POINT: Final = north_of(S1_POINT, -3_300.0)
"""S2 площадки примера решения: в 3300 м к югу от S1."""

T4_POSITION: Final = north_of(S1_POINT, -900.0)
"""T4 в 900 м к югу от S1, т.е. в 2400 м к северу от S2."""

DECISION_SITE: Final = SiteConfig(
    stations=(Station(S1_ID, S1_POINT), Station(DECISION_S2_ID, DECISION_S2_POINT)),
    unload_point=north_of(S1_POINT, -8_000.0),
    assignments={T1: S1_ID, T2: S1_ID, T3: S1_ID, T4: S1_ID},
)
"""Площадка примера решения: S1, S2 и точка разгрузки далеко от всех машин; все за S1."""

T4_ETA_S1: Final = NOW + 90
"""12:01:30 — 900 м / 10 м/с."""

T4_ETA_S2: Final = NOW + 240
"""12:04:00 — 2400 м / 10 м/с."""


def _decision_run() -> SiteRun:
    """Прогоняет телеметрию примера на площадке решения: T1–T3 как в разделе «Очередь», T4."""
    run = SiteRun(DECISION_SITE)
    run.feed_all(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(NOW, north_of(S1_POINT, 1_200.0), 36.0, unit_id=T2),
            tm(NOW, north_of(S1_POINT, 3_000.0), 36.0, unit_id=T3),
            tm(NOW, T4_POSITION, 36.0, unit_id=T4),
            tm(NOW, T1_POSITION, 0.0),
        ]
    )
    return run


def _decision_queues(run: SiteRun, *, include_t4: bool) -> dict[str, StationQueue]:
    """Очереди S1 и S2 на 12:00:00: к S1 едут T2, T3 (и T4 — если include_t4); к S2 никто."""
    to_s1 = [run.tracks[u] for u in (T1, T2, T3)]
    if include_t4:
        to_s1.append(run.tracks[T4])
    return {
        S1_ID: build_station_queue(run.occupancy[S1_ID], to_s1, DECISION_SITE, NOW),
        DECISION_S2_ID: build_station_queue(run.occupancy[DECISION_S2_ID], [], DECISION_SITE, NOW),
    }


def test_example_decision_geometry_900m_to_s1_2400m_to_s2() -> None:
    """Пример ТЗ: T4 в 900 м от S1 и в 2400 м от S2; T2, T3 вне радиусов станций."""
    assert round(distance_m(T4_POSITION, S1_POINT), 3) == 900.0
    assert round(distance_m(T4_POSITION, DECISION_S2_POINT), 3) == 2400.0
    assert distance_m(north_of(S1_POINT, 1_200.0), DECISION_S2_POINT) > 1_500.0


def test_example_decision_site_keeps_s1_queue_of_task_table() -> None:
    """Пример ТЗ: на площадке решения очередь к S1 без T4 совпадает с таблицей ТЗ."""
    run = _decision_run()
    assert _decision_queues(run, include_t4=False)[S1_ID] == _example_queue()


def test_example_t4_is_at_decision_point() -> None:
    """Пример ТЗ: T4 едет к своей S1 и ближе 1500 м к ней — момент расчёта рекомендации."""
    run = _decision_run()
    track = run.tracks[T4]
    assert track.phase is UnitPhase.TO_STATION
    assert is_decision_point(track, DECISION_SITE.station(S1_ID), DECISION_SITE.rules)


def test_example_t4_eta_and_wait_at_s1_is_80() -> None:
    """Пример ТЗ: приезд T4 к S1 в 12:01:30, освобождение после T1 в 12:02:50 → 80 с.

    T2 приедет в 12:02:00 — позже T4 — и на ожидание T4 не влияет.
    """
    run = _decision_run()
    queue = _decision_queues(run, include_t4=False)[S1_ID]
    eta = estimate_arrival(
        run.tracks[T4], DECISION_SITE.station(S1_ID), run.occupancy[S1_ID], DECISION_SITE, NOW
    )
    assert eta == T4_ETA_S1
    assert _iso(eta) == "2026-09-15T12:01:30Z"
    assert wait_before(queue, eta, T4) == 80


def test_example_t4_wait_at_s1_same_with_t4_in_queue() -> None:
    """Пример ТЗ: в своей очереди T4 стоит второй (12:01:30, ждёт 80) — wait_before тот же."""
    run = _decision_run()
    queue = _decision_queues(run, include_t4=True)[S1_ID]
    own = next(e for e in queue.entries if e.unit_id == T4)
    assert own == QueueEntry(T4, T4_ETA_S1, T1_FREE_AT, T1_FREE_AT + 230, 80)
    assert wait_before(queue, T4_ETA_S1, T4) == 80


def test_example_t4_eta_and_wait_at_s2_is_0() -> None:
    """Пример ТЗ: приезд T4 к S2 в 12:04:00, очереди у S2 нет → ожидание 0."""
    run = _decision_run()
    queue = _decision_queues(run, include_t4=False)[DECISION_S2_ID]
    eta = estimate_arrival(
        run.tracks[T4],
        DECISION_SITE.station(DECISION_S2_ID),
        run.occupancy[DECISION_S2_ID],
        DECISION_SITE,
        NOW,
    )
    assert queue.entries == ()
    assert eta == T4_ETA_S2
    assert _iso(eta) == "2026-09-15T12:04:00Z"
    assert wait_before(queue, eta, T4) == 0


@pytest.mark.parametrize("include_t4", [False, True])
def test_example_decision_recommends_s2_with_gain_80(include_t4: bool) -> None:
    """Пример ТЗ, decision.v1: T4, 12:00:00, from S1, to S2, gain_seconds 80.

    Результат одинаков, есть ли T4 в очереди своей S1 или нет.
    """
    run = _decision_run()
    decision = recommend(
        run.tracks[T4],
        _decision_queues(run, include_t4=include_t4),
        run.occupancy,
        DECISION_SITE,
        NOW,
    )
    assert decision == Recommendation(T4, NOW, S1_ID, DECISION_S2_ID, 80)
    assert _iso(decision.at) == "2026-09-15T12:00:00Z"


# ---------------------------------------------------------------------------
# Сквозной пример: весь поток телеметрии примера через агрегат площадки
# (ТЗ, «Пример», «Публикация», «Рекомендация» п.4)
# ---------------------------------------------------------------------------

_T1_EVERY_SECOND: Final = [tm(ts, T1_POSITION, 0.0) for ts in range(T1_OCCUPIED_AT, NOW + 1)]
"""T1 стоит в 15 м от S1 и передаёт позицию каждую секунду с 11:59:00 по 12:00:00."""

_T1_TWO_MESSAGES: Final = [tm(T1_OCCUPIED_AT, T1_POSITION, 0.0), tm(NOW, T1_POSITION, 0.0)]
"""T1 стоит в 15 м от S1: сообщения только в 11:59:00 и 12:00:00."""


def _end_to_end(t1_messages: list[Telemetry]) -> list[SiteOutput]:
    """Прогоняет поток примера через SiteState: T1, затем T2, T3, T4 в 12:00:00."""
    state = SiteState(DECISION_SITE)
    stream = [
        *t1_messages,
        tm(NOW, north_of(S1_POINT, 1_200.0), 36.0, unit_id=T2),
        tm(NOW, north_of(S1_POINT, 3_000.0), 36.0, unit_id=T3),
        tm(NOW, T4_POSITION, 36.0, unit_id=T4),
    ]
    outputs: list[SiteOutput] = []
    for msg in stream:
        outputs.extend(state.apply(msg))
    assert state.now == NOW
    return outputs


_T1_STREAMS: Final = pytest.mark.parametrize(
    "t1_messages",
    [
        pytest.param(_T1_EVERY_SECOND, id="t1-every-second"),
        pytest.param(_T1_TWO_MESSAGES, id="t1-two-messages"),
    ],
)


@_T1_STREAMS
def test_end_to_end_t4_recommended_to_s2_with_gain_80(t1_messages: list[Telemetry]) -> None:
    """Пример ТЗ, decision.v1 для T4: рекомендация S1 → S2 в 12:00:00, выигрыш 80 с.

    Первое сообщение T4 уже ближе 1500 м к S1 в состоянии «к станции» — решение сразу.
    """
    outputs = _end_to_end(t1_messages)

    t4_decisions = [
        o for o in outputs if isinstance(o, Recommendation | Rejection) and o.unit_id == T4
    ]
    assert t4_decisions == [Recommendation(T4, NOW, S1_ID, DECISION_S2_ID, 80)]
    assert _iso(t4_decisions[0].at) == "2026-09-15T12:00:00Z"


@_T1_STREAMS
def test_end_to_end_other_decisions_only_t2_no_gain(t1_messages: list[Telemetry]) -> None:
    """Пример ТЗ: T2 в 1200 м от S1 тоже в точке решения — отказ no_gain (выигрыш 50 < 60).

    T2 у S1 ждёт 50 с, у S2 (4500 м) — 0 с; T3 в 3000 м и T1 на станции решений не получают.
    """
    outputs = _end_to_end(t1_messages)

    decisions = [o for o in outputs if isinstance(o, Recommendation | Rejection)]
    assert decisions == [
        Rejection(T2, NOW, RejectReason.NO_GAIN),
        Recommendation(T4, NOW, S1_ID, DECISION_S2_ID, 80),
    ]


@_T1_STREAMS
def test_end_to_end_published_queues_match_task(t1_messages: list[Telemetry]) -> None:
    """Пример ТЗ: итоговая очередь S1 — таблица ТЗ без T4; очередь S2 — только T4.

    После рекомендации T4 считается едущей к S2: приезд 12:04:00, начало 12:04:00,
    освобождение 12:07:50, ожидание 0.
    """
    outputs = _end_to_end(t1_messages)

    last = {o.station_id: o for o in outputs if isinstance(o, StationQueue)}
    assert last[S1_ID] == _example_queue()
    assert last[DECISION_S2_ID] == StationQueue(
        DECISION_S2_ID, NOW, (QueueEntry(T4, T4_ETA_S2, T4_ETA_S2, T4_ETA_S2 + 230, 0),)
    )
    assert _iso(T4_ETA_S2 + 230) == "2026-09-15T12:07:50Z"


@_T1_STREAMS
def test_end_to_end_consecutive_queues_differ(t1_messages: list[Telemetry]) -> None:
    """ТЗ «Публикация»: подряд опубликованные очереди одной станции различаются.

    T1, стоящая каждую секунду на месте, не порождает повторных публикаций очереди S1.
    Очередь S1 публикуется ровно 3 раза: занятие T1 (11:59:00), добавление T2 и добавление T3
    (12:00:00). Рекомендация T4 очередь S1 не меняет: T4 сразу считается едущей к S2.
    """
    outputs = _end_to_end(t1_messages)

    published: dict[str, tuple[QueueEntry, ...]] = {}
    for o in outputs:
        if isinstance(o, StationQueue):
            assert o.entries != published.get(o.station_id, ())
            published[o.station_id] = o.entries
    s1_count = sum(1 for o in outputs if isinstance(o, StationQueue) and o.station_id == S1_ID)
    assert s1_count == 3
