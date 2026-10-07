"""Тесты агрегата площадки SiteState: обработка потока телеметрии целиком.

ТЗ: «Время» («сейчас» — наибольший учтённый ts), «Порядок и дубли», «Публикация»
(queue.v1 при изменении очереди, одинаковые подряд не публикуются; decision.v1 на каждый
расчёт), «Рекомендация» (один раз за заезд, п.4 — перенаправление до конца обслуживания,
п.5 — следующая рекомендация после посещения станции), «Коды отказа».

Площадка: S1, S2 в 3300 м к югу от S1, точка разгрузки в 8000 м к югу от S1.
T1, T2, T4 закреплены за S1, T5 — за S2; X не закреплена. Позиции задаются смещением
по меридиану от S1 (отрицательное — к югу, в сторону S2 и точки разгрузки).
"""

from __future__ import annotations

from dataclasses import replace
from typing import Final

import pytest
from pydantic import TypeAdapter

from tests.sitekit import S1_POINT, north_of, tm
from vqueue.domain.model import Point, SiteConfig, Station, Telemetry
from vqueue.domain.occupancy import StationOccupancy
from vqueue.domain.queue import QueueEntry, StationQueue
from vqueue.domain.recommendation import Recommendation, Rejection, RejectReason
from vqueue.domain.site import SiteOutput, SiteSnapshot, SiteState

S1: Final = "S1"
S2: Final = "S2"
T1: Final = "T1"
T2: Final = "T2"
T4: Final = "T4"
T5: Final = "T5"
STRANGER: Final = "X"

T0: Final = 1_789_473_600
"""2026-09-15T12:00:00Z."""

MOVING: Final = 36.0
STOPPED: Final = 0.0


def pos(meters: float) -> Point:
    """Точка в meters к северу от S1 (отрицательное — к югу)."""
    return north_of(S1_POINT, meters)


S2_OFFSET: Final = -3_300.0
UNLOAD_OFFSET: Final = -8_000.0

SITE: Final = SiteConfig(
    stations=(Station(S1, pos(0.0)), Station(S2, pos(S2_OFFSET))),
    unload_point=pos(UNLOAD_OFFSET),
    assignments={T1: S1, T2: S1, T4: S1, T5: S2},
)

AT_S1: Final = pos(15.0)
"""В радиусе S1."""
AT_S2: Final = pos(S2_OFFSET + 15.0)
"""В радиусе S2."""
BEYOND_HORIZON: Final = pos(-25_000.0)
"""Дальше горизонта 30 минут и от S1, и от S2."""


def feed(state: SiteState, messages: list[Telemetry]) -> list[SiteOutput]:
    """Применяет сообщения по порядку и возвращает все выходы подряд."""
    out: list[SiteOutput] = []
    for msg in messages:
        out.extend(state.apply(msg))
    return out


def decisions(outputs: list[SiteOutput]) -> list[Recommendation | Rejection]:
    """Решения (рекомендации и отказы) из выходов."""
    return [o for o in outputs if isinstance(o, Recommendation | Rejection)]


def queues(outputs: list[SiteOutput]) -> list[StationQueue]:
    """Опубликованные очереди из выходов."""
    return [o for o in outputs if isinstance(o, StationQueue)]


def units(queue: StationQueue) -> list[str]:
    """Машины очереди по порядку."""
    return [e.unit_id for e in queue.entries]


def t1_occupies_s1() -> SiteState:
    """T1 стоит на S1 с T0: заняла станцию до T0+230."""
    state = SiteState(SITE)
    state.apply(tm(T0, AT_S1, STOPPED, unit_id=T1))
    return state


RECOMMENDATION: Final = Recommendation(T4, T0 + 60, S1, S2, 80)
"""T4 в 900 м от S1 в T0+60: у S1 приезд T0+150, освобождение T0+230 → 80 с; у S2 — 0 с."""


def t4_recommended_to_s2() -> tuple[SiteState, list[SiteOutput]]:
    """T1 заняла S1 в T0; первое сообщение T4 — в 900 м от S1 (2400 м от S2) в T0+60."""
    state = t1_occupies_s1()
    out = state.apply(tm(T0 + 60, pos(-900.0), MOVING, unit_id=T4))
    return state, out


# ---------------------------------------------------------------------------
# Закрепление, порядок и дубли, «сейчас»
# ---------------------------------------------------------------------------


def test_apply_unassigned_unit_ignored_without_state_change() -> None:
    """Машина без закрепления не учитывается: выходов нет, «сейчас» и состояние не меняются."""
    state = SiteState(SITE)
    initial = state.snapshot()

    assert state.apply(tm(T0, pos(-900.0), MOVING, unit_id=STRANGER)) == []
    assert state.now is None
    assert state.snapshot() == initial


def test_apply_unassigned_unit_after_start_does_not_move_now() -> None:
    """Сообщение незакреплённой машины с бо́льшим ts не сдвигает «сейчас»."""
    state = t1_occupies_s1()
    before = state.snapshot()

    assert state.apply(tm(T0 + 100, AT_S2, STOPPED, unit_id=STRANGER)) == []
    assert state.now == T0
    assert state.snapshot() == before


@pytest.mark.parametrize(
    "repeat",
    [
        pytest.param(tm(T0 + 5, pos(-2_000.0), MOVING, unit_id=T2), id="exact-duplicate"),
        pytest.param(tm(T0 + 5, pos(-1_000.0), STOPPED, unit_id=T2), id="same-ts-other-data"),
        pytest.param(tm(T0 + 4, pos(-1_000.0), MOVING, unit_id=T2), id="older-ts"),
    ],
)
def test_apply_message_not_newer_than_unit_last_ts_ignored(repeat: Telemetry) -> None:
    """ТЗ «Порядок и дубли»: ts не больше последнего учтённого ts машины → игнор.

    Выходов нет, «сейчас» и снимок состояния не меняются (в т.ч. нет решения,
    хотя повтор ближе 1500 м к своей станции).
    """
    state = SiteState(SITE)
    state.apply(tm(T0, AT_S1, STOPPED, unit_id=T1))
    state.apply(tm(T0 + 5, pos(-2_000.0), MOVING, unit_id=T2))
    state.apply(tm(T0 + 20, AT_S1, STOPPED, unit_id=T1))
    before = state.snapshot()

    assert state.apply(repeat) == []
    assert state.now == T0 + 20
    assert state.snapshot() == before


def test_apply_late_but_newer_for_unit_counted_now_not_decreased() -> None:
    """ТЗ «Порядок и дубли»: опоздавшее сообщение с более новым для машины ts учитывается.

    T1 в T0+40 сдвинула «сейчас»; первое сообщение T2 с ts T0+20 опоздало, но для T2 новое:
    T2 попадает в очередь S1 (приезд T0+220, свежесть 20 с), «сейчас» остаётся T0+40.
    """
    state = t1_occupies_s1()
    state.apply(tm(T0 + 40, AT_S1, STOPPED, unit_id=T1))

    out = state.apply(tm(T0 + 20, pos(-2_000.0), MOVING, unit_id=T2))

    assert state.now == T0 + 40
    assert out == [
        StationQueue(
            S1,
            T0 + 40,
            (
                QueueEntry(T1, None, T0, T0 + 230, 0),
                QueueEntry(T2, T0 + 220, T0 + 230, T0 + 460, 10),
            ),
        )
    ]
    tracks = {t.unit_id: t for t in state.snapshot().tracks}
    assert tracks[T2].last_ts == T0 + 20


def test_now_is_max_counted_ts_and_none_before_first_message() -> None:
    """ТЗ «Время»: «сейчас» — наибольший ts среди учтённых сообщений, до них — None."""
    state = SiteState(SITE)
    assert state.now is None

    state.apply(tm(T0, BEYOND_HORIZON, MOVING, unit_id=T5))
    assert state.now == T0
    state.apply(tm(T0 + 50, BEYOND_HORIZON, MOVING, unit_id=T2))
    assert state.now == T0 + 50
    state.apply(tm(T0 + 30, BEYOND_HORIZON, MOVING, unit_id=T1))
    assert state.now == T0 + 50
    state.apply(tm(T0 + 51, BEYOND_HORIZON, MOVING, unit_id=T5))
    assert state.now == T0 + 51


def test_outputs_at_equals_now_not_message_ts() -> None:
    """ТЗ, поле at: «сейчас» на момент расчёта, а не ts опоздавшего сообщения.

    Очередь, опубликованная по опоздавшему сообщению, имеет at = «сейчас».
    """
    state = SiteState(SITE)
    state.apply(tm(T0 + 25, BEYOND_HORIZON, MOVING, unit_id=T5))

    out = state.apply(tm(T0, pos(-2_000.0), MOVING, unit_id=T2))

    assert [q.at for q in queues(out)] == [T0 + 25]
    assert all(o.at == state.now for o in out)


# ---------------------------------------------------------------------------
# Публикация очередей
# ---------------------------------------------------------------------------


def test_publish_empty_queues_at_start_not_published() -> None:
    """ТЗ «Публикация»: пустая очередь на старте не публикуется (изменения нет)."""
    state = SiteState(SITE)

    assert state.apply(tm(T0, BEYOND_HORIZON, MOVING, unit_id=T2)) == []
    assert state.apply(tm(T0 + 1, BEYOND_HORIZON, MOVING, unit_id=T5)) == []
    assert state.now == T0 + 1


def test_publish_queue_changed_station_queue_published() -> None:
    """ТЗ «Публикация»: машина заняла S1 — состав очереди S1 изменился, она публикуется."""
    state = SiteState(SITE)

    out = state.apply(tm(T0, AT_S1, STOPPED, unit_id=T1))

    assert out == [StationQueue(S1, T0, (QueueEntry(T1, None, T0, T0 + 230, 0),))]


def test_publish_same_queue_again_not_published() -> None:
    """ТЗ «Публикация»: одинаковые подряд очереди не публикуются (at не в счёт)."""
    state = t1_occupies_s1()

    assert state.apply(tm(T0 + 1, AT_S1, STOPPED, unit_id=T1)) == []
    assert state.apply(tm(T0 + 2, AT_S1, STOPPED, unit_id=T1)) == []


def test_publish_only_eta_shifted_queue_published() -> None:
    """ТЗ «Публикация»: изменилось только время в очереди (приезд сдвинулся) — публикуется.

    T2 стоит на месте в 2000 м от S1: приезд сдвигается на секунду с каждым сообщением.
    Сообщение, где T2 приблизилась на 10 м за секунду, приезд не меняет — не публикуется.
    """
    state = SiteState(SITE)
    first = state.apply(tm(T0, pos(-2_000.0), MOVING, unit_id=T2))
    shifted = state.apply(tm(T0 + 1, pos(-2_000.0), MOVING, unit_id=T2))
    same = state.apply(tm(T0 + 2, pos(-1_990.0), MOVING, unit_id=T2))

    assert first == [StationQueue(S1, T0, (QueueEntry(T2, T0 + 200, T0 + 200, T0 + 430, 0),))]
    assert shifted == [StationQueue(S1, T0 + 1, (QueueEntry(T2, T0 + 201, T0 + 201, T0 + 431, 0),))]
    assert same == []


def test_publish_queue_became_empty_published_with_no_entries() -> None:
    """ТЗ «Очередь» п.5 и «Публикация»: позиция T2 устарела (> 30 с) — очередь S1 опустела.

    Ровно 30 с — позиция ещё свежая, очередь та же (не публикуется); на 31-й секунде
    публикуется пустая очередь S1.
    """
    state = SiteState(SITE)
    state.apply(tm(T0, pos(-2_000.0), MOVING, unit_id=T2))

    assert state.apply(tm(T0 + 30, BEYOND_HORIZON, MOVING, unit_id=T5)) == []
    assert state.apply(tm(T0 + 31, BEYOND_HORIZON, MOVING, unit_id=T5)) == [
        StationQueue(S1, T0 + 31, ())
    ]


def test_publish_order_decision_first_then_queues_in_site_stations_order() -> None:
    """ТЗ «Публикация»: решение идёт первым, затем изменившиеся очереди в порядке станций.

    T4 в 1600 м от S1 — в очереди S1. Затем в 1400 м: рекомендация к S2 (у S1 ожидание 69 с,
    у S2 — 0), T4 уходит из очереди S1 в очередь S2 — меняются обе очереди.
    """
    state = t1_occupies_s1()
    before = state.apply(tm(T0 + 20, pos(-1_600.0), MOVING, unit_id=T4))
    assert [units(q) for q in queues(before)] == [[T1, T4]]

    out = state.apply(tm(T0 + 21, pos(-1_400.0), MOVING, unit_id=T4))

    assert out == [
        Recommendation(T4, T0 + 21, S1, S2, 69),
        StationQueue(S1, T0 + 21, (QueueEntry(T1, None, T0, T0 + 230, 0),)),
        StationQueue(S2, T0 + 21, (QueueEntry(T4, T0 + 211, T0 + 211, T0 + 441, 0),)),
    ]


# ---------------------------------------------------------------------------
# Решение: один раз за заезд (ТЗ «Рекомендация», п.5)
# ---------------------------------------------------------------------------


def test_decision_first_message_inside_radius_to_station_decided() -> None:
    """ТЗ «Рекомендация»: первое сообщение уже ближе 1500 м в состоянии «к станции» → решение.

    S1 и S2 свободны: ожидание 0 у обеих, выигрыш 0 → отказ no_gain.
    """
    state = SiteState(SITE)

    out = state.apply(tm(T0, pos(-900.0), MOVING, unit_id=T2))

    assert decisions(out) == [Rejection(T2, T0, RejectReason.NO_GAIN)]
    assert out[0] == Rejection(T2, T0, RejectReason.NO_GAIN)


def test_decision_radius_strict_outside_no_decision_inside_decided() -> None:
    """ТЗ «Рекомендация»: решение, когда машина впервые ближе 1500 м (1500.5 м — ещё нет)."""
    state = SiteState(SITE)

    assert decisions(state.apply(tm(T0, pos(-1_500.5), MOVING, unit_id=T2))) == []
    assert decisions(state.apply(tm(T0 + 1, pos(-1_499.5), MOVING, unit_id=T2))) == [
        Rejection(T2, T0 + 1, RejectReason.NO_GAIN)
    ]


def test_decision_repeated_messages_inside_radius_no_new_decisions() -> None:
    """ТЗ «Рекомендация»: один раз за заезд — отказ no_gain тоже расходует решение."""
    state = SiteState(SITE)
    first = state.apply(tm(T0, pos(-900.0), MOVING, unit_id=T2))

    rest = feed(
        state,
        [tm(T0 + i, pos(-900.0 + 10 * i), MOVING, unit_id=T2) for i in range(1, 6)],
    )

    assert decisions(first) == [Rejection(T2, T0, RejectReason.NO_GAIN)]
    assert decisions(rest) == []
    assert state.snapshot().decided == (T2,)


def test_decision_left_radius_and_returned_without_visit_no_new_decision() -> None:
    """ТЗ «Рекомендация» п.5: без посещения станции новое решение невозможно.

    Машина после решения отъехала за 1500 м и снова вернулась — решения нет.
    """
    state = SiteState(SITE)
    state.apply(tm(T0, pos(-900.0), MOVING, unit_id=T2))

    out = feed(
        state,
        [
            tm(T0 + 100, pos(-2_000.0), MOVING, unit_id=T2),
            tm(T0 + 200, pos(-1_000.0), MOVING, unit_id=T2),
        ],
    )

    assert decisions(out) == []


def test_decision_after_station_visit_and_new_approach_decided_again() -> None:
    """ТЗ «Рекомендация» п.5: после посещения станции следующий заезд даёт новое решение.

    T2: решение в 900 м → на S1 → к разгрузке → разгрузка → к станции → в 1400 м от S1.
    """
    state = SiteState(SITE)
    first = state.apply(tm(T0, pos(-900.0), MOVING, unit_id=T2))
    trip = feed(
        state,
        [
            tm(T0 + 90, AT_S1, STOPPED, unit_id=T2),
            tm(T0 + 300, pos(-100.0), MOVING, unit_id=T2),
            tm(T0 + 1_000, pos(UNLOAD_OFFSET), MOVING, unit_id=T2),
            tm(T0 + 1_100, pos(-7_000.0), MOVING, unit_id=T2),
        ],
    )
    second = state.apply(tm(T0 + 1_660, pos(-1_400.0), MOVING, unit_id=T2))

    assert decisions(first) == [Rejection(T2, T0, RejectReason.NO_GAIN)]
    assert decisions(trip) == []
    assert decisions(second) == [Rejection(T2, T0 + 1_660, RejectReason.NO_GAIN)]


def test_redirect_dropped_on_unload_entry_without_station_visit() -> None:
    """Машина после рекомендации проехала мимо станций без стоянки и уехала на разгрузку.

    Въезд на разгрузку начинает новый заезд к своей станции (п.4): перенаправление снимается,
    T4 уходит из очереди S2. Новое решение — только после посещения станции (п.5).
    """
    state, out = t4_recommended_to_s2()
    assert decisions(out) == [RECOMMENDATION]
    feed(
        state,
        [
            tm(T0 + 300, pos(S2_OFFSET - 500.0), MOVING, unit_id=T4),
            tm(T0 + 800, pos(UNLOAD_OFFSET), MOVING, unit_id=T4),
        ],
    )
    snap = state.snapshot()
    assert snap.redirects == ()
    assert snap.decided == (T4,)

    again = state.apply(tm(T0 + 1_500, pos(-1_000.0), MOVING, unit_id=T4))

    assert decisions(again) == []
    published = dict(state.snapshot().published)
    assert T4 in [e.unit_id for e in published[S1]]
    assert T4 not in [e.unit_id for e in published[S2]]


def test_decision_to_unload_inside_radius_no_decision() -> None:
    """ТЗ «Рекомендация»: решение только в состоянии «к станции».

    T2 отъезжает от S1 к разгрузке и проезжает в 100 и 900 м от S1 — решения нет.
    """
    state = SiteState(SITE)

    out = feed(
        state,
        [
            tm(T0, AT_S1, STOPPED, unit_id=T2),
            tm(T0 + 10, pos(-100.0), MOVING, unit_id=T2),
            tm(T0 + 90, pos(-900.0), MOVING, unit_id=T2),
        ],
    )

    assert decisions(out) == []


def test_decision_stale_telemetry_rejected_and_consumes_decision() -> None:
    """ТЗ «Коды отказа»: сообщение старше «сейчас» на 31 с → stale_telemetry, at = «сейчас».

    Отказ расходует решение: следующее свежее сообщение внутри 1500 м решения не даёт.
    """
    state = SiteState(SITE)
    state.apply(tm(T0 + 100, AT_S1, STOPPED, unit_id=T1))

    out = state.apply(tm(T0 + 69, pos(-900.0), MOVING, unit_id=T2))
    later = state.apply(tm(T0 + 101, pos(-890.0), MOVING, unit_id=T2))

    assert decisions(out) == [Rejection(T2, T0 + 100, RejectReason.STALE_TELEMETRY)]
    assert decisions(later) == []


def test_decision_late_by_exactly_freshness_not_stale() -> None:
    """ТЗ «Коды отказа»: опоздание ровно 30 с — не stale_telemetry, считается рекомендация.

    T1 заняла S1 в T0+100 (до T0+330). T2 в 900 м с ts T0+70: приезд к S1 не раньше
    «сейчас» T0+100 и не раньше T0+160 → ожидание 170 с; к S2 приезд T0+310 → 0 с.
    """
    state = SiteState(SITE)
    state.apply(tm(T0 + 100, AT_S1, STOPPED, unit_id=T1))

    out = state.apply(tm(T0 + 70, pos(-900.0), MOVING, unit_id=T2))

    assert decisions(out) == [Recommendation(T2, T0 + 100, S1, S2, 170)]


# ---------------------------------------------------------------------------
# Перенаправление (ТЗ «Рекомендация», п.4)
# ---------------------------------------------------------------------------


def test_redirect_recommended_unit_in_recommended_queue_not_in_own() -> None:
    """ТЗ «Рекомендация» п.4: после рекомендации машина в очереди S2, а не своей S1.

    Очередь S1 не изменилась (только T1) и не публикуется; публикуется очередь S2 с T4:
    приезд 12:04:00 (T0+300), ожидание 0.
    """
    state, out = t4_recommended_to_s2()

    assert out == [
        RECOMMENDATION,
        StationQueue(S2, T0 + 60, (QueueEntry(T4, T0 + 300, T0 + 300, T0 + 530, 0),)),
    ]
    snap = state.snapshot()
    assert snap.redirects == ((T4, S2),)
    assert snap.decided == (T4,)


def test_redirect_unchanged_while_driving_queue_not_republished() -> None:
    """ТЗ «Рекомендация» п.4: едущая к S2 машина остаётся в её очереди; та же очередь — молчание.

    T4 сместилась на 10 м к S2 за секунду: приезд к S2 тот же — ничего не публикуется.
    """
    state, _ = t4_recommended_to_s2()

    assert state.apply(tm(T0 + 61, pos(-910.0), MOVING, unit_id=T4)) == []
    assert state.snapshot().redirects == ((T4, S2),)


def test_redirect_unit_silent_over_freshness_leaves_and_returns_to_recommended_queue() -> None:
    """ТЗ «Очередь» п.5 и «Рекомендация» п.4: замолчавшая перенаправленная машина.

    T4 после рекомендации к S2 молчит: через 30 с позиция ещё свежая (ничего не меняется),
    через 31 с T4 исключается из очереди S2 (публикуется пустая). Свежая позиция возвращает
    T4 в очередь S2, а не своей S1; перенаправление сохраняется, нового решения нет.
    """
    state, _ = t4_recommended_to_s2()

    assert state.apply(tm(T0 + 90, AT_S1, STOPPED, unit_id=T1)) == []
    assert state.apply(tm(T0 + 91, AT_S1, STOPPED, unit_id=T1)) == [StationQueue(S2, T0 + 91, ())]
    assert state.snapshot().redirects == ((T4, S2),)

    back = state.apply(tm(T0 + 95, pos(-1_000.0), MOVING, unit_id=T4))

    assert back == [StationQueue(S2, T0 + 95, (QueueEntry(T4, T0 + 325, T0 + 325, T0 + 555, 0),))]
    assert state.snapshot().redirects == ((T4, S2),)


def test_redirect_after_service_at_recommended_next_trip_to_own_station() -> None:
    """ТЗ «Рекомендация» п.4: после обслуживания на S2 и отъезда машина снова едет к своей S1.

    На S2 T4 занимает станцию; при отъезде перенаправление снимается: T4 «к разгрузке»
    в очереди S1 (путь 4600 м до разгрузки + 8000 м до S1 → 1260 с), очередь S2 пустеет.
    """
    state, _ = t4_recommended_to_s2()

    at_s2 = state.apply(tm(T0 + 300, AT_S2, STOPPED, unit_id=T4))
    assert queues(at_s2)[-1] == StationQueue(
        S2, T0 + 300, (QueueEntry(T4, None, T0 + 300, T0 + 530, 0),)
    )
    assert decisions(at_s2) == []

    left = state.apply(tm(T0 + 530, pos(S2_OFFSET - 100.0), MOVING, unit_id=T4))

    assert left == [
        StationQueue(S1, T0 + 530, (QueueEntry(T4, T0 + 1_790, T0 + 1_790, T0 + 2_020, 0),)),
        StationQueue(S2, T0 + 530, ()),
    ]
    assert state.snapshot().redirects == ()


def test_redirect_after_service_next_approach_decided_for_own_station() -> None:
    """ТЗ «Рекомендация» п.4–5: следующий заезд начинается к своей станции и даёт решение.

    После S2 и разгрузки T4 в 1400 м от S1: обе станции свободны → отказ no_gain,
    T4 в очереди своей S1.
    """
    state, _ = t4_recommended_to_s2()
    feed(
        state,
        [
            tm(T0 + 300, AT_S2, STOPPED, unit_id=T4),
            tm(T0 + 530, pos(S2_OFFSET - 100.0), MOVING, unit_id=T4),
            tm(T0 + 1_000, pos(UNLOAD_OFFSET), MOVING, unit_id=T4),
            tm(T0 + 1_100, pos(-7_000.0), MOVING, unit_id=T4),
        ],
    )

    out = state.apply(tm(T0 + 1_600, pos(-1_400.0), MOVING, unit_id=T4))

    assert decisions(out) == [Rejection(T4, T0 + 1_600, RejectReason.NO_GAIN)]
    assert out[0] == decisions(out)[0]
    assert queues(out) == [
        StationQueue(S1, T0 + 1_600, (QueueEntry(T4, T0 + 1_740, T0 + 1_740, T0 + 1_970, 0),))
    ]
    assert state.snapshot().redirects == ()


def test_redirect_ignored_advice_unit_queued_at_own_station_where_it_stands() -> None:
    """Машина проигнорировала совет и встала на свою S1: она в очереди S1, а не S2.

    S1 занята T1 до T0+230: T4 ждёт в радиусе, приезд — момент входа T0+150, ожидание 80.
    Пока T4 на станции, перенаправление ещё действует, но очередь — по станции, где стоит.
    """
    state, _ = t4_recommended_to_s2()

    out = state.apply(tm(T0 + 150, pos(-20.0), STOPPED, unit_id=T4))

    assert out == [
        StationQueue(
            S1,
            T0 + 150,
            (
                QueueEntry(T1, None, T0, T0 + 230, 0),
                QueueEntry(T4, T0 + 150, T0 + 230, T0 + 460, 80),
            ),
        ),
        StationQueue(S2, T0 + 150, ()),
    ]
    snap = state.snapshot()
    assert snap.redirects == ((T4, S2),)
    assert snap.decided == ()


def test_redirect_ignored_advice_removed_on_departure() -> None:
    """Перенаправление снимается, когда машина уезжает со станции (обслуживание завершено).

    T4 стоит на S1, занимает её в T0+230 после T1, уезжает в T0+460 — перенаправления нет,
    в очереди S2 T4 не появляется.
    """
    state, _ = t4_recommended_to_s2()
    feed(
        state,
        [
            tm(T0 + 150, pos(-20.0), STOPPED, unit_id=T4),
            tm(T0 + 230, pos(-20.0), STOPPED, unit_id=T4),
        ],
    )
    assert state.snapshot().redirects == ((T4, S2),)

    out = state.apply(tm(T0 + 460, pos(-100.0), MOVING, unit_id=T4))

    assert state.snapshot().redirects == ()
    assert all(T4 not in units(q) for q in queues(out) if q.station_id == S2)
    assert decisions(out) == []


# ---------------------------------------------------------------------------
# Снимок состояния и восстановление
# ---------------------------------------------------------------------------

STREAM: Final = [
    tm(T0, AT_S1, STOPPED, unit_id=T1),
    tm(T0 + 5, pos(-2_000.0), MOVING, unit_id=T2),
    tm(T0 + 5, pos(-2_000.0), MOVING, unit_id=T2),
    tm(T0 + 20, pos(-1_600.0), MOVING, unit_id=T4),
    tm(T0 + 21, pos(-1_400.0), MOVING, unit_id=T4),
    tm(T0 + 15, pos(-25_000.0), MOVING, unit_id=T5),
    tm(T0 + 30, AT_S1, STOPPED, unit_id=T1),
    tm(T0 + 25, pos(-1_800.0), MOVING, unit_id=T2),
    tm(T0 + 22, pos(-1_000.0), MOVING, unit_id=STRANGER),
    tm(T0 + 200, AT_S2, STOPPED, unit_id=T4),
    tm(T0 + 120, pos(-900.0), MOVING, unit_id=T2),
    tm(T0 + 230, AT_S1, STOPPED, unit_id=T1),
    tm(T0 + 231, pos(-100.0), MOVING, unit_id=T1),
    tm(T0 + 210, AT_S1, STOPPED, unit_id=T2),
    tm(T0 + 430, pos(S2_OFFSET - 100.0), MOVING, unit_id=T4),
    tm(T0 + 440, AT_S1, STOPPED, unit_id=T2),
]
"""Поток с занятием, рекомендацией, отказом, дублем, опозданиями и незакреплённой машиной."""


SPLITS: Final = [
    pytest.param(0, id="start"),
    pytest.param(5, id="after-recommendation"),
    pytest.param(10, id="middle-unit-at-recommended"),
    pytest.param(len(STREAM), id="end"),
]
"""Характерные точки потока STREAM для восстановления; произвольные — в property-тесте."""


def test_snapshot_initial_state() -> None:
    """Начальный снимок: «сейчас» нет, треков нет, станции свободны, опубликованы пустые очереди."""
    snap = SiteState(SITE).snapshot()

    assert snap == SiteSnapshot(
        now=None,
        tracks=(),
        occupancies=(StationOccupancy(S1), StationOccupancy(S2)),
        redirects=(),
        decided=(),
        published=((S1, ()), (S2, ())),
    )


def test_snapshot_reflects_state_tracks_sorted_and_published() -> None:
    """Снимок: треки по unit_id, занятость в порядке станций, опубликованные очереди."""
    state = SiteState(SITE)
    outputs = feed(state, STREAM[:6])

    snap = state.snapshot()

    assert snap.now == T0 + 21
    assert [t.unit_id for t in snap.tracks] == [T1, T2, T4, T5]
    assert [o.station_id for o in snap.occupancies] == [S1, S2]
    last = {q.station_id: q.entries for q in queues(outputs)}
    assert snap.published == ((S1, last[S1]), (S2, last[S2]))
    assert snap.redirects == ((T4, S2),)


@pytest.mark.parametrize("split", SPLITS)
def test_snapshot_restore_same_snapshot(split: int) -> None:
    """Снимок восстановленного объекта равен исходному снимку (произвольные точки — property)."""
    state = SiteState(SITE)
    feed(state, STREAM[:split])
    snap = state.snapshot()

    restored = SiteState(SITE, snap)

    assert restored.snapshot() == snap
    assert restored.now == state.now


@pytest.mark.parametrize("split", SPLITS)
def test_snapshot_restore_continuation_same_outputs(split: int) -> None:
    """Восстановленный из снимка объект на продолжении потока даёт те же выходы."""
    original = SiteState(SITE)
    feed(original, STREAM[:split])
    restored = SiteState(SITE, original.snapshot())

    for msg in STREAM[split:]:
        assert restored.apply(msg) == original.apply(msg)
    assert restored.snapshot() == original.snapshot()


def test_snapshot_not_affected_by_later_apply() -> None:
    """Снимок — неизменяемое значение: дальнейшая обработка не меняет ни его, ни исходный."""
    state = SiteState(SITE)
    feed(state, STREAM[:5])
    snap = state.snapshot()
    copy = SiteState(SITE, snap).snapshot()

    restored = SiteState(SITE, snap)
    feed(restored, STREAM[5:])
    feed(state, STREAM[5:])

    assert snap == copy


def test_snapshot_extra_station_raises_value_error() -> None:
    """Снимок площадки без одной из станций не восстанавливается: ValueError."""
    bigger = SiteConfig(
        stations=(*SITE.stations, Station("S3", pos(4_000.0))),
        unload_point=pos(UNLOAD_OFFSET),
        assignments=SITE.assignments,
    )

    with pytest.raises(ValueError):
        SiteState(bigger, SiteState(SITE).snapshot())


@pytest.mark.parametrize("split", [0, 1, 5, 10, len(STREAM)])
def test_snapshot_json_round_trip_equal(split: int) -> None:
    """Снимок проходит JSON через pydantic без потерь (граница для адаптера хранилища)."""
    adapter: TypeAdapter[SiteSnapshot] = TypeAdapter(SiteSnapshot)
    state = SiteState(SITE)
    feed(state, STREAM[:split])
    snap = state.snapshot()

    restored = adapter.validate_json(adapter.dump_json(snap))

    assert restored == snap
    assert SiteState(SITE, restored).snapshot() == snap


INITIAL: Final = SiteState(SITE).snapshot()


@pytest.mark.parametrize(
    "broken",
    [
        pytest.param(
            replace(INITIAL, occupancies=(StationOccupancy(S1), StationOccupancy("S3"))),
            id="occupancies",
        ),
        pytest.param(
            replace(INITIAL, occupancies=(StationOccupancy(S1),)), id="occupancies-missing"
        ),
        pytest.param(replace(INITIAL, published=((S1, ()), ("S3", ()))), id="published"),
        pytest.param(replace(INITIAL, published=((S1, ()),)), id="published-missing"),
        pytest.param(
            replace(
                INITIAL,
                occupancies=(StationOccupancy(S1), StationOccupancy("S3")),
                published=((S1, ()), ("S3", ())),
            ),
            id="other-site-stations",
        ),
        pytest.param(replace(INITIAL, published=((S1, ()), (S1, ()))), id="published-duplicate"),
        pytest.param(
            replace(INITIAL, occupancies=(StationOccupancy(S2), StationOccupancy(S1))),
            id="occupancies-other-order",
        ),
        pytest.param(replace(INITIAL, published=((S2, ()), (S1, ()))), id="published-other-order"),
        pytest.param(
            replace(
                INITIAL,
                occupancies=(StationOccupancy(S2), StationOccupancy(S1)),
                published=((S2, ()), (S1, ())),
            ),
            id="all-other-order",
        ),
    ],
)
def test_snapshot_partially_mismatched_stations_raises_value_error(broken: SiteSnapshot) -> None:
    """Списки станций занятости и опубликованных очередей должны совпадать с site.stations.

    Другой состав, пропуск, дубликат или другой порядок station_id → ValueError.
    """
    with pytest.raises(ValueError):
        SiteState(SITE, broken)


WIDER: Final = SiteConfig(
    stations=SITE.stations,
    unload_point=SITE.unload_point,
    assignments={**SITE.assignments, "T9": S1},
)
"""Та же площадка, но T9 ещё закреплена за S1 (конфигурация до рестарта)."""


def test_snapshot_unassigned_unit_track_dropped_on_restore() -> None:
    """Закрепление T9 убрали между рестартами: её трек отбрасывается при восстановлении.

    T9 едет к S1 и была в её очереди; после восстановления трека нет, сообщения T9 дают [],
    очередь S1 строится без неё.
    """
    before = SiteState(WIDER)
    published = before.apply(tm(T0, pos(-2_000.0), MOVING, unit_id="T9"))
    assert [units(q) for q in queues(published)] == [["T9"]]

    state = SiteState(SITE, before.snapshot())

    assert [t.unit_id for t in state.snapshot().tracks] == []
    assert state.apply(tm(T0 + 1, pos(-2_000.0), MOVING, unit_id="T9")) == []
    out = state.apply(tm(T0 + 1, AT_S1, STOPPED, unit_id=T1))
    assert out == [StationQueue(S1, T0 + 1, (QueueEntry(T1, None, T0 + 1, T0 + 231, 0),))]


def test_snapshot_unassigned_unit_redirect_and_decision_dropped_on_restore() -> None:
    """Перенаправление и отметка решения раскреплённой T9 не восстанавливаются вместе с треком."""
    before = SiteState(WIDER)
    before.apply(tm(T0, AT_S1, STOPPED, unit_id=T1))
    out = before.apply(tm(T0 + 60, pos(-900.0), MOVING, unit_id="T9"))
    assert decisions(out) == [Recommendation("T9", T0 + 60, S1, S2, 80)]

    snap = SiteState(SITE, before.snapshot()).snapshot()

    assert snap.redirects == ()
    assert snap.decided == ()


def test_snapshot_unassigned_unit_waiting_at_station_dropped_on_restore() -> None:
    """Раскреплённая T9 стояла в радиусе S1 (на станции, ждала за T1) — после рестарта её нет.

    До рестарта очередь S1: T1 (заняла), T9 (ждёт с T0+10). После восстановления трек T9
    отброшен: ближайшая публикация S1 — только T1.
    """
    before = SiteState(WIDER)
    feed(
        before,
        [
            tm(T0, AT_S1, STOPPED, unit_id=T1),
            tm(T0 + 10, pos(-20.0), STOPPED, unit_id="T9"),
        ],
    )
    assert before.snapshot().published[0] == (
        S1,
        (
            QueueEntry(T1, None, T0, T0 + 230, 0),
            QueueEntry("T9", T0 + 10, T0 + 230, T0 + 460, 220),
        ),
    )

    state = SiteState(SITE, before.snapshot())

    assert [t.unit_id for t in state.snapshot().tracks] == [T1]
    assert state.apply(tm(T0 + 11, pos(-20.0), STOPPED, unit_id="T9")) == []
    out = state.apply(tm(T0 + 11, AT_S1, STOPPED, unit_id=T1))
    assert out == [StationQueue(S1, T0 + 11, (QueueEntry(T1, None, T0, T0 + 230, 0),))]
