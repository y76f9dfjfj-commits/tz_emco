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

Файл разбит на разделы по этапам расчёта: занятие станции (T1), очередь, решение.
Сейчас покрыт раздел «Занятие станции».
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Final

from tests.sitekit import FAR_POINT, S1_ID, S1_POINT, UNIT_ID, SiteRun, make_site, north_of, tm
from vqueue.domain.geo import distance_m
from vqueue.domain.model import Telemetry, UnitPhase
from vqueue.domain.occupancy import Occupant, StationOccupancy
from vqueue.domain.unit_fsm import UnitTrack

# ---------------------------------------------------------------------------
# Общие данные примера
# ---------------------------------------------------------------------------

SITE: Final = make_site()

T1: Final = UNIT_ID
"""В sitekit машина T1 закреплена за S1 — как в примере."""

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
    calc_moment = T1_OCCUPIED_AT + 60  # 12:00:00
    _, occ = _replay(
        [
            tm(T1_OCCUPIED_AT, T1_POSITION, 0.0),
            tm(calc_moment, T1_POSITION, 0.0),
        ]
    )

    assert _iso(calc_moment) == "2026-09-15T12:00:00Z"
    assert occ.is_busy(calc_moment)
    assert occ.is_busy(T1_FREE_AT - 1)
    assert not occ.is_busy(T1_FREE_AT)


# ---------------------------------------------------------------------------
# Очередь (ТЗ, «Очередь») — будет добавлено на этапе очереди.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Решение для T4 (ТЗ, «Рекомендация») — будет добавлено на этапе рекомендации.
# ---------------------------------------------------------------------------
