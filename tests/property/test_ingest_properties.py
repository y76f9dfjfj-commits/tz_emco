"""Property-тесты свёртки телеметрии с фильтром порядка и дублей.

ТЗ, «Порядок и дубли»: сообщение с ts, не превышающим последний учтённый ts той же машины,
игнорируется — это покрывает и дубли, и нарушение порядка. Опоздавшее сообщение с более
новым ts учитывается.
"""

from __future__ import annotations

import random
from collections.abc import Iterable

from hypothesis import given
from hypothesis import strategies as st

from tests.sitekit import (
    FAR_POINT,
    S1_POINT,
    S2_POINT,
    UNLOAD_POINT,
    make_site,
    north_of,
    tm,
)
from vqueue.domain.ingest import is_in_order
from vqueue.domain.model import Telemetry
from vqueue.domain.unit_fsm import UnitTrack, advance

SITE = make_site()

# Характерные точки: в радиусах станций и разгрузки, на их краях и в пути.
POSITIONS = (
    FAR_POINT,
    S1_POINT,
    north_of(S1_POINT, 15.0),
    north_of(S1_POINT, 49.0),
    north_of(S1_POINT, 51.0),
    north_of(S2_POINT, 10.0),
    UNLOAD_POINT,
    north_of(UNLOAD_POINT, 30.0),
    north_of(UNLOAD_POINT, 60.0),
)
SPEEDS = (0.0, 0.5, 0.99, 1.0, 20.0)

T_START = 1_757_930_000

positions = st.sampled_from(POSITIONS)
speeds = st.sampled_from(SPEEDS)


def fold(messages: Iterable[Telemetry]) -> UnitTrack | None:
    """Свёртка потока сообщений одной машины: учитываются только сообщения «по порядку»."""
    track: UnitTrack | None = None
    for msg in messages:
        if is_in_order(track.last_ts if track is not None else None, msg.ts):
            track = advance(track, msg, SITE)
    return track


@st.composite
def ordered_stream(draw: st.DrawFn) -> list[Telemetry]:
    """Поток сообщений со строго возрастающими ts (шаг 1–5 с), не пустой."""
    n = draw(st.integers(min_value=1, max_value=30))
    steps = draw(st.lists(st.integers(min_value=1, max_value=5), min_size=n, max_size=n))
    result: list[Telemetry] = []
    ts = T_START
    for step in steps:
        ts += step
        result.append(tm(ts, draw(positions), draw(speeds)))
    return result


@st.composite
def stream_with_noise(draw: st.DrawFn) -> tuple[list[Telemetry], list[Telemetry]]:
    """Пара (чистый поток, тот же поток со вставленными дублями и опозданиями).

    Шум, вставленный после k-го сообщения чистого потока, — либо точная копия одного
    из сообщений 0..k, либо произвольное сообщение с ts ≤ ts k-го (опоздание до 60 с).
    """
    clean = draw(ordered_stream())
    noisy: list[Telemetry] = []
    for k, msg in enumerate(clean):
        noisy.append(msg)
        extra = draw(st.integers(min_value=0, max_value=3))
        for _ in range(extra):
            if draw(st.booleans()):
                noisy.append(clean[draw(st.integers(min_value=0, max_value=k))])
            else:
                lag = draw(st.integers(min_value=0, max_value=60))
                noisy.append(tm(msg.ts - lag, draw(positions), draw(speeds)))
    return clean, noisy


@given(stream_with_noise())
def test_fold_ignores_duplicates_and_late_messages(
    streams: tuple[list[Telemetry], list[Telemetry]],
) -> None:
    """Дубли и сообщения с ts ≤ учтённого в любых местах не меняют итоговое состояние."""
    clean, noisy = streams
    assert fold(noisy) == fold(clean)


@given(stream_with_noise())
def test_fold_last_ts_is_max_accounted_ts(
    streams: tuple[list[Telemetry], list[Telemetry]],
) -> None:
    """Итоговый last_ts — максимум ts учтённых сообщений (последнее сообщение чистого потока)."""
    clean, noisy = streams
    track = fold(noisy)
    assert track is not None
    assert track.last_ts == max(m.ts for m in clean)
    assert track.position == clean[-1].position


@given(ordered_stream(), st.randoms(use_true_random=False))
def test_fold_any_order_last_ts_is_max_ts(
    clean: list[Telemetry],
    rnd: random.Random,
) -> None:
    """При любом порядке прихода итоговый last_ts — максимальный ts в потоке."""
    shuffled = list(clean)
    rnd.shuffle(shuffled)
    track = fold(shuffled)
    assert track is not None
    assert track.last_ts == max(m.ts for m in clean)


@given(ordered_stream())
def test_fold_ordered_stream_accepts_every_message(clean: list[Telemetry]) -> None:
    """Поток со строго возрастающими ts учитывается целиком: свёртка = цепочка advance."""
    expected: UnitTrack | None = None
    for msg in clean:
        expected = advance(expected, msg, SITE)
    assert fold(clean) == expected
