"""Сбои доставки телеметрии: дубли и опоздания, детерминированные по seed."""

from __future__ import annotations

import heapq
import random
from collections.abc import Sequence

from vqueue.domain.model import Telemetry


class FaultInjector:
    """Вносит в поток сообщений дубли и опоздания.

    Опоздание откладывает отправку сообщения на 1..max_delay_s секунд; ts сообщения
    не меняется. Дубль — та же копия сообщения, отправляемая сразу за оригиналом
    (в тот же срок отправки, включая отложенный), поэтому дубли и опоздания
    независимы: дубль сам по себе порядок не нарушает.
    """

    def __init__(
        self,
        seed: int,
        dup_rate: float = 0.0,
        late_rate: float = 0.0,
        max_delay_s: int = 60,
    ) -> None:
        """Создаёт источник сбоев.

        Args:
            seed: Начальное значение генератора случайных чисел.
            dup_rate: Вероятность дубля сообщения, [0, 1].
            late_rate: Вероятность опоздания сообщения, [0, 1].
            max_delay_s: Наибольшая задержка опоздавшего сообщения, с (не меньше 1).

        Raises:
            ValueError: Вероятность вне [0, 1] или max_delay_s меньше 1.
        """
        for name, rate in (("dup_rate", dup_rate), ("late_rate", late_rate)):
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"{name} должна быть в [0, 1], получено {rate!r}")
        if max_delay_s < 1:
            raise ValueError(f"max_delay_s должна быть не меньше 1, получено {max_delay_s!r}")
        self._rng = random.Random(seed)  # nosec B311 — симуляция, не криптография.
        self._dup_rate = dup_rate
        self._late_rate = late_rate
        self._max_delay_s = max_delay_s
        # Куча (срок отправки, порядковый номер поступления, сообщение).
        self._pending: list[tuple[int, int, Telemetry]] = []
        self._seq = 0

    def feed(self, now_ts: int, messages: Sequence[Telemetry]) -> list[Telemetry]:
        """Принимает сообщения, сгенерированные в now_ts, и выдаёт сообщения к отправке.

        Args:
            now_ts: Текущее виртуальное время, секунды epoch.
            messages: Сообщения, сгенерированные в этот момент.

        Returns:
            Сообщения со сроком отправки не позже now_ts (в том числе ранее
            отложенные), упорядоченные по сроку, при равенстве — по поступлению.
        """
        for msg in messages:
            due = now_ts
            if self._rng.random() < self._late_rate:
                due += self._rng.randint(1, self._max_delay_s)
            copies = 2 if self._rng.random() < self._dup_rate else 1
            for _ in range(copies):
                heapq.heappush(self._pending, (due, self._seq, msg))
                self._seq += 1
        ready: list[Telemetry] = []
        while self._pending and self._pending[0][0] <= now_ts:
            ready.append(heapq.heappop(self._pending)[2])
        return ready

    def flush(self) -> list[Telemetry]:
        """Выдаёт все отложенные сообщения (в конце прогона).

        Returns:
            Отложенные сообщения по сроку отправки, при равенстве — по поступлению.
        """
        ready = [entry[2] for entry in sorted(self._pending)]
        self._pending.clear()
        return ready
