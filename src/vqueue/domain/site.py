"""Агрегат площадки: обработка потока телеметрии в очереди станций и решения."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TypeAlias

from vqueue.domain.ingest import is_in_order
from vqueue.domain.model import SiteConfig, Telemetry, UnitPhase
from vqueue.domain.occupancy import StationOccupancy, update_occupancy
from vqueue.domain.queue import QueueEntry, StationQueue, build_station_queue
from vqueue.domain.recommendation import (
    Decision,
    Recommendation,
    is_decision_point,
    recommend,
)
from vqueue.domain.unit_fsm import UnitTrack, advance

SiteOutput: TypeAlias = StationQueue | Decision
"""Результат обработки сообщения: очередь станции к публикации или решение."""


@dataclass(frozen=True, slots=True)
class SiteSnapshot:
    """Полное состояние площадки для восстановления после рестарта.

    Attributes:
        now: «Сейчас» — наибольший учтённый ts; None до первого сообщения.
        tracks: Состояния машин, отсортированы по unit_id.
        occupancies: Занятость станций в порядке site.stations.
        redirects: Действующие перенаправления (unit_id, рекомендованная станция),
            отсортированы по unit_id.
        decided: Машины, для которых решение в текущем заезде уже принято,
            отсортированы (кортеж, а не множество: стабильный порядок в JSON).
        published: Пары (station_id, записи последней опубликованной очереди)
            в порядке site.stations.

    Словари представлены кортежами пар: снимок неизменяем, хешируем и без потерь
    сериализуется в JSON.
    """

    now: int | None
    tracks: tuple[UnitTrack, ...]
    occupancies: tuple[StationOccupancy, ...]
    redirects: tuple[tuple[str, str], ...]
    decided: tuple[str, ...]
    published: tuple[tuple[str, tuple[QueueEntry, ...]], ...]


class SiteState:
    """Состояние площадки, обновляемое потоком телеметрии.

    Хранит треки машин, занятость станций, перенаправления, принятые за заезд
    решения и последние опубликованные очереди. Состояние изменяется только
    внутри объекта; наружу отдаются неизменяемые значения.
    """

    def __init__(self, site: SiteConfig, snapshot: SiteSnapshot | None = None) -> None:
        """Создаёт начальное состояние площадки или восстанавливает его из снимка.

        Args:
            site: Конфигурация площадки.
            snapshot: Снимок, полученный из snapshot(); None — начальное состояние.

        Raises:
            ValueError: Станции снимка не совпадают со станциями конфигурации.
        """
        self._site = site
        station_ids = [s.station_id for s in site.stations]
        if snapshot is None:
            self._now: int | None = None
            self._tracks: dict[str, UnitTrack] = {}
            self._occupancies = {sid: StationOccupancy(sid) for sid in station_ids}
            self._redirects: dict[str, str] = {}
            self._decided: set[str] = set()
            # Пустая очередь считается уже опубликованной: при старте не публикуется.
            self._published: dict[str, tuple[QueueEntry, ...]] = {sid: () for sid in station_ids}
            return

        occupancy_ids = [occ.station_id for occ in snapshot.occupancies]
        published_ids = [sid for sid, _ in snapshot.published]
        if occupancy_ids != station_ids or published_ids != station_ids:
            raise ValueError(
                f"Станции снимка {occupancy_ids} не совпадают со станциями площадки {station_ids}"
            )
        self._now = snapshot.now
        # Машины, раскреплённые в конфигурации между рестартами, не восстанавливаются.
        self._tracks = {
            track.unit_id: track
            for track in snapshot.tracks
            if site.home_station_id(track.unit_id) is not None
        }
        self._occupancies = {occ.station_id: occ for occ in snapshot.occupancies}
        self._redirects = {unit: sid for unit, sid in snapshot.redirects if unit in self._tracks}
        self._decided = {unit for unit in snapshot.decided if unit in self._tracks}
        self._published = dict(snapshot.published)

    @property
    def now(self) -> int | None:
        """«Сейчас» — наибольший учтённый ts; None до первого сообщения."""
        return self._now

    def apply(self, msg: Telemetry) -> list[SiteOutput]:
        """Учитывает сообщение телеметрии.

        Args:
            msg: Сообщение телеметрии.

        Returns:
            Решение (если расчёт рекомендации состоялся) первым, затем изменившиеся
            очереди станций в порядке site.stations. Пустой список, если сообщение
            проигнорировано (машина не закреплена, дубль или нарушение порядка).
        """
        site = self._site
        unit = msg.unit_id
        home = site.home_station_id(unit)
        if home is None:
            return []
        prev = self._tracks.get(unit)
        if not is_in_order(None if prev is None else prev.last_ts, msg.ts):
            return []

        track = advance(prev, msg, site)
        self._tracks[unit] = track
        now = msg.ts if self._now is None else max(self._now, msg.ts)
        self._now = now
        for sid, occ in self._occupancies.items():
            self._occupancies[sid] = update_occupancy(occ, track, site.rules)

        # Учёт заезда: отъезд со станции завершает перенаправление,
        # а побывавшая на станции машина снова может получить рекомендацию.
        left_station = (
            prev is not None
            and prev.phase is UnitPhase.AT_STATION
            and track.phase is not UnitPhase.AT_STATION
        )
        # Въезд на разгрузку начинает новый заезд к своей станции (п.4), даже если
        # стоянка на рекомендованной не была замечена. Новое решение — только после
        # посещения станции (п.5), поэтому отметка решения здесь не снимается.
        entered_unload = track.in_unload_zone and (prev is None or not prev.in_unload_zone)
        if left_station or entered_unload:
            self._redirects.pop(unit, None)
        if track.phase is UnitPhase.AT_STATION:
            self._decided.discard(unit)

        outputs: list[SiteOutput] = []
        if unit not in self._decided and is_decision_point(track, site.station(home), site.rules):
            decision = recommend(track, self._queues(now), self._occupancies, site, now)
            # Отказ тоже расходует решение за заезд.
            self._decided.add(unit)
            if isinstance(decision, Recommendation):
                self._redirects[unit] = decision.to_station
            outputs.append(decision)

        # Очереди пересчитываются уже с учётом нового перенаправления.
        for sid, queue in self._queues(now).items():
            # Сравнение по entries: поле at меняется на каждом сообщении.
            if queue.entries != self._published[sid]:
                self._published[sid] = queue.entries
                outputs.append(queue)
        return outputs

    def snapshot(self) -> SiteSnapshot:
        """Возвращает неизменяемый снимок полного состояния площадки.

        Returns:
            Снимок, из которого SiteState восстанавливается с тем же поведением.
        """
        return SiteSnapshot(
            now=self._now,
            tracks=tuple(sorted(self._tracks.values(), key=lambda t: t.unit_id)),
            occupancies=tuple(self._occupancies.values()),
            redirects=tuple(sorted(self._redirects.items())),
            decided=tuple(sorted(self._decided)),
            published=tuple(self._published.items()),
        )

    def _target(self, track: UnitTrack) -> str:
        """Станция, в очереди которой учитывается машина.

        Стоящая на станции — у этой станции; иначе — рекомендованная, если есть
        действующее перенаправление, либо своя. Учитываемые машины всегда
        закреплены: сообщения и треки незакреплённых отбрасываются.
        """
        if track.phase is UnitPhase.AT_STATION and track.station_id is not None:
            return track.station_id
        return self._redirects.get(track.unit_id) or self._site.assignments[track.unit_id]

    def _queues(self, now: int) -> dict[str, StationQueue]:
        """Очереди всех станций на момент now в порядке site.stations.

        Треки раскладываются в порядке unit_id: расчёт не зависит от порядка
        поступления машин.
        """
        by_station: dict[str, list[UnitTrack]] = {sid: [] for sid in self._occupancies}
        for track in sorted(self._tracks.values(), key=lambda t: t.unit_id):
            by_station[self._target(track)].append(track)
        return {
            sid: build_station_queue(occ, by_station[sid], self._site, now)
            for sid, occ in self._occupancies.items()
        }
