"""Имена топиков Kafka процессора виртуальных очередей."""

from __future__ import annotations

from typing import Final

TELEMETRY: Final = "telemetry.v1"
"""Вход: телеметрия, ключ unit_uuid; одна партиция на площадку — единый порядок сообщений."""

QUEUE: Final = "queue.v1"
"""Выход: очереди станций, ключ station_uuid."""

DECISION: Final = "decision.v1"
"""Выход: решения по рекомендациям, ключ unit_uuid."""

STATE: Final = "vqueue.state.v1"
"""Состояние: compacted, одна партиция, ключ site_id — снимок SiteState."""
