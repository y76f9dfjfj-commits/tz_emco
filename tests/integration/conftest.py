"""Фикстуры интеграционных тестов: адрес брокера, чистые топики, запуск процессора."""

from __future__ import annotations

import os
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.integration.kafkakit import (
    LauncherFactory,
    ProcessorLauncher,
    active_members,
    broker_unavailable_reason,
    recreate_topics,
)

DEFAULT_BOOTSTRAP = "127.0.0.1:9094"

STAND_GROUP = "vqueue-processor-site-1"
"""Консьюмер-группа процессора стенда docker compose (SITE_ID=site-1)."""

STAND_LEAVE_WAIT_S = 60.0
"""Сколько ждать, пока остановленный стенд покинет группу.

Статический участник при остановке не выходит из группы и числится в ней до
session.timeout.ms (45 с по умолчанию).
"""


@pytest.fixture(scope="session")
def bootstrap() -> str:
    """Адрес брокера (KAFKA_BOOTSTRAP); пропуск, если брокер недоступен или работает стенд."""
    address = os.environ.get("KAFKA_BOOTSTRAP") or DEFAULT_BOOTSTRAP
    reason = broker_unavailable_reason(address)
    if reason is not None:
        pytest.skip(reason)
    deadline = time.monotonic() + STAND_LEAVE_WAIT_S
    while active_members(address, STAND_GROUP) > 0:
        if time.monotonic() >= deadline:
            pytest.skip(
                "стенд запущен: остановите processor/generator (make test-int делает это сам); "
                f"группа {STAND_GROUP} не опустела за {STAND_LEAVE_WAIT_S:.0f} с"
            )
        time.sleep(2.0)
    return address


@pytest.fixture
def fresh_topics(bootstrap: str) -> str:
    """Пересоздаёт четыре топика процессора; возвращает адрес брокера."""
    recreate_topics(bootstrap)
    return bootstrap


@pytest.fixture
def run_id() -> str:
    """Уникальный суффикс идентификаторов теста (группы, SITE_ID, INSTANCE_ID)."""
    return uuid.uuid4().hex[:12]


@pytest.fixture
def launcher(fresh_topics: str, tmp_path: Path, run_id: str) -> Iterator[LauncherFactory]:
    """Фабрика запускателя с уникальными SITE_ID, INSTANCE_ID и группой теста.

    В конце теста все запущенные процессы убиваются, их журналы печатаются (видны при падении).
    """
    created: list[ProcessorLauncher] = []

    def make(site_config: Path) -> ProcessorLauncher:
        made = ProcessorLauncher(
            bootstrap=fresh_topics,
            site_config=site_config,
            workdir=tmp_path,
            site_id=f"it-site-{run_id}",
            instance_id=f"it-instance-{run_id}",
            consumer_group=f"it-processor-{run_id}",
        )
        created.append(made)
        return made

    try:
        yield make
    finally:
        for made in created:
            made.stop_all()
