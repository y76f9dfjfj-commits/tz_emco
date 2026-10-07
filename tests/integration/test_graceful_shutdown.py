"""Сценарий C: штатная остановка процессора по SIGTERM.

Работающий процессор (партиция назначена, /health/ready → 200) по SIGTERM завершает текущий
батч и выходит с кодом 0 не дольше чем за 15 с.
"""

from __future__ import annotations

import signal
import subprocess  # nosec B404
from pathlib import Path
from typing import Final

import pytest

from tests.integration.kafkakit import LauncherFactory

pytestmark = pytest.mark.integration

SITE_TOML: Final = Path(__file__).resolve().parents[2] / "config" / "site.toml"
GRACE_SECONDS: Final = 15.0


def test_sigterm_running_processor_exits_with_code_0_within_15s(launcher: LauncherFactory) -> None:
    """SIGTERM работающему процессору → код возврата 0 за ≤ 15 с."""
    proc = launcher(SITE_TOML).start()
    proc.wait_ready(timeout_s=60)

    proc.popen.send_signal(signal.SIGTERM)
    try:
        code = proc.popen.wait(timeout=GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        pytest.fail(f"процессор не завершился за {GRACE_SECONDS} с после SIGTERM")

    assert code == 0
