"""Тесты health-check: готовность по назначению партиций и свежести poll, HTTP-эндпоинты.

HealthState готов, когда назначены партиции и последний poll не старше
max_poll_age_s (время — time.monotonic, это liveness, а не бизнес-время ТЗ);
GET /health/live → 200, /health/ready → 200 | 503, прочее → 404.
"""

from __future__ import annotations

import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass
from http.server import ThreadingHTTPServer

import pytest

from vqueue.adapters import health as health_module
from vqueue.adapters.health import HealthState, start_health_server


@dataclass
class FakeClock:
    """Управляемые монотонные часы для проверки свежести poll."""

    now: float = 1_000.0

    def __call__(self) -> float:
        """Текущее показание часов."""
        return self.now


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> FakeClock:
    """Подменяет time.monotonic (и импортированный в модуль monotonic, если он есть)."""
    fake = FakeClock()
    monkeypatch.setattr(time, "monotonic", fake)
    if hasattr(health_module, "monotonic"):
        monkeypatch.setattr(health_module, "monotonic", fake)
    return fake


# ---------------------------------------------------------------------------
# HealthState
# ---------------------------------------------------------------------------


def test_health_state_initially_not_ready() -> None:
    """Без назначенных партиций и poll — не готов."""
    assert not HealthState().is_ready(30.0)


def test_health_state_assigned_without_poll_not_ready(clock: FakeClock) -> None:
    """Партиции назначены, но poll ещё не было — не готов."""
    state = HealthState()
    state.mark_assigned(True)

    assert not state.is_ready(30.0)


def test_health_state_poll_without_assignment_not_ready(clock: FakeClock) -> None:
    """Poll был, но партиции не назначены — не готов."""
    state = HealthState()
    state.mark_poll()

    assert not state.is_ready(30.0)


def test_health_state_assigned_and_fresh_poll_ready(clock: FakeClock) -> None:
    """Назначены партиции и свежий poll — готов."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()
    clock.now += 5.0

    assert state.is_ready(30.0)


def test_health_state_poll_age_equal_limit_ready(clock: FakeClock) -> None:
    """Граница: poll ровно max_poll_age_s назад — «не старше», ещё готов."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()
    clock.now += 30.0

    assert state.is_ready(30.0)


def test_health_state_poll_older_than_limit_not_ready(clock: FakeClock) -> None:
    """Последний poll старше max_poll_age_s — не готов."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()
    clock.now += 30.5

    assert not state.is_ready(30.0)


def test_health_state_new_poll_restores_readiness(clock: FakeClock) -> None:
    """После устаревания новый poll снова делает сервис готовым."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()
    clock.now += 100.0
    assert not state.is_ready(30.0)

    state.mark_poll()

    assert state.is_ready(30.0)


def test_health_state_revoked_not_ready(clock: FakeClock) -> None:
    """Отзыв партиций (mark_assigned(False)) снимает готовность."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()

    state.mark_assigned(False)

    assert not state.is_ready(30.0)


def test_health_state_real_clock_stale_poll_not_ready() -> None:
    """С реальными монотонными часами: poll 50 мс назад старше лимита 10 мс."""
    state = HealthState()
    state.mark_assigned(True)
    state.mark_poll()
    time.sleep(0.05)

    assert not state.is_ready(0.01)
    assert state.is_ready(60.0)


def test_health_state_concurrent_marks_consistent() -> None:
    """Потокобезопасность: параллельные mark_poll/mark_assigned/is_ready не падают."""
    state = HealthState()
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            for _ in range(500):
                state.mark_assigned(True)
                state.mark_poll()
                state.is_ready(60.0)
        except BaseException as exc:  # pragma: no cover - фиксируем для assert ниже
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert errors == []
    assert state.is_ready(60.0)


# ---------------------------------------------------------------------------
# HTTP-сервер
# ---------------------------------------------------------------------------


@pytest.fixture
def health_state() -> HealthState:
    """Состояние здоровья для сервера."""
    return HealthState()


@pytest.fixture
def server(health_state: HealthState) -> Iterator[ThreadingHTTPServer]:
    """Сервер на эфемерном порту; останавливается после теста."""
    srv = start_health_server(health_state, 0, max_poll_age_s=30.0)
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


def _get(srv: ThreadingHTTPServer, path: str) -> tuple[int, str]:
    """GET-запрос к серверу; возвращает статус и тело."""
    port = srv.server_address[1]
    url = f"http://127.0.0.1:{port}{path}"
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            return int(resp.status), resp.read().decode()
    except urllib.error.HTTPError as err:
        return err.code, err.read().decode()


def test_health_server_ephemeral_port_bound(server: ThreadingHTTPServer) -> None:
    """Порт 0 — эфемерный: сервер слушает реальный ненулевой порт."""
    assert server.server_address[1] != 0


def test_health_live_returns_200_ok(server: ThreadingHTTPServer) -> None:
    """GET /health/live → 200 "ok" независимо от готовности."""
    status, body = _get(server, "/health/live")

    assert status == 200
    assert body.strip() == "ok"


def test_health_ready_not_ready_returns_503(server: ThreadingHTTPServer) -> None:
    """GET /health/ready при неготовности → 503 "not ready"."""
    status, body = _get(server, "/health/ready")

    assert status == 503
    assert body.strip() == "not ready"


def test_health_ready_ready_returns_200(
    server: ThreadingHTTPServer, health_state: HealthState
) -> None:
    """GET /health/ready при готовности → 200 "ready"."""
    health_state.mark_assigned(True)
    health_state.mark_poll()

    status, body = _get(server, "/health/ready")

    assert status == 200
    assert body.strip() == "ready"


def test_health_ready_uses_server_max_poll_age(health_state: HealthState) -> None:
    """Сервер проверяет готовность с переданным max_poll_age_s."""
    health_state.mark_assigned(True)
    health_state.mark_poll()
    time.sleep(0.05)
    srv = start_health_server(health_state, 0, max_poll_age_s=0.01)
    try:
        status, _ = _get(srv, "/health/ready")
    finally:
        srv.shutdown()
        srv.server_close()

    assert status == 503


@pytest.mark.parametrize("path", ["/", "/health", "/health/unknown", "/metrics"])
def test_health_unknown_path_returns_404(server: ThreadingHTTPServer, path: str) -> None:
    """Неизвестный путь → 404."""
    status, _ = _get(server, path)

    assert status == 404


def test_health_server_runs_in_daemon_thread(health_state: HealthState) -> None:
    """Сервер обслуживает запросы в daemon-потоке, не блокируя вызывающего."""
    before = set(threading.enumerate())
    srv = start_health_server(health_state, 0)
    try:
        started = set(threading.enumerate()) - before
        status, _ = _get(srv, "/health/live")
    finally:
        srv.shutdown()
        srv.server_close()

    assert status == 200
    assert started
    assert all(t.daemon for t in started)


def test_health_server_no_request_logging(
    server: ThreadingHTTPServer, capsys: pytest.CaptureFixture[str]
) -> None:
    """Без логов на каждый запрос (стандартный вывод в stderr подавлен)."""
    _get(server, "/health/live")
    _get(server, "/health/ready")
    _get(server, "/nope")

    captured = capsys.readouterr()
    assert captured.err == ""
    assert captured.out == ""
