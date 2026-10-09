"""Health-check процессора: liveness и readiness по HTTP."""

from __future__ import annotations

import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

_POLL_INTERVAL_S = 0.05


class HealthState:
    """Потокобезопасное состояние готовности процессора.

    Время последнего poll берётся из time.monotonic(): это liveness процесса,
    а не бизнес-время, и в расчёты домена оно не попадает.
    """

    def __init__(self) -> None:
        """Создаёт состояние: партиции не назначены, poll ещё не было."""
        self._lock = threading.Lock()
        self._assigned = False
        self._last_poll: float | None = None

    def mark_assigned(self, assigned: bool) -> None:
        """Отмечает, назначены ли консьюмеру партиции входного топика.

        Args:
            assigned: True после назначения партиций, False после их отзыва или потери.
        """
        with self._lock:
            self._assigned = assigned

    def mark_poll(self) -> None:
        """Отмечает успешный опрос брокера."""
        now = time.monotonic()
        with self._lock:
            self._last_poll = now

    def is_ready(self, max_poll_age_s: float) -> bool:
        """Проверяет готовность: партиции назначены и последний poll достаточно свежий.

        Args:
            max_poll_age_s: Наибольший допустимый возраст последнего poll, с.

        Returns:
            True, если процессор готов.
        """
        now = time.monotonic()
        with self._lock:
            return (
                self._assigned
                and self._last_poll is not None
                and now - self._last_poll <= max_poll_age_s
            )


def start_health_server(
    state: HealthState, port: int, max_poll_age_s: float = 30.0
) -> ThreadingHTTPServer:
    """Запускает HTTP-сервер health-check в фоновом daemon-потоке.

    Маршруты: GET /health/live → 200 "ok"; GET /health/ready → 200 "ready"
    или 503 "not ready"; прочие пути → 404.

    Args:
        state: Состояние готовности процессора.
        port: TCP-порт на всех интерфейсах; 0 — выбрать свободный.
        max_poll_age_s: Наибольший возраст последнего poll для готовности, с.

    Returns:
        Запущенный сервер; остановка — shutdown() и server_close().
    """

    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path == "/health/live":
                self._reply(HTTPStatus.OK, "ok")
            elif self.path == "/health/ready":
                if state.is_ready(max_poll_age_s):
                    self._reply(HTTPStatus.OK, "ready")
                else:
                    self._reply(HTTPStatus.SERVICE_UNAVAILABLE, "not ready")
            else:
                self._reply(HTTPStatus.NOT_FOUND, "not found")

        def _reply(self, status: HTTPStatus, body: str) -> None:
            payload = body.encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: Any) -> None:
            # Пробы приходят часто: журнал на каждый запрос только зашумляет логи.
            pass

    # "" — все интерфейсы: пробы оркестратора приходят не с localhost.
    server = ThreadingHTTPServer(("", port), _Handler)
    server.daemon_threads = True
    # Короткий интервал опроса флага остановки: shutdown() не ждёт стандартные 0,5 с.
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": _POLL_INTERVAL_S},
        name="health-server",
        daemon=True,
    )
    thread.start()
    return server
