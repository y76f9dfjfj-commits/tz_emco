# Образ сервиса виртуальных очередей: процессор (по умолчанию) и генератор телеметрии
# (переопределением команды: python -m vqueue.simulator.generator_main ...).
# Сборка: docker build -t vqueue . — платформы linux/amd64 и linux/arm64.

# Закреплённый патч-релиз: сборка воспроизводима, обновление базы — осознанная правка тега.
ARG PYTHON_IMAGE=python:3.11.17-slim-bookworm
# Та же версия poetry, что в CI (POETRY_VERSION в .github/workflows/ci.yml).
ARG POETRY_VERSION=2.4.2

# --- Стадия сборки: пакет и runtime-зависимости в изолированное venv ---
FROM ${PYTHON_IMAGE} AS build
ARG POETRY_VERSION

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    VIRTUAL_ENV=/opt/venv

# poetry — в отдельном venv, чтобы его зависимости не попали в итоговый образ.
RUN python -m venv /opt/poetry \
    && /opt/poetry/bin/pip install "poetry==${POETRY_VERSION}" \
    && python -m venv /opt/venv
ENV PATH=/opt/venv/bin:$PATH

WORKDIR /src
# Сначала только манифест и lock: слой зависимостей кешируется, пока они не меняются.
COPY pyproject.toml poetry.lock ./
# Зависимости строго по poetry.lock, только группа main; poetry ставит их в активное
# venv (VIRTUAL_ENV) — свой не создаёт.
RUN /opt/poetry/bin/poetry sync --only main --no-root
# README.md нужен сборщику poetry-core (поле readme в pyproject.toml).
COPY README.md ./
COPY src ./src
# Сам пакет без разрешения зависимостей: они уже установлены из lock.
RUN pip install --no-deps .

# --- Итоговый образ: только интерпретатор, venv и конфигурация площадки ---
FROM ${PYTHON_IMAGE} AS runtime

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SITE_CONFIG=/app/config/site.toml \
    HEALTH_PORT=8080

RUN groupadd --system --gid 10001 vqueue \
    && useradd --system --uid 10001 --gid vqueue --no-create-home --home-dir /app vqueue

COPY --from=build /opt/venv /opt/venv
WORKDIR /app
COPY config ./config

USER 10001:10001
EXPOSE 8080

# Проверка относится к процессору (команда по умолчанию): /health/ready отвечает 200 после
# назначения партиции и восстановления снимка. curl в slim-образе нет — проверка через urllib
# (не-2xx и отказ соединения дают код 1). У генератора health-сервера нет: в docker-compose
# проверка для него отключена, вне compose — docker run --no-healthcheck ... generator_main.
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health/ready' % os.environ.get('HEALTH_PORT', '8080'), timeout=3)"]

# Exec-форма: python — PID 1 и сам обрабатывает SIGTERM (штатная остановка после текущего батча).
CMD ["python", "-m", "vqueue.processor_main"]
