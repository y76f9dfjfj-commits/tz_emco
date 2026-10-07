# Единая точка входа для разработки и CI. Список целей: `make help`.

.DEFAULT_GOAL := help
.PHONY: help install format lint typecheck security test test-int cov check clean \
	up down reset logs ps topics kafka-up run-processor run-generator \
	consume-queue consume-decision health

RUN := poetry run
COMPOSE := docker compose
# Адрес брокера стенда docker compose со стороны хоста.
HOST_BOOTSTRAP := 127.0.0.1:9094
# Параметры чтения топиков: по умолчанию первые 20 сообщений с начала или выход после 10 с
# тишины (queue.v1 пишется непрерывно — одного таймаута мало, решения редки — «только новые»
# часто пусты). Поток новых сообщений: make consume-decision CONSUME_OPTS=
CONSUME_OPTS ?= --from-beginning --max-messages 20 --timeout-ms 10000
KAFKA_BIN := /opt/kafka/bin

# Чтение топика с ключами внутри контейнера брокера; только зафиксированные транзакции.
define consume
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-console-consumer.sh \
		--bootstrap-server localhost:9092 --topic $(1) --isolation-level read_committed \
		--formatter-property print.key=true --formatter-property key.separator=' ' \
		$(CONSUME_OPTS)
endef

help: ## Показать список целей
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Установить зависимости и git-хуки
	poetry install
	$(RUN) pre-commit install

format: ## Отформатировать код и исправить автоисправимое
	$(RUN) ruff format .
	$(RUN) ruff check --fix .

lint: ## Проверить стиль и линтеры
	$(RUN) ruff format --check .
	$(RUN) ruff check .

typecheck: ## Проверить типы (mypy --strict)
	$(RUN) mypy

security: ## Проверить код (bandit) и секреты (gitleaks)
	$(RUN) bandit -c pyproject.toml -q -r src
	$(RUN) pre-commit run gitleaks --all-files

# Код 5 у pytest означает «тесты не найдены» — на старте проекта это не ошибка.
test: ## Запустить тесты без брокера
	$(RUN) pytest -m "not integration" -q || [ $$? -eq 5 ]

# Процессор и генератор стенда останавливаются: генератор писал бы в telemetry.v1 параллельно
# с тестами. Брокер остаётся; stop для незапущенных сервисов не ошибка.
test-int: ## Интеграционные тесты с брокером на 127.0.0.1:9094 (make kafka-up)
	$(COMPOSE) stop processor generator
	KAFKA_BOOTSTRAP=$(HOST_BOOTSTRAP) $(RUN) pytest -m integration -q

cov: ## Тесты с покрытием и порогом
	$(RUN) pytest -m "not integration" -q --cov --cov-report=term-missing

check: lint typecheck security cov ## Все проверки перед коммитом (с порогом покрытия)

clean: ## Удалить кеши и артефакты
	rm -rf .mypy_cache .ruff_cache .pytest_cache .hypothesis .coverage coverage.xml htmlcov dist build
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +

# --- Стенд docker compose ---

up: ## Собрать образ и поднять стенд (брокер, топики, процессор, генератор)
	$(COMPOSE) up -d --build

down: ## Остановить стенд, данные брокера сохраняются
	$(COMPOSE) down

# Подтверждение через printf + read: /bin/sh в Debian (dash) не поддерживает read -p.
reset: ## ВНИМАНИЕ: остановить стенд и удалить данные брокера (с подтверждением)
	@printf 'Удалить все данные брокера стенда (топики, снимки, offsets)? [y/N] '; \
		read answer; \
		case "$$answer" in [yY]) ;; *) echo 'Отменено.'; exit 1 ;; esac
	$(COMPOSE) down --volumes --remove-orphans

logs: ## Логи стенда в реальном времени
	$(COMPOSE) logs -f --tail=100

ps: ## Состояние контейнеров стенда
	$(COMPOSE) ps -a

topics: ## Описание топиков (партиции, конфигурация)
	$(COMPOSE) exec -T kafka $(KAFKA_BIN)/kafka-topics.sh --bootstrap-server localhost:9092 \
		--describe --exclude-internal

kafka-up: ## Только брокер и топики (для интеграционных тестов и локального запуска)
	$(COMPOSE) up -d --wait kafka
	$(COMPOSE) run --rm kafka-init

# Health-порт 8081: не конфликтует с процессором стенда на 8080. Своя консьюмер-группа и SITE_ID:
# при поднятом стенде локальный процессор не делит с ним партицию telemetry.v1, а читает
# поток независимо и хранит свой снимок (ключ site-local). INSTANCE_ID не задан —
# динамическое членство.
run-processor: ## Процессор локально через poetry (брокер 127.0.0.1:9094, health :8081)
	KAFKA_BOOTSTRAP=$(HOST_BOOTSTRAP) HEALTH_PORT=8081 SITE_ID=site-local \
		CONSUMER_GROUP=vqueue-processor-local $(RUN) python -m vqueue.processor_main

run-generator: ## Генератор локально через poetry (брокер 127.0.0.1:9094, ускорение 10)
	$(RUN) python -m vqueue.simulator.generator_main --bootstrap $(HOST_BOOTSTRAP) --speedup 10

consume-queue: ## Прочитать queue.v1 (read_committed, с ключами; CONSUME_OPTS — параметры)
	$(call consume,queue.v1)

consume-decision: ## Прочитать decision.v1 (read_committed, с ключами; CONSUME_OPTS — параметры)
	$(call consume,decision.v1)

health: ## Готовность процессора стенда (/health/ready)
	curl -fsS 127.0.0.1:8080/health/ready
	@echo
