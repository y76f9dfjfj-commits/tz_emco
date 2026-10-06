# Единая точка входа для разработки и CI. Список целей: `make help`.

.DEFAULT_GOAL := help
.PHONY: help install format lint typecheck security test test-int cov check clean

RUN := poetry run

help: ## Показать список целей
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

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

test-int: ## Запустить интеграционные тесты (нужен Kafka)
	$(RUN) pytest -m integration -q

cov: ## Тесты с покрытием и порогом
	$(RUN) pytest -m "not integration" -q --cov --cov-report=term-missing

check: lint typecheck security test ## Все проверки перед коммитом

clean: ## Удалить кеши и артефакты
	rm -rf .mypy_cache .ruff_cache .pytest_cache .hypothesis .coverage coverage.xml htmlcov dist build
	find . -path ./.venv -prune -o -name __pycache__ -type d -exec rm -rf {} +
