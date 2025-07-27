.PHONY: help install install-dev clean test lint format typecheck security build run docker-up docker-down docs

# Default target
help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

# Development setup
install: ## Install production dependencies
	pip install -e .

install-dev: ## Install development dependencies
	pip install -e .[dev,vector]
	pre-commit install

# Code quality
clean: ## Clean build artifacts and cache
	find . -type d -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
	find . -name '*.pyc' -delete
	find . -name '*.pyo' -delete
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .coverage htmlcov/ .mypy_cache/ .ruff_cache/

test: ## Run tests with coverage
	pytest -v --cov=src/agent_orchestrated_etl --cov-report=html --cov-report=term-missing

test-fast: ## Run tests without coverage
	pytest -v -x

test-unit: ## Run unit tests only
	pytest tests/unit/ -v

test-integration: ## Run integration tests only
	pytest tests/integration/ -v -m integration

test-e2e: ## Run end-to-end tests only
	pytest tests/e2e/ -v -m e2e

lint: ## Run linting
	ruff check src/ tests/ --fix

format: ## Format code
	ruff format src/ tests/

typecheck: ## Run type checking
	mypy src/ tests/

security: ## Run security checks
	bandit -r src/ -f json -o bandit-report.json
	safety check --json --output safety-report.json

quality: ## Run code quality analysis
	radon cc src/agent_orchestrated_etl -s -a

# Build and packaging
build: ## Build package
	python -m build

build-docker: ## Build Docker image
	docker build -t agent-orchestrated-etl:latest .

build-docker-dev: ## Build Docker image for development
	docker build --target development -t agent-orchestrated-etl:dev .

# Running the application
run: ## Run the application locally
	python -m uvicorn src.agent_orchestrated_etl.api:app --reload --host 0.0.0.0 --port 8000

run-cli: ## Run CLI command (usage: make run-cli ARGS="--help")
	python -m agent_orchestrated_etl.cli $(ARGS)

# Docker Compose
docker-up: ## Start all services with Docker Compose
	docker-compose up -d

docker-down: ## Stop all services
	docker-compose down

docker-logs: ## Show logs from all services
	docker-compose logs -f

docker-build: ## Build all Docker images
	docker-compose build

# Database operations
db-init: ## Initialize databases
	docker-compose exec postgres psql -U postgres -f /docker-entrypoint-initdb.d/init-db.sql

db-reset: ## Reset databases (destructive)
	docker-compose down -v
	docker-compose up -d postgres
	sleep 5
	make db-init

# Airflow operations
airflow-init: ## Initialize Airflow
	docker-compose exec airflow-webserver airflow db init

airflow-user: ## Create Airflow admin user
	docker-compose exec airflow-webserver airflow users create \
		--username admin --firstname Admin --lastname User \
		--role Admin --email admin@example.com --password admin

# Monitoring
monitoring-up: ## Start monitoring stack
	docker-compose up -d prometheus grafana

# Documentation
docs: ## Serve documentation locally
	mkdocs serve

docs-build: ## Build documentation
	mkdocs build

# Release management
version-patch: ## Bump patch version
	bump2version patch

version-minor: ## Bump minor version
	bump2version minor

version-major: ## Bump major version
	bump2version major

# CI/CD helpers
ci-setup: ## Setup CI environment
	pip install -e .[dev,vector]

ci-test: ## Run CI test suite
	pytest --cov=src/agent_orchestrated_etl --cov-report=xml --cov-fail-under=80

ci-security: ## Run CI security checks
	bandit -r src/ -f json -o bandit-report.json
	safety check --json --output safety-report.json
	detect-secrets scan --baseline .secrets.baseline

ci-quality: ## Run CI quality checks
	ruff check src/ tests/
	mypy src/ tests/

# Environment management
env-create: ## Create development environment file
	cp .env.example .env
	@echo "Created .env file. Please update with your specific configuration."

env-validate: ## Validate environment configuration
	python -c "from src.agent_orchestrated_etl.config import get_settings; print('Environment configuration is valid')"

# Development helpers
dev-setup: ## Complete development setup
	make install-dev
	make env-create
	make docker-up
	@echo "Development environment is ready!"

dev-reset: ## Reset development environment
	make docker-down
	make clean
	docker system prune -f
	make docker-up

# Performance testing
perf-test: ## Run performance tests
	pytest tests/ -m performance -v

benchmark: ## Run benchmarks
	pytest tests/ --benchmark-only

# Utilities
check-deps: ## Check for dependency updates
	pip list --outdated

update-deps: ## Update dependencies (be careful!)
	pip-review --auto

pre-commit-all: ## Run pre-commit on all files
	pre-commit run --all-files