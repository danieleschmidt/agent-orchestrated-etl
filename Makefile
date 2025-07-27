# Makefile for Agent-Orchestrated-ETL
# =============================================================================

.PHONY: help install install-dev clean test lint format typecheck security docs build docker-build docker-run docker-compose docker-down deploy

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python
PIP := pip
PROJECT_NAME := agent-orchestrated-etl
SRC_DIR := src
TESTS_DIR := tests
DOCS_DIR := docs

# Docker variables
DOCKER_IMAGE := $(PROJECT_NAME)
DOCKER_TAG := latest
DOCKER_REGISTRY := ghcr.io

# Colors for output
BLUE := \033[0;34m
GREEN := \033[0;32m
YELLOW := \033[1;33m
RED := \033[0;31m
NC := \033[0m # No Color

# =============================================================================
# Help
# =============================================================================

help: ## Show this help message
	@echo "$(BLUE)Agent-Orchestrated-ETL Makefile$(NC)"
	@echo "$(YELLOW)Available targets:$(NC)"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# =============================================================================
# Installation and Setup
# =============================================================================

install: ## Install production dependencies
	@echo "$(BLUE)Installing production dependencies...$(NC)"
	$(PIP) install -e .

install-dev: ## Install development dependencies
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	$(PIP) install -e .[dev]
	pre-commit install

clean: ## Clean build artifacts and cache
	@echo "$(BLUE)Cleaning build artifacts...$(NC)"
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -f .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# =============================================================================
# Development Tools
# =============================================================================

test: ## Run all tests
	@echo "$(BLUE)Running tests...$(NC)"
	$(PYTHON) -m pytest $(TESTS_DIR) -v

test-unit: ## Run unit tests only
	@echo "$(BLUE)Running unit tests...$(NC)"
	$(PYTHON) -m pytest $(TESTS_DIR) -v -m "not integration"

test-integration: ## Run integration tests only
	@echo "$(BLUE)Running integration tests...$(NC)"
	$(PYTHON) -m pytest $(TESTS_DIR) -v -m integration

test-coverage: ## Run tests with coverage report
	@echo "$(BLUE)Running tests with coverage...$(NC)"
	$(PYTHON) -m pytest $(TESTS_DIR) --cov=$(SRC_DIR) --cov-report=html --cov-report=term-missing

test-watch: ## Run tests in watch mode
	@echo "$(BLUE)Running tests in watch mode...$(NC)"
	$(PYTHON) -m pytest $(TESTS_DIR) -f

lint: ## Run linting checks
	@echo "$(BLUE)Running linting checks...$(NC)"
	$(PYTHON) -m ruff check $(SRC_DIR) $(TESTS_DIR)

lint-fix: ## Fix linting issues automatically
	@echo "$(BLUE)Fixing linting issues...$(NC)"
	$(PYTHON) -m ruff check --fix $(SRC_DIR) $(TESTS_DIR)

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	$(PYTHON) -m black $(SRC_DIR) $(TESTS_DIR)

format-check: ## Check code formatting
	@echo "$(BLUE)Checking code formatting...$(NC)"
	$(PYTHON) -m black --check $(SRC_DIR) $(TESTS_DIR)

typecheck: ## Run type checking with mypy
	@echo "$(BLUE)Running type checks...$(NC)"
	$(PYTHON) -m mypy $(SRC_DIR)

security: ## Run security checks
	@echo "$(BLUE)Running security checks...$(NC)"
	$(PYTHON) -m bandit -r $(SRC_DIR)
	$(PYTHON) -m safety check

secrets: ## Scan for secrets
	@echo "$(BLUE)Scanning for secrets...$(NC)"
	detect-secrets scan --all-files --baseline .secrets.baseline

quality: ## Run code quality analysis
	@echo "$(BLUE)Running code quality analysis...$(NC)"
	$(PYTHON) -m radon cc $(SRC_DIR) -s -a

pre-commit: ## Run all pre-commit hooks
	@echo "$(BLUE)Running pre-commit hooks...$(NC)"
	pre-commit run --all-files

validate: ## Run all validation checks
	@echo "$(BLUE)Running all validation checks...$(NC)"
	$(MAKE) lint
	$(MAKE) typecheck
	$(MAKE) test
	$(MAKE) security

# =============================================================================
# Build and Package
# =============================================================================

build: ## Build the package
	@echo "$(BLUE)Building package...$(NC)"
	$(PYTHON) -m build

build-wheel: ## Build wheel package
	@echo "$(BLUE)Building wheel package...$(NC)"
	$(PYTHON) -m build --wheel

build-sdist: ## Build source distribution
	@echo "$(BLUE)Building source distribution...$(NC)"
	$(PYTHON) -m build --sdist

# =============================================================================
# Docker Commands
# =============================================================================

docker-build: ## Build Docker image
	@echo "$(BLUE)Building Docker image...$(NC)"
	docker build -t $(DOCKER_IMAGE):$(DOCKER_TAG) .

docker-build-dev: ## Build Docker image for development
	@echo "$(BLUE)Building development Docker image...$(NC)"
	docker build --target development -t $(DOCKER_IMAGE):dev .

docker-run: ## Run Docker container
	@echo "$(BLUE)Running Docker container...$(NC)"
	docker run -it --rm -p 8080:8080 $(DOCKER_IMAGE):$(DOCKER_TAG)

docker-run-dev: ## Run Docker container in development mode
	@echo "$(BLUE)Running development Docker container...$(NC)"
	docker run -it --rm -v $$(pwd):/app $(DOCKER_IMAGE):dev

docker-compose: ## Start all services with docker-compose
	@echo "$(BLUE)Starting services with docker-compose...$(NC)"
	docker-compose up -d

docker-compose-build: ## Build and start all services
	@echo "$(BLUE)Building and starting services...$(NC)"
	docker-compose up -d --build

docker-down: ## Stop all docker-compose services
	@echo "$(BLUE)Stopping docker-compose services...$(NC)"
	docker-compose down

docker-logs: ## Show docker-compose logs
	@echo "$(BLUE)Showing docker-compose logs...$(NC)"
	docker-compose logs -f

docker-clean: ## Clean Docker images and containers
	@echo "$(BLUE)Cleaning Docker artifacts...$(NC)"
	docker system prune -f

# =============================================================================
# Documentation
# =============================================================================

docs: ## Build documentation
	@echo "$(BLUE)Building documentation...$(NC)"
	mkdocs build

docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving documentation...$(NC)"
	mkdocs serve

docs-deploy: ## Deploy documentation
	@echo "$(BLUE)Deploying documentation...$(NC)"
	mkdocs gh-deploy

# =============================================================================
# Application Commands
# =============================================================================

run: ## Run the application
	@echo "$(BLUE)Running application...$(NC)"
	$(PYTHON) -m agent_orchestrated_etl.cli

run-pipeline: ## Run a sample pipeline
	@echo "$(BLUE)Running sample pipeline...$(NC)"
	$(PYTHON) -m agent_orchestrated_etl.cli run_pipeline mock --output results.json

generate-dag: ## Generate a sample DAG
	@echo "$(BLUE)Generating sample DAG...$(NC)"
	$(PYTHON) -m agent_orchestrated_etl.cli generate_dag mock sample_dag.py

# =============================================================================
# Database Commands
# =============================================================================

db-init: ## Initialize database
	@echo "$(BLUE)Initializing database...$(NC)"
	$(PYTHON) -c "from agent_orchestrated_etl.config import init_database; init_database()"

db-migrate: ## Run database migrations
	@echo "$(BLUE)Running database migrations...$(NC)"
	alembic upgrade head

db-reset: ## Reset database
	@echo "$(BLUE)Resetting database...$(NC)"
	$(PYTHON) -c "from agent_orchestrated_etl.config import reset_database; reset_database()"

# =============================================================================
# Deployment
# =============================================================================

deploy-staging: ## Deploy to staging environment
	@echo "$(BLUE)Deploying to staging...$(NC)"
	docker build -t $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):staging .
	docker push $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):staging

deploy-prod: ## Deploy to production environment
	@echo "$(BLUE)Deploying to production...$(NC)"
	docker build -t $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG) .
	docker push $(DOCKER_REGISTRY)/$(DOCKER_IMAGE):$(DOCKER_TAG)

# =============================================================================
# Monitoring and Health
# =============================================================================

health-check: ## Run health checks
	@echo "$(BLUE)Running health checks...$(NC)"
	curl -f http://localhost:8080/health || exit 1

logs: ## Show application logs
	@echo "$(BLUE)Showing application logs...$(NC)"
	tail -f logs/app.log

monitor: ## Start monitoring services
	@echo "$(BLUE)Starting monitoring services...$(NC)"
	docker-compose -f docker-compose.yml -f docker-compose.monitoring.yml up -d

# =============================================================================
# Utility Commands
# =============================================================================

version: ## Show version information
	@echo "$(BLUE)Agent-Orchestrated-ETL$(NC)"
	@$(PYTHON) -c "from agent_orchestrated_etl import __version__; print(f'Version: {__version__}')"

env-check: ## Check environment setup
	@echo "$(BLUE)Checking environment...$(NC)"
	@$(PYTHON) --version
	@$(PIP) --version
	@which docker >/dev/null && echo "Docker: $(shell docker --version)" || echo "$(RED)Docker not found$(NC)"
	@which docker-compose >/dev/null && echo "Docker Compose: $(shell docker-compose --version)" || echo "$(RED)Docker Compose not found$(NC)"

dependencies: ## Show dependency tree
	@echo "$(BLUE)Dependency tree:$(NC)"
	$(PIP) list

update-deps: ## Update dependencies
	@echo "$(BLUE)Updating dependencies...$(NC)"
	$(PIP) install --upgrade pip
	$(PIP) install --upgrade -e .[dev]