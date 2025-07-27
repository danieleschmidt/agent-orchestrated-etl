# Multi-stage Dockerfile for Agent-Orchestrated ETL
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VENV_IN_PROJECT=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml ./
COPY README.md ./

# Development stage
FROM base as development

# Install development dependencies
RUN pip install -e .[dev,vector]

# Copy source code
COPY --chown=appuser:appuser . .

# Switch to non-root user
USER appuser

EXPOSE 8000 8080 5555

CMD ["python", "-m", "uvicorn", "src.agent_orchestrated_etl.api:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]

# Production build stage
FROM base as builder

# Install build dependencies
RUN pip install build

# Copy source code
COPY . .

# Build wheel
RUN python -m build

# Production runtime stage
FROM python:3.11-slim as production

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy built wheel from builder stage
COPY --from=builder /app/dist/*.whl ./

# Install application
RUN pip install *.whl[vector] && rm *.whl

# Copy configuration files
COPY --chown=appuser:appuser docs/ ./docs/
COPY --chown=appuser:appuser scripts/ ./scripts/

# Create necessary directories
RUN mkdir -p /app/data /app/logs /app/airflow && \
    chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

CMD ["python", "-m", "uvicorn", "src.agent_orchestrated_etl.api:app", "--host", "0.0.0.0", "--port", "8000"]

# Security scanning stage
FROM production as security

USER root

# Install security scanning tools
RUN pip install safety bandit

# Run security scans
COPY . /tmp/src
RUN safety check --json --output /tmp/safety-report.json || true
RUN bandit -r /tmp/src -f json -o /tmp/bandit-report.json || true

USER appuser