# Multi-stage build for Agent-Orchestrated-ETL
# =============================================================================

# Build stage
FROM python:3.11-slim as builder

# Set build arguments
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=0.0.1

# Add metadata labels
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="agent-orchestrated-etl" \
      org.label-schema.description="Hybrid Airflow + LangChain ETL orchestration" \
      org.label-schema.url="https://github.com/your-org/agent-orchestrated-etl" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/your-org/agent-orchestrated-etl" \
      org.label-schema.vendor="Terragon Labs" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"

# Set environment variables for build
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN groupadd -r appuser && useradd -r -g appuser appuser

# Set working directory
WORKDIR /app

# Copy dependency files
COPY pyproject.toml README.md ./

# Install Python dependencies
RUN pip install --upgrade pip && \
    pip install build && \
    pip install .

# =============================================================================

# Production stage
FROM python:3.11-slim as production

# Set runtime arguments
ARG BUILD_DATE
ARG VCS_REF
ARG VERSION=0.0.1

# Add metadata labels
LABEL org.label-schema.build-date=$BUILD_DATE \
      org.label-schema.name="agent-orchestrated-etl" \
      org.label-schema.description="Hybrid Airflow + LangChain ETL orchestration" \
      org.label-schema.url="https://github.com/your-org/agent-orchestrated-etl" \
      org.label-schema.vcs-ref=$VCS_REF \
      org.label-schema.vcs-url="https://github.com/your-org/agent-orchestrated-etl" \
      org.label-schema.vendor="Terragon Labs" \
      org.label-schema.version=$VERSION \
      org.label-schema.schema-version="1.0"

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    APP_USER=appuser \
    APP_HOME=/app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean

# Create non-root user
RUN groupadd -r $APP_USER && useradd -r -g $APP_USER $APP_USER

# Set working directory
WORKDIR $APP_HOME

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.11/site-packages/ /usr/local/lib/python3.11/site-packages/
COPY --from=builder /usr/local/bin/ /usr/local/bin/

# Copy application code
COPY --chown=$APP_USER:$APP_USER src/ ./src/
COPY --chown=$APP_USER:$APP_USER pyproject.toml README.md ./

# Create necessary directories
RUN mkdir -p logs data tmp && \
    chown -R $APP_USER:$APP_USER logs data tmp

# Install production dependencies only
RUN pip install --no-cache-dir -e .

# Health check
HEALTHCHECK --interval=30s --timeout=30s --start-period=5s --retries=3 \
    CMD python -c "import agent_orchestrated_etl; print('OK')" || exit 1

# Switch to non-root user
USER $APP_USER

# Expose ports
EXPOSE 8080 8793

# Set default command
ENTRYPOINT ["python", "-m", "agent_orchestrated_etl.cli"]
CMD ["--help"]

# =============================================================================

# Development stage
FROM production as development

# Switch back to root for package installation
USER root

# Install development dependencies
RUN pip install --no-cache-dir -e .[dev]

# Install additional development tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    vim \
    git \
    make \
    && rm -rf /var/lib/apt/lists/*

# Switch back to app user
USER $APP_USER

# Override default command for development
CMD ["bash"]