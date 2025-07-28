FROM mcr.microsoft.com/devcontainers/python:3.11

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    vim \
    postgresql-client \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Install Python development tools
RUN pip install --upgrade pip setuptools wheel

# Set working directory
WORKDIR /workspace

# Copy requirements and install dependencies
COPY requirements.txt pyproject.toml ./
RUN pip install -e .[dev]

# Install additional development tools
RUN pip install \
    pre-commit \
    detect-secrets \
    safety \
    bandit[toml] \
    semgrep

# Set up git config for containers
RUN git config --global --add safe.directory /workspace

# Create non-root user (will be handled by devcontainer features)
USER vscode
EOF < /dev/null
