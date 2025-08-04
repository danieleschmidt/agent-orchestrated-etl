#!/bin/bash

# Agent-Orchestrated-ETL Development Environment Setup
echo "🚀 Setting up Agent-Orchestrated-ETL development environment..."

# Update system packages
sudo apt-get update && sudo apt-get upgrade -y

# Install additional system dependencies
sudo apt-get install -y \
    postgresql-client \
    redis-tools \
    curl \
    wget \
    jq \
    tree \
    htop \
    vim

# Set up Python environment
echo "🐍 Setting up Python environment..."

# Upgrade pip and install development tools
python -m pip install --upgrade pip setuptools wheel

# Install project dependencies
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
fi

if [ -f "requirements-dev.txt" ]; then
    pip install -r requirements-dev.txt
fi

# Install additional development dependencies
pip install \
    black \
    isort \
    flake8 \
    mypy \
    pytest \
    pytest-cov \
    pytest-asyncio \
    pre-commit \
    jupyter \
    ipython

# Set up pre-commit hooks
echo "🔧 Setting up pre-commit hooks..."
if [ -f ".pre-commit-config.yaml" ]; then
    pre-commit install
fi

# Set up Airflow environment
echo "✈️ Setting up Airflow environment..."
export AIRFLOW_HOME=$(pwd)/airflow

# Create airflow directory if it doesn't exist
mkdir -p $AIRFLOW_HOME

# Set up project structure
echo "📁 Setting up project structure..."

# Create necessary directories
mkdir -p \
    data/raw \
    data/processed \
    data/staging \
    logs \
    output \
    tmp \
    .pytest_cache

# Set up environment variables
echo "🌍 Setting up environment variables..."
if [ ! -f ".env" ]; then
    cp .env.example .env 2>/dev/null || echo "# Development environment variables" > .env
    echo "✅ Created .env file"
fi

# Set up Git hooks and configuration
echo "🔄 Setting up Git configuration..."
git config --local core.autocrlf false
git config --local pull.rebase true

# Set up VS Code workspace settings
echo "⚙️ Setting up VS Code workspace..."
mkdir -p .vscode

# Create development scripts
echo "📝 Creating development scripts..."
mkdir -p scripts/dev

# Create run script
cat > scripts/dev/run-api.sh << 'EOF'
#!/bin/bash
echo "Starting Agent-Orchestrated-ETL API server..."
export PYTHONPATH="$(pwd)/src:$(pwd)"
export ENVIRONMENT="development"
python src/agent_orchestrated_etl/api/app.py
EOF

cat > scripts/dev/run-tests.sh << 'EOF'
#!/bin/bash
echo "Running Agent-Orchestrated-ETL tests..."
export PYTHONPATH="$(pwd)/src:$(pwd)"
pytest tests/ -v --cov=src/agent_orchestrated_etl --cov-report=html --cov-report=term
EOF

cat > scripts/dev/lint.sh << 'EOF'
#!/bin/bash
echo "Running code quality checks..."
black --check --diff src/ tests/
isort --check-only --diff src/ tests/
flake8 src/ tests/
mypy src/ --ignore-missing-imports
EOF

cat > scripts/dev/format.sh << 'EOF'
#!/bin/bash
echo "Formatting code..."
black src/ tests/
isort src/ tests/
EOF

# Make scripts executable
chmod +x scripts/dev/*.sh

# Install additional Python packages for development
echo "📚 Installing additional development packages..."
pip install \
    python-dotenv \
    requests \
    aiohttp \
    pandas \
    numpy \
    sqlalchemy \
    psycopg2-binary \
    redis \
    boto3 \
    pydantic

# Create sample data
mkdir -p data/raw
if [ ! -f "data/raw/sample.csv" ]; then
    cat > data/raw/sample.csv << 'EOF'
id,name,email,age,department
1,Alice Johnson,alice@example.com,28,Engineering
2,Bob Smith,bob@example.com,35,Marketing
3,Carol Davis,carol@example.com,42,Sales
4,David Wilson,david@example.com,31,Engineering
5,Eve Brown,eve@example.com,26,Marketing
EOF
fi

echo "✅ Development environment setup complete!"
echo ""
echo "🚀 Ready to develop! Available services:"
echo "   - API Server: Use 'scripts/dev/run-api.sh' to start"
echo "   - Tests: Use 'scripts/dev/run-tests.sh' to run tests"
echo ""
echo "💡 Pro tips:"
echo "   - Use Ctrl+Shift+P -> 'Python: Select Interpreter' to choose Python"
echo "   - Install recommended VS Code extensions for best experience"
