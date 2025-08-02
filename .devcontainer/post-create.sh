#!/bin/bash

set -e

echo "🚀 Setting up Agent-Orchestrated-ETL development environment..."

# Install Python dependencies
echo "📦 Installing Python dependencies..."
pip install -e .[dev]

# Install pre-commit hooks
echo "🔧 Installing pre-commit hooks..."
pre-commit install

# Initialize Airflow database
echo "🗄️ Initializing Airflow database..."
export AIRFLOW_HOME=/workspace/airflow
airflow db init

# Create default Airflow user
echo "👤 Creating default Airflow user..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# Set up development environment
echo "🔧 Setting up development environment..."
cp .env.example .env 2>/dev/null || true

# Install additional development tools
echo "🛠️ Installing additional development tools..."
npm install -g @commitlint/cli @commitlint/config-conventional

# Create development directories
echo "📁 Creating development directories..."
mkdir -p logs temp data/input data/output

# Set proper permissions
echo "🔐 Setting permissions..."
chmod +x scripts/*.py 2>/dev/null || true
chmod +x .devcontainer/*.sh

# Initialize git hooks
echo "🎣 Setting up git hooks..."
pre-commit install --hook-type commit-msg

# Run initial tests to verify setup
echo "🧪 Running initial tests..."
python -m pytest tests/ --tb=short -q --disable-warnings || echo "⚠️ Some tests failed - this is normal for a new setup"

# Display useful information
echo ""
echo "✅ Development environment setup complete!"
echo ""
echo "🔗 Useful URLs:"
echo "   Airflow UI: http://localhost:8080 (admin/admin)"
echo "   API Docs: http://localhost:5000/docs"
echo "   Jupyter: http://localhost:8888"
echo ""
echo "📋 Quick commands:"
echo "   npm run test        - Run tests"
echo "   npm run lint        - Lint code"
echo "   npm run dev         - Start development server"
echo "   npm run docker:up   - Start all services"
echo ""
echo "🎉 Happy coding!"