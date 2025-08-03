#\!/bin/bash
set -e
echo "🚀 Setting up Agent-Orchestrated-ETL development environment..."

# Update system packages
sudo apt-get update && sudo apt-get upgrade -y

# Install essential development tools
sudo apt-get install -y build-essential curl wget unzip tree jq httpie postgresql-client redis-tools git-lfs

echo "🐍 Setting up Python environment..."
pip install --upgrade pip setuptools wheel
pip install -e ".[dev]"

# Git configuration
git lfs install
pre-commit install || echo "Pre-commit hooks will be installed when available"

# Copy environment file if it doesn't exist
if [ \! -f .env ]; then
    echo "📋 Creating .env file from template..."
    cp .env.example .env
    echo "⚠️  Please update .env with your configuration values"
fi

echo "✅ Development environment setup complete\!"
echo "🚀 Happy coding\!"
EOF < /dev/null
