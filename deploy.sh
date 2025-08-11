#!/bin/bash
set -e

echo "🚀 Starting Autonomous SDLC Production Deployment"

# Check prerequisites
if ! command -v docker &> /dev/null; then
    echo "❌ Docker is required but not installed"
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose is required but not installed"
    exit 1
fi

# Load environment variables
if [ -f .env.production ]; then
    export $(cat .env.production | xargs)
else
    echo "⚠️  .env.production file not found"
    echo "Please create .env.production with your configuration"
    exit 1
fi

echo "📦 Building production images..."
docker-compose -f docker-compose.production.yml build

echo "🌍 Starting services..."
docker-compose -f docker-compose.production.yml up -d

echo "⏳ Waiting for services to start..."
sleep 30

echo "✅ Deployment completed!"
echo "🌐 Application: http://localhost:8080"
echo "📊 Grafana: http://localhost:3000"
echo "📈 Prometheus: http://localhost:9090"

echo "🔍 Running health check..."
./health_check.sh
