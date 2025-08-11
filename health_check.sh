#!/bin/bash
set -e

echo "🔍 Autonomous SDLC Health Check"

# Check services
echo "📦 Checking service status..."
docker-compose -f docker-compose.production.yml ps

# Test application health
echo "🏥 Testing application health..."
if curl -f http://localhost:8080/health 2>/dev/null; then
    echo "✅ Application is healthy"
else
    echo "❌ Application health check failed"
fi

# Test database
echo "🗄️  Testing database..."
if docker-compose -f docker-compose.production.yml exec -T postgres pg_isready -U ${POSTGRES_USER:-sdlc_admin} 2>/dev/null; then
    echo "✅ Database is healthy"
else
    echo "❌ Database connection failed"
fi

# Test Redis
echo "💾 Testing Redis..."
if docker-compose -f docker-compose.production.yml exec -T redis redis-cli ping 2>/dev/null | grep -q PONG; then
    echo "✅ Redis is healthy"
else
    echo "❌ Redis connection failed"
fi

echo "✅ Health check completed"
