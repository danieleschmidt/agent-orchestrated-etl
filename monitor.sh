#!/bin/bash

echo "📊 Autonomous SDLC Monitoring Dashboard"
echo "=================================="

echo "📈 Container Statistics:"
docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}"

echo "\n🌐 Service Endpoints:"
echo "• Application:  http://localhost:8080"
echo "• Grafana:      http://localhost:3000"
echo "• Prometheus:   http://localhost:9090"
echo "• Health:       http://localhost:8080/health"

echo "\n📋 Quick Commands:"
echo "• View logs:    docker-compose -f docker-compose.production.yml logs -f"
echo "• Restart:      docker-compose -f docker-compose.production.yml restart"
echo "• Stop:         docker-compose -f docker-compose.production.yml down"
