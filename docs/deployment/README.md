# Agent Orchestrated ETL - Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the Agent Orchestrated ETL system in various environments, from development to production. The system supports multiple deployment patterns including single-node, distributed, and cloud-native deployments.

## Prerequisites

### System Requirements

#### Minimum Requirements
- **CPU**: 4 cores, 2.0 GHz
- **Memory**: 8 GB RAM
- **Storage**: 20 GB available disk space
- **Network**: 1 Gbps network interface
- **OS**: Linux (Ubuntu 20.04+), macOS 10.15+, Windows 10+

#### Recommended Requirements
- **CPU**: 8+ cores, 3.0 GHz
- **Memory**: 16+ GB RAM
- **Storage**: 100+ GB SSD storage
- **Network**: 10 Gbps network interface
- **OS**: Linux (Ubuntu 22.04 LTS recommended)

### Software Dependencies

#### Core Dependencies
- **Python**: 3.8+ (3.11 recommended)
- **Docker**: 20.10+ (for containerized deployment)
- **Git**: 2.30+ (for source code management)

#### Database Requirements
- **PostgreSQL**: 13+ (primary database support)
- **MySQL**: 8.0+ (optional, for MySQL sources)
- **SQLite**: 3.36+ (development and testing)

#### Optional Dependencies
- **Kubernetes**: 1.24+ (for cloud-native deployment)
- **Redis**: 6.2+ (for distributed caching)
- **Kafka**: 2.8+ (for stream processing)

## Environment Configuration

### Environment Variables

Create a `.env` file in the project root with the following configuration:

```bash
# Environment Configuration
ENVIRONMENT=production  # development, staging, production
DEBUG=false
LOG_LEVEL=INFO

# Database Configuration
DATABASE_URL=postgresql://user:password@localhost:5432/agent_etl
DATABASE_POOL_SIZE=20
DATABASE_MAX_OVERFLOW=30
DATABASE_POOL_TIMEOUT=30

# Vector Store Configuration  
VECTOR_STORE_TYPE=chromadb
CHROMADB_PERSIST_DIRECTORY=./data/chromadb
CHROMADB_HOST=localhost
CHROMADB_PORT=8000

# Agent Configuration
MAX_CONCURRENT_AGENTS=10
AGENT_TASK_TIMEOUT=300
AGENT_MEMORY_LIMIT_MB=1024

# Security Configuration
SECRET_KEY=your-secret-key-here-change-in-production
ENCRYPTION_KEY=your-encryption-key-32-chars-long
JWT_SECRET=your-jwt-secret-key
AUDIT_ENCRYPTION_ENABLED=true

# Performance Configuration
CACHE_TTL_SECONDS=3600
QUERY_CACHE_SIZE=1000
CONNECTION_POOL_SIZE=50

# Monitoring Configuration
PROMETHEUS_ENABLED=true
PROMETHEUS_PORT=9090
GRAFANA_ENABLED=true
GRAFANA_PORT=3000

# External Services
AWS_ACCESS_KEY_ID=your-aws-access-key
AWS_SECRET_ACCESS_KEY=your-aws-secret-key
AWS_DEFAULT_REGION=us-east-1

# Email Configuration (for notifications)
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=notifications@yourcompany.com
SMTP_PASSWORD=your-smtp-password
SMTP_TLS=true
```

### Configuration Files

#### Application Configuration (`config/application.yml`)
```yaml
application:
  name: "Agent Orchestrated ETL"
  version: "1.0.0"
  
agents:
  max_concurrent: 10
  default_timeout: 300
  memory_limit_mb: 1024
  
coordination:
  selection_algorithm: "enhanced"
  load_balancing: true
  audit_trail: true
  fallback_enabled: true
  
performance:
  enable_caching: true
  cache_ttl: 3600
  query_timeout: 30
  connection_pool_size: 50
  
security:
  audit_all_operations: true
  encrypt_sensitive_data: true
  require_authentication: true
  session_timeout: 3600
  
monitoring:
  enable_metrics: true
  enable_tracing: true
  log_level: "INFO"
  health_check_interval: 30
```

## Deployment Options

### Option 1: Local Development Deployment

#### Quick Start
```bash
# Clone repository
git clone https://github.com/yourorg/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your configuration

# Initialize database
python scripts/init_database.py

# Start the system
python -m src.agent_orchestrated_etl.main
```

#### Development Configuration
```bash
# Development-specific environment variables
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG
DATABASE_URL=sqlite:///./data/dev.db
VECTOR_STORE_TYPE=chromadb
```

### Option 2: Docker Deployment

#### Single Container Deployment
```dockerfile
# Dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY config/ ./config/
COPY scripts/ ./scripts/

# Create data directories
RUN mkdir -p /app/data/chromadb /app/logs

# Expose ports
EXPOSE 8000 9090

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/health || exit 1

# Start application
CMD ["python", "-m", "src.agent_orchestrated_etl.main"]
```

#### Docker Compose Deployment
```yaml
# docker-compose.yml
version: '3.8'

services:
  agent-etl:
    build: .
    container_name: agent-etl-app
    ports:
      - "8000:8000"
      - "9090:9090"
    environment:
      - ENVIRONMENT=production
      - DATABASE_URL=postgresql://postgres:password@postgres:5432/agent_etl
      - CHROMADB_HOST=chromadb
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    depends_on:
      - postgres
      - chromadb
      - redis
    restart: unless-stopped
    
  postgres:
    image: postgres:15
    container_name: agent-etl-postgres
    environment:
      - POSTGRES_DB=agent_etl
      - POSTGRES_USER=postgres
      - POSTGRES_PASSWORD=password
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init_db.sql:/docker-entrypoint-initdb.d/init_db.sql
    ports:
      - "5432:5432"
    restart: unless-stopped
    
  chromadb:
    image: chromadb/chroma:latest
    container_name: agent-etl-chromadb
    ports:
      - "8001:8000"
    volumes:
      - chromadb_data:/chroma/chroma
    environment:
      - CHROMA_SERVER_AUTHN_CREDENTIALS_FILE=/chroma/auth.txt
    restart: unless-stopped
    
  redis:
    image: redis:7-alpine
    container_name: agent-etl-redis
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    restart: unless-stopped
    
  prometheus:
    image: prom/prometheus:latest
    container_name: agent-etl-prometheus
    ports:
      - "9091:9090"
    volumes:
      - ./config/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.console.libraries=/etc/prometheus/console_libraries'
      - '--web.console.templates=/etc/prometheus/consoles'
    restart: unless-stopped
    
  grafana:
    image: grafana/grafana:latest
    container_name: agent-etl-grafana
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
      - ./config/grafana:/etc/grafana/provisioning
    restart: unless-stopped

volumes:
  postgres_data:
  chromadb_data:
  redis_data:
  prometheus_data:
  grafana_data:
```

#### Deploy with Docker Compose
```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f agent-etl

# Scale agents (if configured for horizontal scaling)
docker-compose up -d --scale agent-etl=3

# Stop services
docker-compose down

# Clean up (removes volumes)
docker-compose down -v
```

### Option 3: Kubernetes Deployment

#### Namespace and Configuration
```yaml
# k8s/namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: agent-etl
  
---
# k8s/configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: agent-etl-config
  namespace: agent-etl
data:
  application.yml: |
    application:
      name: "Agent Orchestrated ETL"
      version: "1.0.0"
    agents:
      max_concurrent: 10
      default_timeout: 300
    # ... rest of configuration
```

#### Secrets Management
```yaml
# k8s/secrets.yaml
apiVersion: v1
kind: Secret
metadata:
  name: agent-etl-secrets
  namespace: agent-etl
type: Opaque
data:
  database-url: <base64-encoded-database-url>
  secret-key: <base64-encoded-secret-key>
  encryption-key: <base64-encoded-encryption-key>
  aws-access-key: <base64-encoded-aws-key>
  aws-secret-key: <base64-encoded-aws-secret>
```

#### Application Deployment
```yaml
# k8s/deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-etl-app
  namespace: agent-etl
spec:
  replicas: 3
  selector:
    matchLabels:
      app: agent-etl
  template:
    metadata:
      labels:
        app: agent-etl
    spec:
      containers:
      - name: agent-etl
        image: agent-etl:latest
        ports:
        - containerPort: 8000
        - containerPort: 9090
        env:
        - name: ENVIRONMENT
          value: "production"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: agent-etl-secrets
              key: database-url
        volumeMounts:
        - name: config-volume
          mountPath: /app/config
        - name: data-volume
          mountPath: /app/data
        resources:
          requests:
            memory: "2Gi"
            cpu: "1000m"
          limits:
            memory: "4Gi"
            cpu: "2000m"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: config-volume
        configMap:
          name: agent-etl-config
      - name: data-volume
        persistentVolumeClaim:
          claimName: agent-etl-pvc
```

#### Service and Ingress
```yaml
# k8s/service.yaml
apiVersion: v1
kind: Service
metadata:
  name: agent-etl-service
  namespace: agent-etl
spec:
  selector:
    app: agent-etl
  ports:
  - name: http
    port: 80
    targetPort: 8000
  - name: metrics
    port: 9090
    targetPort: 9090
  type: ClusterIP
  
---
# k8s/ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: agent-etl-ingress
  namespace: agent-etl
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
    cert-manager.io/cluster-issuer: "letsencrypt-prod"
spec:
  tls:
  - hosts:
    - agent-etl.yourcompany.com
    secretName: agent-etl-tls
  rules:
  - host: agent-etl.yourcompany.com
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: agent-etl-service
            port:
              number: 80
```

#### Deploy to Kubernetes
```bash
# Apply all configurations
kubectl apply -f k8s/

# Check deployment status
kubectl get pods -n agent-etl
kubectl get services -n agent-etl

# View logs
kubectl logs -f deployment/agent-etl-app -n agent-etl

# Scale deployment
kubectl scale deployment agent-etl-app --replicas=5 -n agent-etl

# Update deployment
kubectl set image deployment/agent-etl-app agent-etl=agent-etl:v1.1.0 -n agent-etl
```

## Production Deployment Checklist

### Security Hardening
- [ ] Change all default passwords and keys
- [ ] Enable TLS/SSL for all external communications
- [ ] Configure firewall rules (only necessary ports open)
- [ ] Enable audit logging for all operations
- [ ] Set up proper backup and disaster recovery
- [ ] Configure secure credential management
- [ ] Enable data encryption at rest and in transit
- [ ] Set up intrusion detection and monitoring

### Performance Optimization
- [ ] Configure appropriate resource limits and requests
- [ ] Set up connection pooling for databases
- [ ] Enable caching for frequently accessed data
- [ ] Configure auto-scaling policies
- [ ] Optimize database indexes and queries
- [ ] Set up CDN for static assets (if applicable)
- [ ] Configure load balancing and health checks

### Monitoring and Observability
- [ ] Set up Prometheus metrics collection
- [ ] Configure Grafana dashboards
- [ ] Set up alerting rules for critical metrics
- [ ] Configure log aggregation and analysis
- [ ] Set up distributed tracing
- [ ] Configure health checks and readiness probes
- [ ] Set up performance monitoring and profiling

### Backup and Recovery
- [ ] Configure automated database backups
- [ ] Set up vector store data backup
- [ ] Configure configuration backup and versioning
- [ ] Test disaster recovery procedures
- [ ] Document recovery procedures and RTO/RPO
- [ ] Set up cross-region backup replication

### Maintenance and Updates
- [ ] Set up automated security updates
- [ ] Configure rolling updates for zero-downtime deployment
- [ ] Set up dependency vulnerability scanning
- [ ] Configure automated testing pipeline
- [ ] Set up deployment rollback procedures
- [ ] Document maintenance procedures

## Troubleshooting

### Common Issues

#### Database Connection Issues
```bash
# Check database connectivity
python scripts/check_database.py

# Test database performance
python scripts/benchmark_database.py

# Initialize/migrate database
python scripts/migrate_database.py
```

#### Vector Store Issues
```bash
# Check ChromaDB connectivity
curl http://localhost:8001/api/v1/heartbeat

# Reset vector store
python scripts/reset_vector_store.py

# Rebuild embeddings
python scripts/rebuild_embeddings.py
```

#### Performance Issues
```bash
# Check system resources
docker stats  # For Docker deployment
kubectl top pods -n agent-etl  # For Kubernetes

# Analyze slow queries
python scripts/analyze_performance.py

# Generate performance report
python scripts/performance_report.py
```

### Log Analysis
```bash
# View application logs
tail -f logs/application.log

# Search for errors
grep "ERROR" logs/application.log | tail -50

# Analyze performance metrics
grep "PERF" logs/application.log | awk '{print $3, $4}'
```

## Scaling Considerations

### Horizontal Scaling
- Deploy multiple agent instances behind a load balancer
- Use external databases (PostgreSQL, Redis) for shared state
- Configure session affinity for stateful operations
- Implement distributed caching for improved performance

### Vertical Scaling
- Increase CPU and memory allocation per instance
- Optimize database connection pool settings
- Tune JVM/Python runtime parameters
- Configure appropriate garbage collection settings

### Database Scaling
- Set up read replicas for query optimization
- Implement database sharding for large datasets
- Configure connection pooling and optimization
- Consider database clustering for high availability

---

*For environment-specific deployment guides and advanced configuration, see the Operations Manual.*