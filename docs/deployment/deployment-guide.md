# Deployment and Configuration Guide

## Overview

This guide provides comprehensive instructions for deploying the Agent Orchestrated ETL system in various environments, from development to production.

## Prerequisites

### System Requirements

**Minimum Requirements**:
- CPU: 2 cores
- RAM: 4GB
- Storage: 10GB available space
- Python: 3.8 or higher
- Network: Outbound HTTPS access

**Recommended for Production**:
- CPU: 8 cores
- RAM: 16GB
- Storage: 100GB SSD
- Network: High-bandwidth, low-latency connections
- Load Balancer: For multi-instance deployments

### Software Dependencies

```bash
# Python Dependencies (automatically installed)
python >= 3.8
pip >= 21.0

# Optional System Dependencies
docker >= 20.10        # For containerized deployments
kubernetes >= 1.20     # For K8s deployments
postgresql >= 12       # For production database
redis >= 6.0          # For caching (optional)
```

## Installation Methods

### 1. Development Installation

**Local Development Setup**:

```bash
# Clone repository
git clone https://github.com/your-org/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\\Scripts\\activate

# Install with development dependencies
pip install -e ".[dev]"

# Install vector search dependencies (optional)
pip install -e ".[vector]"

# Verify installation
python -m pytest tests/ -v
```

**Development Configuration**:

```bash
# Set environment variables
export AGENT_ETL_LOG_LEVEL=DEBUG
export AGENT_ETL_MAX_WORKERS=2
export DATABASE_URL=sqlite:///./dev.db

# Run system locally
python -m agent_orchestrated_etl.cli --help
```

### 2. Production Installation

**System Package Installation**:

```bash
# Install as system package
pip install agent-orchestrated-etl

# Create dedicated user
sudo useradd -r -s /bin/false agent-etl
sudo mkdir -p /opt/agent-etl
sudo chown agent-etl:agent-etl /opt/agent-etl
```

**Configuration Directory Structure**:

```
/opt/agent-etl/
├── config/
│   ├── agent-config.yml
│   ├── database-config.yml
│   └── logging-config.yml
├── logs/
├── data/
└── scripts/
    ├── start-orchestrator.sh
    ├── start-etl-workers.sh
    └── start-monitor.sh
```

### 3. Containerized Deployment

**Docker Compose Setup**:

```yaml
# docker-compose.yml
version: '3.8'

services:
  orchestrator:
    image: agent-etl:latest
    environment:
      - AGENT_TYPE=orchestrator
      - DATABASE_URL=postgresql://user:pass@db:5432/agent_etl
      - REDIS_URL=redis://cache:6379
    depends_on:
      - db
      - cache
    ports:
      - "8000:8000"
    volumes:
      - ./config:/app/config:ro
      - ./logs:/app/logs
    healthcheck:
      test: ["CMD", "python", "-c", "import requests; requests.get('http://localhost:8000/health')"]
      interval: 30s
      timeout: 10s
      retries: 3

  etl-worker:
    image: agent-etl:latest
    environment:
      - AGENT_TYPE=etl
      - DATABASE_URL=postgresql://user:pass@db:5432/agent_etl
      - REDIS_URL=redis://cache:6379
    depends_on:
      - db
      - cache
    volumes:
      - ./config:/app/config:ro
      - ./logs:/app/logs
    deploy:
      replicas: 3
    healthcheck:
      test: ["CMD", "python", "-c", "import psutil; exit(0 if psutil.cpu_percent() < 90 else 1)"]
      interval: 30s
      timeout: 10s
      retries: 3

  monitor:
    image: agent-etl:latest
    environment:
      - AGENT_TYPE=monitor
      - DATABASE_URL=postgresql://user:pass@db:5432/agent_etl
      - WEBSOCKET_PORT=8765
    depends_on:
      - db
    ports:
      - "8765:8765"
    volumes:
      - ./config:/app/config:ro
      - ./logs:/app/logs

  db:
    image: postgres:13
    environment:
      - POSTGRES_DB=agent_etl
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./scripts/init-db.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  cache:
    image: redis:6-alpine
    volumes:
      - redis_data:/data
    ports:
      - "6379:6379"

volumes:
  postgres_data:
  redis_data:
```

**Dockerfile**:

```dockerfile
# Dockerfile
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Create app user
RUN useradd -r -s /bin/false appuser

# Set working directory
WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY pyproject.toml ./

# Install application
RUN pip install -e .

# Create necessary directories
RUN mkdir -p /app/logs /app/config /app/data && \
    chown -R appuser:appuser /app

# Switch to app user
USER appuser

# Expose ports
EXPOSE 8000 8765

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8000/health')"

# Entry point
COPY scripts/docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["orchestrator"]
```

## Configuration Management

### 1. Environment Variables

**Core Configuration**:

```bash
# Agent Configuration
AGENT_ETL_LOG_LEVEL=INFO              # DEBUG, INFO, WARNING, ERROR
AGENT_ETL_MAX_WORKERS=4               # Number of worker processes
AGENT_ETL_MEMORY_LIMIT=512MB          # Memory limit per agent
AGENT_ETL_TASK_TIMEOUT=3600           # Task timeout in seconds

# Database Configuration
DATABASE_URL=postgresql://user:pass@host:5432/db
CONNECTION_POOL_SIZE=5                # Database connection pool size
CONNECTION_TIMEOUT=30                 # Connection timeout in seconds
CONNECTION_MAX_OVERFLOW=10            # Max overflow connections

# Cache Configuration (Optional)
REDIS_URL=redis://localhost:6379     # Redis cache URL
CACHE_TTL=3600                        # Cache TTL in seconds
CACHE_ENABLED=true                    # Enable/disable caching

# AWS Configuration
AWS_REGION=us-west-2                  # AWS region
AWS_ACCESS_KEY_ID=your_key            # AWS access key
AWS_SECRET_ACCESS_KEY=your_secret     # AWS secret key
AWS_SECRETS_MANAGER_ENABLED=true     # Enable AWS Secrets Manager

# Monitoring Configuration
MONITORING_ENABLED=true               # Enable monitoring
WEBSOCKET_PORT=8765                   # WebSocket port for real-time updates
METRICS_PORT=9090                     # Prometheus metrics port
HEALTH_CHECK_PORT=8000                # Health check endpoint port

# Security Configuration
JWT_SECRET_KEY=your_jwt_secret        # JWT signing key
ENCRYPTION_KEY=your_encryption_key    # Data encryption key
AGENT_AUTH_ENABLED=true               # Enable agent authentication

# External Integrations
SLACK_WEBHOOK_URL=https://hooks.slack.com/...  # Slack notifications
EMAIL_SMTP_HOST=smtp.gmail.com        # Email SMTP server
EMAIL_SMTP_PORT=587                   # Email SMTP port
```

### 2. Configuration Files

**Main Configuration File** (`config/agent-config.yml`):

```yaml
# Agent Configuration
agents:
  orchestrator:
    max_concurrent_workflows: 10
    workflow_timeout: 7200
    memory_retention_days: 30
    
  etl:
    max_concurrent_tasks: 5
    batch_size: 1000
    retry_attempts: 3
    specializations:
      - database
      - api
      - file
      
  monitor:
    check_interval: 30
    alert_threshold:
      cpu_percent: 80
      memory_percent: 85
      disk_percent: 90
    notification_channels:
      - email
      - slack

# Database Configuration
database:
  connection_pool:
    size: 5
    max_overflow: 10
    timeout: 30
    recycle: 3600
  
  backup:
    enabled: true
    schedule: "0 2 * * *"  # Daily at 2 AM
    retention_days: 7

# Security Configuration
security:
  authentication:
    jwt_expiry: 3600
    refresh_token_expiry: 86400
  
  encryption:
    algorithm: "AES-256-GCM"
    key_rotation_days: 90
  
  audit:
    log_all_operations: true
    log_retention_days: 365

# Performance Configuration
performance:
  circuit_breaker:
    failure_threshold: 5
    recovery_timeout: 60
    
  retry:
    max_attempts: 3
    backoff_strategy: "exponential"
    base_delay: 1.0
    max_delay: 60.0
  
  caching:
    enabled: true
    ttl: 3600
    max_size: "100MB"

# Logging Configuration
logging:
  level: INFO
  format: "json"
  output:
    - console
    - file
  
  file:
    path: "/app/logs"
    max_size: "100MB"
    backup_count: 5
    rotation: "daily"
```

**Database Configuration** (`config/database-config.yml`):

```yaml
# Database Sources Configuration
data_sources:
  primary_db:
    type: "postgresql"
    host: "db.example.com"
    port: 5432
    database: "production"
    pool_size: 10
    ssl_enabled: true
    
  analytics_db:
    type: "mysql"
    host: "analytics.example.com"
    port: 3306
    database: "analytics"
    pool_size: 5
    
  cache_db:
    type: "redis"
    host: "cache.example.com"
    port: 6379
    db: 0
    
# Data Targets Configuration
data_targets:
  warehouse:
    type: "snowflake"
    account: "your_account"
    warehouse: "COMPUTE_WH"
    database: "DW"
    schema: "PUBLIC"
    
  lake:
    type: "s3"
    bucket: "data-lake-bucket"
    region: "us-west-2"
    prefix: "etl-data/"
```

### 3. Secrets Management

**Using AWS Secrets Manager**:

```python
# Example secrets configuration
secrets = {
    "database_credentials": {
        "secret_name": "prod/database/credentials",
        "keys": ["username", "password"]
    },
    "api_keys": {
        "secret_name": "prod/api/keys",
        "keys": ["salesforce_key", "hubspot_key"]
    }
}
```

**Using Environment Files** (Development):

```bash
# .env file
DATABASE_PASSWORD=your_secure_password
API_KEY_SALESFORCE=your_salesforce_key
JWT_SECRET=your_jwt_secret_key
ENCRYPTION_KEY=your_encryption_key
```

## Deployment Strategies

### 1. Blue-Green Deployment

**Strategy Overview**:

```mermaid
graph LR
    LB[Load Balancer]
    
    subgraph "Blue Environment (Current)"
        B1[Orchestrator Blue]
        B2[ETL Workers Blue]
        B3[Monitor Blue]
    end
    
    subgraph "Green Environment (New)"
        G1[Orchestrator Green]
        G2[ETL Workers Green]  
        G3[Monitor Green]
    end
    
    LB --> B1
    LB --> B2
    LB --> B3
    
    style B1 fill:#e3f2fd
    style B2 fill:#e3f2fd
    style B3 fill:#e3f2fd
    style G1 fill:#e8f5e8
    style G2 fill:#e8f5e8
    style G3 fill:#e8f5e8
```

**Implementation Script**:

```bash
#!/bin/bash
# blue-green-deploy.sh

# Deploy to green environment
docker-compose -f docker-compose.green.yml up -d

# Wait for health checks
echo "Waiting for green environment to be healthy..."
./scripts/wait-for-health.sh green

# Switch traffic to green
echo "Switching traffic to green environment..."
./scripts/switch-traffic.sh green

# Verify green environment
./scripts/verify-deployment.sh green

# Scale down blue environment
echo "Scaling down blue environment..."
docker-compose -f docker-compose.blue.yml down

echo "Deployment completed successfully!"
```

### 2. Rolling Deployment

**Kubernetes Rolling Update**:

```yaml
# k8s/deployment.yml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: agent-etl-workers
spec:
  replicas: 6
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
      maxSurge: 1
  selector:
    matchLabels:
      app: agent-etl-worker
  template:
    metadata:
      labels:
        app: agent-etl-worker
    spec:
      containers:
      - name: etl-worker
        image: agent-etl:v1.2.0
        ports:
        - containerPort: 8000
        env:
        - name: AGENT_TYPE
          value: "etl"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: db-credentials
              key: url
        resources:
          requests:
            memory: "512Mi"
            cpu: "250m"
          limits:
            memory: "1Gi"
            cpu: "500m"
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
```

### 3. Canary Deployment

**Traffic Splitting Configuration**:

```yaml
# Istio VirtualService for canary deployment
apiVersion: networking.istio.io/v1alpha3
kind: VirtualService
metadata:
  name: agent-etl-canary
spec:
  http:
  - match:
    - headers:
        canary:
          exact: "true"
    route:
    - destination:
        host: agent-etl-service
        subset: canary
  - route:
    - destination:
        host: agent-etl-service
        subset: stable
      weight: 90
    - destination:
        host: agent-etl-service
        subset: canary
      weight: 10
```

## Monitoring and Health Checks

### 1. Health Check Endpoints

**HTTP Health Check**:

```python
# Health check implementation
from fastapi import FastAPI
from agent_orchestrated_etl.health import HealthChecker

app = FastAPI()
health_checker = HealthChecker()

@app.get("/health")
async def health_check():
    return await health_checker.check_all()

@app.get("/ready")
async def readiness_check():
    return await health_checker.check_readiness()

@app.get("/metrics")
async def metrics():
    return await health_checker.get_metrics()
```

**Health Check Response**:

```json
{
  "status": "healthy",
  "timestamp": "2025-07-25T10:30:00Z",
  "version": "1.0.0",
  "components": {
    "database": {
      "status": "healthy",
      "response_time": "15ms",
      "connections": 5
    },
    "cache": {
      "status": "healthy",
      "memory_usage": "45%"
    },
    "agents": {
      "orchestrator": {
        "status": "healthy",
        "active_workflows": 3
      },
      "etl_workers": {
        "status": "healthy",
        "active_count": 4,
        "average_load": "60%"
      },
      "monitor": {
        "status": "healthy",
        "last_check": "2025-07-25T10:29:45Z"
      }
    }
  }
}
```

### 2. Monitoring Stack

**Prometheus Configuration**:

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'agent-etl'
    static_configs:
      - targets: ['orchestrator:9090', 'etl-worker:9090', 'monitor:9090']
    metrics_path: /metrics
    scrape_interval: 30s
```

**Grafana Dashboard**:

```json
{
  "dashboard": {
    "title": "Agent ETL System Overview",
    "panels": [
      {
        "title": "Active Workflows",
        "type": "graph",
        "targets": [
          {
            "expr": "agent_etl_active_workflows_total"
          }
        ]
      },
      {
        "title": "Task Success Rate",
        "type": "singlestat",
        "targets": [
          {
            "expr": "rate(agent_etl_tasks_completed_total[5m]) / rate(agent_etl_tasks_started_total[5m]) * 100"
          }
        ]
      }
    ]
  }
}
```

## Security Configuration

### 1. SSL/TLS Configuration

**NGINX SSL Termination**:

```nginx
# nginx.conf
server {
    listen 443 ssl http2;
    server_name agent-etl.example.com;
    
    ssl_certificate /etc/ssl/certs/agent-etl.crt;
    ssl_certificate_key /etc/ssl/private/agent-etl.key;
    
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;
    
    location / {
        proxy_pass http://orchestrator:8000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
    
    location /ws {
        proxy_pass http://monitor:8765;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}
```

### 2. Network Security

**Firewall Rules**:

```bash
# UFW firewall configuration
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Allow SSH
sudo ufw allow 22/tcp

# Allow HTTP/HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Allow monitoring
sudo ufw allow from 10.0.0.0/8 to any port 9090

# Allow database access from app servers only
sudo ufw allow from 10.0.1.0/24 to any port 5432

sudo ufw enable
```

## Troubleshooting

### 1. Common Issues

**Database Connection Issues**:

```bash
# Check database connectivity
python -c "
import sqlalchemy
engine = sqlalchemy.create_engine('$DATABASE_URL')
try:
    conn = engine.connect()
    print('Database connection successful')
    conn.close()
except Exception as e:
    print(f'Database connection failed: {e}')
"
```

**Memory Issues**:

```bash
# Monitor memory usage
docker stats --no-stream
htop
free -h

# Check for memory leaks
python -m memory_profiler scripts/check_memory.py
```

**Agent Communication Issues**:

```bash
# Check agent registry
curl http://localhost:8000/agents/status

# Verify message bus
redis-cli monitor  # If using Redis for message bus

# Check agent logs
docker logs agent-etl_orchestrator_1 --tail=100
```

### 2. Performance Tuning

**Database Optimization**:

```sql
-- Monitor slow queries
SELECT query, mean_time, calls 
FROM pg_stat_statements 
WHERE mean_time > 1000 
ORDER BY mean_time DESC;

-- Check connection pool usage
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';
```

**Agent Performance Tuning**:

```python
# Adjust worker pool sizes
AGENT_ETL_MAX_WORKERS=8
CONNECTION_POOL_SIZE=10

# Enable performance monitoring
MONITORING_ENABLED=true
METRICS_COLLECTION=detailed
```

## Backup and Recovery

### 1. Database Backup

**Automated Backup Script**:

```bash
#!/bin/bash
# backup-database.sh

BACKUP_DIR="/opt/backups"
DATE=$(date +%Y%m%d_%H%M%S)
DB_NAME="agent_etl"

# Create backup
pg_dump $DATABASE_URL > "$BACKUP_DIR/agent_etl_$DATE.sql"

# Compress backup
gzip "$BACKUP_DIR/agent_etl_$DATE.sql"

# Remove old backups (keep 7 days)
find $BACKUP_DIR -name "agent_etl_*.sql.gz" -mtime +7 -delete

echo "Backup completed: agent_etl_$DATE.sql.gz"
```

### 2. Configuration Backup

**Configuration Backup**:

```bash
#!/bin/bash
# backup-config.sh

CONFIG_BACKUP="/opt/backups/config"
DATE=$(date +%Y%m%d_%H%M%S)

# Create config backup
tar -czf "$CONFIG_BACKUP/config_$DATE.tar.gz" \
    /opt/agent-etl/config/ \
    /opt/agent-etl/scripts/

echo "Configuration backup completed: config_$DATE.tar.gz"
```

### 3. Disaster Recovery

**Recovery Procedure**:

```bash
#!/bin/bash
# disaster-recovery.sh

# 1. Restore database
gunzip -c /opt/backups/agent_etl_latest.sql.gz | psql $DATABASE_URL

# 2. Restore configuration
tar -xzf /opt/backups/config/config_latest.tar.gz -C /

# 3. Restart services
docker-compose up -d

# 4. Verify system health
./scripts/verify-deployment.sh

echo "Disaster recovery completed"
```

---

*Last Updated: 2025-07-25*  
*Document Version: 1.0*  
*Maintained by: Terragon Labs - Agent Orchestrated ETL Team*