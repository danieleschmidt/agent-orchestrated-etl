# Production Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying Agent-Orchestrated-ETL in a production environment.

## Prerequisites

### Infrastructure Requirements

#### Compute Resources
- **Minimum**: 4 vCPUs, 8GB RAM, 100GB SSD
- **Recommended**: 8 vCPUs, 16GB RAM, 500GB SSD
- **High Load**: 16 vCPUs, 32GB RAM, 1TB SSD

#### Network Requirements
- Internet connectivity for external data sources
- Internal network access to databases and services
- Load balancer for high availability
- CDN for static content (optional)

#### Database Requirements
- PostgreSQL 13+ (recommended 15+)
- Minimum 4 vCPUs, 8GB RAM for database server
- 100GB+ storage with automatic backup
- Connection pooling (PgBouncer recommended)

#### Cache Requirements
- Redis 6+ (recommended 7+)
- Minimum 2GB RAM
- Persistence enabled for durability

### Software Dependencies

#### Container Runtime
- Docker 20.10+ or containerd
- Kubernetes 1.24+ (for container orchestration)
- Docker Compose 2.0+ (for single-node deployment)

#### Monitoring Stack
- Prometheus for metrics collection
- Grafana for dashboards and visualization
- Alertmanager for alert routing
- Node Exporter for system metrics

#### Security Tools
- Trivy for container scanning
- Falco for runtime security (optional)
- OPA Gatekeeper for policy enforcement (optional)

## Deployment Options

### Option 1: Docker Compose (Single Node)

Suitable for small to medium workloads with basic high availability requirements.

#### Setup Steps

1. **Prepare Environment**
   ```bash
   # Create deployment directory
   mkdir -p /opt/agent-etl
   cd /opt/agent-etl
   
   # Clone repository
   git clone https://github.com/your-org/agent-orchestrated-etl.git .
   git checkout v0.0.1  # Use specific version
   ```

2. **Configure Environment**
   ```bash
   # Copy environment template
   cp .env.example .env
   
   # Edit configuration
   nano .env
   ```

   **Critical Configuration Items:**
   ```bash
   # Application
   ENVIRONMENT=production
   DEBUG=false
   LOG_LEVEL=INFO
   
   # Database (use external PostgreSQL)
   DATABASE_URL=postgresql://username:password@db.example.com:5432/agent_etl
   
   # Redis (use external Redis)
   REDIS_URL=redis://redis.example.com:6379/0
   
   # Security
   JWT_SECRET_KEY=your-super-secure-random-key-here
   API_KEY=your-api-key-here
   
   # AWS (for S3 and Secrets Manager)
   AWS_ACCESS_KEY_ID=your-access-key
   AWS_SECRET_ACCESS_KEY=your-secret-key
   AWS_DEFAULT_REGION=us-east-1
   
   # OpenAI/LangChain
   OPENAI_API_KEY=your-openai-key
   ```

3. **Production Docker Compose**
   
   Create `docker-compose.prod.yml`:
   ```yaml
   version: '3.8'
   
   services:
     app:
       image: agent-orchestrated-etl:v0.0.1
       restart: unless-stopped
       env_file: .env
       ports:
         - "8793:8793"
       volumes:
         - ./logs:/app/logs
         - ./data:/app/data
       depends_on:
         - postgres
         - redis
       healthcheck:
         test: ["CMD", "curl", "-f", "http://localhost:8793/health"]
         interval: 30s
         timeout: 10s
         retries: 3
   
     nginx:
       image: nginx:alpine
       restart: unless-stopped
       ports:
         - "80:80"
         - "443:443"
       volumes:
         - ./nginx.conf:/etc/nginx/nginx.conf
         - ./ssl:/etc/nginx/ssl
       depends_on:
         - app
   
     postgres:
       image: postgres:15-alpine
       restart: unless-stopped
       env_file: .env
       volumes:
         - postgres_data:/var/lib/postgresql/data
         - ./backups:/backups
       ports:
         - "5432:5432"
   
     redis:
       image: redis:7-alpine
       restart: unless-stopped
       volumes:
         - redis_data:/data
       ports:
         - "6379:6379"
       command: redis-server --appendonly yes
   
     prometheus:
       image: prom/prometheus:latest
       restart: unless-stopped
       volumes:
         - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
         - prometheus_data:/prometheus
       ports:
         - "9090:9090"
   
     grafana:
       image: grafana/grafana:latest
       restart: unless-stopped
       environment:
         GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_PASSWORD}
       volumes:
         - grafana_data:/var/lib/grafana
         - ./monitoring/grafana:/etc/grafana/provisioning
       ports:
         - "3000:3000"
   
   volumes:
     postgres_data:
     redis_data:
     prometheus_data:
     grafana_data:
   ```

4. **Deploy Application**
   ```bash
   # Build production image
   docker build -t agent-orchestrated-etl:v0.0.1 --target production .
   
   # Start services
   docker-compose -f docker-compose.prod.yml up -d
   
   # Verify deployment
   docker-compose -f docker-compose.prod.yml ps
   curl http://localhost:8793/health
   ```

### Option 2: Kubernetes (Recommended for Production)

Suitable for large-scale deployments requiring high availability, scalability, and advanced orchestration.

#### Setup Steps

1. **Prepare Kubernetes Manifests**

   Create `k8s/namespace.yaml`:
   ```yaml
   apiVersion: v1
   kind: Namespace
   metadata:
     name: agent-etl
     labels:
       name: agent-etl
   ```

   Create `k8s/configmap.yaml`:
   ```yaml
   apiVersion: v1
   kind: ConfigMap
   metadata:
     name: agent-etl-config
     namespace: agent-etl
   data:
     ENVIRONMENT: "production"
     DEBUG: "false"
     LOG_LEVEL: "INFO"
     PROMETHEUS_ENABLED: "true"
   ```

   Create `k8s/secret.yaml`:
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: agent-etl-secrets
     namespace: agent-etl
   type: Opaque
   data:
     jwt-secret-key: <base64-encoded-secret>
     api-key: <base64-encoded-api-key>
     database-url: <base64-encoded-database-url>
     redis-url: <base64-encoded-redis-url>
     openai-api-key: <base64-encoded-openai-key>
   ```

   Create `k8s/deployment.yaml`:
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: agent-etl-app
     namespace: agent-etl
     labels:
       app: agent-etl
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
           image: agent-orchestrated-etl:v0.0.1
           ports:
           - containerPort: 8793
           envFrom:
           - configMapRef:
               name: agent-etl-config
           - secretRef:
               name: agent-etl-secrets
           livenessProbe:
             httpGet:
               path: /health
               port: 8793
             initialDelaySeconds: 30
             periodSeconds: 10
           readinessProbe:
             httpGet:
               path: /ready
               port: 8793
             initialDelaySeconds: 5
             periodSeconds: 5
           resources:
             requests:
               memory: "512Mi"
               cpu: "250m"
             limits:
               memory: "2Gi"
               cpu: "1000m"
   ```

   Create `k8s/service.yaml`:
   ```yaml
   apiVersion: v1
   kind: Service
   metadata:
     name: agent-etl-service
     namespace: agent-etl
   spec:
     selector:
       app: agent-etl
     ports:
     - protocol: TCP
       port: 80
       targetPort: 8793
     type: ClusterIP
   ```

   Create `k8s/ingress.yaml`:
   ```yaml
   apiVersion: networking.k8s.io/v1
   kind: Ingress
   metadata:
     name: agent-etl-ingress
     namespace: agent-etl
     annotations:
       kubernetes.io/ingress.class: nginx
       cert-manager.io/cluster-issuer: letsencrypt-prod
   spec:
     tls:
     - hosts:
       - api.agent-etl.company.com
       secretName: agent-etl-tls
     rules:
     - host: api.agent-etl.company.com
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

2. **Deploy to Kubernetes**
   ```bash
   # Apply manifests
   kubectl apply -f k8s/namespace.yaml
   kubectl apply -f k8s/configmap.yaml
   kubectl apply -f k8s/secret.yaml
   kubectl apply -f k8s/deployment.yaml
   kubectl apply -f k8s/service.yaml
   kubectl apply -f k8s/ingress.yaml
   
   # Verify deployment
   kubectl get pods -n agent-etl
   kubectl get services -n agent-etl
   kubectl get ingress -n agent-etl
   ```

## Configuration

### Environment Variables

#### Required Variables
- `DATABASE_URL`: PostgreSQL connection string
- `REDIS_URL`: Redis connection string
- `JWT_SECRET_KEY`: Secret key for JWT tokens
- `OPENAI_API_KEY`: OpenAI API key for LangChain

#### Optional Variables
- `LOG_LEVEL`: Logging level (DEBUG, INFO, WARNING, ERROR)
- `PROMETHEUS_ENABLED`: Enable Prometheus metrics
- `SENTRY_DSN`: Sentry error tracking DSN
- `AWS_ACCESS_KEY_ID`: AWS credentials for S3 access
- `SLACK_WEBHOOK_URL`: Slack notifications webhook

### Database Configuration

#### PostgreSQL Setup
```sql
-- Create database and user
CREATE DATABASE agent_etl;
CREATE USER agent_etl_user WITH PASSWORD 'secure_password';
GRANT ALL PRIVILEGES ON DATABASE agent_etl TO agent_etl_user;

-- Create extensions
\c agent_etl
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
```

#### Connection Pooling (PgBouncer)
```ini
[databases]
agent_etl = host=localhost port=5432 dbname=agent_etl

[pgbouncer]
listen_port = 6432
listen_addr = *
auth_type = md5
auth_file = /etc/pgbouncer/userlist.txt
admin_users = postgres
pool_mode = transaction
max_client_conn = 1000
default_pool_size = 25
```

### Redis Configuration

```conf
# redis.conf
maxmemory 2gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec
```

## Security Hardening

### SSL/TLS Configuration

#### Nginx SSL Configuration
```nginx
server {
    listen 443 ssl http2;
    server_name api.agent-etl.company.com;
    
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-CHACHA20-POLY1305;
    ssl_prefer_server_ciphers off;
    
    location / {
        proxy_pass http://app:8793;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
```

### Firewall Configuration

#### UFW (Ubuntu)
```bash
# Allow SSH
ufw allow 22/tcp

# Allow HTTP/HTTPS
ufw allow 80/tcp
ufw allow 443/tcp

# Allow monitoring (restrict to monitoring network)
ufw allow from 10.0.1.0/24 to any port 9090
ufw allow from 10.0.1.0/24 to any port 3000

# Enable firewall
ufw enable
```

### Container Security

#### Docker Security Options
```yaml
services:
  app:
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
    cap_add:
      - CHOWN
      - SETGID
      - SETUID
    read_only: true
    tmpfs:
      - /tmp
      - /var/cache
```

## Monitoring and Alerting

### Prometheus Configuration

Key metrics to monitor:
- Application response time and throughput
- Pipeline execution success/failure rates
- Database connection pool status
- Memory and CPU utilization
- Error rates and types

### Grafana Dashboards

Create dashboards for:
- Application Overview
- Pipeline Performance
- System Resources
- Security Events
- Business Metrics

### Alert Rules

Critical alerts:
- Application down (> 1 minute)
- High error rate (> 5% for 5 minutes)
- Database unavailable
- High memory usage (> 90%)
- Pipeline failures

## Backup and Recovery

### Database Backup

#### Automated Backup Script
```bash
#!/bin/bash
# backup-database.sh

BACKUP_DIR="/backups/postgres"
DATE=$(date +%Y%m%d_%H%M%S)
DB_NAME="agent_etl"

# Create backup
pg_dump -h localhost -U agent_etl_user -d $DB_NAME | gzip > $BACKUP_DIR/backup_$DATE.sql.gz

# Clean old backups (keep last 30 days)
find $BACKUP_DIR -name "backup_*.sql.gz" -mtime +30 -delete

# Upload to S3 (optional)
aws s3 cp $BACKUP_DIR/backup_$DATE.sql.gz s3://your-backup-bucket/postgres/
```

### Application State Backup

Backup configuration and logs:
```bash
#!/bin/bash
# backup-app-state.sh

tar -czf /backups/app/app-state-$(date +%Y%m%d).tar.gz \
  /opt/agent-etl/.env \
  /opt/agent-etl/logs \
  /opt/agent-etl/data
```

### Disaster Recovery

#### Recovery Procedures

1. **Database Recovery**
   ```bash
   # Restore from backup
   gunzip -c backup_20250728_120000.sql.gz | psql -h localhost -U agent_etl_user -d agent_etl
   ```

2. **Application Recovery**
   ```bash
   # Restore configuration and data
   tar -xzf app-state-20250728.tar.gz -C /opt/agent-etl/
   
   # Restart services
   docker-compose -f docker-compose.prod.yml restart
   ```

3. **Full Environment Recreation**
   ```bash
   # Clone repository
   git clone https://github.com/your-org/agent-orchestrated-etl.git
   cd agent-orchestrated-etl
   
   # Restore configuration
   cp /backups/app/.env .
   
   # Deploy
   docker-compose -f docker-compose.prod.yml up -d
   ```

## Performance Tuning

### Application Optimization

#### Python/Django Settings
```python
# Production settings
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'CONN_MAX_AGE': 600,
        'CONN_HEALTH_CHECKS': True,
        'OPTIONS': {
            'MAX_CONNS': 20,
            'MIN_CONNS': 5,
        }
    }
}

# Cache configuration
CACHES = {
    'default': {
        'BACKEND': 'django_redis.cache.RedisCache',
        'LOCATION': 'redis://redis:6379/1',
        'OPTIONS': {
            'CLIENT_CLASS': 'django_redis.client.DefaultClient',
            'CONNECTION_POOL_KWARGS': {
                'max_connections': 50,
                'retry_on_timeout': True,
            }
        }
    }
}
```

#### LangChain Optimization
```python
# Agent configuration
AGENT_CONFIG = {
    'max_concurrent_executions': 10,
    'timeout_seconds': 300,
    'retry_attempts': 3,
    'memory_cache_size': 1000,
}
```

### Database Optimization

#### PostgreSQL Configuration
```conf
# postgresql.conf
shared_buffers = 4GB
effective_cache_size = 12GB
maintenance_work_mem = 1GB
checkpoint_completion_target = 0.9
wal_buffers = 64MB
default_statistics_target = 100
random_page_cost = 1.1
effective_io_concurrency = 200
work_mem = 64MB
min_wal_size = 2GB
max_wal_size = 8GB
```

### Container Resource Limits

```yaml
services:
  app:
    deploy:
      resources:
        limits:
          cpus: '2.0'
          memory: 4G
        reservations:
          cpus: '0.5'
          memory: 1G
```

## Troubleshooting

### Common Issues

#### Application Won't Start
1. Check environment variables
2. Verify database connectivity
3. Check Docker logs: `docker logs agent-etl-app`
4. Verify secrets and configuration

#### High Memory Usage
1. Check for memory leaks in logs
2. Monitor LangChain agent memory usage
3. Adjust container memory limits
4. Consider horizontal scaling

#### Database Connection Issues
1. Check connection pool settings
2. Verify database server health
3. Monitor connection count
4. Check network connectivity

#### Pipeline Failures
1. Check agent logs for errors
2. Verify source data availability
3. Check transformation logic
4. Monitor resource usage during execution

### Debugging Commands

```bash
# Check application logs
docker logs -f agent-etl-app

# Check database connectivity
docker exec -it agent-etl-postgres psql -U agent_etl_user -d agent_etl -c "SELECT 1;"

# Check Redis connectivity
docker exec -it agent-etl-redis redis-cli ping

# Monitor resource usage
docker stats

# Check health endpoints
curl http://localhost:8793/health
curl http://localhost:8793/metrics
```

## Maintenance

### Regular Maintenance Tasks

#### Daily
- Monitor application health and performance
- Check error logs for issues
- Verify backup completion

#### Weekly
- Review and rotate logs
- Update security patches
- Performance analysis

#### Monthly
- Security vulnerability assessment
- Capacity planning review
- Disaster recovery testing

#### Quarterly
- Full security audit
- Performance optimization review
- Documentation updates

### Update Procedures

#### Application Updates
1. Test new version in staging
2. Create database backup
3. Deploy with zero-downtime strategy
4. Verify functionality
5. Monitor for issues

#### Security Updates
1. Review security bulletins
2. Test patches in staging
3. Schedule maintenance window
4. Apply updates
5. Verify system security

This comprehensive deployment guide provides the foundation for a secure, scalable, and maintainable production deployment of Agent-Orchestrated-ETL.