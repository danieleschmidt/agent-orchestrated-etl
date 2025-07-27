# Runbook: Application Down

## Alert Description
**Alert:** `ApplicationDown`  
**Severity:** Critical  
**Component:** Application  

The Agent-Orchestrated-ETL application is not responding or has crashed.

## Immediate Actions

### 1. Verify the Alert
```bash
# Check if the application is actually down
curl -f http://localhost:8793/health

# Check application logs
docker logs agent-etl-app --tail=100

# Check container status
docker ps | grep agent-etl
```

### 2. Quick Recovery Attempts

#### Option A: Restart the Application Container
```bash
# Restart the application container
docker restart agent-etl-app

# Wait 30 seconds and verify
sleep 30
curl -f http://localhost:8793/health
```

#### Option B: Restart All Services
```bash
# If single container restart doesn't work
docker-compose restart

# Verify all services are up
docker-compose ps
```

### 3. Check Dependencies
```bash
# Verify database connectivity
docker exec -it agent-etl-postgres psql -U postgres -d agent_etl -c "SELECT 1;"

# Verify Redis connectivity
docker exec -it agent-etl-redis redis-cli ping
```

## Investigation Steps

### 1. Application Logs Analysis
```bash
# Check application logs for errors
docker logs agent-etl-app --since="1h" | grep -i error

# Check for memory issues
docker logs agent-etl-app --since="1h" | grep -i "memory\|oom"

# Check for connection issues
docker logs agent-etl-app --since="1h" | grep -i "connection\|timeout"
```

### 2. Resource Utilization
```bash
# Check container resource usage
docker stats agent-etl-app --no-stream

# Check host system resources
top
df -h
free -h
```

### 3. Network Connectivity
```bash
# Check if ports are accessible
netstat -tlnp | grep :8793
telnet localhost 8793

# Check network connectivity between containers
docker exec agent-etl-app ping postgres
docker exec agent-etl-app ping redis
```

### 4. Database Health
```bash
# Check database status
docker exec -it agent-etl-postgres pg_isready

# Check for database locks
docker exec -it agent-etl-postgres psql -U postgres -d agent_etl -c "
SELECT pid, state, query, query_start 
FROM pg_stat_activity 
WHERE state != 'idle';"
```

## Common Causes and Solutions

### 1. Out of Memory (OOM)
**Symptoms:** Container restarts frequently, OOM messages in logs
```bash
# Check memory limits
docker inspect agent-etl-app | grep -i memory

# Increase memory limits in docker-compose.yml
deploy:
  resources:
    limits:
      memory: 2G
```

### 2. Database Connection Pool Exhaustion
**Symptoms:** "Connection pool exhausted" errors in logs
```bash
# Check active connections
docker exec -it agent-etl-postgres psql -U postgres -d agent_etl -c "
SELECT count(*) as active_connections 
FROM pg_stat_activity 
WHERE state = 'active';"

# Restart application to reset connection pool
docker restart agent-etl-app
```

### 3. Disk Space Issues
**Symptoms:** "No space left on device" errors
```bash
# Check disk usage
df -h
docker system df

# Clean up Docker artifacts
docker system prune -f
docker volume prune -f
```

### 4. Configuration Issues
**Symptoms:** Application fails to start, configuration errors in logs
```bash
# Verify environment variables
docker exec agent-etl-app env | grep -E "DATABASE_URL|REDIS_URL"

# Check configuration files
docker exec agent-etl-app cat /app/.env
```

## Recovery Procedures

### 1. Full System Recovery
```bash
# Stop all services
docker-compose down

# Remove containers and volumes (if necessary)
docker-compose down -v

# Rebuild and restart
docker-compose up -d --build

# Verify health
sleep 60
curl -f http://localhost:8793/health
```

### 2. Database Recovery
```bash
# If database corruption is suspected
docker-compose stop app
docker exec -it agent-etl-postgres pg_dump -U postgres agent_etl > backup.sql
docker-compose down
docker volume rm agent-etl_postgres_data
docker-compose up -d postgres
# Wait for postgres to initialize
sleep 30
docker exec -i agent-etl-postgres psql -U postgres agent_etl < backup.sql
docker-compose up -d app
```

## Prevention

### 1. Monitoring Setup
- Ensure health checks are configured correctly
- Set up proper alerting thresholds
- Monitor resource usage trends

### 2. Resource Planning
- Regularly review resource usage patterns
- Set appropriate resource limits
- Plan for capacity scaling

### 3. Regular Maintenance
- Implement log rotation
- Schedule regular disk cleanup
- Keep dependencies updated

## Escalation

### When to Escalate
- Recovery procedures fail after 30 minutes
- Data corruption is suspected
- Security breach is suspected
- Multiple systems are affected

### Who to Contact
- **Level 2 Support:** Technical Lead
- **Level 3 Support:** Platform Team
- **Emergency:** On-call Engineering Manager

### Information to Provide
- Alert timestamp and duration
- Steps already taken
- Current system status
- Relevant log excerpts
- Impact assessment

## Post-Incident

### 1. Verify Full Recovery
```bash
# Run comprehensive health checks
curl -f http://localhost:8793/health
curl -f http://localhost:8793/metrics

# Verify all functionality
./scripts/integration-test.sh
```

### 2. Document the Incident
- Root cause analysis
- Timeline of events
- Actions taken
- Lessons learned
- Preventive measures

### 3. Follow-up Actions
- Update monitoring if gaps were identified
- Implement additional preventive measures
- Update this runbook if needed