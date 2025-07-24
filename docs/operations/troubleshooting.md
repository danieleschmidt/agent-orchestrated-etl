# Troubleshooting Guide

## Overview

This guide provides comprehensive troubleshooting procedures for common issues in the Agent Orchestrated ETL system. It includes diagnostic procedures, performance optimization, and recovery strategies.

## Common Issues

### Agent Selection Problems

#### Symptom: No suitable agent found for tasks
**Diagnosis:**
```bash
# Check agent registration status
python scripts/diagnose_agents.py --check-registration

# Verify agent capabilities
python scripts/diagnose_agents.py --check-capabilities

# Analyze selection audit trail
python scripts/analyze_selection.py --recent-failures
```

**Solutions:**
1. Verify agent registration and capabilities
2. Check agent load balancing metrics
3. Review capability matching thresholds
4. Restart agent coordinator if needed

#### Symptom: Poor agent selection performance
**Diagnosis:**
```python
# Check selection timing metrics
from src.agent_orchestrated_etl.agents.coordination import AgentCoordinator
coordinator = AgentCoordinator(hub)
metrics = coordinator.get_load_balancing_metrics()
print(f"Selection time: {metrics.get('avg_selection_time_ms')}ms")
```

**Solutions:**
1. Tune selection algorithm parameters
2. Optimize capability matching logic
3. Increase agent pool size if needed
4. Review performance scoring weights

### Database Connectivity Issues

#### Symptom: Database connection failures
**Diagnosis:**
```bash
# Test database connectivity
python scripts/test_database.py --connection-test

# Check connection pool status
python scripts/diagnose_database.py --pool-status

# Verify credentials and configuration
python scripts/diagnose_database.py --config-check
```

**Solutions:**
1. Verify database server availability
2. Check connection string configuration
3. Review network connectivity and firewall rules
4. Restart connection pool if needed

#### Symptom: Slow query performance
**Diagnosis:**
```sql
-- Analyze slow queries
SELECT query, mean_time, calls, total_time
FROM pg_stat_statements
WHERE mean_time > 1000
ORDER BY mean_time DESC
LIMIT 10;

-- Check database locks
SELECT * FROM pg_locks WHERE NOT granted;
```

**Solutions:**
1. Optimize query indexes
2. Review query execution plans
3. Tune database configuration parameters
4. Implement query result caching

### Vector Store Issues

#### Symptom: ChromaDB connection failures
**Diagnosis:**
```bash
# Check ChromaDB service status
curl http://localhost:8001/api/v1/heartbeat

# Verify data directory permissions
ls -la ./data/chromadb/

# Check memory usage
docker stats chromadb  # For Docker deployment
```

**Solutions:**
1. Restart ChromaDB service
2. Check data directory permissions
3. Verify storage space availability
4. Review ChromaDB configuration

#### Symptom: Poor semantic search performance
**Diagnosis:**
```python
# Test search performance
from src.agent_orchestrated_etl.agents.memory import AgentMemory
memory = AgentMemory("test_agent")
import time

start = time.time()
results = memory.search_memories("test query", limit=10)
search_time = (time.time() - start) * 1000
print(f"Search time: {search_time:.2f}ms")
```

**Solutions:**
1. Rebuild vector embeddings
2. Optimize collection indexing
3. Tune search parameters
4. Consider vector store scaling

### Performance Issues

#### Symptom: High memory usage
**Diagnosis:**
```bash
# Monitor memory usage
top -p $(pgrep -f "agent_orchestrated_etl")

# Check Python memory profiling
python -m memory_profiler scripts/profile_memory.py

# Analyze garbage collection
python -X dev -c "import gc; gc.set_debug(gc.DEBUG_STATS)"
```

**Solutions:**
1. Tune cache size limits
2. Implement memory cleanup procedures
3. Optimize data processing algorithms
4. Consider horizontal scaling

#### Symptom: High CPU usage
**Diagnosis:**
```bash
# Profile CPU usage
python -m cProfile -o profile.stats scripts/run_benchmark.py
python -c "import pstats; pstats.Stats('profile.stats').sort_stats('cumulative').print_stats(10)"

# Check concurrent task load
python scripts/analyze_load.py --cpu-analysis
```

**Solutions:**
1. Optimize data processing algorithms
2. Reduce concurrent task limits
3. Implement task queuing and batching
4. Scale horizontally across multiple instances

### Cache Issues

#### Symptom: Cache miss rates too high
**Diagnosis:**
```python
# Check cache statistics
from src.agent_orchestrated_etl.agents.tools import QueryDataTool
tool = QueryDataTool()
stats = tool.get_cache_statistics()
print(f"Cache hit rate: {stats['hit_rate']:.2%}")
print(f"Cache size: {stats['cache_size']}")
```

**Solutions:**
1. Tune cache size and TTL settings
2. Optimize cache key generation
3. Review cache eviction policies
4. Implement cache warming strategies

#### Symptom: Cache memory bloat
**Diagnosis:**
```bash
# Monitor cache memory usage
python scripts/analyze_cache.py --memory-usage

# Check cache cleanup frequency
python scripts/analyze_cache.py --cleanup-stats
```

**Solutions:**
1. Reduce cache size limits
2. Increase cleanup frequency
3. Implement more aggressive LRU eviction
4. Review cache item sizes

## Performance Optimization

### Query Optimization

#### Slow Database Queries
1. **Analyze Query Plans:**
```sql
EXPLAIN (ANALYZE, BUFFERS) SELECT * FROM your_table WHERE conditions;
```

2. **Add Appropriate Indexes:**
```sql
CREATE INDEX CONCURRENTLY idx_table_column ON your_table(column);
```

3. **Optimize Connection Pooling:**
```python
# Configure optimal pool settings
DATABASE_POOL_SIZE = 20
DATABASE_MAX_OVERFLOW = 30
DATABASE_POOL_TIMEOUT = 30
```

#### Agent Selection Optimization
1. **Tune Selection Weights:**
```python
selection_weights = {
    "capability_weight": 0.4,
    "performance_weight": 0.3,
    "specialization_weight": 0.2,
    "load_balancing_weight": 0.1
}
```

2. **Optimize Capability Matching:**
```python
# Enable capability caching
coordinator.enable_capability_caching = True
coordinator.capability_cache_ttl = 300  # 5 minutes
```

### Memory Optimization

#### Reduce Memory Footprint
1. **Tune Cache Sizes:**
```python
# Optimize cache configurations
QUERY_CACHE_SIZE = 1000
MEMORY_CACHE_SIZE = 500
AUDIT_TRAIL_SIZE = 10000
```

2. **Implement Streaming for Large Datasets:**
```python
# Use streaming for large data processing
def process_large_dataset(data_source):
    for batch in stream_data(data_source, batch_size=1000):
        yield process_batch(batch)
```

3. **Optimize Object Lifecycle:**
```python
# Implement explicit cleanup
def cleanup_resources():
    gc.collect()
    clear_expired_cache_entries()
    cleanup_completed_tasks()
```

### Network Optimization

#### Reduce Network Latency
1. **Connection Pooling:**
```python
# Optimize connection reuse
CONNECTION_POOL_SIZE = 50
CONNECTION_POOL_RECYCLE = 3600
```

2. **Compression for Large Payloads:**
```python
# Enable compression for API responses
ENABLE_COMPRESSION = True
COMPRESSION_LEVEL = 6
```

3. **Batch Operations:**
```python
# Batch multiple operations
async def batch_operations(operations, batch_size=100):
    for i in range(0, len(operations), batch_size):
        batch = operations[i:i + batch_size]
        await execute_batch(batch)
```

## Diagnostic Scripts

### System Health Check
```bash
#!/bin/bash
# scripts/health_check.sh

echo "=== Agent Orchestrated ETL Health Check ==="

# Check Python dependencies
echo "Checking Python dependencies..."
python -c "import pkg_resources; pkg_resources.require(open('requirements.txt').read().split('\n'))"

# Check database connectivity
echo "Checking database connectivity..."
python scripts/test_database.py

# Check vector store
echo "Checking vector store..."
curl -f http://localhost:8001/api/v1/heartbeat

# Check system resources
echo "Checking system resources..."
df -h
free -h
ps aux | grep agent_orchestrated_etl

echo "Health check completed."
```

### Performance Benchmark
```python
#!/usr/bin/env python3
# scripts/benchmark_system.py

import asyncio
import time
from src.agent_orchestrated_etl.agents.coordination import AgentCoordinator
from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent

async def benchmark_agent_selection():
    """Benchmark agent selection performance."""
    coordinator = AgentCoordinator(communication_hub)
    
    start_time = time.time()
    for i in range(100):
        task = create_test_task(f"task_{i}")
        await coordinator.assign_task(task)
    
    total_time = time.time() - start_time
    print(f"Agent selection: {total_time/100*1000:.2f}ms per selection")

async def benchmark_memory_search():
    """Benchmark vector memory search performance."""
    memory = AgentMemory("test_agent")
    
    start_time = time.time()
    for i in range(100):
        results = memory.search_memories("test query", limit=10)
    
    total_time = time.time() - start_time
    print(f"Memory search: {total_time/100*1000:.2f}ms per search")

if __name__ == "__main__":
    asyncio.run(benchmark_agent_selection())
    asyncio.run(benchmark_memory_search())
```

### Configuration Validator
```python
#!/usr/bin/env python3
# scripts/validate_config.py

import os
import yaml
from urllib.parse import urlparse

def validate_environment():
    """Validate environment configuration."""
    required_vars = [
        'DATABASE_URL', 'SECRET_KEY', 'ENCRYPTION_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Missing environment variables: {missing_vars}")
        return False
    
    # Validate database URL format
    db_url = os.getenv('DATABASE_URL')
    try:
        parsed = urlparse(db_url)
        if not all([parsed.scheme, parsed.netloc]):
            print("Invalid DATABASE_URL format")
            return False
    except Exception as e:
        print(f"Error parsing DATABASE_URL: {e}")
        return False
    
    print("Environment configuration valid")
    return True

def validate_application_config():
    """Validate application configuration file."""
    config_path = "config/application.yml"
    
    if not os.path.exists(config_path):
        print(f"Configuration file not found: {config_path}")
        return False
    
    try:
        with open(config_path, 'r') as f:
            config = yaml.safe_load(f)
        
        # Validate required sections
        required_sections = ['agents', 'coordination', 'performance', 'security']
        for section in required_sections:
            if section not in config:
                print(f"Missing configuration section: {section}")
                return False
        
        print("Application configuration valid")
        return True
    
    except Exception as e:
        print(f"Error validating configuration: {e}")
        return False

if __name__ == "__main__":
    env_valid = validate_environment()
    config_valid = validate_application_config()
    
    if env_valid and config_valid:
        print("All configuration valid")
        exit(0)
    else:
        print("Configuration validation failed")
        exit(1)
```

## Recovery Procedures

### Database Recovery
1. **Backup Current State:**
```bash
pg_dump agent_etl > backup_$(date +%Y%m%d_%H%M%S).sql
```

2. **Restore from Backup:**
```bash
psql agent_etl < backup_20250724_150000.sql
```

3. **Rebuild Indexes:**
```sql
REINDEX DATABASE agent_etl;
```

### Vector Store Recovery
1. **Backup Vector Data:**
```bash
tar -czf chromadb_backup_$(date +%Y%m%d_%H%M%S).tar.gz ./data/chromadb/
```

2. **Restore Vector Data:**
```bash
tar -xzf chromadb_backup_20250724_150000.tar.gz -C ./data/
```

3. **Rebuild Embeddings:**
```python
python scripts/rebuild_embeddings.py --force
```

### System Recovery
1. **Graceful Restart:**
```bash
# Stop services gracefully
python scripts/stop_system.py --graceful

# Wait for tasks to complete
sleep 30

# Restart services
python scripts/start_system.py
```

2. **Force Restart (Emergency):**
```bash
# Force stop all processes
pkill -f agent_orchestrated_etl

# Clean up temporary files
rm -rf /tmp/agent_etl_*

# Restart with clean state
python scripts/start_system.py --clean
```

## Monitoring and Alerting

### Key Metrics to Monitor
- Agent selection time (<5ms target)
- Memory search performance (<500ms target)
- Database query performance (<100ms for cached queries)
- Cache hit rates (>90% target)
- Error rates (<1% target)
- System resource utilization (<80% target)

### Alert Thresholds
```yaml
alerts:
  high_priority:
    - agent_selection_time > 10ms
    - memory_search_time > 1000ms
    - error_rate > 5%
    - database_connection_failures > 0
  
  medium_priority:
    - cache_hit_rate < 80%
    - cpu_usage > 80%
    - memory_usage > 85%
    - disk_usage > 90%
  
  low_priority:
    - agent_selection_time > 5ms
    - cache_hit_rate < 90%
    - response_time > 2000ms
```

---

*For advanced troubleshooting scenarios and custom diagnostic procedures, contact the development team or refer to the Operations Manual.*