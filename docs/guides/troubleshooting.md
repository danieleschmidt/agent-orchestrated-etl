# Troubleshooting Guide

## Common Issues and Solutions

### Installation and Setup Issues

#### Docker Services Won't Start

**Problem**: `docker-compose up` fails or services crash immediately

**Diagnosis**:
```bash
# Check Docker status
docker version
docker-compose version

# Check service logs
docker-compose logs

# Check for port conflicts
netstat -tlnp | grep -E "(5432|6379|8080|8793)"
```

**Solutions**:

1. **Port conflicts**:
   ```bash
   # Edit docker-compose.yml to use different ports
   ports:
     - "5433:5432"  # Change PostgreSQL port
     - "6380:6379"  # Change Redis port
   ```

2. **Insufficient resources**:
   ```bash
   # Check available memory
   free -h
   # Increase Docker memory in Docker Desktop settings
   ```

3. **Permission issues**:
   ```bash
   # Fix Docker permissions (Linux)
   sudo usermod -aG docker $USER
   newgrp docker
   ```

#### Python Dependencies Installation Fails

**Problem**: `pip install -e .[dev]` fails with compilation errors

**Solutions**:

1. **Update pip and setuptools**:
   ```bash
   pip install --upgrade pip setuptools wheel
   ```

2. **Install system dependencies** (Ubuntu/Debian):
   ```bash
   sudo apt-get update
   sudo apt-get install build-essential python3-dev libpq-dev
   ```

3. **Use conda environment**:
   ```bash
   conda create -n agent-etl python=3.11
   conda activate agent-etl
   pip install -e .[dev]
   ```

### Runtime Issues

#### Application Won't Start

**Problem**: Agent-ETL application fails to start

**Diagnosis**:
```bash
# Check application logs
docker logs agent-etl-app

# Check health endpoint
curl -v http://localhost:8793/health

# Check environment variables
docker exec agent-etl-app env | grep -E "(DATABASE|REDIS|OPENAI)"
```

**Common Causes**:

1. **Database connection failure**:
   ```bash
   # Verify database is running
   docker exec -it agent-etl-postgres pg_isready
   
   # Test connection manually
   docker exec -it agent-etl-postgres psql -U postgres -d agent_etl -c "SELECT 1;"
   ```

2. **Missing environment variables**:
   ```bash
   # Check .env file exists and has required variables
   cat .env | grep -E "(DATABASE_URL|OPENAI_API_KEY)"
   
   # Fix missing variables
   cp .env.example .env
   # Edit .env with your values
   ```

3. **Redis connection issues**:
   ```bash
   # Test Redis connection
   docker exec -it agent-etl-redis redis-cli ping
   ```

#### Pipeline Execution Failures

**Problem**: Pipelines fail to execute or produce errors

**Diagnosis**:
```bash
# Enable debug mode
export DEBUG=true
python -m agent_orchestrated_etl.cli run_pipeline your-source --verbose

# Check agent logs
grep -i error /path/to/logs/agent-etl.log

# Test with mock data first
python -c "
from agent_orchestrated_etl import DataOrchestrator
o = DataOrchestrator()
p = o.create_pipeline(source='mock://test')
result = p.execute()
print('Mock pipeline success!')
"
```

**Common Solutions**:

1. **Data source access issues**:
   ```python
   # Verify source accessibility
   import pandas as pd
   df = pd.read_csv('your-file.csv')  # For file sources
   
   # For S3 sources, check AWS credentials
   aws s3 ls s3://your-bucket/
   ```

2. **Memory issues with large datasets**:
   ```python
   # Process data in chunks
   pipeline = orchestrator.create_pipeline(
       source="file://large-file.csv",
       operations={
           "extract": {"chunk_size": 10000},
           "transform": your_transform_function
       }
   )
   ```

3. **Timeout issues**:
   ```python
   # Increase timeout
   orchestrator = DataOrchestrator(config={
       "timeout_seconds": 600,  # 10 minutes
       "max_retries": 5
   })
   ```

### Performance Issues

#### Slow Pipeline Execution

**Problem**: Pipelines take too long to execute

**Diagnosis**:
```bash
# Profile pipeline execution
python -m cProfile -o pipeline.prof your_pipeline.py

# Check system resources
top
iostat -x 1
```

**Optimization Strategies**:

1. **Enable parallel processing**:
   ```python
   orchestrator = DataOrchestrator(config={
       "parallel_workers": 8,
       "enable_async": True
   })
   ```

2. **Optimize data processing**:
   ```python
   # Use vectorized operations
   def fast_transform(data):
       import pandas as pd
       df = pd.DataFrame(data)
       # Vectorized operations are faster
       df['new_column'] = df['value'] * 2
       return df.to_dict('records')
   ```

3. **Database connection pooling**:
   ```python
   # In your .env file
   DATABASE_POOL_SIZE=20
   DATABASE_MAX_OVERFLOW=30
   ```

#### High Memory Usage

**Problem**: Application consumes too much memory

**Solutions**:

1. **Process data in chunks**:
   ```python
   def chunked_processing(data, chunk_size=1000):
       for i in range(0, len(data), chunk_size):
           chunk = data[i:i+chunk_size]
           yield process_chunk(chunk)
   ```

2. **Clean up resources**:
   ```python
   import gc
   
   def memory_efficient_transform(data):
       result = process_data(data)
       del data  # Explicit cleanup
       gc.collect()
       return result
   ```

3. **Set memory limits**:
   ```yaml
   # In docker-compose.yml
   services:
     app:
       deploy:
         resources:
           limits:
             memory: 2G
   ```

### Data Issues

#### Data Validation Failures

**Problem**: Data doesn't meet expected schema or quality standards

**Diagnosis**:
```python
# Inspect data structure
import pandas as pd
df = pd.read_csv('your-data.csv')
print(df.info())
print(df.describe())
print(df.isnull().sum())
```

**Solutions**:

1. **Add data validation**:
   ```python
   def validate_data(data):
       required_columns = ['id', 'name', 'value']
       df = pd.DataFrame(data)
       
       missing_cols = set(required_columns) - set(df.columns)
       if missing_cols:
           raise ValueError(f"Missing columns: {missing_cols}")
       
       return data
   
   pipeline = orchestrator.create_pipeline(
       source="file://data.csv",
       operations={"validate": validate_data}
   )
   ```

2. **Handle missing data**:
   ```python
   def clean_data(data):
       df = pd.DataFrame(data)
       df = df.dropna()  # Remove null rows
       df = df.fillna(0)  # Fill nulls with 0
       return df.to_dict('records')
   ```

#### Encoding Issues

**Problem**: Text data has encoding problems

**Solutions**:

1. **Specify encoding**:
   ```python
   # For CSV files
   pipeline = orchestrator.create_pipeline(
       source="file://data.csv",
       operations={"extract": {"encoding": "utf-8"}}
   )
   ```

2. **Handle encoding errors**:
   ```python
   def fix_encoding(data):
       for record in data:
           for key, value in record.items():
               if isinstance(value, str):
                   record[key] = value.encode('utf-8', errors='ignore').decode('utf-8')
       return data
   ```

### Security Issues

#### Authentication Failures

**Problem**: Cannot authenticate with external services

**Solutions**:

1. **Check API keys**:
   ```bash
   # Verify environment variables are set
   echo $OPENAI_API_KEY
   echo $AWS_ACCESS_KEY_ID
   
   # Test API connectivity
   curl -H "Authorization: Bearer $OPENAI_API_KEY" https://api.openai.com/v1/models
   ```

2. **Rotate credentials**:
   ```bash
   # Generate new API keys and update .env
   # Restart services after updating credentials
   docker-compose restart
   ```

#### Permission Denied Errors

**Problem**: Cannot access files or directories

**Solutions**:

1. **Fix file permissions**:
   ```bash
   # Make files readable
   chmod 644 your-data-files/*
   chmod 755 your-directories/
   
   # For Docker volume mounts
   sudo chown -R $USER:$USER ./data/
   ```

2. **Run with proper user**:
   ```yaml
   # In docker-compose.yml
   services:
     app:
       user: "${UID}:${GID}"
   ```

### Monitoring and Debugging

#### Cannot Access Monitoring Dashboards

**Problem**: Grafana or Prometheus not accessible

**Diagnosis**:
```bash
# Check if services are running
docker-compose ps | grep -E "(grafana|prometheus)"

# Check service logs
docker-compose logs grafana
docker-compose logs prometheus

# Test connectivity
curl http://localhost:3000/api/health  # Grafana
curl http://localhost:9090/-/healthy   # Prometheus
```

**Solutions**:

1. **Restart monitoring services**:
   ```bash
   docker-compose restart grafana prometheus
   ```

2. **Check configuration**:
   ```bash
   # Verify Prometheus config
   docker exec -it agent-etl-prometheus promtool check config /etc/prometheus/prometheus.yml
   ```

#### Missing Metrics

**Problem**: Application metrics not appearing in Prometheus

**Solutions**:

1. **Check metrics endpoint**:
   ```bash
   curl http://localhost:8793/metrics
   ```

2. **Verify Prometheus configuration**:
   ```yaml
   # In monitoring/prometheus.yml
   scrape_configs:
     - job_name: 'agent-etl-app'
       static_configs:
         - targets: ['app:8793']
       metrics_path: '/metrics'
   ```

### Development Issues

#### Import Errors

**Problem**: Python cannot import modules

**Solutions**:

1. **Check PYTHONPATH**:
   ```bash
   export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
   ```

2. **Install in development mode**:
   ```bash
   pip install -e .
   ```

3. **Verify module structure**:
   ```bash
   find src/ -name "*.py" | head -10
   ls -la src/agent_orchestrated_etl/
   ```

#### Tests Failing

**Problem**: Unit or integration tests fail

**Diagnosis**:
```bash
# Run with verbose output
pytest tests/ -v -s

# Run specific test
pytest tests/test_specific.py::test_function -v

# Check test dependencies
pip list | grep -E "(pytest|mock)"
```

**Solutions**:

1. **Update test dependencies**:
   ```bash
   pip install -e .[dev]
   ```

2. **Fix test environment**:
   ```bash
   # Set test environment variables
   export ENVIRONMENT=test
   export DATABASE_URL=postgresql://postgres:password@localhost:5432/test_agent_etl
   ```

## Getting Additional Help

### Log Analysis

```bash
# Comprehensive log collection
docker-compose logs --timestamps --tail=1000 > debug-logs.txt

# Search for specific errors
grep -i "error\|exception\|failed" debug-logs.txt

# Monitor logs in real-time
docker-compose logs -f app | grep -E "(ERROR|WARN|CRITICAL)"
```

### System Information

```bash
# Collect system information for bug reports
echo "=== System Info ===" > debug-info.txt
uname -a >> debug-info.txt
docker version >> debug-info.txt
docker-compose version >> debug-info.txt
python --version >> debug-info.txt
pip list >> debug-info.txt

echo "=== Service Status ===" >> debug-info.txt
docker-compose ps >> debug-info.txt

echo "=== Resource Usage ===" >> debug-info.txt
docker stats --no-stream >> debug-info.txt
```

### Community Support

1. **GitHub Issues**: https://github.com/your-org/agent-orchestrated-etl/issues
   - Search existing issues first
   - Include debug information and logs
   - Provide minimal reproduction steps

2. **Documentation**: Check `docs/` directory for detailed guides

3. **Examples**: Look in `examples/` directory for working code samples

4. **Stack Overflow**: Tag questions with `agent-orchestrated-etl`

### Creating Bug Reports

When reporting issues, include:

1. **Environment details**:
   - OS and version
   - Python version
   - Docker version
   - Agent-ETL version

2. **Error information**:
   - Complete error messages
   - Stack traces
   - Relevant log excerpts

3. **Reproduction steps**:
   - Minimal code example
   - Data samples (if not sensitive)
   - Configuration details

4. **Expected vs. actual behavior**:
   - What you expected to happen
   - What actually happened
   - Any workarounds you've tried

Remember: The more detailed your bug report, the faster we can help you solve the issue!