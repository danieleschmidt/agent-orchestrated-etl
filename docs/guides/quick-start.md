# Quick Start Guide

## Overview

Agent-Orchestrated-ETL is an intelligent data pipeline orchestration system that combines Apache Airflow with LangChain agents to create self-optimizing ETL workflows. This guide will get you up and running in under 10 minutes.

## Prerequisites

- **Python 3.8+**: Check with `python --version`
- **Docker & Docker Compose**: Check with `docker --version` and `docker-compose --version`
- **Git**: Check with `git --version`
- **4GB RAM**: Minimum for running all services

## Installation

### Option 1: Quick Docker Setup (Recommended)

```bash
# 1. Clone the repository
git clone https://github.com/your-org/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# 2. Start all services
docker-compose up -d

# 3. Wait for services to be ready (about 60 seconds)
docker-compose logs -f app

# 4. Verify installation
curl http://localhost:8793/health
```

### Option 2: Local Development Setup

```bash
# 1. Clone and navigate
git clone https://github.com/your-org/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# 2. Create virtual environment
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate

# 3. Install the package
pip install -e .[dev]

# 4. Set up environment
cp .env.example .env
# Edit .env with your settings

# 5. Start supporting services
docker-compose up -d postgres redis

# 6. Initialize database
python -c "from agent_orchestrated_etl.config import init_database; init_database()"
```

## First Pipeline

### Create Your First Pipeline

```python
# save as: my_first_pipeline.py
from agent_orchestrated_etl import DataOrchestrator

# Initialize the orchestrator
orchestrator = DataOrchestrator()

# Create a simple pipeline from mock data
pipeline = orchestrator.create_pipeline(
    source="mock://sample-data",
    operations={
        "transform": lambda data: [{"id": item["id"], "value": item["value"] * 2} for item in data],
        "load": lambda data: print(f"Processed {len(data)} records")
    }
)

# Execute the pipeline
print("Starting pipeline execution...")
result = pipeline.execute()
print(f"Pipeline completed! Processed {len(result)} records")
```

Run your first pipeline:

```bash
python my_first_pipeline.py
```

### Using the CLI

```bash
# List available data sources
python -m agent_orchestrated_etl.cli --list-sources

# Create a pipeline from a CSV file
echo "id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300" > sample.csv

python -m agent_orchestrated_etl.cli run_pipeline file://sample.csv --output results.json

# Generate an Airflow DAG
python -m agent_orchestrated_etl.cli generate_dag file://sample.csv my_dag.py
```

## Key Concepts

### Orchestrator Agent
The main coordinator that analyzes data sources and creates optimal pipelines.

```python
orchestrator = DataOrchestrator()
```

### ETL Agents
Specialized agents for different phases:
- **Extraction Agent**: Handles data ingestion
- **Transformation Agent**: Applies data transformations
- **Loading Agent**: Manages data persistence

### Pipeline Creation
Pipelines are created dynamically based on source analysis:

```python
# The orchestrator analyzes the source and creates an optimal pipeline
pipeline = orchestrator.create_pipeline(source="s3://my-bucket/data/")
```

## Data Sources

### Supported Sources

| Source Type | Example | Description |
|-------------|---------|-------------|
| File | `file://path/to/data.csv` | Local files (CSV, JSON, Parquet) |
| S3 | `s3://bucket/prefix/` | AWS S3 objects |
| Database | `postgresql://host:port/db` | SQL databases |
| API | `api://api.example.com/endpoint` | REST APIs |
| Mock | `mock://test-data` | Mock data for testing |

### File Sources

```python
# CSV files
pipeline = orchestrator.create_pipeline(source="file://data.csv")

# JSON files
pipeline = orchestrator.create_pipeline(source="file://data.json")

# Parquet files
pipeline = orchestrator.create_pipeline(source="file://data.parquet")
```

### S3 Sources

```python
# Configure AWS credentials in .env
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
AWS_DEFAULT_REGION=us-east-1

# Create S3 pipeline
pipeline = orchestrator.create_pipeline(source="s3://my-bucket/data/")
```

### Database Sources

```python
# PostgreSQL
pipeline = orchestrator.create_pipeline(
    source="postgresql://user:pass@host:5432/database",
    query="SELECT * FROM users WHERE created_at > '2023-01-01'"
)
```

## Custom Operations

### Transform Operations

```python
def clean_data(data):
    """Remove null values and normalize text."""
    cleaned = []
    for record in data:
        if record.get('value') is not None:
            record['name'] = record['name'].strip().title()
            cleaned.append(record)
    return cleaned

pipeline = orchestrator.create_pipeline(
    source="file://messy_data.csv",
    operations={"transform": clean_data}
)
```

### Load Operations

```python
def save_to_database(data):
    """Save processed data to database."""
    # Your database saving logic here
    print(f"Saved {len(data)} records to database")
    return {"saved_count": len(data)}

pipeline = orchestrator.create_pipeline(
    source="file://data.csv",
    operations={"load": save_to_database}
)
```

## Monitoring

### Health Checks

```bash
# Check application health
curl http://localhost:8793/health

# Check specific components
curl http://localhost:8793/health | jq '.checks'
```

### Metrics

```bash
# View metrics in Prometheus format
curl http://localhost:8793/metrics

# Access Grafana dashboard
open http://localhost:3000
# Username: admin, Password: admin
```

### Logs

```bash
# View application logs
docker-compose logs -f app

# View all service logs
docker-compose logs -f
```

## Web Interfaces

Once your services are running, access these web interfaces:

- **Application Health**: http://localhost:8793/health
- **Grafana Dashboards**: http://localhost:3000 (admin/admin)
- **Prometheus**: http://localhost:9090
- **Airflow UI**: http://localhost:8081 (admin/admin)

## Common Use Cases

### ETL from CSV to Database

```python
def load_to_postgres(data):
    import psycopg2
    conn = psycopg2.connect("postgresql://user:pass@localhost:5432/db")
    # Insert logic here
    return {"inserted": len(data)}

pipeline = orchestrator.create_pipeline(
    source="file://sales_data.csv",
    operations={
        "transform": lambda data: [{"amount": float(r["amount"]), "date": r["date"]} for r in data],
        "load": load_to_postgres
    }
)
```

### API Data Processing

```python
def enrich_with_location(data):
    """Add location data from external API."""
    # Your enrichment logic
    return data

pipeline = orchestrator.create_pipeline(
    source="api://api.example.com/users",
    operations={"transform": enrich_with_location}
)
```

### Scheduled Processing

```python
# Generate Airflow DAG for scheduling
orchestrator.create_pipeline(
    source="s3://data-lake/daily/",
    schedule_interval="@daily"
).save_as_dag("daily_processing.py")
```

## Configuration

### Environment Variables

Key environment variables in `.env`:

```bash
# Application
DEBUG=true
LOG_LEVEL=INFO

# Database
DATABASE_URL=postgresql://postgres:password@localhost:5432/agent_etl

# AI Provider
OPENAI_API_KEY=sk-your-key-here

# AWS (for S3 sources)
AWS_ACCESS_KEY_ID=your-key
AWS_SECRET_ACCESS_KEY=your-secret
```

### Agent Configuration

```python
# Configure agent behavior
orchestrator = DataOrchestrator(config={
    "max_retries": 3,
    "timeout_seconds": 300,
    "parallel_workers": 4,
    "memory_limit": "2GB"
})
```

## Testing

### Test Your Setup

```bash
# Run all tests
make test

# Run integration tests only
make test-integration

# Test with sample data
python -c "
from agent_orchestrated_etl import DataOrchestrator
o = DataOrchestrator()
p = o.create_pipeline(source='mock://test')
result = p.execute()
print(f'Success! Processed {len(result)} records')
"
```

## Troubleshooting

### Common Issues

**Port Already in Use**
```bash
# Find what's using the port
lsof -i :8793
# Kill the process or change ports in docker-compose.yml
```

**Database Connection Failed**
```bash
# Check if postgres is running
docker-compose ps postgres
# Restart if needed
docker-compose restart postgres
```

**Out of Memory**
```bash
# Increase Docker memory limits
# In Docker Desktop: Settings → Resources → Memory
```

### Getting Help

1. Check the logs: `docker-compose logs -f app`
2. Verify health: `curl http://localhost:8793/health`
3. Check GitHub issues: https://github.com/your-org/agent-orchestrated-etl/issues
4. Read the full documentation in `docs/`

## Next Steps

Now that you have Agent-Orchestrated-ETL running:

1. **Explore the API**: Check out `docs/api/api-reference.md`
2. **Learn Architecture**: Read `docs/architecture/system-overview.md`
3. **Deploy to Production**: Follow `docs/deployment/deployment-guide.md`
4. **Develop Custom Agents**: See `docs/developer/agent-development.md`
5. **Set Up Monitoring**: Configure alerts using `monitoring/alert_rules.yml`

## Examples Repository

Find more examples and tutorials:
- Basic ETL patterns
- Advanced transformations
- Custom agent development
- Production deployment configurations

Happy pipeline building! 🚀