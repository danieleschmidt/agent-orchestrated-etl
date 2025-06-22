# Agent-Orchestrated-ETL

Hybrid Airflow + LangChain project where an agent decides which ETL step or retriever to trigger.

## Features
- Intelligent DAG generation based on data source analysis
- LangChain agents that route between extraction, transformation, and loading tasks
- Dynamic pipeline optimization using LLM reasoning
- Integration with popular data sources (S3, PostgreSQL, APIs)
- Real-time monitoring and error recovery through agent decision-making

## Quick Start
```bash
pip install -r requirements.txt
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
python setup_agent.py
airflow webserver --port 8080
```

## Architecture
- **Orchestrator Agent**: Analyzes incoming data and decides pipeline structure
- **ETL Agents**: Specialized agents for extract, transform, and load operations
- **Monitor Agent**: Watches pipeline health and suggests optimizations

## Usage
```python
from agent_etl import DataOrchestrator

orchestrator = DataOrchestrator()
pipeline = orchestrator.create_pipeline(
    source="s3://my-bucket/data/",
    target="postgresql://localhost/warehouse"
)
pipeline.execute()
```

## Roadmap
1. Add support for streaming data sources
2. Implement cost optimization recommendations
3. Build Slack/Teams integration for pipeline notifications

## License
MIT
