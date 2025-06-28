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
from agent_orchestrated_etl import DataOrchestrator

orch = DataOrchestrator()
pipeline = orch.create_pipeline(
    source="s3://my-bucket/data/",
)
pipeline.execute()

# override the load step
custom_pipeline = orch.create_pipeline(
    source="s3://my-bucket/data/",
    operations={"load": lambda data: print("loaded", data)},
)
custom_pipeline.execute()

# create a pipeline from a REST API
api_pipeline = orch.create_pipeline(
    source="api://example.com/endpoint",
)
api_pipeline.execute()
```

You can also execute a pipeline via the CLI. Add `--list-tasks` to preview the
execution order without running any steps. Use `--list-sources` to see the
supported data sources. Pass `--monitor` with a file path to capture task
events. Logs are appended to the file as each step runs. Monitor logs are
written even if pipeline creation or execution fails, and the command will exit
with code ``1`` on errors:

```bash
run_pipeline s3 --output results.json --airflow dag.py --dag-id my_dag
run_pipeline s3 --list-tasks
generate_dag s3 dag.py --list-tasks
generate_dag --list-sources
run_pipeline --list-sources
run_pipeline s3 --monitor events.log
```

## Roadmap
1. Add support for streaming data sources
2. Implement cost optimization recommendations
3. Build Slack/Teams integration for pipeline notifications

## License
MIT
