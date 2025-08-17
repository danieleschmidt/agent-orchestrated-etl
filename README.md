# 🤖 Agent-Orchestrated-ETL

> **Intelligent, Self-Optimizing Data Pipelines Powered by AI Agents**

A revolutionary hybrid system combining Apache Airflow's robust workflow orchestration with LangChain's intelligent agent capabilities to create adaptive, self-healing data pipelines that learn and optimize automatically.

[![Build Status](https://github.com/danieleschmidt/agent-orchestrated-etl/workflows/CI/badge.svg)](https://github.com/danieleschmidt/agent-orchestrated-etl/actions)
[![Coverage](https://codecov.io/gh/danieleschmidt/agent-orchestrated-etl/branch/main/graph/badge.svg)](https://codecov.io/gh/danieleschmidt/agent-orchestrated-etl)
[![Documentation](https://img.shields.io/badge/docs-latest-blue.svg)](https://danieleschmidt.github.io/agent-orchestrated-etl/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

## 📋 Table of Contents

- [Key Features](#-key-features)
- [Quick Start](#-quick-start)
- [Architecture Overview](#️-architecture-overview)
- [Usage Examples](#-usage-examples)
- [CLI Commands](#️-cli-commands)
- [Development Setup](#-development-setup)
- [Contributing](#-contributing)
- [Documentation](#-documentation)
- [License](#-license)

## ✨ Key Features

🧠 **AI-Driven Pipeline Generation** - Automatically analyzes data sources and generates optimal ETL workflows  
🔄 **Self-Healing Pipelines** - Agents detect failures and implement recovery strategies autonomously  
⚡ **Dynamic Optimization** - Real-time performance tuning based on execution patterns  
🔌 **Universal Connectors** - Native support for S3, PostgreSQL, APIs, files, and more  
📊 **Intelligent Monitoring** - Proactive issue detection with automated resolution suggestions  
🚀 **Production Ready** - Enterprise-grade security, scaling, and compliance features  
🎯 **Autonomous SDLC** - Self-improving system with progressive enhancement generations  
⚡ **Advanced Scaling** - Multi-level caching, load balancing, and predictive auto-scaling  
🛡️ **Comprehensive Security** - Multi-layer validation, encryption, and threat detection  
🔬 **Research-Ready** - Built-in frameworks for ML experimentation and benchmarking

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose (optional, for containerized setup)

### Installation

```bash
# Clone the repository
git clone https://github.com/danieleschmidt/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# Install dependencies
pip install -r requirements.txt

# Setup Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init

# Initialize agent system
python setup_agent.py

# Start the system
airflow webserver --port 8080
```

### Docker Setup (Recommended)

```bash
# Start all services
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
```

## 🏗️ Architecture Overview

Our intelligent agent architecture consists of three core components:

- **🎯 Orchestrator Agent**: Analyzes data sources and orchestrates optimal pipeline creation
- **⚙️ ETL Agents**: Specialized agents handling extraction, transformation, and loading operations  
- **👁️ Monitor Agent**: Continuously monitors pipeline health and suggests performance optimizations

[📖 Detailed Architecture Documentation](docs/architecture/system-overview.md)

## 💡 Usage Examples
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

## 🖥️ CLI Commands

Execute pipelines directly from the command line with powerful options:

```bash
# Run pipeline with monitoring
run_pipeline s3 --output results.json --monitor events.log

# Preview pipeline without execution
run_pipeline s3 --list-tasks

# Generate Airflow DAG
generate_dag s3 dag.py --dag-id my_dag

# List available data sources
run_pipeline --list-sources
```

**CLI Options:**
- `--list-tasks`: Preview execution order without running
- `--list-sources`: Show supported data sources  
- `--monitor <file>`: Capture task events to file
- `--output <file>`: Specify output location
- `--dag-id <id>`: Set custom DAG identifier

## 🔧 Development Setup
Install development dependencies and enable pre-commit hooks:

```bash
# Install in development mode with all dependencies
pip install -e .[dev]

# Setup pre-commit hooks for code quality
pre-commit install

# Run tests with coverage
pytest -q
coverage run -m pytest -q
coverage report -m

# Check code complexity
radon cc src/agent_orchestrated_etl -s -a

# Run linting and formatting
black . && isort . && flake8
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

### Quick Contributing Steps
1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📚 Documentation

- [📖 Full Documentation](docs/)
- [🏗️ Architecture Guide](docs/architecture/system-overview.md)
- [🚀 Quick Start Guide](docs/guides/quick-start.md)
- [🔧 Development Guide](docs/developer/onboarding.md)
- [📋 API Reference](docs/api/api-reference.md)
- [🗺️ Project Roadmap](docs/ROADMAP.md)

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

**[⭐ Star us on GitHub](https://github.com/danieleschmidt/agent-orchestrated-etl)** | **[📝 Report Issues](https://github.com/danieleschmidt/agent-orchestrated-etl/issues)** | **[💬 Join Discussions](https://github.com/danieleschmidt/agent-orchestrated-etl/discussions)**

Made with ❤️ by the Agent-Orchestrated-ETL team

</div>
