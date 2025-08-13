import asyncio
import json
import pathlib
import shutil
import sys
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Generator
from unittest.mock import AsyncMock, Mock

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

try:
    from agent_orchestrated_etl.agents.base_agent import BaseAgent
    from agent_orchestrated_etl.agents.etl_agent import ETLAgent
    from agent_orchestrated_etl.agents.monitor_agent import MonitorAgent
    from agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent
    from agent_orchestrated_etl.config import Config
    LEGACY_MODULES_AVAILABLE = True
except ImportError:
    LEGACY_MODULES_AVAILABLE = False

# Import new service modules
from src.agent_orchestrated_etl.database.connection import DatabaseManager
from src.agent_orchestrated_etl.services.integration_service import IntegrationService
from src.agent_orchestrated_etl.services.intelligence_service import IntelligenceService
from src.agent_orchestrated_etl.services.optimization_service import OptimizationService
from src.agent_orchestrated_etl.services.pipeline_service import PipelineService

# Test configuration
TEST_CONFIG = {
    "database": {
        "url": "sqlite:///:memory:",
        "pool_size": 1,
    },
    "redis": {
        "url": "redis://localhost:6379/1",
        "timeout": 1,
    },
    "agents": {
        "orchestrator": {
            "max_concurrent_pipelines": 2,
            "decision_timeout": 5,
        },
        "etl": {
            "batch_size": 10,
            "retry_attempts": 1,
        },
        "monitor": {
            "check_interval": 1,
            "alert_thresholds": {
                "error_rate": 0.1,
                "latency_p95": 1000,
            },
        },
    },
    "data_sources": {
        "postgres": {
            "connection_pool_size": 1,
            "query_timeout": 10,
        },
        "s3": {
            "multipart_threshold": "1MB",
            "retry_config": {
                "max_attempts": 1,
                "backoff_mode": "linear",
            },
        },
    },
    "pipeline": {
        "default_timeout": 30,
        "max_retries": 1,
        "checkpoint_interval": 5,
    },
    "logging": {
        "level": "DEBUG",
        "format": "json",
    },
}

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def test_config() -> Dict[str, Any]:
    """Provide test configuration."""
    return TEST_CONFIG.copy()

@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test files."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path, ignore_errors=True)

@pytest.fixture
def mock_config(test_config: Dict[str, Any]) -> Mock:
    """Mock configuration object."""
    config = Mock(spec=Config)
    config.get.side_effect = lambda key, default=None: _get_nested_config(test_config, key, default)
    config.update_runtime_config = Mock()
    return config

@pytest.fixture
def mock_base_agent() -> Mock:
    """Mock base agent for testing."""
    if not LEGACY_MODULES_AVAILABLE:
        pytest.skip("Legacy agent modules not available")

    agent = Mock(spec=BaseAgent)
    agent.agent_id = "test-agent-001"
    agent.agent_type = "test"
    agent.status = "idle"
    agent.initialize = Mock()
    agent.shutdown = Mock()
    return agent

@pytest.fixture
def mock_orchestrator_agent(mock_config: Mock) -> Mock:
    """Mock orchestrator agent."""
    agent = Mock(spec=OrchestratorAgent)
    agent.agent_id = "orchestrator-001"
    agent.agent_type = "orchestrator"
    agent.config = mock_config
    agent.create_pipeline = Mock()
    agent.optimize_pipeline = Mock()
    agent.coordinate_agents = Mock()
    return agent

@pytest.fixture
def mock_etl_agent(mock_config: Mock) -> Mock:
    """Mock ETL agent."""
    agent = Mock(spec=ETLAgent)
    agent.agent_id = "etl-001"
    agent.agent_type = "etl"
    agent.config = mock_config
    agent.extract_data = Mock()
    agent.transform_data = Mock()
    agent.load_data = Mock()
    return agent

@pytest.fixture
def mock_monitor_agent(mock_config: Mock) -> Mock:
    """Mock monitor agent."""
    agent = Mock(spec=MonitorAgent)
    agent.agent_id = "monitor-001"
    agent.agent_type = "monitor"
    agent.config = mock_config
    agent.check_pipeline_health = Mock()
    agent.generate_alerts = Mock()
    agent.collect_metrics = Mock()
    return agent

@pytest.fixture
def sample_pipeline_config() -> Dict[str, Any]:
    """Sample pipeline configuration for testing."""
    return {
        "pipeline_id": "test-pipeline-001",
        "name": "Test Data Pipeline",
        "source": {
            "type": "s3",
            "bucket": "test-bucket",
            "prefix": "data/",
        },
        "transformations": [
            {
                "type": "filter",
                "condition": "value > 0",
            },
            {
                "type": "aggregate",
                "function": "sum",
                "group_by": ["category"],
            },
        ],
        "destination": {
            "type": "postgres",
            "table": "processed_data",
            "mode": "append",
        },
        "schedule": {
            "type": "cron",
            "expression": "0 0 * * *",
        },
        "metadata": {
            "owner": "test-team",
            "description": "Test pipeline for unit testing",
            "tags": ["test", "development"],
        },
    }

@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Sample data for testing."""
    return {
        "records": [
            {"id": 1, "name": "Alice", "category": "A", "value": 100},
            {"id": 2, "name": "Bob", "category": "B", "value": 200},
            {"id": 3, "name": "Charlie", "category": "A", "value": 150},
            {"id": 4, "name": "Diana", "category": "C", "value": 300},
        ],
        "metadata": {
            "total_records": 4,
            "schema_version": "1.0",
            "created_at": "2025-01-01T00:00:00Z",
        },
    }

@pytest.fixture
def mock_database():
    """Mock database connection."""
    db = Mock()
    db.execute = Mock(return_value=Mock(fetchall=Mock(return_value=[])))
    db.commit = Mock()
    db.rollback = Mock()
    db.close = Mock()
    return db

@pytest.fixture
def mock_s3_client():
    """Mock S3 client."""
    s3 = Mock()
    s3.list_objects_v2 = Mock(return_value={"Contents": []})
    s3.get_object = Mock(return_value={"Body": Mock(read=Mock(return_value=b"test data"))})
    s3.put_object = Mock()
    s3.delete_object = Mock()
    return s3

@pytest.fixture
def mock_redis_client():
    """Mock Redis client."""
    redis = Mock()
    redis.get = Mock(return_value=None)
    redis.set = Mock(return_value=True)
    redis.delete = Mock(return_value=1)
    redis.exists = Mock(return_value=0)
    redis.ping = Mock(return_value=True)
    return redis

@pytest.fixture
def mock_llm_client():
    """Mock LLM client for agent testing."""
    llm = Mock()
    llm.generate = Mock(return_value="Mock LLM response")
    llm.embed = Mock(return_value=[0.1, 0.2, 0.3])
    llm.token_count = Mock(return_value=10)
    return llm

@pytest.fixture(autouse=True)
def setup_test_environment(monkeypatch):
    """Setup test environment variables."""
    test_env_vars = {
        "ENVIRONMENT": "test",
        "LOG_LEVEL": "DEBUG",
        "DATABASE_URL": "sqlite:///:memory:",
        "REDIS_URL": "redis://localhost:6379/1",
        "OPENAI_API_KEY": "test-key",
        "AWS_ACCESS_KEY_ID": "test-access-key",
        "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        "AWS_DEFAULT_REGION": "us-east-1",
    }

    for key, value in test_env_vars.items():
        monkeypatch.setenv(key, value)

def _get_nested_config(config: Dict[str, Any], key: str, default: Any = None) -> Any:
    """Helper function to get nested configuration values."""
    keys = key.split('.')
    value = config

    for k in keys:
        if isinstance(value, dict) and k in value:
            value = value[k]
        else:
            return default

    return value

# Test markers
pytest_plugins = ["pytest_asyncio"]

# Custom test markers
def pytest_configure(config):
    """Configure custom pytest markers."""
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')"
    )
    config.addinivalue_line(
        "markers", "integration: marks tests as integration tests"
    )
    config.addinivalue_line(
        "markers", "e2e: marks tests as end-to-end tests"
    )
    config.addinivalue_line(
        "markers", "performance: marks tests as performance tests"
    )
    config.addinivalue_line(
        "markers", "security: marks tests as security tests"
    )
    config.addinivalue_line(
        "markers", "contract: marks tests as contract tests"
    )


# New service-based fixtures
@pytest.fixture
def mock_db_manager():
    """Mock database manager for testing."""
    mock_manager = AsyncMock(spec=DatabaseManager)

    # Configure common mock behaviors
    mock_manager.initialize.return_value = None
    mock_manager.close.return_value = None
    mock_manager.execute_query.return_value = []
    mock_manager.get_health_status.return_value = {
        "status": "healthy",
        "response_time_ms": 50,
        "timestamp": datetime.now().timestamp()
    }

    return mock_manager


@pytest.fixture
def mock_intelligence_service():
    """Mock intelligence service for testing."""
    mock_service = AsyncMock(spec=IntelligenceService)

    # Configure default responses
    mock_service.analyze_pipeline_requirements.return_value = {
        "recommended_transformations": ["data_validation", "data_cleaning"],
        "estimated_complexity": "medium",
        "resource_requirements": {"memory_gb": 4, "cpu_cores": 2},
        "compliance_recommendations": []
    }

    mock_service.optimize_execution_strategy.return_value = {
        "optimization_recommendations": ["increase_parallelism"],
        "predicted_improvements": {"execution_time_reduction_percent": 20},
        "confidence_score": 0.8
    }

    mock_service.predict_pipeline_behavior.return_value = {
        "predicted_success_probability": 0.9,
        "estimated_execution_time_minutes": 25,
        "estimated_resource_usage": {"memory_gb": 3.5, "cpu_cores": 2},
        "risk_factors": [],
        "optimization_opportunities": []
    }

    return mock_service


@pytest.fixture
def mock_optimization_service():
    """Mock optimization service for testing."""
    mock_service = AsyncMock(spec=OptimizationService)

    # Configure default responses
    mock_service.optimize_pipeline_configuration.return_value = {
        "optimized_config": {
            "batch_size": 2000,
            "parallel_workers": 4,
            "memory_limit_gb": 8
        },
        "expected_improvements": {
            "execution_time_reduction_percent": 25,
            "cost_reduction_percent": 15
        },
        "confidence_score": 0.85
    }

    mock_service.real_time_optimization.return_value = {
        "optimization_triggered": False,
        "status": "stable",
        "monitoring_recommendations": []
    }

    return mock_service


@pytest.fixture
def mock_integration_service():
    """Mock integration service for testing."""
    mock_service = AsyncMock(spec=IntegrationService)

    # Configure default responses
    mock_service.execute_integration.return_value = {
        "success": True,
        "status_code": 200,
        "response": {"data": "test"},
        "execution_time_ms": 150,
        "integration_id": "test-integration"
    }

    mock_service.get_integration_health.return_value = {
        "integration_id": "test-integration",
        "success_rate": 0.95,
        "avg_response_time_ms": 150,
        "health_score": 0.9
    }

    return mock_service


@pytest.fixture
def pipeline_service_with_mocks(mock_db_manager, mock_intelligence_service, mock_optimization_service):
    """Create PipelineService instance with mocked dependencies."""
    return PipelineService(
        db_manager=mock_db_manager,
        intelligence_service=mock_intelligence_service,
        optimization_service=mock_optimization_service
    )


@pytest.fixture
def intelligence_service_with_mocks(mock_db_manager):
    """Create IntelligenceService instance with mocked dependencies."""
    return IntelligenceService(db_manager=mock_db_manager)


@pytest.fixture
def optimization_service_with_mocks(mock_db_manager):
    """Create OptimizationService instance with mocked dependencies."""
    return OptimizationService(db_manager=mock_db_manager)


@pytest.fixture
def integration_service_with_mocks(mock_db_manager):
    """Create IntegrationService instance with mocked dependencies."""
    return IntegrationService(db_manager=mock_db_manager)


@pytest.fixture
def sample_csv_file(temp_dir):
    """Create sample CSV file for testing."""
    csv_content = """id,name,email,age,department
1,Alice Johnson,alice@example.com,28,Engineering
2,Bob Smith,bob@example.com,35,Marketing
3,Carol Davis,carol@example.com,42,Sales
4,David Wilson,david@example.com,31,Engineering
5,Eve Brown,eve@example.com,26,Marketing
"""

    file_path = temp_dir / "sample.csv"
    file_path.write_text(csv_content)
    return file_path


@pytest.fixture
def sample_json_file(temp_dir):
    """Create sample JSON file for testing."""
    json_data = {
        "users": [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": False},
            {"id": 3, "name": "Carol", "active": True}
        ],
        "metadata": {
            "total_count": 3,
            "generated_at": "2023-01-01T00:00:00Z"
        }
    }

    file_path = temp_dir / "sample.json"
    file_path.write_text(json.dumps(json_data, indent=2))
    return file_path


@pytest.fixture
def sample_performance_data():
    """Sample performance data for testing."""
    return {
        "avg_execution_time_minutes": 25.5,
        "avg_memory_usage_gb": 3.2,
        "avg_cpu_utilization": 0.65,
        "throughput_records_per_second": 1250,
        "success_rate": 0.95,
        "error_rate": 0.05,
        "cost_per_execution": 2.50,
        "last_30_days": [
            {"date": "2023-01-01", "execution_time": 28, "success": True},
            {"date": "2023-01-02", "execution_time": 24, "success": True},
            {"date": "2023-01-03", "execution_time": 32, "success": False}
        ]
    }


@pytest.fixture
def sample_execution_context():
    """Sample execution context for testing."""
    return {
        "execution_id": "exec-test-001",
        "user_id": "test_user",
        "priority": "medium",
        "timeout_minutes": 60,
        "resources": {
            "memory_gb": 4,
            "cpu_cores": 2
        },
        "environment": "test",
        "notifications": {
            "on_success": ["email:user@example.com"],
            "on_failure": ["slack:alerts-channel"]
        }
    }


@pytest.fixture(autouse=True)
def reset_service_singletons():
    """Reset service singleton instances between tests."""
    # Reset any global state or singletons here
    yield

    # Cleanup after test
    # Reset global database manager if needed
    import src.agent_orchestrated_etl.database.connection as db_conn
    db_conn._database_manager = None
