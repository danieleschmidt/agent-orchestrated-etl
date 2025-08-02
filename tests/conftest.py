import sys
import pathlib
import pytest
import asyncio
from typing import Generator, Dict, Any
from unittest.mock import Mock, MagicMock
import tempfile
import shutil
from pathlib import Path
import os

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'src'))

from agent_orchestrated_etl.config import Config
from agent_orchestrated_etl.agents.base_agent import BaseAgent
from agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent
from agent_orchestrated_etl.agents.etl_agent import ETLAgent
from agent_orchestrated_etl.agents.monitor_agent import MonitorAgent

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