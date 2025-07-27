"""Test fixtures for sample data."""

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import pytest


@pytest.fixture
def sample_csv_data() -> str:
    """Sample CSV data for testing extraction."""
    return """id,name,email,age,department
1,John Doe,john.doe@example.com,30,Engineering
2,Jane Smith,jane.smith@example.com,28,Marketing
3,Bob Johnson,bob.johnson@example.com,35,Sales
4,Alice Brown,alice.brown@example.com,32,Engineering
5,Charlie Wilson,charlie.wilson@example.com,29,HR"""


@pytest.fixture
def sample_json_data() -> Dict[str, Any]:
    """Sample JSON data for testing extraction."""
    return {
        "users": [
            {"id": 1, "name": "John Doe", "active": True, "metadata": {"role": "admin"}},
            {"id": 2, "name": "Jane Smith", "active": False, "metadata": {"role": "user"}},
            {"id": 3, "name": "Bob Johnson", "active": True, "metadata": {"role": "user"}},
        ],
        "pagination": {"page": 1, "total_pages": 1, "total_records": 3},
    }


@pytest.fixture
def sample_database_records() -> List[Dict[str, Any]]:
    """Sample database records for testing."""
    return [
        {"id": 1, "product_name": "Widget A", "price": 19.99, "category": "Electronics"},
        {"id": 2, "product_name": "Widget B", "price": 29.99, "category": "Electronics"},
        {"id": 3, "product_name": "Tool C", "price": 39.99, "category": "Tools"},
        {"id": 4, "product_name": "Book D", "price": 15.99, "category": "Books"},
    ]


@pytest.fixture
def sample_dataframe(sample_database_records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Sample pandas DataFrame for testing transformations."""
    return pd.DataFrame(sample_database_records)


@pytest.fixture
def malformed_csv_data() -> str:
    """Malformed CSV data for testing error handling."""
    return """id,name,email,age
1,John Doe,john.doe@example.com,30
2,Jane Smith,invalid-email,twenty-eight
3,Bob Johnson,bob.johnson@example.com"""


@pytest.fixture
def sample_s3_objects() -> List[Dict[str, Any]]:
    """Sample S3 object metadata for testing."""
    return [
        {
            "Key": "data/2023/01/sales.csv",
            "Size": 1024,
            "LastModified": "2023-01-01T00:00:00Z",
            "ETag": '"abc123"',
        },
        {
            "Key": "data/2023/01/customers.json",
            "Size": 2048,
            "LastModified": "2023-01-02T00:00:00Z",
            "ETag": '"def456"',
        },
    ]


@pytest.fixture
def sample_api_response() -> Dict[str, Any]:
    """Sample API response for testing."""
    return {
        "status": "success",
        "data": [
            {"timestamp": "2023-01-01T00:00:00Z", "value": 100, "metric": "cpu_usage"},
            {"timestamp": "2023-01-01T01:00:00Z", "value": 85, "metric": "cpu_usage"},
            {"timestamp": "2023-01-01T02:00:00Z", "value": 92, "metric": "cpu_usage"},
        ],
        "metadata": {"source": "monitoring", "version": "1.0"},
    }


@pytest.fixture
def temporary_csv_file(tmp_path: Path, sample_csv_data: str) -> Path:
    """Create a temporary CSV file with sample data."""
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(sample_csv_data)
    return csv_file


@pytest.fixture
def temporary_json_file(tmp_path: Path, sample_json_data: Dict[str, Any]) -> Path:
    """Create a temporary JSON file with sample data."""
    json_file = tmp_path / "sample.json"
    json_file.write_text(json.dumps(sample_json_data, indent=2))
    return json_file


@pytest.fixture
def sample_pipeline_config() -> Dict[str, Any]:
    """Sample pipeline configuration for testing."""
    return {
        "source": {
            "type": "s3",
            "bucket": "test-bucket",
            "prefix": "data/",
        },
        "transformation": {
            "rules": [
                {"type": "filter", "column": "age", "operator": ">", "value": 25},
                {"type": "rename", "mapping": {"email": "email_address"}},
            ]
        },
        "destination": {
            "type": "postgresql",
            "table": "processed_users",
            "mode": "append",
        },
        "monitoring": {"enabled": True, "alert_threshold": 0.95},
    }


@pytest.fixture
def sample_dag_metadata() -> Dict[str, Any]:
    """Sample DAG metadata for testing."""
    return {
        "dag_id": "test_pipeline_20230101",
        "schedule_interval": "@daily",
        "start_date": "2023-01-01",
        "catchup": False,
        "tags": ["etl", "daily", "automated"],
        "max_active_runs": 1,
        "default_args": {
            "owner": "data-team",
            "retries": 3,
            "retry_delay": "5m",
            "email_on_failure": True,
            "email_on_retry": False,
        },
    }