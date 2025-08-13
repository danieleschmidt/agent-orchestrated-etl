"""Test data factories for creating test objects."""

from __future__ import annotations

import time
import uuid
from typing import Any, Dict, List, Optional

from agent_orchestrated_etl.models import (
    AgentConfiguration,
    DataQualityMetrics,
    DataSchema,
    DataSource,
    FieldSchema,
    PipelineConfig,
    PipelineExecution,
    TaskDefinition,
    TaskExecution,
)


class PipelineConfigFactory:
    """Factory for creating pipeline configuration test objects."""

    @staticmethod
    def create(
        pipeline_id: Optional[str] = None,
        name: Optional[str] = None,
        tasks: Optional[List[TaskDefinition]] = None,
        **kwargs
    ) -> PipelineConfig:
        """Create a pipeline configuration for testing."""
        pipeline_id = pipeline_id or f"test-pipeline-{uuid.uuid4().hex[:8]}"
        name = name or f"Test Pipeline {pipeline_id}"

        if not tasks:
            tasks = [
                TaskDefinitionFactory.create(
                    task_id="extract",
                    name="Extract Data",
                    task_type="extract"
                ),
                TaskDefinitionFactory.create(
                    task_id="transform",
                    name="Transform Data",
                    task_type="transform",
                    dependencies=["extract"]
                ),
                TaskDefinitionFactory.create(
                    task_id="load",
                    name="Load Data",
                    task_type="load",
                    dependencies=["transform"]
                )
            ]

        return PipelineConfig(
            pipeline_id=pipeline_id,
            name=name,
            description=f"Test pipeline configuration for {name}",
            data_source={
                "type": "file",
                "path": "/tmp/test_data.csv",
                "format": "csv"
            },
            tasks=tasks,
            validation_rules=[
                {
                    "type": "completeness",
                    "threshold": 0.9
                }
            ],
            transformation_rules=[
                {
                    "type": "add_field",
                    "field_name": "processed_at",
                    "field_value": time.time()
                }
            ],
            **kwargs
        )


class TaskDefinitionFactory:
    """Factory for creating task definition test objects."""

    @staticmethod
    def create(
        task_id: Optional[str] = None,
        name: Optional[str] = None,
        task_type: str = "generic",
        **kwargs
    ) -> TaskDefinition:
        """Create a task definition for testing."""
        task_id = task_id or f"task-{uuid.uuid4().hex[:8]}"
        name = name or f"Test Task {task_id}"

        return TaskDefinition(
            task_id=task_id,
            name=name,
            task_type=task_type,
            description=f"Test task: {name}",
            inputs={"test_input": "test_value"},
            timeout_seconds=300.0,
            retry_config={
                "max_retries": 3,
                "backoff_factor": 2.0
            },
            resource_requirements={
                "cpu": "100m",
                "memory": "256Mi"
            },
            **kwargs
        )


class PipelineExecutionFactory:
    """Factory for creating pipeline execution test objects."""

    @staticmethod
    def create(
        execution_id: Optional[str] = None,
        pipeline_id: Optional[str] = None,
        **kwargs
    ) -> PipelineExecution:
        """Create a pipeline execution for testing."""
        execution_id = execution_id or str(uuid.uuid4())
        pipeline_id = pipeline_id or f"test-pipeline-{uuid.uuid4().hex[:8]}"

        return PipelineExecution(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            status="pending",
            total_tasks=3,
            orchestrated_by="test-orchestrator",
            **kwargs
        )


class TaskExecutionFactory:
    """Factory for creating task execution test objects."""

    @staticmethod
    def create(
        execution_id: Optional[str] = None,
        task_id: Optional[str] = None,
        pipeline_execution_id: Optional[str] = None,
        **kwargs
    ) -> TaskExecution:
        """Create a task execution for testing."""
        execution_id = execution_id or str(uuid.uuid4())
        task_id = task_id or f"task-{uuid.uuid4().hex[:8]}"
        pipeline_execution_id = pipeline_execution_id or str(uuid.uuid4())

        return TaskExecution(
            execution_id=execution_id,
            task_id=task_id,
            pipeline_execution_id=pipeline_execution_id,
            status="pending",
            attempt_number=1,
            max_retries=3,
            **kwargs
        )


class AgentConfigurationFactory:
    """Factory for creating agent configuration test objects."""

    @staticmethod
    def create(
        agent_id: Optional[str] = None,
        name: Optional[str] = None,
        role: str = "generic_agent",
        **kwargs
    ) -> AgentConfiguration:
        """Create an agent configuration for testing."""
        agent_id = agent_id or f"test-agent-{uuid.uuid4().hex[:8]}"
        name = name or f"Test Agent {agent_id}"

        return AgentConfiguration(
            agent_id=agent_id,
            name=name,
            role=role,
            description=f"Test agent configuration for {name}",
            max_concurrent_tasks=5,
            task_timeout_seconds=300.0,
            capabilities=[],
            specializations=["testing"],
            **kwargs
        )


class DataQualityMetricsFactory:
    """Factory for creating data quality metrics test objects."""

    @staticmethod
    def create(
        metrics_id: Optional[str] = None,
        data_source_id: Optional[str] = None,
        overall_quality_score: float = 0.85,
        **kwargs
    ) -> DataQualityMetrics:
        """Create data quality metrics for testing."""
        metrics_id = metrics_id or str(uuid.uuid4())
        data_source_id = data_source_id or f"test-source-{uuid.uuid4().hex[:8]}"

        return DataQualityMetrics(
            metrics_id=metrics_id,
            data_source_id=data_source_id,
            overall_quality_score=overall_quality_score,
            dimension_scores={
                "completeness": 0.90,
                "accuracy": 0.85,
                "consistency": 0.80,
                "validity": 0.88,
                "uniqueness": 0.92,
                "timeliness": 0.75,
                "integrity": 0.87
            },
            record_count=1000,
            completeness_metrics={
                "total_fields": 10,
                "complete_fields": 9,
                "null_counts": {"field1": 10}
            },
            accuracy_metrics={
                "total_records": 1000,
                "accurate_records": 850
            },
            field_profiles={
                "field1": {
                    "type": "string",
                    "null_count": 10,
                    "unique_count": 950
                }
            },
            **kwargs
        )


class DataSourceFactory:
    """Factory for creating data source test objects."""

    @staticmethod
    def create(
        source_id: Optional[str] = None,
        name: Optional[str] = None,
        source_type: str = "file",
        **kwargs
    ) -> DataSource:
        """Create a data source for testing."""
        source_id = source_id or f"test-source-{uuid.uuid4().hex[:8]}"
        name = name or f"Test Data Source {source_id}"

        connection_configs = {
            "file": {
                "path": "/tmp/test_data.csv",
                "format": "csv"
            },
            "database": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "username": "test_user"
            },
            "s3": {
                "bucket": "test-bucket",
                "prefix": "test-data/",
                "region": "us-east-1"
            },
            "api": {
                "base_url": "https://api.example.com",
                "authentication": {"type": "api_key"}
            }
        }

        return DataSource(
            source_id=source_id,
            name=name,
            source_type=source_type,
            connection_config=connection_configs.get(source_type, {}),
            data_format="csv",
            description=f"Test data source: {name}",
            **kwargs
        )


class DataSchemaFactory:
    """Factory for creating data schema test objects."""

    @staticmethod
    def create(
        schema_id: Optional[str] = None,
        name: Optional[str] = None,
        fields: Optional[List[FieldSchema]] = None,
        **kwargs
    ) -> DataSchema:
        """Create a data schema for testing."""
        schema_id = schema_id or f"test-schema-{uuid.uuid4().hex[:8]}"
        name = name or f"Test Schema {schema_id}"

        if not fields:
            fields = [
                FieldSchema(
                    name="id",
                    data_type="integer",
                    required=True,
                    unique=True,
                    description="Unique identifier"
                ),
                FieldSchema(
                    name="name",
                    data_type="string",
                    required=True,
                    max_length=100,
                    description="Record name"
                ),
                FieldSchema(
                    name="value",
                    data_type="float",
                    required=False,
                    min_value=0.0,
                    description="Numeric value"
                ),
                FieldSchema(
                    name="created_at",
                    data_type="timestamp",
                    required=True,
                    description="Creation timestamp"
                )
            ]

        return DataSchema(
            schema_id=schema_id,
            name=name,
            description=f"Test data schema: {name}",
            fields=fields,
            primary_key=["id"],
            **kwargs
        )


class TestDataFactory:
    """Factory for creating various test data sets."""

    @staticmethod
    def create_sample_records(count: int = 10) -> List[Dict[str, Any]]:
        """Create sample data records for testing."""
        records = []
        categories = ["A", "B", "C"]

        for i in range(count):
            records.append({
                "id": i + 1,
                "name": f"Record {i + 1}",
                "category": categories[i % len(categories)],
                "value": (i + 1) * 10.5,
                "is_active": i % 2 == 0,
                "created_at": time.time() - (count - i) * 3600,
                "metadata": {
                    "source": "test",
                    "version": "1.0"
                }
            })

        return records

    @staticmethod
    def create_sample_pipeline_config() -> Dict[str, Any]:
        """Create sample pipeline configuration data."""
        return {
            "pipeline_id": "sample-pipeline",
            "name": "Sample Data Pipeline",
            "description": "A sample pipeline for testing",
            "data_source": {
                "type": "file",
                "path": "/tmp/sample_data.csv",
                "format": "csv"
            },
            "transformations": [
                {
                    "type": "filter",
                    "condition": "value > 50"
                },
                {
                    "type": "aggregate",
                    "function": "sum",
                    "group_by": ["category"]
                }
            ],
            "destination": {
                "type": "database",
                "table": "processed_data"
            },
            "schedule": {
                "type": "interval",
                "minutes": 60
            }
        }

    @staticmethod
    def create_sample_agent_metrics() -> Dict[str, Any]:
        """Create sample agent performance metrics."""
        return {
            "agent_id": "test-agent-001",
            "measurement_period_start": time.time() - 3600,
            "measurement_period_end": time.time(),
            "tasks_completed": 25,
            "tasks_failed": 2,
            "average_task_duration": 45.5,
            "success_rate": 0.92,
            "quality_score": 0.88,
            "cpu_utilization": {
                "average": 65.5,
                "peak": 85.2
            },
            "memory_utilization": {
                "average": 512.0,
                "peak": 768.0
            }
        }

    @staticmethod
    def create_error_scenarios() -> List[Dict[str, Any]]:
        """Create various error scenarios for testing."""
        return [
            {
                "type": "connection_error",
                "message": "Failed to connect to database",
                "code": "DB_CONNECTION_FAILED",
                "retryable": True
            },
            {
                "type": "validation_error",
                "message": "Data validation failed",
                "code": "VALIDATION_FAILED",
                "retryable": False,
                "details": {
                    "field": "email",
                    "constraint": "format"
                }
            },
            {
                "type": "timeout_error",
                "message": "Operation timed out",
                "code": "TIMEOUT",
                "retryable": True,
                "timeout_seconds": 30
            },
            {
                "type": "permission_error",
                "message": "Access denied",
                "code": "ACCESS_DENIED",
                "retryable": False
            },
            {
                "type": "resource_error",
                "message": "Insufficient resources",
                "code": "RESOURCE_EXHAUSTED",
                "retryable": True
            }
        ]


class MockResponseFactory:
    """Factory for creating mock responses for external services."""

    @staticmethod
    def create_s3_list_response(objects: Optional[List[str]] = None) -> Dict[str, Any]:
        """Create mock S3 list objects response."""
        if objects is None:
            objects = ["data/file1.csv", "data/file2.csv"]

        return {
            "Contents": [
                {
                    "Key": obj,
                    "Size": 1024,
                    "LastModified": time.time(),
                    "ETag": f'"{uuid.uuid4().hex}"'
                }
                for obj in objects
            ]
        }

    @staticmethod
    def create_database_response(rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Create mock database query response."""
        if rows is None:
            rows = TestDataFactory.create_sample_records(5)

        return {
            "rows": rows,
            "row_count": len(rows),
            "columns": list(rows[0].keys()) if rows else [],
            "execution_time": 0.05
        }

    @staticmethod
    def create_api_response(data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Create mock API response."""
        if data is None:
            data = {"message": "success", "data": TestDataFactory.create_sample_records(3)}

        return {
            "status_code": 200,
            "headers": {"Content-Type": "application/json"},
            "json": data,
            "response_time": 0.25
        }

    @staticmethod
    def create_llm_response(content: Optional[str] = None) -> Dict[str, Any]:
        """Create mock LLM response."""
        if content is None:
            content = "This is a mock response from the language model."

        return {
            "choices": [
                {
                    "message": {"content": content},
                    "finish_reason": "stop"
                }
            ],
            "usage": {
                "prompt_tokens": 50,
                "completion_tokens": 25,
                "total_tokens": 75
            }
        }
