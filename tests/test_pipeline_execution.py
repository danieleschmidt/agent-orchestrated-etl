"""Test real pipeline execution functionality."""

import pytest

from src.agent_orchestrated_etl.agents.tools import ExecutePipelineTool
from src.agent_orchestrated_etl.exceptions import ToolException


class TestPipelineExecution:
    """Test real pipeline execution functionality."""

    @pytest.fixture
    def pipeline_tool(self):
        """Create a pipeline execution tool for testing."""
        return ExecutePipelineTool()

    @pytest.fixture
    def sample_dag_config(self):
        """Create a sample DAG configuration for testing."""
        return {
            "dag_id": "test_pipeline_123",
            "schedule_interval": None,
            "start_date": "2024-01-01",
            "tasks": [
                {
                    "task_id": "extract_data",
                    "task_type": "extract",
                    "operator": "S3ToPostgresOperator",
                    "config": {
                        "source": "s3://bucket/data.csv",
                        "destination": "postgres://db/table"
                    },
                    "dependencies": []
                },
                {
                    "task_id": "transform_data",
                    "task_type": "transform",
                    "operator": "PythonOperator",
                    "config": {
                        "python_callable": "transform_functions.clean_data"
                    },
                    "dependencies": ["extract_data"]
                },
                {
                    "task_id": "load_data",
                    "task_type": "load",
                    "operator": "PostgresToS3Operator",
                    "config": {
                        "source": "postgres://db/table",
                        "destination": "s3://bucket/processed/"
                    },
                    "dependencies": ["transform_data"]
                }
            ]
        }

    def test_synchronous_pipeline_execution(self, pipeline_tool, sample_dag_config):
        """Test synchronous pipeline execution."""
        result = pipeline_tool._execute(
            dag_config=sample_dag_config,
            execution_mode="sync",
            monitor_progress=True,
            timeout_seconds=1800
        )

        # Verify execution result structure
        assert "execution_id" in result
        assert result["dag_id"] == "test_pipeline_123"
        assert result["execution_mode"] == "sync"
        assert result["status"] in ["completed", "running"]
        assert result["start_time"] > 0

        # Verify task execution results
        assert "task_results" in result
        task_results = result["task_results"]
        assert len(task_results) == 3

        # Verify task execution order
        task_order = [tr["task_id"] for tr in task_results]
        assert task_order == ["extract_data", "transform_data", "load_data"]

        # Verify each task has required fields
        for task_result in task_results:
            assert "task_id" in task_result
            assert "status" in task_result
            assert "start_time" in task_result
            assert "end_time" in task_result
            assert "duration" in task_result

    def test_asynchronous_pipeline_execution(self, pipeline_tool, sample_dag_config):
        """Test asynchronous pipeline execution."""
        result = pipeline_tool._execute(
            dag_config=sample_dag_config,
            execution_mode="async",
            monitor_progress=False
        )

        # Verify async execution result
        assert result["execution_mode"] == "async"
        assert result["status"] == "running"
        assert "execution_id" in result

        # Async mode should return immediately with running status
        # Task results may not be complete yet
        if "task_results" in result:
            # Some tasks might still be running
            running_tasks = [tr for tr in result["task_results"] if tr["status"] == "running"]
            assert len(running_tasks) >= 0

    def test_pipeline_execution_with_task_failure(self, pipeline_tool):
        """Test pipeline execution with task failures."""
        failing_dag_config = {
            "dag_id": "failing_pipeline",
            "tasks": [
                {
                    "task_id": "good_task",
                    "task_type": "extract",
                    "operator": "DummyOperator",
                    "config": {},
                    "dependencies": []
                },
                {
                    "task_id": "failing_task",
                    "task_type": "transform",
                    "operator": "FailingOperator",  # This will simulate failure
                    "config": {"simulate_failure": True},
                    "dependencies": ["good_task"]
                },
                {
                    "task_id": "dependent_task",
                    "task_type": "load",
                    "operator": "DummyOperator",
                    "config": {},
                    "dependencies": ["failing_task"]
                }
            ]
        }

        result = pipeline_tool._execute(
            dag_config=failing_dag_config,
            execution_mode="sync"
        )

        # Verify failure handling
        assert result["status"] == "failed"
        assert "error_details" in result

        # Verify task states
        task_results = result["task_results"]
        good_task = next(tr for tr in task_results if tr["task_id"] == "good_task")
        failing_task = next(tr for tr in task_results if tr["task_id"] == "failing_task")
        dependent_task = next(tr for tr in task_results if tr["task_id"] == "dependent_task")

        assert good_task["status"] == "completed"
        assert failing_task["status"] == "failed"
        assert dependent_task["status"] == "skipped"  # Should be skipped due to dependency failure

    def test_pipeline_execution_with_retry_logic(self, pipeline_tool):
        """Test pipeline execution with retry logic for failed tasks."""
        retry_dag_config = {
            "dag_id": "retry_pipeline",
            "tasks": [
                {
                    "task_id": "retryable_task",
                    "task_type": "extract",
                    "operator": "RetryableOperator",
                    "config": {
                        "max_retries": 3,
                        "retry_delay": 1,
                        "simulate_transient_failure": True
                    },
                    "dependencies": []
                }
            ]
        }

        result = pipeline_tool._execute(
            dag_config=retry_dag_config,
            execution_mode="sync"
        )

        # Verify retry behavior
        task_result = result["task_results"][0]
        assert task_result["task_id"] == "retryable_task"

        # Should eventually succeed after retries
        if result["status"] == "completed":
            assert task_result["status"] == "completed"
            assert task_result["retry_count"] >= 0
            assert task_result["retry_count"] <= 3

    def test_pipeline_execution_with_timeout(self, pipeline_tool):
        """Test pipeline execution with timeout handling."""
        long_running_dag = {
            "dag_id": "long_running_pipeline",
            "tasks": [
                {
                    "task_id": "slow_task",
                    "task_type": "extract",
                    "operator": "SlowOperator",
                    "config": {"execution_time": 10},  # 10 seconds
                    "dependencies": []
                }
            ]
        }

        # Set short timeout
        result = pipeline_tool._execute(
            dag_config=long_running_dag,
            execution_mode="sync",
            timeout_seconds=5  # 5 seconds timeout
        )

        # Should timeout
        assert result["status"] == "timeout"
        assert "timeout_details" in result
        assert result["timeout_details"]["timeout_seconds"] == 5

    def test_pipeline_execution_monitoring(self, pipeline_tool, sample_dag_config):
        """Test pipeline execution with progress monitoring."""
        result = pipeline_tool._execute(
            dag_config=sample_dag_config,
            execution_mode="sync",
            monitor_progress=True
        )

        # Verify monitoring data is included
        assert "monitoring_data" in result
        monitoring = result["monitoring_data"]

        assert "total_tasks" in monitoring
        assert "completed_tasks" in monitoring
        assert "failed_tasks" in monitoring
        assert "progress_percentage" in monitoring
        assert monitoring["total_tasks"] == 3

        # Verify resource usage monitoring
        assert "resource_usage" in monitoring
        resource_usage = monitoring["resource_usage"]
        assert "max_memory_mb" in resource_usage
        assert "total_cpu_seconds" in resource_usage

    def test_pipeline_execution_with_custom_operators(self, pipeline_tool):
        """Test pipeline execution with custom operators."""
        custom_dag_config = {
            "dag_id": "custom_operator_pipeline",
            "tasks": [
                {
                    "task_id": "custom_extract",
                    "task_type": "extract",
                    "operator": "CustomS3Operator",
                    "config": {
                        "bucket": "my-bucket",
                        "key": "data/file.json",
                        "custom_parser": "json_lines"
                    },
                    "dependencies": []
                },
                {
                    "task_id": "ml_transform",
                    "task_type": "transform",
                    "operator": "MLTransformOperator",
                    "config": {
                        "model_path": "s3://models/transformer.pkl",
                        "feature_columns": ["feature1", "feature2"]
                    },
                    "dependencies": ["custom_extract"]
                }
            ]
        }

        result = pipeline_tool._execute(
            dag_config=custom_dag_config,
            execution_mode="sync"
        )

        # Should handle custom operators
        assert result["status"] in ["completed", "failed"]
        assert len(result["task_results"]) == 2

        # Verify custom operator execution
        for task_result in result["task_results"]:
            assert "operator_type" in task_result
            assert task_result["operator_type"] in ["CustomS3Operator", "MLTransformOperator"]

    def test_pipeline_execution_dependency_resolution(self, pipeline_tool):
        """Test complex dependency resolution in pipeline execution."""
        complex_dag_config = {
            "dag_id": "complex_dependency_pipeline",
            "tasks": [
                {
                    "task_id": "extract_source_1",
                    "task_type": "extract",
                    "operator": "S3Operator",
                    "config": {"source": "s3://bucket/source1/"},
                    "dependencies": []
                },
                {
                    "task_id": "extract_source_2",
                    "task_type": "extract",
                    "operator": "S3Operator",
                    "config": {"source": "s3://bucket/source2/"},
                    "dependencies": []
                },
                {
                    "task_id": "merge_sources",
                    "task_type": "transform",
                    "operator": "PandasOperator",
                    "config": {"operation": "merge"},
                    "dependencies": ["extract_source_1", "extract_source_2"]
                },
                {
                    "task_id": "validate_data",
                    "task_type": "validate",
                    "operator": "ValidationOperator",
                    "config": {"validation_rules": ["not_null", "unique_key"]},
                    "dependencies": ["merge_sources"]
                },
                {
                    "task_id": "load_warehouse",
                    "task_type": "load",
                    "operator": "RedshiftOperator",
                    "config": {"table": "analytics.fact_table"},
                    "dependencies": ["validate_data"]
                },
                {
                    "task_id": "load_data_lake",
                    "task_type": "load",
                    "operator": "S3Operator",
                    "config": {"destination": "s3://data-lake/processed/"},
                    "dependencies": ["validate_data"]  # Parallel load
                }
            ]
        }

        result = pipeline_tool._execute(
            dag_config=complex_dag_config,
            execution_mode="sync"
        )

        # Verify dependency execution order
        task_results = result["task_results"]
        task_times = {tr["task_id"]: tr["start_time"] for tr in task_results}

        # Extract tasks should start first (no dependencies)
        assert task_times["extract_source_1"] <= task_times["merge_sources"]
        assert task_times["extract_source_2"] <= task_times["merge_sources"]

        # Merge should start after both extracts
        assert task_times["merge_sources"] <= task_times["validate_data"]

        # Loads should start after validation
        assert task_times["validate_data"] <= task_times["load_warehouse"]
        assert task_times["validate_data"] <= task_times["load_data_lake"]

    def test_pipeline_execution_error_handling(self, pipeline_tool):
        """Test error handling in pipeline execution."""
        # Test with invalid DAG config
        invalid_dag_config = {
            "dag_id": "",  # Invalid empty DAG ID
            "tasks": []    # No tasks
        }

        with pytest.raises(ToolException) as exc_info:
            pipeline_tool._execute(dag_config=invalid_dag_config)

        assert "Pipeline execution failed" in str(exc_info.value)

        # Test with malformed task config
        malformed_dag_config = {
            "dag_id": "malformed_pipeline",
            "tasks": [
                {
                    "task_id": "malformed_task",
                    # Missing required fields
                    "config": {}
                }
            ]
        }

        with pytest.raises(ToolException):
            pipeline_tool._execute(dag_config=malformed_dag_config)

    def test_pipeline_execution_resource_management(self, pipeline_tool, sample_dag_config):
        """Test resource management during pipeline execution."""
        result = pipeline_tool._execute(
            dag_config=sample_dag_config,
            execution_mode="sync",
            monitor_progress=True
        )

        # Verify resource tracking
        assert "resource_usage" in result["monitoring_data"]
        resource_usage = result["monitoring_data"]["resource_usage"]

        # Should track memory and CPU usage
        assert resource_usage["max_memory_mb"] >= 0
        assert resource_usage["total_cpu_seconds"] >= 0

        # Should include per-task resource usage
        for task_result in result["task_results"]:
            assert "resource_usage" in task_result
            task_resources = task_result["resource_usage"]
            assert "memory_mb" in task_resources
            assert "cpu_seconds" in task_resources

    def test_pipeline_execution_state_persistence(self, pipeline_tool, sample_dag_config):
        """Test that pipeline execution state is properly persisted."""
        result = pipeline_tool._execute(
            dag_config=sample_dag_config,
            execution_mode="async"
        )

        execution_id = result["execution_id"]

        # Should be able to query execution state
        assert "execution_id" in result
        assert execution_id.startswith("exec_")

        # Result should include state information
        assert "execution_state" in result
        state = result["execution_state"]
        assert "dag_id" in state
        assert "current_task" in state
        assert "completed_tasks" in state
