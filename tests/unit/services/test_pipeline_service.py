"""Unit tests for PipelineService."""

import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent_orchestrated_etl.models.pipeline_models import (
    PipelineStatus,
)
from src.agent_orchestrated_etl.services.pipeline_service import PipelineService


class TestPipelineService:
    """Test cases for PipelineService."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = AsyncMock()
        self.mock_intelligence_service = AsyncMock()
        self.mock_optimization_service = AsyncMock()

        self.pipeline_service = PipelineService(
            db_manager=self.mock_db_manager,
            intelligence_service=self.mock_intelligence_service,
            optimization_service=self.mock_optimization_service
        )

    @pytest.mark.asyncio
    async def test_create_pipeline_success(self):
        """Test successful pipeline creation."""
        source_config = {
            "type": "file",
            "path": "/data/test.csv",
            "format": "csv"
        }
        transformation_rules = [
            {"type": "data_cleaning", "parameters": {"remove_nulls": True}}
        ]
        load_config = {
            "type": "database",
            "table": "processed_data",
            "connection": "postgres://localhost/test"
        }

        # Mock intelligence service analysis
        self.mock_intelligence_service.analyze_pipeline_requirements.return_value = {
            "recommended_transformations": ["data_validation"],
            "estimated_complexity": "medium",
            "resource_requirements": {"memory": "2GB", "cpu": "2 cores"}
        }

        # Mock optimization service
        self.mock_optimization_service.optimize_pipeline_configuration.return_value = {
            "batch_size": 1000,
            "parallel_tasks": 4,
            "memory_limit": "2GB"
        }

        result = await self.pipeline_service.create_pipeline(
            source_config=source_config,
            transformation_rules=transformation_rules,
            load_config=load_config,
            pipeline_id="test-pipeline-001"
        )

        assert result["pipeline_id"] == "test-pipeline-001"
        assert result["status"] == "created"
        assert "source_analysis" in result
        assert result["transformations"] == transformation_rules
        assert result["load_target"]["type"] == "database"

        # Verify intelligence service was called
        self.mock_intelligence_service.analyze_pipeline_requirements.assert_called_once()

        # Verify optimization service was called
        self.mock_optimization_service.optimize_pipeline_configuration.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_pipeline_with_auto_id(self):
        """Test pipeline creation with auto-generated ID."""
        source_config = {"type": "api", "url": "https://api.example.com/data"}
        transformation_rules = []
        load_config = {"type": "file", "path": "/output/result.json"}

        self.mock_intelligence_service.analyze_pipeline_requirements.return_value = {
            "recommended_transformations": [],
            "estimated_complexity": "low"
        }
        self.mock_optimization_service.optimize_pipeline_configuration.return_value = {
            "batch_size": 500
        }

        result = await self.pipeline_service.create_pipeline(
            source_config=source_config,
            transformation_rules=transformation_rules,
            load_config=load_config
        )

        assert "pipeline_id" in result
        assert result["pipeline_id"].startswith("pipeline_")
        assert result["status"] == "created"

    @pytest.mark.asyncio
    async def test_execute_pipeline_success(self):
        """Test successful pipeline execution."""
        pipeline_id = "test-pipeline-001"
        execution_context = {
            "user_id": "user123",
            "priority": "high",
            "timeout": 3600
        }

        # Mock pipeline execution creation
        mock_execution = MagicMock()
        mock_execution.id = "exec-123"
        mock_execution.status = PipelineStatus.RUNNING

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.return_value = mock_execution.id
            mock_repo.get_pipeline_execution.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.execute_pipeline(
                pipeline_id=pipeline_id,
                execution_context=execution_context
            )

            assert result["execution_id"] == mock_execution.id
            assert result["pipeline_id"] == pipeline_id
            assert result["status"] == "running"
            assert result["context"] == execution_context

            # Verify repository calls
            mock_repo.create_pipeline_execution.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_pipeline_not_found(self):
        """Test pipeline execution when pipeline doesn't exist."""
        pipeline_id = "non-existent-pipeline"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.side_effect = Exception("Pipeline not found")
            mock_repo_class.return_value = mock_repo

            with pytest.raises(Exception, match="Pipeline not found"):
                await self.pipeline_service.execute_pipeline(pipeline_id=pipeline_id)

    @pytest.mark.asyncio
    async def test_get_pipeline_status_success(self):
        """Test getting pipeline status."""
        pipeline_id = "test-pipeline-001"

        mock_execution = MagicMock()
        mock_execution.pipeline_id = pipeline_id
        mock_execution.status = PipelineStatus.COMPLETED
        mock_execution.start_time = datetime.now()
        mock_execution.end_time = datetime.now()
        mock_execution.metrics = {"processed_records": 1000, "errors": 0}

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_latest_execution.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.get_pipeline_status(pipeline_id)

            assert result["pipeline_id"] == pipeline_id
            assert result["status"] == "completed"
            assert result["metrics"]["processed_records"] == 1000

    @pytest.mark.asyncio
    async def test_get_pipeline_status_not_found(self):
        """Test getting status for non-existent pipeline."""
        pipeline_id = "non-existent-pipeline"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_latest_execution.return_value = None
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.get_pipeline_status(pipeline_id)

            assert result is None

    @pytest.mark.asyncio
    async def test_list_pipelines(self):
        """Test listing all pipelines."""
        mock_executions = [
            MagicMock(pipeline_id="pipeline-001", status=PipelineStatus.RUNNING),
            MagicMock(pipeline_id="pipeline-002", status=PipelineStatus.COMPLETED),
            MagicMock(pipeline_id="pipeline-003", status=PipelineStatus.FAILED)
        ]

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.list_pipeline_executions.return_value = mock_executions
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.list_pipelines()

            assert len(result) == 3
            assert result[0]["pipeline_id"] == "pipeline-001"
            assert result[0]["status"] == "running"

    @pytest.mark.asyncio
    async def test_analyze_source_file(self):
        """Test source analysis for file sources."""
        source_config = {
            "type": "file",
            "path": "/data/test.csv",
            "format": "csv"
        }

        with patch('os.path.exists', return_value=True), \
             patch('os.path.getsize', return_value=1024000), \
             patch('builtins.open', create=True) as mock_open:

            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            mock_file.readline.return_value = "id,name,email\n"
            mock_open.return_value = mock_file

            result = await self.pipeline_service._analyze_source(source_config)

            assert result["type"] == "file"
            assert result["exists"] is True
            assert result["size_bytes"] == 1024000
            assert result["estimated_records"] > 0
            assert "id" in result["sample_columns"]

    @pytest.mark.asyncio
    async def test_analyze_source_database(self):
        """Test source analysis for database sources."""
        source_config = {
            "type": "database",
            "connection": "postgresql://localhost/test",
            "table": "users"
        }

        # Mock database connection test
        self.mock_db_manager.execute_query.return_value = [{"count": 1000}]

        result = await self.pipeline_service._analyze_source(source_config)

        assert result["type"] == "database"
        assert result["record_count"] == 1000
        assert result["connection_status"] == "success"

    @pytest.mark.asyncio
    async def test_analyze_source_api(self):
        """Test source analysis for API sources."""
        source_config = {
            "type": "api",
            "url": "https://api.example.com/data",
            "method": "GET"
        }

        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.headers = {"content-type": "application/json"}
            mock_response.json.return_value = {"data": [{"id": 1, "name": "test"}]}
            mock_get.return_value.__aenter__.return_value = mock_response

            result = await self.pipeline_service._analyze_source(source_config)

            assert result["type"] == "api"
            assert result["status_code"] == 200
            assert result["content_type"] == "application/json"
            assert result["sample_response"] is not None

    @pytest.mark.asyncio
    async def test_monitor_execution(self):
        """Test pipeline execution monitoring."""
        execution_id = "exec-123"

        mock_execution = MagicMock()
        mock_execution.id = execution_id
        mock_execution.status = PipelineStatus.RUNNING
        mock_execution.progress = 0.5
        mock_execution.metrics = {"processed_records": 500}

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_pipeline_execution.return_value = mock_execution
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.monitor_execution(execution_id)

            assert result["execution_id"] == execution_id
            assert result["status"] == "running"
            assert result["progress"] == 0.5
            assert result["metrics"]["processed_records"] == 500

    @pytest.mark.asyncio
    async def test_cancel_execution(self):
        """Test pipeline execution cancellation."""
        execution_id = "exec-123"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.update_pipeline_execution.return_value = True
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.cancel_execution(execution_id)

            assert result["execution_id"] == execution_id
            assert result["status"] == "cancelled"

            # Verify repository call
            mock_repo.update_pipeline_execution.assert_called_once_with(
                execution_id, status=PipelineStatus.CANCELLED
            )

    @pytest.mark.asyncio
    async def test_get_execution_logs(self):
        """Test getting execution logs."""
        execution_id = "exec-123"

        mock_logs = [
            {"timestamp": "2023-01-01T10:00:00Z", "level": "INFO", "message": "Starting pipeline"},
            {"timestamp": "2023-01-01T10:01:00Z", "level": "DEBUG", "message": "Processing batch 1"},
            {"timestamp": "2023-01-01T10:02:00Z", "level": "ERROR", "message": "Failed to process record"}
        ]

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_execution_logs.return_value = mock_logs
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.get_execution_logs(execution_id)

            assert len(result["logs"]) == 3
            assert result["logs"][0]["level"] == "INFO"
            assert result["logs"][2]["level"] == "ERROR"

    @pytest.mark.asyncio
    async def test_retry_failed_execution(self):
        """Test retrying a failed pipeline execution."""
        execution_id = "exec-123"

        mock_execution = MagicMock()
        mock_execution.id = execution_id
        mock_execution.pipeline_id = "pipeline-001"
        mock_execution.status = PipelineStatus.FAILED

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_pipeline_execution.return_value = mock_execution
            mock_repo.create_pipeline_execution.return_value = "exec-124"
            mock_repo_class.return_value = mock_repo

            result = await self.pipeline_service.retry_execution(execution_id)

            assert result["original_execution_id"] == execution_id
            assert result["new_execution_id"] == "exec-124"
            assert result["status"] == "retry_scheduled"

    def test_init_with_default_services(self):
        """Test initialization with default services."""
        service = PipelineService(db_manager=self.mock_db_manager)

        assert service.db_manager == self.mock_db_manager
        assert service.intelligence_service is not None
        assert service.optimization_service is not None

    @pytest.mark.asyncio
    async def test_error_handling_in_execution(self):
        """Test error handling during pipeline execution."""
        pipeline_id = "test-pipeline-001"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.side_effect = Exception("Database error")
            mock_repo_class.return_value = mock_repo

            with pytest.raises(Exception, match="Database error"):
                await self.pipeline_service.execute_pipeline(pipeline_id)

    @pytest.mark.asyncio
    async def test_concurrent_pipeline_execution(self):
        """Test concurrent pipeline executions."""
        pipeline_ids = ["pipeline-001", "pipeline-002", "pipeline-003"]

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.side_effect = ["exec-1", "exec-2", "exec-3"]
            mock_repo_class.return_value = mock_repo

            # Execute pipelines concurrently
            tasks = [
                self.pipeline_service.execute_pipeline(pid)
                for pid in pipeline_ids
            ]
            results = await asyncio.gather(*tasks)

            assert len(results) == 3
            assert all("execution_id" in result for result in results)
