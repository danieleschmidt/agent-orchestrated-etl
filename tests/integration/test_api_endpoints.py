"""Integration tests for API endpoints."""

from unittest.mock import AsyncMock, patch

import pytest

try:
    from fastapi.testclient import TestClient
    from httpx import AsyncClient
    FASTAPI_AVAILABLE = True
except ImportError:
    TestClient = None
    AsyncClient = None
    FASTAPI_AVAILABLE = False

from src.agent_orchestrated_etl.api.app import create_app


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestHealthEndpoints:
    """Test health check endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_health_check_healthy(self):
        """Test health check when system is healthy."""
        with patch('src.agent_orchestrated_etl.database.connection.get_database_manager') as mock_db:
            mock_manager = AsyncMock()
            mock_manager.get_health_status.return_value = {"status": "healthy", "response_time_ms": 50}
            mock_db.return_value = mock_manager

            response = self.client.get("/health/")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "healthy"
            assert "timestamp" in data
            assert "details" in data
            assert data["details"]["database"]["status"] == "healthy"

    def test_health_check_unhealthy(self):
        """Test health check when system is unhealthy."""
        with patch('src.agent_orchestrated_etl.database.connection.get_database_manager') as mock_db:
            mock_manager = AsyncMock()
            mock_manager.get_health_status.side_effect = Exception("Database connection failed")
            mock_db.return_value = mock_manager

            response = self.client.get("/health/")

            assert response.status_code == 200  # Health endpoint should always return 200
            data = response.json()
            assert data["status"] == "unhealthy"
            assert "error" in data["details"]

    def test_readiness_check_ready(self):
        """Test readiness check when system is ready."""
        with patch('src.agent_orchestrated_etl.database.connection.get_database_manager') as mock_db:
            mock_manager = AsyncMock()
            mock_manager.execute_query.return_value = {"result": 1}
            mock_db.return_value = mock_manager

            response = self.client.get("/health/ready")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ready"

    def test_readiness_check_not_ready(self):
        """Test readiness check when system is not ready."""
        with patch('src.agent_orchestrated_etl.database.connection.get_database_manager') as mock_db:
            mock_manager = AsyncMock()
            mock_manager.execute_query.side_effect = Exception("Database not ready")
            mock_db.return_value = mock_manager

            response = self.client.get("/health/ready")

            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "not_ready"
            assert "error" in data


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestPipelineEndpoints:
    """Test pipeline management endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_create_pipeline(self):
        """Test pipeline creation."""
        pipeline_data = {
            "pipeline_id": "test-pipeline-001",
            "name": "Test Pipeline",
            "description": "A test pipeline for integration testing",
            "data_source": {
                "type": "file",
                "path": "/data/test.csv"
            },
            "transformations": [
                {
                    "type": "data_cleaning",
                    "parameters": {"remove_nulls": True}
                }
            ],
            "destination": {
                "type": "database",
                "table": "processed_data"
            }
        }

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.return_value = "exec-123"
            mock_repo_class.return_value = mock_repo

            response = self.client.post("/api/v1/pipelines/", json=pipeline_data)

            assert response.status_code == 200
            data = response.json()
            assert data["pipeline_id"] == "test-pipeline-001"
            assert data["name"] == "Test Pipeline"
            assert data["status"] == "created"

    def test_get_pipeline(self):
        """Test getting pipeline by ID."""
        pipeline_id = "test-pipeline-001"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_execution = AsyncMock()
            mock_execution.pipeline_id = pipeline_id
            mock_execution.status.value = "completed"
            mock_repo.list_pipeline_executions.return_value = [mock_execution]
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/pipelines/{pipeline_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["pipeline_id"] == pipeline_id
            assert data["status"] == "completed"

    def test_get_pipeline_not_found(self):
        """Test getting non-existent pipeline."""
        pipeline_id = "non-existent-pipeline"

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.list_pipeline_executions.return_value = []
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/pipelines/{pipeline_id}")

            assert response.status_code == 404
            assert "Pipeline not found" in response.json()["detail"]

    def test_list_pipelines(self):
        """Test listing pipelines."""
        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_execution1 = AsyncMock()
            mock_execution1.pipeline_id = "pipeline-001"
            mock_execution1.status.value = "completed"
            mock_execution2 = AsyncMock()
            mock_execution2.pipeline_id = "pipeline-002"
            mock_execution2.status.value = "running"
            mock_repo.list_pipeline_executions.return_value = [mock_execution1, mock_execution2]
            mock_repo_class.return_value = mock_repo

            response = self.client.get("/api/v1/pipelines/")

            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
            assert data[0]["pipeline_id"] == "pipeline-001"
            assert data[1]["pipeline_id"] == "pipeline-002"

    def test_execute_pipeline(self):
        """Test pipeline execution."""
        pipeline_id = "test-pipeline-001"
        execution_params = {
            "input_data": "/data/input.csv",
            "output_path": "/data/output/"
        }

        response = self.client.post(
            f"/api/v1/pipelines/{pipeline_id}/execute",
            json=execution_params
        )

        assert response.status_code == 202
        data = response.json()
        assert data["pipeline_id"] == pipeline_id
        assert data["status"] == "scheduled"
        assert "execution_id" in data
        assert data["parameters"] == execution_params


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestAgentEndpoints:
    """Test agent management endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_list_agents(self):
        """Test listing agents."""
        with patch('src.agent_orchestrated_etl.database.repositories.AgentRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_agents = [
                {
                    "id": "agent-001",
                    "status": "active",
                    "last_heartbeat": 1234567890.0,
                    "metrics": {"tasks_completed": 10}
                },
                {
                    "id": "agent-002",
                    "status": "idle",
                    "last_heartbeat": 1234567885.0,
                    "metrics": {"tasks_completed": 5}
                }
            ]
            mock_repo.list_active_agents.return_value = mock_agents
            mock_repo_class.return_value = mock_repo

            response = self.client.get("/api/v1/agents/")

            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
            assert data[0]["agent_id"] == "agent-001"
            assert data[0]["status"] == "active"
            assert data[1]["agent_id"] == "agent-002"
            assert data[1]["status"] == "idle"

    def test_get_agent_status(self):
        """Test getting agent status."""
        agent_id = "agent-001"

        with patch('src.agent_orchestrated_etl.database.repositories.AgentRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_agent = {
                "id": agent_id,
                "status": "active",
                "last_heartbeat": 1234567890.0,
                "metrics": {"tasks_completed": 10, "cpu_usage": 45.2}
            }
            mock_repo.get_agent_status.return_value = mock_agent
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/agents/{agent_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["agent_id"] == agent_id
            assert data["status"] == "active"
            assert data["metrics"]["tasks_completed"] == 10

    def test_get_agent_status_not_found(self):
        """Test getting status for non-existent agent."""
        agent_id = "non-existent-agent"

        with patch('src.agent_orchestrated_etl.database.repositories.AgentRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_agent_status.return_value = None
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/agents/{agent_id}")

            assert response.status_code == 404
            assert "Agent not found" in response.json()["detail"]

    def test_agent_heartbeat(self):
        """Test agent heartbeat."""
        agent_id = "agent-001"
        metrics_data = {
            "cpu_usage": 67.3,
            "memory_usage": 45.8,
            "tasks_completed": 15,
            "tasks_failed": 1
        }

        with patch('src.agent_orchestrated_etl.database.repositories.AgentRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.update_agent_status.return_value = True
            mock_repo_class.return_value = mock_repo

            response = self.client.post(
                f"/api/v1/agents/{agent_id}/heartbeat",
                json=metrics_data
            )

            assert response.status_code == 200
            data = response.json()
            assert data["agent_id"] == agent_id
            assert data["status"] == "acknowledged"
            assert "timestamp" in data

            # Verify that update_agent_status was called with correct parameters
            mock_repo.update_agent_status.assert_called_once_with(
                agent_id=agent_id,
                status="active",
                metrics=metrics_data
            )


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestDataQualityEndpoints:
    """Test data quality endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_get_latest_quality_metrics(self):
        """Test getting latest quality metrics."""
        data_source_id = "data-source-001"

        with patch('src.agent_orchestrated_etl.database.repositories.QualityMetricsRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_metrics = AsyncMock()
            mock_metrics.data_source_id = data_source_id
            mock_metrics.overall_quality_score = 0.85
            mock_metrics.measurement_timestamp = 1234567890.0
            mock_metrics.dimension_scores = {"completeness": 0.9, "accuracy": 0.8}
            mock_repo.get_latest_quality_metrics.return_value = mock_metrics
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/data-quality/{data_source_id}/latest")

            assert response.status_code == 200
            data = response.json()
            assert data["data_source_id"] == data_source_id
            assert data["overall_score"] == 0.85
            assert data["dimension_scores"]["completeness"] == 0.9

    def test_get_quality_metrics_not_found(self):
        """Test getting quality metrics for non-existent data source."""
        data_source_id = "non-existent-source"

        with patch('src.agent_orchestrated_etl.database.repositories.QualityMetricsRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.get_latest_quality_metrics.return_value = None
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/data-quality/{data_source_id}/latest")

            assert response.status_code == 404
            assert "No quality metrics found" in response.json()["detail"]

    def test_get_quality_trends(self):
        """Test getting quality trends."""
        data_source_id = "data-source-001"

        with patch('src.agent_orchestrated_etl.database.repositories.QualityMetricsRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_trends = [
                {
                    "measurement_timestamp": 1234567800.0,
                    "overall_score": 0.82,
                    "dimension_scores": {"completeness": 0.85, "accuracy": 0.79},
                    "record_count": 1000
                },
                {
                    "measurement_timestamp": 1234567890.0,
                    "overall_score": 0.85,
                    "dimension_scores": {"completeness": 0.9, "accuracy": 0.8},
                    "record_count": 1050
                }
            ]
            mock_repo.get_quality_trends.return_value = mock_trends
            mock_repo_class.return_value = mock_repo

            response = self.client.get(f"/api/v1/data-quality/{data_source_id}/trends?days=7")

            assert response.status_code == 200
            data = response.json()
            assert data["data_source_id"] == data_source_id
            assert data["period_days"] == 7
            assert data["total_measurements"] == 2
            assert len(data["trends"]) == 2


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestMonitoringEndpoints:
    """Test monitoring endpoints."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_get_metrics(self):
        """Test getting application metrics."""
        response = self.client.get("/monitoring/metrics")

        assert response.status_code == 200
        data = response.json()
        assert "timestamp" in data
        assert "application" in data
        assert data["application"]["name"] == "agent-orchestrated-etl"
        assert "system" in data
        assert "pipelines" in data
        assert "agents" in data

    def test_get_prometheus_metrics(self):
        """Test getting Prometheus format metrics."""
        response = self.client.get("/monitoring/prometheus")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")

        content = response.text
        assert "agent_etl_pipelines_total" in content
        assert "agent_etl_agents_total" in content
        assert "agent_etl_uptime_seconds" in content


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestAPIMiddleware:
    """Test API middleware functionality."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_cors_headers(self):
        """Test CORS headers are properly set."""
        response = self.client.options("/health/", headers={
            "Origin": "http://localhost:3000",
            "Access-Control-Request-Method": "GET"
        })

        assert response.status_code == 200
        assert "access-control-allow-origin" in response.headers

    def test_process_time_header(self):
        """Test that process time header is added."""
        response = self.client.get("/health/")

        assert response.status_code == 200
        assert "x-process-time" in response.headers

        process_time = float(response.headers["x-process-time"])
        assert process_time >= 0.0

    def test_gzip_compression(self):
        """Test gzip compression for large responses."""
        # Create a large response by requesting multiple times
        response = self.client.get("/api/v1/pipelines/", headers={
            "Accept-Encoding": "gzip"
        })

        assert response.status_code == 200
        # Note: Actual compression testing would require larger responses
        # This is just testing that the middleware is configured


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestAPIErrorHandling:
    """Test API error handling."""

    def setup_method(self):
        """Set up test client."""
        self.app = create_app()
        self.client = TestClient(self.app)

    def test_validation_error_handling(self):
        """Test validation error handling."""
        # Send invalid JSON to trigger validation error
        invalid_pipeline_data = {
            "name": "Test Pipeline",  # Missing required pipeline_id
            "data_source": "invalid_source"  # Should be dict, not string
        }

        response = self.client.post("/api/v1/pipelines/", json=invalid_pipeline_data)

        # Should return 422 for validation errors (FastAPI default)
        assert response.status_code == 422

    def test_database_error_handling(self):
        """Test database error handling."""
        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            from src.agent_orchestrated_etl.exceptions import DatabaseException
            mock_repo.create_pipeline_execution.side_effect = DatabaseException("Database connection failed")
            mock_repo_class.return_value = mock_repo

            pipeline_data = {
                "pipeline_id": "test-pipeline-001",
                "name": "Test Pipeline",
                "data_source": {"type": "file", "path": "/test.csv"},
                "transformations": [],
                "destination": {"type": "database", "table": "test"}
            }

            response = self.client.post("/api/v1/pipelines/", json=pipeline_data)

            assert response.status_code == 500

    def test_general_error_handling(self):
        """Test general error handling."""
        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.list_pipeline_executions.side_effect = Exception("Unexpected error")
            mock_repo_class.return_value = mock_repo

            response = self.client.get("/api/v1/pipelines/test-pipeline")

            assert response.status_code == 500
            data = response.json()
            assert data["error"] == "internal_server_error"
            assert data["type"] == "InternalServerError"


# Async test utilities
@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
@pytest.mark.asyncio
class TestAsyncAPIEndpoints:
    """Test API endpoints using async client."""

    async def test_health_check_async(self):
        """Test health check using async client."""
        app = create_app()

        with patch('src.agent_orchestrated_etl.database.connection.get_database_manager') as mock_db:
            mock_manager = AsyncMock()
            mock_manager.get_health_status.return_value = {"status": "healthy"}
            mock_db.return_value = mock_manager

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.get("/health/")

                assert response.status_code == 200
                data = response.json()
                assert data["status"] == "healthy"

    async def test_create_pipeline_async(self):
        """Test pipeline creation using async client."""
        app = create_app()

        pipeline_data = {
            "pipeline_id": "async-pipeline-001",
            "name": "Async Test Pipeline",
            "data_source": {"type": "file", "path": "/data/test.csv"},
            "transformations": [],
            "destination": {"type": "database", "table": "test"}
        }

        with patch('src.agent_orchestrated_etl.database.repositories.PipelineRepository') as mock_repo_class:
            mock_repo = AsyncMock()
            mock_repo.create_pipeline_execution.return_value = "exec-async-123"
            mock_repo_class.return_value = mock_repo

            async with AsyncClient(app=app, base_url="http://test") as client:
                response = await client.post("/api/v1/pipelines/", json=pipeline_data)

                assert response.status_code == 200
                data = response.json()
                assert data["pipeline_id"] == "async-pipeline-001"
