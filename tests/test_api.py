"""Tests for API endpoints."""

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

from agent_orchestrated_etl.api.app import create_app
from agent_orchestrated_etl.models import PipelineExecution
from tests.utils.test_factories import (
    DataQualityMetricsFactory,
)


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestHealthEndpoints:
    """Test health check endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    def test_health_check_healthy(self, client):
        """Test health check when system is healthy."""
        with patch('agent_orchestrated_etl.database.get_database_manager') as mock_db:
            mock_db.return_value.get_health_status = AsyncMock(
                return_value={"status": "healthy", "response_time_ms": 10.5}
            )

            response = client.get("/health/")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] in ["healthy", "degraded"]
            assert "timestamp" in data
            assert "details" in data

    def test_health_check_unhealthy(self, client):
        """Test health check when system is unhealthy."""
        with patch('agent_orchestrated_etl.database.get_database_manager') as mock_db:
            mock_db.return_value.get_health_status = AsyncMock(
                side_effect=Exception("Database connection failed")
            )

            response = client.get("/health/")

            assert response.status_code == 200  # Health endpoint should always return 200
            data = response.json()
            assert data["status"] == "unhealthy"
            assert "error" in data["details"]

    def test_readiness_check_ready(self, client):
        """Test readiness check when service is ready."""
        with patch('agent_orchestrated_etl.database.get_database_manager') as mock_db:
            mock_db.return_value.execute_query = AsyncMock(return_value={"1": 1})

            response = client.get("/health/ready")

            assert response.status_code == 200
            data = response.json()
            assert data["status"] == "ready"

    def test_readiness_check_not_ready(self, client):
        """Test readiness check when service is not ready."""
        with patch('agent_orchestrated_etl.database.get_database_manager') as mock_db:
            mock_db.return_value.execute_query = AsyncMock(
                side_effect=Exception("Database not available")
            )

            response = client.get("/health/ready")

            assert response.status_code == 503
            data = response.json()
            assert data["status"] == "not_ready"


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestPipelineEndpoints:
    """Test pipeline management endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    @pytest.fixture
    def sample_pipeline_request(self):
        """Sample pipeline creation request."""
        return {
            "pipeline_id": "test-pipeline-001",
            "name": "Test Pipeline",
            "description": "A test pipeline for unit testing",
            "data_source": {
                "type": "file",
                "path": "/tmp/test_data.csv",
                "format": "csv"
            },
            "transformations": [
                {
                    "type": "filter",
                    "condition": "value > 0"
                }
            ],
            "destination": {
                "type": "database",
                "table": "processed_data"
            }
        }

    def test_create_pipeline(self, client, sample_pipeline_request):
        """Test creating a new pipeline."""
        with patch('agent_orchestrated_etl.database.PipelineRepository') as mock_repo:
            mock_repo.return_value.create_pipeline_execution = AsyncMock(
                return_value="execution-123"
            )

            response = client.post(
                "/api/v1/pipelines/",
                json=sample_pipeline_request
            )

            assert response.status_code == 200
            data = response.json()
            assert data["pipeline_id"] == sample_pipeline_request["pipeline_id"]
            assert data["name"] == sample_pipeline_request["name"]
            assert data["status"] == "created"

    def test_get_pipeline(self, client):
        """Test getting a pipeline by ID."""
        pipeline_id = "test-pipeline-001"

        with patch('agent_orchestrated_etl.database.PipelineRepository') as mock_repo:
            mock_execution = PipelineExecution(
                execution_id="exec-123",
                pipeline_id=pipeline_id,
                status="running"
            )
            mock_repo.return_value.list_pipeline_executions = AsyncMock(
                return_value=[mock_execution]
            )

            response = client.get(f"/api/v1/pipelines/{pipeline_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["pipeline_id"] == pipeline_id
            assert data["status"] == "running"

    def test_get_pipeline_not_found(self, client):
        """Test getting a non-existent pipeline."""
        with patch('agent_orchestrated_etl.database.PipelineRepository') as mock_repo:
            mock_repo.return_value.list_pipeline_executions = AsyncMock(
                return_value=[]
            )

            response = client.get("/api/v1/pipelines/nonexistent")

            assert response.status_code == 404

    def test_list_pipelines(self, client):
        """Test listing pipelines."""
        with patch('agent_orchestrated_etl.database.PipelineRepository') as mock_repo:
            mock_executions = [
                PipelineExecution(
                    execution_id="exec-1",
                    pipeline_id="pipeline-1",
                    status="completed"
                ),
                PipelineExecution(
                    execution_id="exec-2",
                    pipeline_id="pipeline-2",
                    status="running"
                )
            ]
            mock_repo.return_value.list_pipeline_executions = AsyncMock(
                return_value=mock_executions
            )

            response = client.get("/api/v1/pipelines/")

            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
            assert data[0]["pipeline_id"] == "pipeline-1"
            assert data[1]["pipeline_id"] == "pipeline-2"

    def test_execute_pipeline(self, client):
        """Test executing a pipeline."""
        pipeline_id = "test-pipeline-001"
        parameters = {"param1": "value1"}

        response = client.post(
            f"/api/v1/pipelines/{pipeline_id}/execute",
            json=parameters
        )

        assert response.status_code == 202
        data = response.json()
        assert data["pipeline_id"] == pipeline_id
        assert data["status"] == "scheduled"
        assert data["parameters"] == parameters


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestAgentEndpoints:
    """Test agent management endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    def test_list_agents(self, client):
        """Test listing agents."""
        with patch('agent_orchestrated_etl.database.AgentRepository') as mock_repo:
            mock_agents = [
                {
                    "id": "agent-1",
                    "status": "active",
                    "last_heartbeat": 1234567890,
                    "metrics": {"tasks_completed": 10}
                },
                {
                    "id": "agent-2",
                    "status": "idle",
                    "last_heartbeat": 1234567880,
                    "metrics": {"tasks_completed": 5}
                }
            ]
            mock_repo.return_value.list_active_agents = AsyncMock(
                return_value=mock_agents
            )

            response = client.get("/api/v1/agents/")

            assert response.status_code == 200
            data = response.json()
            assert len(data) == 2
            assert data[0]["agent_id"] == "agent-1"
            assert data[1]["agent_id"] == "agent-2"

    def test_get_agent_status(self, client):
        """Test getting agent status."""
        agent_id = "test-agent-001"

        with patch('agent_orchestrated_etl.database.AgentRepository') as mock_repo:
            mock_agent = {
                "id": agent_id,
                "status": "active",
                "last_heartbeat": 1234567890,
                "metrics": {"tasks_completed": 10}
            }
            mock_repo.return_value.get_agent_status = AsyncMock(
                return_value=mock_agent
            )

            response = client.get(f"/api/v1/agents/{agent_id}")

            assert response.status_code == 200
            data = response.json()
            assert data["agent_id"] == agent_id
            assert data["status"] == "active"

    def test_agent_heartbeat(self, client):
        """Test agent heartbeat endpoint."""
        agent_id = "test-agent-001"
        metrics = {"tasks_completed": 15, "cpu_usage": 0.65}

        with patch('agent_orchestrated_etl.database.AgentRepository') as mock_repo:
            mock_repo.return_value.update_agent_status = AsyncMock(
                return_value=True
            )

            response = client.post(
                f"/api/v1/agents/{agent_id}/heartbeat",
                json=metrics
            )

            assert response.status_code == 200
            data = response.json()
            assert data["agent_id"] == agent_id
            assert data["status"] == "acknowledged"


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestDataQualityEndpoints:
    """Test data quality endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    def test_get_latest_quality_metrics(self, client):
        """Test getting latest quality metrics."""
        data_source_id = "test-source-001"

        with patch('agent_orchestrated_etl.database.QualityMetricsRepository') as mock_repo:
            mock_metrics = DataQualityMetricsFactory.create(
                data_source_id=data_source_id,
                overall_quality_score=0.85
            )
            mock_repo.return_value.get_latest_quality_metrics = AsyncMock(
                return_value=mock_metrics
            )

            response = client.get(f"/api/v1/data-quality/{data_source_id}/latest")

            assert response.status_code == 200
            data = response.json()
            assert data["data_source_id"] == data_source_id
            assert data["overall_score"] == 0.85

    def test_get_quality_trends(self, client):
        """Test getting quality trends."""
        data_source_id = "test-source-001"

        with patch('agent_orchestrated_etl.database.QualityMetricsRepository') as mock_repo:
            mock_trends = [
                {
                    "measurement_timestamp": 1234567890,
                    "overall_score": 0.85,
                    "record_count": 1000
                },
                {
                    "measurement_timestamp": 1234567950,
                    "overall_score": 0.87,
                    "record_count": 1050
                }
            ]
            mock_repo.return_value.get_quality_trends = AsyncMock(
                return_value=mock_trends
            )

            response = client.get(
                f"/api/v1/data-quality/{data_source_id}/trends?days=7"
            )

            assert response.status_code == 200
            data = response.json()
            assert data["data_source_id"] == data_source_id
            assert data["period_days"] == 7
            assert len(data["trends"]) == 2


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestMonitoringEndpoints:
    """Test monitoring endpoints."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    def test_get_metrics(self, client):
        """Test getting application metrics."""
        response = client.get("/monitoring/metrics")

        assert response.status_code == 200
        data = response.json()
        assert "timestamp" in data
        assert "application" in data
        assert "system" in data
        assert "pipelines" in data
        assert "agents" in data

    def test_get_prometheus_metrics(self, client):
        """Test getting Prometheus format metrics."""
        response = client.get("/monitoring/prometheus")

        assert response.status_code == 200
        assert response.headers["content-type"] == "text/plain; charset=utf-8"

        content = response.text
        assert "agent_etl_pipelines_total" in content
        assert "agent_etl_agents_total" in content
        assert "agent_etl_uptime_seconds" in content


@pytest.mark.skipif(not FASTAPI_AVAILABLE, reason="FastAPI not available")
class TestAPIMiddleware:
    """Test API middleware functionality."""

    @pytest.fixture
    def client(self):
        """Create test client."""
        app = create_app()
        return TestClient(app)

    def test_cors_headers(self, client):
        """Test CORS headers are present."""
        response = client.options("/health/")

        # Check that CORS headers are present
        assert "access-control-allow-origin" in response.headers

    def test_process_time_header(self, client):
        """Test that process time header is added."""
        response = client.get("/health/")

        assert "x-process-time" in response.headers
        process_time = float(response.headers["x-process-time"])
        assert process_time >= 0

    def test_error_handling(self, client):
        """Test error handling middleware."""
        with patch('agent_orchestrated_etl.database.get_database_manager') as mock_db:
            mock_db.side_effect = Exception("Database error")

            response = client.get("/health/")

            # Should still return a response, not crash
            assert response.status_code in [200, 500, 503]


@pytest.mark.skipif(FASTAPI_AVAILABLE, reason="Test for when FastAPI is not available")
class TestAPIWithoutFastAPI:
    """Test API behavior when FastAPI is not available."""

    def test_create_app_without_fastapi(self):
        """Test that create_app raises ImportError when FastAPI is not available."""
        with pytest.raises(ImportError, match="FastAPI is not available"):
            create_app()

    def test_routers_are_none_without_fastapi(self):
        """Test that routers are None when FastAPI is not available."""
        from agent_orchestrated_etl.api.routers import (
            agent_router,
            health_router,
            pipeline_router,
        )

        assert health_router is None
        assert pipeline_router is None
        assert agent_router is None
