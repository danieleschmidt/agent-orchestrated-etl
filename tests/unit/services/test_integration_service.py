"""Unit tests for IntegrationService."""

import pytest
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent_orchestrated_etl.services.integration_service import IntegrationService


class TestIntegrationService:
    """Test cases for IntegrationService."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = AsyncMock()
        self.integration_service = IntegrationService(db_manager=self.mock_db_manager)

    @pytest.mark.asyncio
    async def test_execute_integration_success(self):
        """Test successful integration execution."""
        integration_id = "api-integration-001"
        operation = "fetch_data"
        params = {
            "url": "https://api.example.com/data",
            "method": "GET",
            "headers": {"Authorization": "Bearer token123"}
        }
        context = {
            "timeout": 30,
            "retry_attempts": 3,
            "pipeline_id": "test-pipeline-001"
        }
        
        # Mock successful HTTP response
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"data": [{"id": 1, "name": "test"}]}
            mock_response.headers = {"content-type": "application/json"}
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is True
            assert result["status_code"] == 200
            assert "data" in result["response"]
            assert result["execution_time_ms"] > 0
            assert result["integration_id"] == integration_id

    @pytest.mark.asyncio
    async def test_execute_integration_with_retry(self):
        """Test integration execution with retry logic."""
        integration_id = "retry-integration-001"
        operation = "fetch_data"
        params = {"url": "https://api.example.com/data", "method": "GET"}
        context = {"retry_attempts": 3, "retry_delay": 1}
        
        # Mock first two calls fail, third succeeds
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response_fail = AsyncMock()
            mock_response_fail.status = 500
            mock_response_fail.raise_for_status.side_effect = Exception("Server Error")
            
            mock_response_success = AsyncMock()
            mock_response_success.status = 200
            mock_response_success.json.return_value = {"data": "success"}
            
            mock_get.return_value.__aenter__.side_effect = [
                mock_response_fail,  # First attempt fails
                mock_response_fail,  # Second attempt fails
                mock_response_success  # Third attempt succeeds
            ]
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is True
            assert result["retry_count"] == 2
            assert result["status_code"] == 200
            assert mock_get.call_count == 3

    @pytest.mark.asyncio
    async def test_execute_integration_circuit_breaker_open(self):
        """Test integration execution when circuit breaker is open."""
        integration_id = "circuit-breaker-integration"
        operation = "fetch_data"
        params = {"url": "https://api.example.com/data"}
        context = {}
        
        # Mock circuit breaker state as open
        with patch.object(self.integration_service, '_check_circuit_breaker') as mock_circuit:
            mock_circuit.return_value = {"state": "open", "next_attempt": datetime.now() + timedelta(minutes=5)}
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is False
            assert result["error"] == "circuit_breaker_open"
            assert "next_attempt" in result

    @pytest.mark.asyncio
    async def test_execute_integration_rate_limited(self):
        """Test integration execution when rate limited."""
        integration_id = "rate-limited-integration"
        operation = "fetch_data"
        params = {"url": "https://api.example.com/data"}
        context = {}
        
        # Mock rate limit check
        with patch.object(self.integration_service, '_check_rate_limit') as mock_rate_limit:
            mock_rate_limit.return_value = {"allowed": False, "retry_after": 60}
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is False
            assert result["error"] == "rate_limited"
            assert result["retry_after_seconds"] == 60

    @pytest.mark.asyncio
    async def test_execute_database_integration(self):
        """Test database integration execution."""
        integration_id = "db-integration-001"
        operation = "execute_query"
        params = {
            "query": "SELECT * FROM users WHERE active = $1",
            "parameters": [True],
            "connection_string": "postgresql://user:pass@localhost/db"
        }
        context = {"timeout": 30}
        
        # Mock database execution
        self.mock_db_manager.execute_query.return_value = [
            {"id": 1, "name": "Alice", "active": True},
            {"id": 2, "name": "Bob", "active": True}
        ]
        
        result = await self.integration_service.execute_integration(
            integration_id, operation, params, context
        )
        
        assert result["success"] is True
        assert len(result["response"]) == 2
        assert result["response"][0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_execute_file_integration(self):
        """Test file system integration execution."""
        integration_id = "file-integration-001"
        operation = "read_file"
        params = {
            "file_path": "/data/test.csv",
            "format": "csv",
            "encoding": "utf-8"
        }
        context = {}
        
        # Mock file operations
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', create=True) as mock_open:
            
            mock_file = MagicMock()
            mock_file.__enter__.return_value = mock_file
            mock_file.read.return_value = "id,name\n1,Alice\n2,Bob"
            mock_open.return_value = mock_file
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is True
            assert "id,name" in result["response"]
            assert result["file_size"] > 0

    @pytest.mark.asyncio
    async def test_execute_s3_integration(self):
        """Test S3 integration execution."""
        integration_id = "s3-integration-001"
        operation = "upload_file"
        params = {
            "bucket": "test-bucket",
            "key": "data/output.json",
            "content": '{"data": "test"}',
            "content_type": "application/json"
        }
        context = {"aws_region": "us-east-1"}
        
        # Mock S3 operations
        with patch('boto3.client') as mock_boto3:
            mock_s3_client = MagicMock()
            mock_s3_client.put_object.return_value = {
                "ETag": "abc123",
                "ResponseMetadata": {"HTTPStatusCode": 200}
            }
            mock_boto3.return_value = mock_s3_client
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is True
            assert result["s3_etag"] == "abc123"
            assert result["status_code"] == 200

    def test_check_circuit_breaker_closed(self):
        """Test circuit breaker in closed state."""
        integration_id = "test-integration"
        
        # Mock no previous failures
        self.integration_service.circuit_breakers[integration_id] = {
            "state": "closed",
            "failure_count": 0,
            "last_failure": None,
            "failure_threshold": 5,
            "timeout_seconds": 60
        }
        
        result = self.integration_service._check_circuit_breaker(integration_id)
        
        assert result["state"] == "closed"
        assert result["allowed"] is True

    def test_check_circuit_breaker_open(self):
        """Test circuit breaker in open state."""
        integration_id = "test-integration"
        
        # Mock circuit breaker in open state
        self.integration_service.circuit_breakers[integration_id] = {
            "state": "open",
            "failure_count": 6,
            "last_failure": datetime.now(),
            "failure_threshold": 5,
            "timeout_seconds": 60
        }
        
        result = self.integration_service._check_circuit_breaker(integration_id)
        
        assert result["state"] == "open"
        assert result["allowed"] is False

    def test_check_circuit_breaker_half_open(self):
        """Test circuit breaker in half-open state."""
        integration_id = "test-integration"
        
        # Mock circuit breaker transitioning to half-open
        self.integration_service.circuit_breakers[integration_id] = {
            "state": "open",
            "failure_count": 6,
            "last_failure": datetime.now() - timedelta(minutes=2),  # Past timeout
            "failure_threshold": 5,
            "timeout_seconds": 60
        }
        
        result = self.integration_service._check_circuit_breaker(integration_id)
        
        assert result["state"] == "half_open"
        assert result["allowed"] is True

    @pytest.mark.asyncio
    async def test_check_rate_limit_within_limit(self):
        """Test rate limiting when within limits."""
        integration_id = "rate-limited-api"
        
        # Mock rate limit state
        current_time = datetime.now()
        self.integration_service.rate_limits[integration_id] = {
            "requests": 5,
            "window_start": current_time - timedelta(seconds=30),
            "limit": 100,
            "window_seconds": 60
        }
        
        result = await self.integration_service._check_rate_limit(integration_id)
        
        assert result["allowed"] is True
        assert result["remaining_requests"] == 95

    @pytest.mark.asyncio
    async def test_check_rate_limit_exceeded(self):
        """Test rate limiting when limit exceeded."""
        integration_id = "rate-limited-api"
        
        # Mock rate limit exceeded
        current_time = datetime.now()
        self.integration_service.rate_limits[integration_id] = {
            "requests": 100,
            "window_start": current_time - timedelta(seconds=30),
            "limit": 100,
            "window_seconds": 60
        }
        
        result = await self.integration_service._check_rate_limit(integration_id)
        
        assert result["allowed"] is False
        assert result["retry_after"] > 0

    @pytest.mark.asyncio
    async def test_record_integration_metrics(self):
        """Test recording integration metrics."""
        integration_id = "metrics-integration"
        operation = "fetch_data"
        execution_time_ms = 250
        success = True
        status_code = 200
        
        await self.integration_service._record_metrics(
            integration_id, operation, execution_time_ms, success, status_code
        )
        
        # Should have recorded metrics in database
        self.mock_db_manager.execute_query.assert_called()
        
        # Verify metrics data structure
        call_args = self.mock_db_manager.execute_query.call_args
        query = call_args[0][0]
        assert "INSERT" in query
        assert integration_id in str(call_args)

    @pytest.mark.asyncio
    async def test_get_integration_health(self):
        """Test getting integration health status."""
        integration_id = "health-integration"
        
        # Mock health metrics from database
        self.mock_db_manager.execute_query.return_value = [{
            "success_rate": 0.95,
            "avg_response_time_ms": 150,
            "total_requests": 1000,
            "last_24h_errors": 5,
            "circuit_breaker_state": "closed"
        }]
        
        result = await self.integration_service.get_integration_health(integration_id)
        
        assert result["integration_id"] == integration_id
        assert result["success_rate"] == 0.95
        assert result["avg_response_time_ms"] == 150
        assert result["health_score"] > 0.8  # Should be healthy

    @pytest.mark.asyncio
    async def test_update_circuit_breaker_on_success(self):
        """Test circuit breaker update on successful execution."""
        integration_id = "cb-success-integration"
        
        # Initialize circuit breaker with some failures
        self.integration_service.circuit_breakers[integration_id] = {
            "state": "half_open",
            "failure_count": 3,
            "success_count": 0,
            "failure_threshold": 5,
            "success_threshold": 2
        }
        
        await self.integration_service._update_circuit_breaker(integration_id, success=True)
        
        cb_state = self.integration_service.circuit_breakers[integration_id]
        assert cb_state["success_count"] == 1
        
        # After enough successes, should close circuit breaker
        await self.integration_service._update_circuit_breaker(integration_id, success=True)
        cb_state = self.integration_service.circuit_breakers[integration_id]
        assert cb_state["state"] == "closed"
        assert cb_state["failure_count"] == 0

    @pytest.mark.asyncio
    async def test_update_circuit_breaker_on_failure(self):
        """Test circuit breaker update on failed execution."""
        integration_id = "cb-failure-integration"
        
        # Initialize circuit breaker
        self.integration_service.circuit_breakers[integration_id] = {
            "state": "closed",
            "failure_count": 4,  # One below threshold
            "failure_threshold": 5,
            "timeout_seconds": 60
        }
        
        await self.integration_service._update_circuit_breaker(integration_id, success=False)
        
        cb_state = self.integration_service.circuit_breakers[integration_id]
        assert cb_state["failure_count"] == 5
        assert cb_state["state"] == "open"  # Should open after reaching threshold
        assert cb_state["last_failure"] is not None

    @pytest.mark.asyncio
    async def test_batch_integration_execution(self):
        """Test batch execution of multiple integrations."""
        integrations = [
            {"id": "batch-1", "operation": "fetch_data", "params": {"url": "https://api1.com"}},
            {"id": "batch-2", "operation": "fetch_data", "params": {"url": "https://api2.com"}},
            {"id": "batch-3", "operation": "fetch_data", "params": {"url": "https://api3.com"}}
        ]
        context = {"timeout": 30, "parallel": True}
        
        # Mock HTTP responses
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"data": "test"}
            mock_get.return_value.__aenter__.return_value = mock_response
            
            results = await self.integration_service.execute_batch_integrations(
                integrations, context
            )
            
            assert len(results) == 3
            assert all(result["success"] for result in results)
            assert mock_get.call_count == 3

    @pytest.mark.asyncio
    async def test_integration_timeout_handling(self):
        """Test integration timeout handling."""
        integration_id = "timeout-integration"
        operation = "fetch_data"
        params = {"url": "https://slow-api.com/data"}
        context = {"timeout": 1}  # Very short timeout
        
        # Mock slow response
        with patch('aiohttp.ClientSession.get') as mock_get:
            async def slow_response(*args, **kwargs):
                await asyncio.sleep(2)  # Longer than timeout
                mock_response = AsyncMock()
                mock_response.status = 200
                return mock_response
            
            mock_get.return_value.__aenter__ = slow_response
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is False
            assert "timeout" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_integration_authentication_handling(self):
        """Test integration with authentication."""
        integration_id = "auth-integration"
        operation = "fetch_data"
        params = {
            "url": "https://api.example.com/data",
            "auth_type": "bearer",
            "auth_token": "secret-token-123"
        }
        context = {}
        
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"authenticated": True}
            mock_get.return_value.__aenter__.return_value = mock_response
            
            result = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result["success"] is True
            
            # Verify authorization header was set
            call_args = mock_get.call_args
            headers = call_args[1].get("headers", {})
            assert "Authorization" in headers
            assert headers["Authorization"] == "Bearer secret-token-123"

    @pytest.mark.asyncio
    async def test_error_handling_and_logging(self):
        """Test comprehensive error handling and logging."""
        integration_id = "error-integration"
        operation = "invalid_operation"
        params = {}
        context = {}
        
        result = await self.integration_service.execute_integration(
            integration_id, operation, params, context
        )
        
        assert result["success"] is False
        assert "error" in result
        assert result["integration_id"] == integration_id
        
        # Should have logged error metrics
        self.mock_db_manager.execute_query.assert_called()

    @pytest.mark.asyncio
    async def test_integration_caching(self):
        """Test integration response caching."""
        integration_id = "cached-integration"
        operation = "fetch_data"
        params = {
            "url": "https://api.example.com/data",
            "cache_ttl": 300  # 5 minutes
        }
        context = {}
        
        # Mock first call to API
        with patch('aiohttp.ClientSession.get') as mock_get:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"data": "cached_value", "timestamp": "2023-01-01"}
            mock_get.return_value.__aenter__.return_value = mock_response
            
            # First call should hit API
            result1 = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            # Second call should use cache
            result2 = await self.integration_service.execute_integration(
                integration_id, operation, params, context
            )
            
            assert result1["success"] is True
            assert result2["success"] is True
            assert result2["from_cache"] is True
            assert mock_get.call_count == 1  # Only called once due to caching