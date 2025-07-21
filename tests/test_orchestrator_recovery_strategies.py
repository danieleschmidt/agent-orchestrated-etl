"""Test sophisticated recovery strategies in orchestrator agent."""

import asyncio
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent
from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole
from src.agent_orchestrated_etl.exceptions import NetworkException, ExternalServiceException, RateLimitException


class TestOrchestratorRecoveryStrategies:
    """Test sophisticated recovery strategies in orchestrator agent."""

    @pytest.fixture
    def orchestrator_agent(self):
        """Create an orchestrator agent for testing."""
        config = AgentConfig(
            name="TestOrchestratorAgent",
            role=AgentRole.ORCHESTRATOR,
            max_concurrent_tasks=5,
        )
        
        agent = OrchestratorAgent(config=config)
        return agent

    @pytest.fixture
    def sample_workflow(self):
        """Create a sample workflow for testing."""
        return {
            "workflow_id": "test_workflow_123",
            "started_at": time.time() - 300,  # 5 minutes ago
            "supports_rollback": True,
            "checkpoints": [
                {
                    "name": "after_extract",
                    "timestamp": time.time() - 200,
                    "state": "extracted"
                }
            ]
        }

    @pytest.fixture
    def sample_step(self):
        """Create a sample step for testing."""
        return {
            "name": "transform_data",
            "type": "transform",
            "attempt_number": 1,
            "max_retries": 3,
            "recovery_config": {
                "retry_on_failure": True,
                "continue_on_failure": False,
                "fallback_enabled": True,
                "circuit_breaker_enabled": True,
                "retry_base_delay": 1.0,
                "backoff_factor": 2.0,
                "max_retry_delay": 60.0
            }
        }

    @pytest.mark.asyncio
    async def test_network_error_retry_strategy(self, orchestrator_agent, sample_step, sample_workflow):
        """Test retry strategy for network errors."""
        error = NetworkException("Connection timeout")
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(sample_step, error, sample_workflow)
        
        # Verify retry strategy is selected
        assert recovery_strategy["strategy"] == "retry_with_exponential_backoff"
        assert recovery_strategy["retry_attempt"] == 2
        assert recovery_strategy["delay_seconds"] > 0
        assert "Network connectivity issue" in recovery_strategy["reason"]
        
        # Verify failure is recorded in workflow
        assert len(sample_workflow["failure_history"]) == 1
        failure_record = sample_workflow["failure_history"][0]
        assert failure_record["step_name"] == "transform_data"
        assert failure_record["error_type"] == "NetworkException"
        
        # Verify memory storage was called
        orchestrator_agent.memory.store_entry.assert_called_once()

    @pytest.mark.asyncio
    async def test_rate_limit_error_analysis(self, orchestrator_agent, sample_step, sample_workflow):
        """Test rate limit error analysis and recovery."""
        error = RateLimitException("API rate limit exceeded")
        
        error_analysis = orchestrator_agent._analyze_error(error, sample_step, sample_workflow)
        
        # Verify rate limit analysis
        assert error_analysis["is_retryable"] is True
        assert error_analysis["retry_reason"] == "Rate limiting, will backoff"
        assert error_analysis["fallback_available"] is True
        assert error_analysis["triggers_circuit_breaker"] is False
        assert error_analysis["supports_degradation"] is True
        assert error_analysis["can_defer"] is True

    @pytest.mark.asyncio
    async def test_authentication_error_no_retry(self, orchestrator_agent, sample_step, sample_workflow):
        """Test that authentication errors are not retried."""
        error = Exception("Authentication failed: unauthorized access")
        
        error_analysis = orchestrator_agent._analyze_error(error, sample_step, sample_workflow)
        
        # Verify authentication errors are not retryable
        assert error_analysis["is_retryable"] is False
        assert error_analysis["retry_reason"] == "Authentication/authorization failure"
        assert error_analysis["fallback_available"] is False

    @pytest.mark.asyncio
    async def test_fallback_strategy_for_extract_step(self, orchestrator_agent, sample_workflow):
        """Test fallback strategy for extract step."""
        extract_step = {
            "name": "extract_data",
            "type": "extract",
            "attempt_number": 2,
            "max_retries": 3,
            "recovery_config": {
                "fallback_enabled": True,
                "retry_on_failure": False  # Force fallback instead of retry
            }
        }
        
        error = ExternalServiceException("Data source unavailable")
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(extract_step, error, sample_workflow)
        
        # Verify fallback strategy is selected
        assert recovery_strategy["strategy"] == "fallback_approach"
        assert "alternative data source" in recovery_strategy["fallback_config"]["description"]
        assert recovery_strategy["fallback_config"]["approach"] == "alternative_source"

    @pytest.mark.asyncio
    async def test_skip_with_warning_strategy(self, orchestrator_agent, sample_workflow):
        """Test skip with warning strategy for non-critical steps."""
        validate_step = {
            "name": "validate_data",
            "type": "validate",
            "attempt_number": 3,
            "max_retries": 3,
            "recovery_config": {
                "continue_on_failure": True,
                "retry_on_failure": False
            }
        }
        
        error = Exception("Validation rule failed")
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(validate_step, error, sample_workflow)
        
        # Verify skip strategy is selected
        assert recovery_strategy["strategy"] == "skip_with_warning"
        assert "Skipping non-critical step" in recovery_strategy["message"]
        assert recovery_strategy["impact_assessment"]["impact_level"] == "low"

    @pytest.mark.asyncio
    async def test_graceful_degradation_strategy(self, orchestrator_agent, sample_step, sample_workflow):
        """Test graceful degradation strategy."""
        # Modify step to enable degradation
        sample_step["recovery_config"]["fallback_enabled"] = False
        sample_step["recovery_config"]["retry_on_failure"] = False
        
        error = RateLimitException("Rate limit exceeded")  # Supports degradation
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(sample_step, error, sample_workflow)
        
        # Verify degradation strategy is selected
        assert recovery_strategy["strategy"] == "graceful_degradation"
        assert "Degrading functionality" in recovery_strategy["message"]
        assert "simplified_rules" in recovery_strategy["degradation_config"]

    @pytest.mark.asyncio
    async def test_dead_letter_queue_strategy(self, orchestrator_agent, sample_workflow):
        """Test dead letter queue strategy."""
        load_step = {
            "name": "load_data",
            "type": "load", 
            "attempt_number": 4,  # Exceeded retries
            "max_retries": 3,
            "recovery_config": {
                "retry_on_failure": False,
                "fallback_enabled": False,
                "continue_on_failure": False
            }
        }
        
        error = NetworkException("Database connection failed")  # Can be deferred
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(load_step, error, sample_workflow)
        
        # Verify dead letter queue strategy is selected
        assert recovery_strategy["strategy"] == "dead_letter_queue"
        assert "dead letter queue" in recovery_strategy["message"]
        assert recovery_strategy["queue_id"].startswith("dlq_test_workflow_123")

    @pytest.mark.asyncio
    async def test_rollback_to_checkpoint_strategy(self, orchestrator_agent, sample_workflow):
        """Test rollback to checkpoint strategy."""
        critical_step = {
            "name": "critical_load",
            "type": "critical",
            "attempt_number": 4,
            "max_retries": 3,
            "recovery_config": {
                "retry_on_failure": False,
                "fallback_enabled": False,
                "continue_on_failure": False
            }
        }
        
        error = Exception("Critical system failure")  # Not deferrable
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(critical_step, error, sample_workflow)
        
        # Verify rollback strategy is selected
        assert recovery_strategy["strategy"] == "rollback_to_checkpoint"
        assert "Rolling back workflow" in recovery_strategy["message"]
        assert recovery_strategy["rollback_point"]["name"] == "after_extract"

    @pytest.mark.asyncio
    async def test_fail_workflow_last_resort(self, orchestrator_agent, sample_step, sample_workflow):
        """Test fail workflow as last resort."""
        # Remove rollback support and make step critical
        sample_workflow["supports_rollback"] = False
        sample_step["type"] = "critical"
        sample_step["attempt_number"] = 4  # Exceeded retries
        sample_step["recovery_config"] = {
            "retry_on_failure": False,
            "fallback_enabled": False,
            "continue_on_failure": False
        }
        
        error = ValueError("Configuration error")  # Not retryable, not deferrable
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(sample_step, error, sample_workflow)
        
        # Verify fail workflow strategy is selected
        assert recovery_strategy["strategy"] == "fail_workflow"
        assert "Critical failure" in recovery_strategy["message"]
        assert "No viable recovery strategy" in recovery_strategy["reason"]

    @pytest.mark.asyncio
    async def test_exponential_backoff_calculation(self, orchestrator_agent, sample_step, sample_workflow):
        """Test exponential backoff calculation with jitter."""
        sample_step["attempt_number"] = 3  # Third attempt
        sample_step["recovery_config"]["retry_base_delay"] = 2.0
        sample_step["recovery_config"]["backoff_factor"] = 2.0
        sample_step["recovery_config"]["max_retry_delay"] = 30.0
        
        error = NetworkException("Temporary network issue")
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        recovery_strategy = await orchestrator_agent._handle_step_failure(sample_step, error, sample_workflow)
        
        # Verify exponential backoff calculation
        assert recovery_strategy["strategy"] == "retry_with_exponential_backoff"
        delay = recovery_strategy["delay_seconds"]
        
        # Base calculation: 2.0 * (2.0 ** (3-1)) = 2.0 * 4 = 8.0
        # With jitter, should be between 6.4 and 9.6 seconds
        assert 6.0 <= delay <= 10.0
        assert delay <= 30.0  # Should not exceed max_retry_delay

    @pytest.mark.asyncio
    async def test_impact_assessment_for_skip_strategy(self, orchestrator_agent, sample_workflow):
        """Test impact assessment when skipping different step types."""
        steps = [
            {"name": "extract", "type": "extract"},
            {"name": "transform", "type": "transform"},
            {"name": "validate", "type": "validate"},
            {"name": "load", "type": "load"}
        ]
        
        expected_impacts = {
            "extract": "high",
            "transform": "medium", 
            "validate": "low",
            "load": "high"
        }
        
        for step in steps:
            impact = await orchestrator_agent._assess_skip_impact(step, sample_workflow)
            assert impact["impact_level"] == expected_impacts[step["type"]]
            
            # Check business continuity impact
            if step["type"] in ["extract", "load"]:
                assert impact["business_continuity_impact"] is True
            else:
                assert impact["business_continuity_impact"] is False

    @pytest.mark.asyncio
    async def test_fallback_configurations(self, orchestrator_agent, sample_workflow):
        """Test fallback configurations for different step types."""
        error = Exception("Generic error")
        
        fallback_tests = [
            {
                "step": {"type": "extract", "name": "extract_test"},
                "expected_approach": "alternative_source",
                "expected_description": "cached data or alternative data source"
            },
            {
                "step": {"type": "transform", "name": "transform_test"},
                "expected_approach": "simplified_transform",
                "expected_description": "simplified transformation rules"
            },
            {
                "step": {"type": "load", "name": "load_test"},
                "expected_approach": "alternative_destination",
                "expected_description": "alternative destination or queue"
            },
            {
                "step": {"type": "validate", "name": "validate_test"},
                "expected_approach": "relaxed_validation",
                "expected_description": "relaxed validation rules"
            }
        ]
        
        for test_case in fallback_tests:
            fallback = await orchestrator_agent._get_fallback_approach(test_case["step"], error, sample_workflow)
            assert fallback["approach"] == test_case["expected_approach"]
            assert test_case["expected_description"] in fallback["description"]

    @pytest.mark.asyncio
    async def test_degradation_configurations(self, orchestrator_agent, sample_workflow):
        """Test degradation configurations for different step types."""
        error = Exception("Generic error")
        
        degradation_tests = [
            {
                "step": {"type": "extract", "name": "extract_test"},
                "expected_keys": ["reduced_data_volume", "sample_percentage", "skip_optional_fields"]
            },
            {
                "step": {"type": "transform", "name": "transform_test"},
                "expected_keys": ["simplified_rules", "skip_non_essential_transforms", "relaxed_data_types"]
            },
            {
                "step": {"type": "validate", "name": "validate_test"},
                "expected_keys": ["essential_checks_only", "warning_instead_of_error", "skip_referential_integrity"]
            }
        ]
        
        for test_case in degradation_tests:
            degradation = await orchestrator_agent._get_degradation_config(test_case["step"], error, sample_workflow)
            for key in test_case["expected_keys"]:
                assert key in degradation

    @pytest.mark.asyncio
    async def test_failure_history_tracking(self, orchestrator_agent, sample_step, sample_workflow):
        """Test that failure history is properly tracked."""
        errors = [
            NetworkException("First network error"),
            RateLimitException("Rate limit error"),
            Exception("Generic error")
        ]
        
        # Mock memory storage
        orchestrator_agent.memory.store_entry = AsyncMock()
        
        for i, error in enumerate(errors):
            sample_step["attempt_number"] = i + 1
            await orchestrator_agent._handle_step_failure(sample_step, error, sample_workflow)
        
        # Verify failure history tracking
        assert len(sample_workflow["failure_history"]) == 3
        
        for i, failure in enumerate(sample_workflow["failure_history"]):
            assert failure["step_name"] == "transform_data"
            assert failure["attempt_number"] == i + 1
            assert failure["error_type"] == type(errors[i]).__name__
            assert failure["timestamp"] > 0

    @pytest.mark.asyncio
    async def test_attempted_strategies_tracking(self, orchestrator_agent, sample_workflow):
        """Test tracking of attempted recovery strategies."""
        # Add some failure history with recovery strategies
        sample_workflow["failure_history"] = [
            {"recovery_strategy": "retry_with_exponential_backoff"},
            {"recovery_strategy": "fallback_approach"},
            {"recovery_strategy": "retry_with_exponential_backoff"},  # Duplicate
            {"error": "some error"}  # No recovery strategy
        ]
        
        attempted_strategies = await orchestrator_agent._get_attempted_strategies(sample_workflow)
        
        # Should return unique strategies only
        assert set(attempted_strategies) == {"retry_with_exponential_backoff", "fallback_approach"}
        assert len(attempted_strategies) == 2

    @pytest.mark.asyncio
    async def test_rollback_point_selection(self, orchestrator_agent, sample_workflow):
        """Test rollback point selection logic."""
        # Add multiple checkpoints
        sample_workflow["checkpoints"] = [
            {"name": "start", "timestamp": time.time() - 500},
            {"name": "after_extract", "timestamp": time.time() - 300},
            {"name": "after_transform", "timestamp": time.time() - 100}
        ]
        
        rollback_point = await orchestrator_agent._find_rollback_point(sample_workflow)
        
        # Should select the most recent checkpoint
        assert rollback_point["name"] == "after_transform"
        assert rollback_point["timestamp"] == sample_workflow["checkpoints"][2]["timestamp"]

    @pytest.mark.asyncio
    async def test_rollback_point_default_when_no_checkpoints(self, orchestrator_agent):
        """Test default rollback point when no checkpoints exist."""
        workflow_without_checkpoints = {
            "workflow_id": "test_workflow",
            "started_at": time.time() - 1000
        }
        
        rollback_point = await orchestrator_agent._find_rollback_point(workflow_without_checkpoints)
        
        # Should return default rollback point
        assert rollback_point["name"] == "workflow_start"
        assert rollback_point["state"] == "initial"
        assert rollback_point["timestamp"] == workflow_without_checkpoints["started_at"]