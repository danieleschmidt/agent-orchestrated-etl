"""Test enhanced monitor agent functionality."""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole
from src.agent_orchestrated_etl.agents.communication import AgentCommunicationHub
from src.agent_orchestrated_etl.agents.monitor_agent import MonitorAgent


class TestMonitorAgentEnhancements:
    """Test enhanced monitor agent functionality."""

    @pytest.fixture
    def monitor_agent(self):
        """Create a monitor agent for testing."""
        config = AgentConfig(
            name="TestMonitorAgent",
            role=AgentRole.MONITOR,
            max_concurrent_tasks=5,
        )

        # Mock communication hub
        communication_hub = MagicMock(spec=AgentCommunicationHub)

        agent = MonitorAgent(
            config=config,
            communication_hub=communication_hub,
            monitoring_scope="test"
        )

        return agent

    @pytest.mark.asyncio
    async def test_pipeline_status_monitoring(self, monitor_agent):
        """Test pipeline status monitoring functionality."""
        # Mock orchestrator agents response
        orchestrator_agents = [
            {"agent_id": "orchestrator_1", "role": "orchestrator"}
        ]

        # Mock pipeline status response
        pipeline_status = {
            "pipelines": [
                {
                    "pipeline_id": "test_pipeline_1",
                    "status": "running",
                    "started_at": time.time() - 300,  # 5 minutes ago
                    "tasks": [
                        {"task_id": "extract", "status": "completed"},
                        {"task_id": "transform", "status": "running"},
                        {"task_id": "load", "status": "pending"}
                    ]
                },
                {
                    "pipeline_id": "test_pipeline_2",
                    "status": "failed",
                    "started_at": time.time() - 600,  # 10 minutes ago
                    "tasks": [
                        {"task_id": "extract", "status": "completed"},
                        {"task_id": "transform", "status": "failed"}
                    ]
                }
            ]
        }

        # Mock communication hub methods
        monitor_agent.communication_hub.get_agents_by_role = AsyncMock(return_value=orchestrator_agents)
        monitor_agent.communication_hub.send_message = AsyncMock(return_value=pipeline_status)

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run pipeline status check
        await monitor_agent._check_pipeline_status()

        # Verify pipeline metrics were stored
        assert "pipelines" in monitor_agent.performance_metrics
        pipeline_metrics = monitor_agent.performance_metrics["pipelines"]
        assert len(pipeline_metrics) == 2

        # Check first pipeline (running)
        running_pipeline = next(p for p in pipeline_metrics if p["pipeline_id"] == "test_pipeline_1")
        assert running_pipeline["status"] == "running"
        assert running_pipeline["total_tasks"] == 3
        assert running_pipeline["completed_tasks"] == 1
        assert running_pipeline["running_tasks"] == 1
        assert running_pipeline["failed_tasks"] == 0

        # Check second pipeline (failed)
        failed_pipeline = next(p for p in pipeline_metrics if p["pipeline_id"] == "test_pipeline_2")
        assert failed_pipeline["status"] == "failed"
        assert failed_pipeline["total_tasks"] == 2
        assert failed_pipeline["completed_tasks"] == 1
        assert failed_pipeline["failed_tasks"] == 1

        # Verify alert was triggered for failed pipeline
        monitor_agent._trigger_alert.assert_called()
        alert_calls = monitor_agent._trigger_alert.call_args_list
        failure_alert = next(call for call in alert_calls if call[0][0] == "pipeline_failure")
        assert failure_alert[0][1] == "Pipeline test_pipeline_2 has failed"
        assert failure_alert[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_agent_health_monitoring(self, monitor_agent):
        """Test agent health monitoring functionality."""
        # Mock all agents response
        all_agents = [
            {"agent_id": "etl_agent_1", "role": "etl"},
            {"agent_id": "orchestrator_1", "role": "orchestrator"},
            {"agent_id": monitor_agent.config.agent_id, "role": "monitor"}  # Should be skipped
        ]

        # Mock health check responses
        health_responses = {
            "etl_agent_1": {
                "status": "healthy",
                "load": 0.3,
                "memory_usage": 45.2,
                "active_tasks": 2
            },
            "orchestrator_1": None  # Unresponsive agent
        }

        async def mock_send_message(agent_id, message, timeout=None):
            return health_responses.get(agent_id)

        # Mock communication hub methods
        monitor_agent.communication_hub.get_all_agents = AsyncMock(return_value=all_agents)
        monitor_agent.communication_hub.send_message = AsyncMock(side_effect=mock_send_message)

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run agent health check
        await monitor_agent._check_agent_health()

        # Verify agent health metrics were stored
        assert "agent_health" in monitor_agent.performance_metrics
        agent_health_metrics = monitor_agent.performance_metrics["agent_health"]
        assert len(agent_health_metrics) == 2  # Should exclude self

        # Check healthy agent
        healthy_agent = next(m for m in agent_health_metrics if m["agent_id"] == "etl_agent_1")
        assert healthy_agent["status"] == "healthy"
        assert healthy_agent["agent_role"] == "etl"
        assert healthy_agent["response_time"] > 0

        # Check unresponsive agent
        unresponsive_agent = next(m for m in agent_health_metrics if m["agent_id"] == "orchestrator_1")
        assert unresponsive_agent["status"] == "unresponsive"
        assert unresponsive_agent["agent_role"] == "orchestrator"
        assert unresponsive_agent["error_message"] == "No response to health check"

        # Verify health status was stored
        assert monitor_agent.health_status["etl_agent_1"] == "healthy"
        assert monitor_agent.health_status["orchestrator_1"] == "unresponsive"

        # Verify alert was triggered for unresponsive agent
        monitor_agent._trigger_alert.assert_called()
        alert_calls = monitor_agent._trigger_alert.call_args_list
        unhealthy_alert = next(call for call in alert_calls if call[0][0] == "agent_unhealthy")
        assert "orchestrator_1" in unhealthy_alert[0][1]
        assert "unresponsive" in unhealthy_alert[0][1]
        assert unhealthy_alert[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_system_health_metrics(self, monitor_agent):
        """Test system health metrics calculation."""
        # Simulate health checks for system health calculation
        health_checks = [
            {"agent_id": "agent1", "status": "healthy", "response_time": 100},
            {"agent_id": "agent2", "status": "healthy", "response_time": 150},
            {"agent_id": "agent3", "status": "degraded", "response_time": 300},
            {"agent_id": "agent4", "status": "unresponsive", "response_time": 5000},
            {"agent_id": "agent5", "status": "healthy", "response_time": 120}
        ]

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run system health update
        await monitor_agent._update_system_health_metrics(health_checks)

        # Verify system health metrics were calculated correctly
        assert "system_health" in monitor_agent.performance_metrics
        system_health = monitor_agent.performance_metrics["system_health"][-1]

        assert system_health["total_agents"] == 5
        assert system_health["healthy_agents"] == 3
        assert system_health["degraded_agents"] == 1
        assert system_health["unhealthy_agents"] == 1
        assert system_health["health_percentage"] == 60.0  # 3/5 * 100
        assert system_health["system_status"] == "unhealthy"  # <70% healthy

        # Verify alert was triggered for poor system health
        monitor_agent._trigger_alert.assert_called()
        alert_calls = monitor_agent._trigger_alert.call_args_list
        system_alert = next(call for call in alert_calls if call[0][0] == "system_unhealthy")
        assert "60.0%" in system_alert[0][1]
        assert system_alert[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_pipeline_timeout_detection(self, monitor_agent):
        """Test pipeline timeout detection."""
        # Set up a long-running pipeline that exceeds timeout threshold
        current_time = time.time()
        long_running_pipeline = {
            "timestamp": current_time,
            "pipeline_id": "slow_pipeline",
            "status": "running",
            "started_at": current_time - 8000,  # 8000 seconds ago (> 7200 threshold)
            "duration": 8000,
            "total_tasks": 3,
            "completed_tasks": 1,
            "failed_tasks": 0,
            "running_tasks": 2
        }

        # Add to performance metrics
        monitor_agent.performance_metrics["pipelines"] = [long_running_pipeline]

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run timeout check
        await monitor_agent._check_pipeline_timeouts()

        # Verify timeout alert was triggered
        monitor_agent._trigger_alert.assert_called_once()
        alert_call = monitor_agent._trigger_alert.call_args
        assert alert_call[0][0] == "pipeline_timeout"
        assert "slow_pipeline" in alert_call[0][1]
        assert "8000 seconds" in alert_call[0][1]
        assert alert_call[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_high_task_failure_rate_alert(self, monitor_agent):
        """Test high task failure rate alert."""
        # Set up pipeline with high failure rate
        high_failure_pipeline = {
            "pipeline_id": "failing_pipeline",
            "status": "running",
            "duration": 1800,
            "total_tasks": 10,
            "completed_tasks": 4,
            "failed_tasks": 5,  # 50% failure rate
            "running_tasks": 1
        }

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run pipeline alert check
        await monitor_agent._check_pipeline_alerts(high_failure_pipeline)

        # Verify high failure rate alert was triggered
        monitor_agent._trigger_alert.assert_called()
        alert_calls = monitor_agent._trigger_alert.call_args_list
        failure_rate_alert = next(call for call in alert_calls if "failure_rate" in call[0][0])
        assert "failing_pipeline" in failure_rate_alert[0][1]
        assert "50.0%" in failure_rate_alert[0][1]
        assert failure_rate_alert[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_agent_slow_response_alert(self, monitor_agent):
        """Test agent slow response time alert."""
        # Set up agent with slow response
        slow_agent_metric = {
            "agent_id": "slow_agent",
            "agent_role": "etl",
            "status": "healthy",
            "response_time": 12000  # 12 seconds (> 10s critical threshold)
        }

        # Mock alert triggering
        monitor_agent._trigger_alert = AsyncMock()

        # Run agent health alert check
        await monitor_agent._check_agent_health_alerts(slow_agent_metric)

        # Verify slow response alert was triggered
        monitor_agent._trigger_alert.assert_called()
        alert_call = monitor_agent._trigger_alert.call_args
        assert alert_call[0][0] == "agent_slow_response"
        assert "slow_agent" in alert_call[0][1]
        assert "12000ms" in alert_call[0][1]
        assert alert_call[0][2] == "critical"

    @pytest.mark.asyncio
    async def test_metrics_cleanup(self, monitor_agent):
        """Test that old metrics are properly cleaned up."""
        current_time = time.time()

        # Add old and new metrics
        old_pipeline_metric = {
            "timestamp": current_time - (25 * 60 * 60),  # 25 hours ago
            "pipeline_id": "old_pipeline"
        }
        new_pipeline_metric = {
            "timestamp": current_time - (1 * 60 * 60),   # 1 hour ago
            "pipeline_id": "new_pipeline"
        }

        monitor_agent.performance_metrics["pipelines"] = [old_pipeline_metric, new_pipeline_metric]

        # Mock communication hub to avoid actual calls
        monitor_agent.communication_hub.get_agents_by_role = AsyncMock(return_value=[])

        # Run pipeline status check (which includes cleanup)
        await monitor_agent._check_pipeline_status()

        # Verify old metrics were cleaned up
        remaining_metrics = monitor_agent.performance_metrics["pipelines"]
        assert len(remaining_metrics) == 1
        assert remaining_metrics[0]["pipeline_id"] == "new_pipeline"

    def test_default_thresholds_and_intervals(self, monitor_agent):
        """Test that default alert thresholds and monitoring intervals are set correctly."""
        # Check alert thresholds
        assert monitor_agent.alert_thresholds["pipeline_duration"]["warning"] == 3600.0
        assert monitor_agent.alert_thresholds["pipeline_duration"]["critical"] == 7200.0
        assert monitor_agent.alert_thresholds["error_rate"]["warning"] == 5.0
        assert monitor_agent.alert_thresholds["error_rate"]["critical"] == 10.0
        assert monitor_agent.alert_thresholds["response_time"]["warning"] == 5000.0
        assert monitor_agent.alert_thresholds["response_time"]["critical"] == 10000.0

        # Check monitoring intervals
        assert monitor_agent.monitoring_intervals["pipeline_status"] == 60.0
        assert monitor_agent.monitoring_intervals["agent_health"] == 60.0
        assert monitor_agent.monitoring_intervals["system_health"] == 30.0
        assert monitor_agent.monitoring_intervals["performance_metrics"] == 120.0

    @pytest.mark.asyncio
    async def test_memory_storage_for_critical_events(self, monitor_agent):
        """Test that critical events are stored in agent memory."""
        # Mock memory storage
        monitor_agent.memory.store_entry = AsyncMock()

        # Process a failed pipeline
        failed_pipeline = {
            "pipeline_id": "failed_pipeline",
            "status": "failed",
            "started_at": time.time() - 300,
            "tasks": [{"task_id": "extract", "status": "failed"}]
        }

        await monitor_agent._process_pipeline_status([failed_pipeline])

        # Verify memory entry was stored
        monitor_agent.memory.store_entry.assert_called()
        memory_call = monitor_agent.memory.store_entry.call_args
        assert "failed_pipeline" in memory_call[1]["content"]
        assert "failed" in memory_call[1]["content"]
        assert memory_call[1]["importance"].name == "HIGH"  # Failed pipelines are high importance
