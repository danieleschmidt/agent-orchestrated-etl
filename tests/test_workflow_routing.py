"""Test workflow routing functionality in orchestrator agent."""

import pytest
from unittest.mock import AsyncMock, MagicMock
from src.agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent
from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole, AgentTask


class TestWorkflowRouting:
    """Test workflow routing functionality."""

    @pytest.fixture
    def orchestrator_agent(self):
        """Create an orchestrator agent for testing."""
        config = AgentConfig(
            name="TestOrchestratorAgent",
            role=AgentRole.ORCHESTRATOR,
            max_concurrent_tasks=5,
        )
        
        agent = OrchestratorAgent(config=config)
        
        # Mock the tool registry and memory
        agent.tool_registry = MagicMock()
        agent.memory = MagicMock()
        agent.memory.store_entry = AsyncMock()
        
        return agent

    @pytest.mark.asyncio
    async def test_batch_processing_workflow_routing(self, orchestrator_agent):
        """Test routing workflow for batch processing target."""
        task = AgentTask(
            task_id="test_workflow_routing_batch",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "s3://bucket/batch-data/",
                    "target": "batch_processing",
                    "priority": "standard"
                },
                "workflow_id": "batch_workflow_001"
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "batch_schema", "tables": ["events"], "volume": "large"},  # analyze_data_source
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "load"}], "optimized_for": "batch"},  # generate_execution_plan
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify batch processing routing was applied
        assert result["workflow_type"] == "batch_processing"
        assert result["optimization_strategy"] == "throughput_optimized"
        assert result["resource_allocation"]["parallelism"] == "high"
        assert result["execution_priority"] == "standard"

    @pytest.mark.asyncio
    async def test_streaming_workflow_routing(self, orchestrator_agent):
        """Test routing workflow for streaming/real-time target."""
        task = AgentTask(
            task_id="test_workflow_routing_streaming",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "kafka://topic/streaming-events",
                    "target": "streaming",
                    "priority": "high"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "event_schema", "tables": ["stream"], "volume": "continuous"},
            {"steps": [{"type": "stream_extract"}, {"type": "transform"}, {"type": "stream_load"}], "optimized_for": "latency"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify streaming routing was applied
        assert result["workflow_type"] == "streaming"
        assert result["optimization_strategy"] == "latency_optimized"
        assert result["resource_allocation"]["memory_mode"] == "streaming"
        assert result["execution_priority"] == "high"

    @pytest.mark.asyncio
    async def test_data_warehouse_workflow_routing(self, orchestrator_agent):
        """Test routing workflow for data warehouse target."""
        task = AgentTask(
            task_id="test_workflow_routing_dwh",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "postgres://db/transactional",
                    "target": "data_warehouse",
                    "destination": "redshift://warehouse/tables"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "relational_schema", "tables": ["orders", "customers"], "volume": "medium"},
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "warehouse_load"}], "optimized_for": "analytics"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify data warehouse routing was applied
        assert result["workflow_type"] == "data_warehouse"
        assert result["optimization_strategy"] == "analytics_optimized"
        assert result["resource_allocation"]["compute_mode"] == "analytical"
        assert "data_quality_checks" in result["additional_steps"]

    @pytest.mark.asyncio
    async def test_machine_learning_workflow_routing(self, orchestrator_agent):
        """Test routing workflow for ML pipeline target."""
        task = AgentTask(
            task_id="test_workflow_routing_ml",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "s3://bucket/training-data/",
                    "target": "machine_learning",
                    "model_type": "classification"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "feature_schema", "tables": ["features"], "volume": "large"},
            {"steps": [{"type": "extract"}, {"type": "feature_engineering"}, {"type": "model_training"}], "optimized_for": "ml"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify ML routing was applied
        assert result["workflow_type"] == "machine_learning"
        assert result["optimization_strategy"] == "ml_optimized"
        assert "feature_engineering" in [step["type"] for step in result["execution_plan"]["steps"]]
        assert result["resource_allocation"]["gpu_enabled"] == True

    @pytest.mark.asyncio
    async def test_data_lake_workflow_routing(self, orchestrator_agent):
        """Test routing workflow for data lake target."""
        task = AgentTask(
            task_id="test_workflow_routing_lake",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "mixed://sources/raw-data/",
                    "target": "data_lake",
                    "storage_tier": "hot"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "mixed_schema", "tables": ["raw"], "volume": "very_large"},
            {"steps": [{"type": "extract"}, {"type": "partition"}, {"type": "lake_store"}], "optimized_for": "storage"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify data lake routing was applied
        assert result["workflow_type"] == "data_lake"
        assert result["optimization_strategy"] == "storage_optimized"
        assert result["resource_allocation"]["partitioning"] == "enabled"
        assert result["storage_configuration"]["tier"] == "hot"

    @pytest.mark.asyncio
    async def test_default_workflow_routing(self, orchestrator_agent):
        """Test default workflow routing when no target specified."""
        task = AgentTask(
            task_id="test_workflow_routing_default",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "file://local/data.csv"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "csv_schema", "tables": ["data"], "volume": "small"},
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "load"}], "optimized_for": "general"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify default routing was applied
        assert result["workflow_type"] == "general_purpose"
        assert result["optimization_strategy"] == "balanced"
        assert result["execution_priority"] == "normal"

    @pytest.mark.asyncio
    async def test_conditional_routing_based_on_data_volume(self, orchestrator_agent):
        """Test conditional routing based on data volume analysis."""
        task = AgentTask(
            task_id="test_conditional_routing",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "s3://bucket/large-dataset/",
                    "target": "auto"  # Auto-routing based on data characteristics
                }
            }
        )
        
        # Mock tool responses - simulate large volume dataset
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "large_schema", "tables": ["events"], "volume": "very_large", "estimated_size": "10TB"},
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "load"}], "optimized_for": "batch"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify auto-routing selected batch processing for large volume
        assert result["workflow_type"] == "batch_processing"
        assert result["routing_decision"]["reason"] == "large_data_volume"
        assert result["routing_decision"]["trigger"] == "volume_threshold_exceeded"

    @pytest.mark.asyncio
    async def test_routing_with_custom_parameters(self, orchestrator_agent):
        """Test workflow routing with custom parameters and constraints."""
        task = AgentTask(
            task_id="test_custom_routing",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "api://service/data",
                    "target": "batch_processing",
                    "custom_params": {
                        "max_parallel_tasks": 5,
                        "timeout_minutes": 30,
                        "retry_strategy": "exponential_backoff"
                    }
                },
                "constraints": {
                    "memory_limit": "8GB",
                    "execution_window": "2h"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "api_schema", "tables": ["api_data"], "volume": "medium"},
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "load"}], "optimized_for": "batch"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify custom parameters were applied
        assert result["resource_allocation"]["max_parallel_tasks"] == 5
        assert result["execution_configuration"]["timeout_minutes"] == 30
        assert result["execution_configuration"]["retry_strategy"] == "exponential_backoff"
        assert result["constraints"]["memory_limit"] == "8GB"

    @pytest.mark.asyncio
    async def test_routing_decision_audit_trail(self, orchestrator_agent):
        """Test that routing decisions are properly logged and auditable."""
        task = AgentTask(
            task_id="test_routing_audit",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "kafka://stream/events",
                    "target": "streaming"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "stream_schema", "tables": ["events"], "volume": "continuous"},
            {"steps": [{"type": "stream_extract"}, {"type": "transform"}, {"type": "stream_load"}], "optimized_for": "latency"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify audit trail is present
        assert "routing_decision" in result
        routing_decision = result["routing_decision"]
        assert routing_decision["target_specified"] == "streaming"
        assert routing_decision["routing_strategy"] == "target_based"
        assert routing_decision["timestamp"] > 0
        assert "decision_factors" in routing_decision
        
        # Verify memory storage was called for audit trail
        orchestrator_agent.memory.store_entry.assert_called()

    @pytest.mark.asyncio
    async def test_routing_failure_handling(self, orchestrator_agent):
        """Test routing behavior when invalid target is specified."""
        task = AgentTask(
            task_id="test_routing_failure",
            task_type="create_workflow",
            inputs={
                "requirements": {
                    "data_source": "file://data.txt",
                    "target": "invalid_target_type"
                }
            }
        )
        
        # Mock tool responses
        orchestrator_agent._use_tool = AsyncMock()
        orchestrator_agent._use_tool.side_effect = [
            {"schema": "text_schema", "tables": ["text"], "volume": "small"},
            {"steps": [{"type": "extract"}, {"type": "transform"}, {"type": "load"}], "optimized_for": "general"},
        ]
        
        result = await orchestrator_agent._create_workflow(task)
        
        # Verify fallback to default routing
        assert result["workflow_type"] == "general_purpose"
        assert result["routing_decision"]["fallback_reason"] == "invalid_target_specified"
        assert result["routing_decision"]["original_target"] == "invalid_target_type"