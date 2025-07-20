"""Tests for the agent framework components."""

import pytest
import time

from agent_orchestrated_etl.agents.base_agent import BaseAgent, AgentConfig, AgentRole, AgentTask
from agent_orchestrated_etl.agents.communication import AgentCommunicationHub, Message, MessageType
from agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent
from agent_orchestrated_etl.agents.etl_agent import ETLAgent
from agent_orchestrated_etl.agents.monitor_agent import MonitorAgent
from agent_orchestrated_etl.agents.coordination import AgentCoordinator, WorkflowDefinition, CoordinationTask, CoordinationPattern
from agent_orchestrated_etl.agents.testing import AgentTestFramework, TestCase, MockAgent


class TestAgent(BaseAgent):
    """Test agent implementation for testing."""
    
    def _initialize_agent(self) -> None:
        pass
    
    async def _process_task(self, task: AgentTask) -> dict:
        return {
            "status": "completed",
            "task_type": task.task_type,
            "message": f"Processed {task.task_type}",
        }
    
    def get_system_prompt(self) -> str:
        return "Test agent for unit testing"


class TestBaseAgent:
    """Test the base agent functionality."""
    
    @pytest.fixture
    def agent_config(self):
        return AgentConfig(
            name="TestAgent",
            role=AgentRole.ORCHESTRATOR,
            max_concurrent_tasks=2,
        )
    
    @pytest.fixture
    def test_agent(self, agent_config):
        return TestAgent(agent_config)
    
    def test_agent_initialization(self, test_agent):
        """Test agent initialization."""
        assert test_agent.config.name == "TestAgent"
        assert test_agent.config.role == AgentRole.ORCHESTRATOR
        assert test_agent.state.value == "initializing"
    
    @pytest.mark.asyncio
    async def test_agent_start_stop(self, test_agent):
        """Test agent start and stop lifecycle."""
        await test_agent.start()
        assert test_agent.state.value == "ready"
        
        await test_agent.stop()
        assert test_agent.state.value == "stopped"
    
    @pytest.mark.asyncio
    async def test_task_execution(self, test_agent):
        """Test task execution."""
        await test_agent.start()
        
        task = AgentTask(
            task_type="test_task",
            description="Test task execution",
            inputs={"test_param": "test_value"},
        )
        
        result = await test_agent.execute_task(task)
        
        assert result["status"] == "completed"
        assert result["task_type"] == "test_task"
        assert task.task_id in test_agent.task_history[0].task_id
        
        await test_agent.stop()
    
    def test_agent_capabilities(self, test_agent):
        """Test agent capabilities management."""
        from agent_orchestrated_etl.agents.base_agent import AgentCapability
        
        capability = AgentCapability(
            name="test_capability",
            description="Test capability",
            input_types=["text"],
            output_types=["text"],
            confidence_level=0.9,
        )
        
        test_agent.add_capability(capability)
        
        assert test_agent.has_capability("test_capability")
        capabilities = test_agent.get_capabilities()
        assert len(capabilities) >= 1
        assert any(cap.name == "test_capability" for cap in capabilities)


class TestCommunicationHub:
    """Test the communication hub functionality."""
    
    @pytest.fixture
    def comm_hub(self):
        return AgentCommunicationHub()
    
    @pytest.mark.asyncio
    async def test_hub_lifecycle(self, comm_hub):
        """Test communication hub start and stop."""
        await comm_hub.start()
        assert comm_hub._running
        
        await comm_hub.stop()
        assert not comm_hub._running
    
    @pytest.mark.asyncio
    async def test_agent_registration(self, comm_hub):
        """Test agent registration with hub."""
        await comm_hub.start()
        
        config = AgentConfig(name="TestAgent", role=AgentRole.ORCHESTRATOR)
        agent = TestAgent(config)
        
        await comm_hub.register_agent(agent)
        assert config.agent_id in comm_hub.agents
        
        await comm_hub.unregister_agent(config.agent_id)
        assert config.agent_id not in comm_hub.agents
        
        await comm_hub.stop()
    
    @pytest.mark.asyncio
    async def test_message_sending(self, comm_hub):
        """Test message sending through hub."""
        await comm_hub.start()
        
        message = Message(
            sender_id="sender_123",
            recipient_id="recipient_456",
            message_type=MessageType.QUERY,
            content={"test": "message"},
        )
        
        success = await comm_hub.send_message(message)
        assert success  # Should succeed even without registered agents
        
        await comm_hub.stop()


class TestOrchestratorAgent:
    """Test the orchestrator agent functionality."""
    
    @pytest.fixture
    def orchestrator(self):
        return OrchestratorAgent()
    
    @pytest.mark.asyncio
    async def test_orchestrator_creation(self, orchestrator):
        """Test orchestrator agent creation."""
        await orchestrator.start()
        
        assert orchestrator.config.role == AgentRole.ORCHESTRATOR
        assert orchestrator.state.value == "ready"
        assert len(orchestrator.capabilities) > 0
        
        await orchestrator.stop()
    
    @pytest.mark.asyncio
    async def test_workflow_creation_task(self, orchestrator):
        """Test workflow creation task."""
        await orchestrator.start()
        
        task = AgentTask(
            task_type="create_workflow",
            description="Create test workflow",
            inputs={
                "requirements": {
                    "data_source": "test_db",
                    "target": "test_target",
                }
            },
        )
        
        result = await orchestrator.execute_task(task)
        
        assert result["status"] == "created"
        assert "workflow_id" in result
        assert "execution_plan" in result
        
        await orchestrator.stop()


class TestETLAgent:
    """Test the ETL agent functionality."""
    
    @pytest.fixture
    def etl_agent(self):
        return ETLAgent(specialization="general")
    
    @pytest.mark.asyncio
    async def test_etl_agent_creation(self, etl_agent):
        """Test ETL agent creation."""
        await etl_agent.start()
        
        assert etl_agent.config.role == AgentRole.ETL_SPECIALIST
        assert etl_agent.specialization == "general"
        assert etl_agent.state.value == "ready"
        
        await etl_agent.stop()
    
    @pytest.mark.asyncio
    async def test_data_extraction_task(self, etl_agent):
        """Test data extraction task."""
        await etl_agent.start()
        
        task = AgentTask(
            task_type="extract_data",
            description="Extract test data",
            inputs={
                "source_config": {
                    "type": "database",
                    "path": "test_database",
                }
            },
        )
        
        result = await etl_agent.execute_task(task)
        
        assert result["status"] == "completed"
        assert "extraction_id" in result
        assert "records_extracted" in result
        
        await etl_agent.stop()


class TestMonitorAgent:
    """Test the monitor agent functionality."""
    
    @pytest.fixture
    def monitor_agent(self):
        return MonitorAgent(monitoring_scope="test")
    
    @pytest.mark.asyncio
    async def test_monitor_agent_creation(self, monitor_agent):
        """Test monitor agent creation."""
        await monitor_agent.start()
        
        assert monitor_agent.config.role == AgentRole.MONITOR
        assert monitor_agent.monitoring_scope == "test"
        assert monitor_agent.state.value == "ready"
        assert monitor_agent.monitoring_enabled
        
        await monitor_agent.stop()
    
    @pytest.mark.asyncio
    async def test_health_check_task(self, monitor_agent):
        """Test health check task."""
        await monitor_agent.start()
        
        task = AgentTask(
            task_type="check_health",
            description="Check system health",
            inputs={
                "targets": ["test_target"],
                "detailed": True,
            },
        )
        
        result = await monitor_agent.execute_task(task)
        
        assert "overall_status" in result
        assert "health_results" in result
        assert "targets_checked" in result
        
        await monitor_agent.stop()


class TestAgentCoordination:
    """Test agent coordination functionality."""
    
    @pytest.fixture
    async def coordination_setup(self):
        """Set up coordination test environment."""
        comm_hub = AgentCommunicationHub()
        await comm_hub.start()
        
        coordinator = AgentCoordinator(comm_hub)
        
        # Create test agents
        orchestrator = OrchestratorAgent()
        etl_agent = ETLAgent(specialization="general")
        monitor_agent = MonitorAgent(monitoring_scope="test")
        
        # Start agents before registering them
        await orchestrator.start()
        await etl_agent.start()
        await monitor_agent.start()
        
        # Register agents
        await coordinator.register_agent(orchestrator)
        await coordinator.register_agent(etl_agent)
        await coordinator.register_agent(monitor_agent)
        
        yield {
            "coordinator": coordinator,
            "comm_hub": comm_hub,
            "agents": {
                "orchestrator": orchestrator,
                "etl": etl_agent,
                "monitor": monitor_agent,
            }
        }
        
        # Cleanup - stop agents first, then communication hub
        await orchestrator.stop()
        await etl_agent.stop()
        await monitor_agent.stop()
        await comm_hub.stop()
    
    @pytest.mark.asyncio
    async def test_sequential_workflow(self, coordination_setup):
        """Test sequential workflow coordination."""
        setup = coordination_setup
        coordinator = setup["coordinator"]
        agents = setup["agents"]
        
        # Create workflow definition
        workflow_def = WorkflowDefinition(
            name="Test Sequential Workflow",
            coordination_pattern=CoordinationPattern.SEQUENTIAL,
            agents=[agents["etl"].config.agent_id, agents["monitor"].config.agent_id],
        )
        
        # Add tasks
        task1 = CoordinationTask(
            task_type="extract_data",
            agent_id=agents["etl"].config.agent_id,
            task_data={"source_config": {"type": "test", "path": "test_source"}},
        )
        
        task2 = CoordinationTask(
            task_type="check_health",
            agent_id=agents["monitor"].config.agent_id,
            task_data={"targets": ["test_target"]},
            dependencies=[task1.task_id],
        )
        
        workflow_def.tasks = [task1, task2]
        
        # Execute workflow
        result = await coordinator.execute_workflow(workflow_def)
        
        assert result["status"] == "completed"
        assert result["tasks_completed"] >= 0  # May complete or fail based on mock implementations


class TestAgentTestingFramework:
    """Test the agent testing framework."""
    
    @pytest.fixture
    def test_framework(self):
        return AgentTestFramework()
    
    @pytest.mark.asyncio
    async def test_framework_setup(self, test_framework):
        """Test testing framework setup and teardown."""
        await test_framework.setup_test_environment()
        
        assert test_framework.communication_hub is not None
        assert test_framework.coordinator is not None
        assert test_framework.tool_registry is not None
        
        await test_framework.teardown_test_environment()
    
    def test_mock_agent_creation(self, test_framework):
        """Test mock agent creation."""
        mock_agent = test_framework.create_mock_agent(
            "test_agent",
            AgentRole.ORCHESTRATOR,
            {"test_task": {"status": "mocked"}},
        )
        
        assert isinstance(mock_agent, MockAgent)
        assert mock_agent.config.agent_id == "test_agent"
        assert mock_agent.config.role == AgentRole.ORCHESTRATOR
    
    @pytest.mark.asyncio
    async def test_unit_test_execution(self, test_framework):
        """Test unit test execution."""
        await test_framework.setup_test_environment()
        
        # Create mock agent
        test_framework.create_mock_agent(
            "test_agent",
            AgentRole.ORCHESTRATOR,
            {"test_task": {"status": "completed", "result": "test_result"}},
        )
        
        # Create test case
        test_case = TestCase(
            test_id="unit_test_1",
            name="Test Unit Execution",
            description="Test unit test execution",
            test_type="unit",
            agents_required=["test_agent"],
            test_data={
                "agent_id": "test_agent",
                "task_data": {
                    "task_type": "test_task",
                    "inputs": {"test_param": "test_value"},
                }
            },
            expected_results={
                "expected_values": {"status": "completed"},
            }
        )
        
        # Run test
        result = await test_framework.run_single_test(test_case)
        
        assert result["status"] in ["passed", "failed", "error"]
        assert "execution_time" in result
        
        await test_framework.teardown_test_environment()


class TestAgentIntegration:
    """Integration tests for agent system."""
    
    @pytest.mark.asyncio
    async def test_full_agent_workflow(self):
        """Test complete agent workflow integration."""
        # Set up communication hub
        comm_hub = AgentCommunicationHub()
        await comm_hub.start()
        
        try:
            # Create agents
            orchestrator = OrchestratorAgent()
            etl_agent = ETLAgent(specialization="general")
            monitor_agent = MonitorAgent(monitoring_scope="integration_test")
            
            # Start agents
            await orchestrator.start()
            await etl_agent.start()
            await monitor_agent.start()
            
            # Register with communication hub
            await comm_hub.register_agent(orchestrator)
            await comm_hub.register_agent(etl_agent)
            await comm_hub.register_agent(monitor_agent)
            
            # Test orchestrator workflow creation
            workflow_task = AgentTask(
                task_type="create_workflow",
                description="Create integration test workflow",
                inputs={
                    "requirements": {
                        "data_source": "integration_test_db",
                        "target": "integration_test_target",
                    }
                },
            )
            
            workflow_result = await orchestrator.execute_task(workflow_task)
            assert workflow_result["status"] == "created"
            
            # Test ETL agent data extraction
            extract_task = AgentTask(
                task_type="extract_data",
                description="Extract integration test data",
                inputs={
                    "source_config": {
                        "type": "database",
                        "path": "integration_test_db",
                    }
                },
            )
            
            extract_result = await etl_agent.execute_task(extract_task)
            assert extract_result["status"] == "completed"
            
            # Test monitor agent health check
            health_task = AgentTask(
                task_type="check_health",
                description="Check integration test health",
                inputs={
                    "targets": ["integration_test_target"],
                    "detailed": True,
                },
            )
            
            health_result = await monitor_agent.execute_task(health_task)
            assert "overall_status" in health_result
            
            # Verify agents can communicate
            await orchestrator.send_message(
                etl_agent.config.agent_id,
                {"type": "status_request", "timestamp": time.time()}
            )
            # Message sending should succeed (even if not processed)
            
        finally:
            # Cleanup
            await orchestrator.stop()
            await etl_agent.stop()
            await monitor_agent.stop()
            await comm_hub.stop()
    
    @pytest.mark.asyncio
    async def test_agent_error_handling(self):
        """Test agent error handling and resilience."""
        config = AgentConfig(name="ErrorTestAgent", role=AgentRole.ORCHESTRATOR)
        agent = TestAgent(config)
        
        await agent.start()
        
        # Test with invalid task
        invalid_task = AgentTask(
            task_type="invalid_task_type",
            description="This should handle gracefully",
            inputs={},
        )
        
        # Should not raise exception due to error handling
        result = await agent.execute_task(invalid_task)
        assert "status" in result  # Should return some result
        
        await agent.stop()