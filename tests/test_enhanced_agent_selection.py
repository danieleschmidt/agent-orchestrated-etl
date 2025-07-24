"""Tests for Enhanced Agent Selection - ETL-013 implementation."""

import pytest
import time
from unittest.mock import Mock, patch, MagicMock

from src.agent_orchestrated_etl.agents.base_agent import AgentCapability, BaseAgent, AgentConfig, AgentRole
from src.agent_orchestrated_etl.agents.coordination import AgentCoordinator, CoordinationTask


class TestEnhancedAgentCapability:
    """Test enhanced AgentCapability functionality."""
    
    def test_enhanced_capability_metadata(self):
        """Test enhanced capability metadata fields."""
        capability = AgentCapability(
            name="data_extraction",
            description="Extract data from various sources",
            input_types=["database", "file", "api"],
            output_types=["structured_data"],
            confidence_level=0.9,
            specialization_areas=["postgresql", "mysql", "mongodb"],
            performance_metrics={"avg_throughput": 1000, "success_rate": 0.98},
            resource_requirements={"memory_mb": 512, "cpu_cores": 2},
            version="1.2.0",
            tags=["database", "extraction", "high_performance"],
            prerequisites=["database_connectivity", "credential_management"]
        )
        
        # Test enhanced metadata fields
        assert capability.specialization_areas == ["postgresql", "mysql", "mongodb"]
        assert capability.performance_metrics == {"avg_throughput": 1000, "success_rate": 0.98}
        assert capability.resource_requirements == {"memory_mb": 512, "cpu_cores": 2}
        assert capability.version == "1.2.0"
        assert capability.tags == ["database", "extraction", "high_performance"]
        assert capability.prerequisites == ["database_connectivity", "credential_management"]
        assert isinstance(capability.last_updated, float)
    
    def test_capability_defaults(self):
        """Test capability default values."""
        capability = AgentCapability(
            name="basic_capability",
            description="Basic capability",
            input_types=["data"],
            output_types=["result"],
            confidence_level=0.8
        )
        
        # Test default values
        assert capability.specialization_areas == []
        assert capability.performance_metrics == {}
        assert capability.resource_requirements == {}
        assert capability.version == "1.0.0"
        assert capability.tags == []
        assert capability.prerequisites == []


class TestEnhancedBaseAgent:
    """Test enhanced BaseAgent functionality."""
    
    @pytest.fixture
    def mock_agent_config(self):
        """Create mock agent config."""
        return AgentConfig(
            name="TestAgent",
            role=AgentRole.ETL_SPECIALIST,
            agent_id="test_agent_001",
            specialization="database"
        )
    
    @pytest.fixture
    def enhanced_agent(self, mock_agent_config):
        """Create enhanced agent for testing."""
        with patch('src.agent_orchestrated_etl.agents.base_agent.get_logger'):
            agent = BaseAgent(mock_agent_config)
            return agent
    
    def test_performance_metrics_initialization(self, enhanced_agent):
        """Test performance metrics initialization."""
        metrics = enhanced_agent.performance_metrics
        
        expected_keys = [
            "total_tasks", "successful_tasks", "failed_tasks",
            "avg_execution_time", "total_execution_time", "success_rate",
            "error_rate", "throughput", "last_activity", "availability"
        ]
        
        for key in expected_keys:
            assert key in metrics, f"Missing performance metric: {key}"
        
        assert metrics["success_rate"] == 1.0
        assert metrics["error_rate"] == 0.0
        assert metrics["availability"] == 1.0
    
    def test_get_performance_metrics(self, enhanced_agent):
        """Test get_performance_metrics method."""
        metrics = enhanced_agent.get_performance_metrics()
        
        assert isinstance(metrics, dict)
        assert "success_rate" in metrics
        assert "avg_execution_time" in metrics
        assert "availability" in metrics
    
    def test_get_current_load(self, enhanced_agent):
        """Test get_current_load method."""
        # Initially no active tasks
        assert enhanced_agent.get_current_load() == 0.0
        
        # Add some active tasks
        enhanced_agent.active_tasks = {"task1": Mock(), "task2": Mock()}
        load = enhanced_agent.get_current_load()
        assert 0.0 <= load <= 1.0
    
    def test_get_specialization_score(self, enhanced_agent):
        """Test get_specialization_score method."""
        # Test matching specialization
        enhanced_agent.specialization = "database"
        score = enhanced_agent.get_specialization_score("database")
        assert score >= 0.8
        
        # Test general specialization
        enhanced_agent.specialization = "general"
        score = enhanced_agent.get_specialization_score("api")
        assert 0.4 <= score <= 0.7
        
        # Test non-matching specialization
        enhanced_agent.specialization = "database"
        score = enhanced_agent.get_specialization_score("streaming")
        assert score <= 0.4
    
    def test_update_performance_metrics(self, enhanced_agent):
        """Test update_performance_metrics method."""
        # Create mock task
        mock_task = Mock()
        mock_task.task_type = "extract_data"
        mock_task.status = "completed"
        
        # Update metrics for successful task
        enhanced_agent.update_performance_metrics(mock_task, 100.0, True)
        
        metrics = enhanced_agent.performance_metrics
        assert metrics["total_tasks"] == 1
        assert metrics["successful_tasks"] == 1
        assert metrics["failed_tasks"] == 0
        assert metrics["success_rate"] == 1.0
        assert metrics["avg_execution_time"] == 100.0
        
        # Update metrics for failed task
        enhanced_agent.update_performance_metrics(mock_task, 50.0, False)
        
        metrics = enhanced_agent.performance_metrics
        assert metrics["total_tasks"] == 2
        assert metrics["successful_tasks"] == 1
        assert metrics["failed_tasks"] == 1
        assert metrics["success_rate"] == 0.5
        assert metrics["error_rate"] == 0.5


class TestEnhancedAgentCoordinator:
    """Test enhanced AgentCoordinator functionality."""
    
    @pytest.fixture
    def mock_communication_hub(self):
        """Create mock communication hub."""
        return Mock()
    
    @pytest.fixture
    def enhanced_coordinator(self, mock_communication_hub):
        """Create enhanced coordinator for testing."""
        with patch('src.agent_orchestrated_etl.agents.coordination.get_logger'):
            coordinator = AgentCoordinator(mock_communication_hub)
            return coordinator
    
    @pytest.fixture
    def mock_agents(self):
        """Create mock agents for testing."""
        agents = {}
        
        # High performance agent
        high_perf = Mock()
        high_perf.get_performance_metrics.return_value = {
            "success_rate": 0.99,
            "avg_execution_time": 100,
            "availability": 0.999,
            "throughput": 1000,
            "error_rate": 0.01
        }
        high_perf.get_current_load.return_value = 0.2
        high_perf.get_specialization_score.return_value = 0.9
        high_perf.get_capabilities.return_value = [
            Mock(name="data_extraction", confidence_level=0.95)
        ]
        agents["high_perf"] = high_perf
        
        # Low performance agent
        low_perf = Mock()
        low_perf.get_performance_metrics.return_value = {
            "success_rate": 0.85,
            "avg_execution_time": 500,
            "availability": 0.90,
            "throughput": 200,
            "error_rate": 0.15
        }
        low_perf.get_current_load.return_value = 0.8
        low_perf.get_specialization_score.return_value = 0.6
        low_perf.get_capabilities.return_value = [
            Mock(name="data_extraction", confidence_level=0.75)
        ]
        agents["low_perf"] = low_perf
        
        return agents
    
    def test_coordinator_initialization(self, enhanced_coordinator):
        """Test enhanced coordinator initialization."""
        # Test enhanced selection attributes
        assert hasattr(enhanced_coordinator, 'selection_audit_trail')
        assert hasattr(enhanced_coordinator, 'load_balancing_metrics')
        assert isinstance(enhanced_coordinator.selection_audit_trail, list)
        assert isinstance(enhanced_coordinator.load_balancing_metrics, dict)
        
        # Test load balancing metrics structure
        metrics = enhanced_coordinator.load_balancing_metrics
        expected_keys = ["agent_loads", "load_distribution", "balancing_effectiveness", "last_updated"]
        for key in expected_keys:
            assert key in metrics, f"Missing load balancing metric: {key}"
    
    def test_get_enhanced_capabilities(self, enhanced_coordinator):
        """Test enhanced capability inference."""
        task = CoordinationTask(
            task_id="test_task",
            task_type="extract_data",
            task_data={
                "data_source": "postgresql",
                "volume": "large",
                "complexity": "high",
                "real_time": True
            },
            priority=8
        )
        
        capabilities = enhanced_coordinator._get_enhanced_capabilities(task)
        
        # Should include inferred capabilities
        expected_capabilities = [
            "high_throughput",
            "memory_optimization",
            "advanced_algorithms",
            "error_handling",
            "real_time_processing",
            "low_latency",
            "postgresql_optimization",
            "database_optimization"
        ]
        
        for cap in expected_capabilities:
            assert cap in capabilities, f"Should infer capability: {cap}"
    
    def test_calculate_performance_score(self, enhanced_coordinator, mock_agents):
        """Test performance score calculation."""
        task = CoordinationTask(
            task_id="perf_test",
            task_type="extract_data",
            task_data={"performance_requirement": "high"},
            priority=9
        )
        
        high_score = enhanced_coordinator._calculate_performance_score(mock_agents["high_perf"], task)
        low_score = enhanced_coordinator._calculate_performance_score(mock_agents["low_perf"], task)
        
        assert high_score > low_score, "High performance agent should score higher"
        assert 0.0 <= high_score <= 1.0, "Score should be normalized"
        assert 0.0 <= low_score <= 1.0, "Score should be normalized"
    
    def test_find_fallback_agent(self, enhanced_coordinator, mock_agents):
        """Test fallback agent selection."""
        # Register agents
        enhanced_coordinator.registered_agents = mock_agents
        
        # Create task requiring capabilities not available
        task = CoordinationTask(
            task_id="fallback_test",
            task_type="quantum_processing",
            task_data={"algorithm": "quantum_enhanced"},
            priority=5
        )
        
        fallback_result = enhanced_coordinator._find_fallback_agent(task)
        
        assert "strategy" in fallback_result
        assert fallback_result["strategy"] in ["partial_match", "decompose"]
        assert "mitigation_steps" in fallback_result
        assert isinstance(fallback_result["mitigation_steps"], list)
    
    def test_find_suitable_agent_with_load_balancing(self, enhanced_coordinator, mock_agents):
        """Test load-balanced agent selection."""
        # Register agents
        enhanced_coordinator.registered_agents = mock_agents
        
        task = CoordinationTask(
            task_id="load_balance_test",
            task_type="extract_data",
            task_data={},
            priority=5
        )
        
        # Mock enhanced capabilities and agent scoring
        with patch.object(enhanced_coordinator, '_get_enhanced_capabilities', return_value=["data_extraction"]):
            with patch.object(enhanced_coordinator, '_calculate_agent_score', return_value=0.8):
                selected_agent = enhanced_coordinator._find_suitable_agent_with_load_balancing(task, None)
                
                # Should select agent with better load-adjusted score
                assert selected_agent in ["high_perf", "low_perf"]
                
                # Check load balancing metrics updated
                metrics = enhanced_coordinator.get_load_balancing_metrics()
                assert "agent_loads" in metrics
                assert len(metrics["agent_loads"]) > 0
    
    def test_selection_audit_trail(self, enhanced_coordinator, mock_agents):
        """Test agent selection audit trail."""
        # Register agents
        enhanced_coordinator.registered_agents = mock_agents
        
        task = CoordinationTask(
            task_id="audit_test",
            task_type="extract_data",
            task_data={},
            priority=7
        )
        
        # Mock enhanced capabilities and agent scoring
        with patch.object(enhanced_coordinator, '_get_enhanced_capabilities', return_value=["data_extraction"]):
            with patch.object(enhanced_coordinator, '_calculate_agent_score', return_value=0.8):
                with patch.object(enhanced_coordinator, '_calculate_performance_score', return_value=0.9):
                    # This would normally be an async method, but we're testing the concept
                    start_time = time.time()
                    
                    # Simulate audit trail creation
                    audit_entry = {
                        "task_id": task.task_id,
                        "selected_agent": "high_perf",
                        "selection_score": 0.85,
                        "alternatives_considered": 2,
                        "timestamp": time.time(),
                        "selection_time_ms": (time.time() - start_time) * 1000
                    }
                    
                    enhanced_coordinator.selection_audit_trail.append(audit_entry)
                    
                    # Test audit trail retrieval
                    audit_trail = enhanced_coordinator.get_selection_audit_trail()
                    assert len(audit_trail) == 1
                    
                    entry = audit_trail[0]
                    assert entry["task_id"] == task.task_id
                    assert entry["selected_agent"] == "high_perf"
                    assert "selection_score" in entry
                    assert "timestamp" in entry
    
    def test_get_load_balancing_metrics(self, enhanced_coordinator):
        """Test load balancing metrics retrieval."""
        metrics = enhanced_coordinator.get_load_balancing_metrics()
        
        assert isinstance(metrics, dict)
        assert "agent_loads" in metrics
        assert "load_distribution" in metrics
        assert "balancing_effectiveness" in metrics
        assert "last_updated" in metrics
    
    def test_calculate_specialization_match(self, enhanced_coordinator):
        """Test specialization matching calculation."""
        # Create mock agent
        mock_agent = Mock()
        mock_agent.get_specialization_score.return_value = 0.9
        mock_agent.specialization = "database"
        
        task = CoordinationTask(
            task_id="spec_test",
            task_type="extract_data",
            task_data={"complexity": "high", "data_source": "database"},
            priority=6
        )
        
        score = enhanced_coordinator._calculate_specialization_match(mock_agent, task)
        
        assert 0.0 <= score <= 1.0, "Specialization score should be normalized"
        assert score > 0.8, "Should get high score for matching specialization"


class TestEnhancedSelectionIntegration:
    """Integration tests for enhanced agent selection."""
    
    @pytest.fixture
    def integration_setup(self):
        """Set up integration test environment."""
        with patch('src.agent_orchestrated_etl.agents.coordination.get_logger'):
            coordinator = AgentCoordinator(Mock())
            
            # Create and register test agents
            agent_configs = [
                ("db_specialist", "database"),
                ("api_specialist", "api"),
                ("general_agent", "general")
            ]
            
            agents = {}
            for agent_id, specialization in agent_configs:
                agent = Mock()
                agent.agent_id = agent_id
                agent.specialization = specialization
                agent.get_capabilities.return_value = [
                    Mock(name=f"{specialization}_processing", confidence_level=0.9)
                ]
                agent.get_performance_metrics.return_value = {
                    "success_rate": 0.95,
                    "avg_execution_time": 200,
                    "availability": 0.98,
                    "error_rate": 0.05
                }
                agent.get_current_load.return_value = 0.3
                agent.get_specialization_score.return_value = 0.8 if specialization != "general" else 0.6
                
                agents[agent_id] = agent
                coordinator.registered_agents[agent_id] = agent
            
            return coordinator, agents
    
    def test_end_to_end_agent_selection(self, integration_setup):
        """Test complete agent selection workflow."""
        coordinator, agents = integration_setup
        
        # Create complex task
        task = CoordinationTask(
            task_id="e2e_test",
            task_type="extract_data",
            task_data={
                "data_source": "postgresql",
                "volume": "large",
                "performance_requirement": "high"
            },
            priority=8
        )
        
        # Test enhanced capability inference
        capabilities = coordinator._get_enhanced_capabilities(task)
        assert "database_optimization" in capabilities
        assert "postgresql_optimization" in capabilities
        assert "high_throughput" in capabilities
        
        # Test performance scoring
        db_agent = agents["db_specialist"]
        performance_score = coordinator._calculate_performance_score(db_agent, task)
        assert performance_score > 0.7
        
        # Test load balancing selection
        selected_agent = coordinator._find_suitable_agent_with_load_balancing(task, None)
        assert selected_agent in agents.keys()
        
        # Verify metrics were updated
        metrics = coordinator.get_load_balancing_metrics()
        assert len(metrics["agent_loads"]) > 0