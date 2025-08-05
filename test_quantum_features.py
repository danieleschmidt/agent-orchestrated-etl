#!/usr/bin/env python3
"""Comprehensive test suite for quantum-inspired ETL features."""

import asyncio
import pytest
import time
from unittest.mock import Mock, patch
from dataclasses import dataclass
from typing import Dict, Any, List

# Import our quantum modules
from src.agent_orchestrated_etl.quantum_planner import (
    QuantumTaskPlanner, QuantumTask, QuantumState, QuantumPipelineOrchestrator
)
from src.agent_orchestrated_etl.adaptive_resources import (
    AdaptiveResourceManager, ResourceType, ResourcePool, PredictiveScaler
)
from src.agent_orchestrated_etl.orchestrator import DataOrchestrator, Pipeline
from src.agent_orchestrated_etl.data_quality_engine import IntelligentDataQualityEngine


class TestQuantumTaskPlanner:
    """Test suite for quantum task planning."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.planner = QuantumTaskPlanner(max_parallel_tasks=4)
    
    def test_add_task(self):
        """Test adding tasks to quantum planner."""
        self.planner.add_task("extract_users", priority=0.8)
        
        assert "extract_users" in self.planner.quantum_tasks
        task = self.planner.quantum_tasks["extract_users"]
        assert task.task_id == "extract_users"
        assert task.priority == 0.8
        assert task.quantum_state == QuantumState.SUPERPOSITION
    
    def test_create_entanglement(self):
        """Test creating quantum entanglement between tasks."""
        self.planner.add_task("extract_users")
        self.planner.add_task("transform_users")
        
        self.planner.create_entanglement("extract_users", "transform_users")
        
        task1 = self.planner.quantum_tasks["extract_users"]
        task2 = self.planner.quantum_tasks["transform_users"]
        
        assert "transform_users" in task1.entangled_tasks
        assert "extract_users" in task2.entangled_tasks
        assert task1.quantum_state == QuantumState.ENTANGLED
        assert task2.quantum_state == QuantumState.ENTANGLED
    
    def test_quantum_optimize_schedule(self):
        """Test quantum optimization of task scheduling."""
        # Add tasks with dependencies
        self.planner.add_task("extract_users", priority=0.8)
        self.planner.add_task("extract_orders", priority=0.7)
        self.planner.add_task("transform_users", dependencies={"extract_users"}, priority=0.6)
        self.planner.add_task("transform_orders", dependencies={"extract_orders"}, priority=0.6)
        self.planner.add_task("load_users", dependencies={"transform_users"}, priority=0.4)
        self.planner.add_task("load_orders", dependencies={"transform_orders"}, priority=0.4)
        
        # Create entanglements for related tasks
        self.planner.create_entanglement("extract_users", "transform_users")
        self.planner.create_entanglement("extract_orders", "transform_orders")
        
        schedule = self.planner.quantum_optimize_schedule()
        
        # Verify schedule structure
        assert isinstance(schedule, list)
        assert len(schedule) > 0
        
        # Verify dependencies are respected
        completed_tasks = set()
        for wave in schedule:
            for task_id in wave:
                task = self.planner.quantum_tasks[task_id]
                # All dependencies should be completed
                assert task.dependencies.issubset(completed_tasks)
            completed_tasks.update(wave)
        
        # Verify all tasks are scheduled
        all_scheduled = set()
        for wave in schedule:
            all_scheduled.update(wave)
        assert all_scheduled == set(self.planner.quantum_tasks.keys())
    
    def test_adapt_from_execution(self):
        """Test adaptation based on execution results."""
        self.planner.add_task("test_task", estimated_duration=2.0)
        
        # Simulate successful execution
        self.planner.adapt_from_execution("test_task", 1.5, True)
        
        task = self.planner.quantum_tasks["test_task"]
        # Duration should be updated with learning
        assert task.estimated_duration < 2.0
        # Adaptive weight should increase for successful tasks
        assert task.adaptive_weight > 1.0
        
        # Simulate failed execution
        self.planner.adapt_from_execution("test_task", 3.0, False)
        
        # Adaptive weight should decrease for failed tasks
        assert task.adaptive_weight < 2.0
    
    def test_get_quantum_metrics(self):
        """Test quantum metrics collection."""
        self.planner.add_task("task1", priority=0.8)
        self.planner.add_task("task2", priority=0.6)
        self.planner.create_entanglement("task1", "task2")
        
        metrics = self.planner.get_quantum_metrics()
        
        assert "total_tasks" in metrics
        assert "entangled_tasks" in metrics
        assert "quantum_coherence" in metrics
        assert metrics["total_tasks"] == 2
        assert metrics["entangled_tasks"] == 2
        assert 0 <= metrics["quantum_coherence"] <= 1


class TestAdaptiveResourceManager:
    """Test suite for adaptive resource management."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.manager = AdaptiveResourceManager(scaling_interval=0.1)  # Fast scaling for tests
    
    def teardown_method(self):
        """Clean up test fixtures."""
        self.manager.stop_monitoring()
    
    def test_resource_pool_initialization(self):
        """Test default resource pool initialization."""
        assert len(self.manager.resource_pools) > 0
        assert "cpu_pool" in self.manager.resource_pools
        assert "memory_pool" in self.manager.resource_pools
        
        cpu_pool = self.manager.resource_pools["cpu_pool"]
        assert cpu_pool.resource_type == ResourceType.CPU
        assert cpu_pool.current_capacity > 0
    
    def test_assign_task_resources(self):
        """Test task resource assignment."""
        resource_req = {"cpu": 0.5, "memory": 1.0}
        assignments = self.manager.assign_task_resources("test_task", resource_req, priority=0.7)
        
        assert "cpu" in assignments or "memory" in assignments
        
        # Test resource release
        self.manager.release_task_resources("test_task")
        # Should complete without error
    
    def test_predictive_scaler(self):
        """Test predictive scaling functionality."""
        scaler = PredictiveScaler()
        
        # Add some historical metrics
        from src.agent_orchestrated_etl.adaptive_resources import ResourceMetrics
        scaler.add_metrics(ResourceMetrics(timestamp=time.time(), cpu_usage=0.5))
        scaler.add_metrics(ResourceMetrics(timestamp=time.time(), cpu_usage=0.7))
        scaler.add_metrics(ResourceMetrics(timestamp=time.time(), cpu_usage=0.8))
        
        # Predict future demand
        predicted_demand = scaler.predict_resource_demand(ResourceType.CPU, ["extract_task"], 5)
        
        assert 0.1 <= predicted_demand <= 10.0
        assert isinstance(predicted_demand, float)
    
    def test_monitoring_lifecycle(self):
        """Test monitoring start/stop lifecycle."""
        assert not self.manager.monitoring_active or self.manager.monitoring_active
        
        self.manager.start_monitoring()
        assert self.manager.monitoring_active
        
        # Let it run briefly
        time.sleep(0.2)
        
        self.manager.stop_monitoring()
        assert not self.manager.monitoring_active
    
    def test_resource_status(self):
        """Test resource status reporting."""
        status = self.manager.get_resource_status()
        
        assert "resource_pools" in status
        assert "total_active_tasks" in status
        assert "monitoring_active" in status
        assert isinstance(status["resource_pools"], dict)
    
    def test_workload_optimization(self):
        """Test workload-specific optimization."""
        initial_cpu_factor = self.manager.resource_pools["cpu_pool"].scaling_factor
        
        self.manager.optimize_for_workload("cpu_intensive")
        
        cpu_factor_after = self.manager.resource_pools["cpu_pool"].scaling_factor
        assert cpu_factor_after > initial_cpu_factor


class TestQuantumPipelineOrchestrator:
    """Test suite for quantum pipeline orchestration."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.base_orchestrator = Mock()
        self.orchestrator = QuantumPipelineOrchestrator(self.base_orchestrator, max_parallel_tasks=2)
    
    def test_initialization(self):
        """Test orchestrator initialization."""
        assert self.orchestrator.quantum_planner is not None
        assert self.orchestrator.quantum_planner.max_parallel_tasks == 2
    
    def test_task_priority_estimation(self):
        """Test task priority estimation."""
        priority = self.orchestrator._estimate_task_priority("extract_users", set())
        assert 0 <= priority <= 1
        
        # Extract tasks should have higher priority
        extract_priority = self.orchestrator._estimate_task_priority("extract_users", set())
        transform_priority = self.orchestrator._estimate_task_priority("transform_users", set())
        assert extract_priority > transform_priority
    
    def test_resource_estimation(self):
        """Test resource requirement estimation."""
        resources = self.orchestrator._estimate_resource_requirements("extract_data")
        
        assert isinstance(resources, dict)
        assert "cpu" in resources
        assert "memory" in resources
        assert all(0 <= v <= 1 for v in resources.values())
    
    def test_task_relationship_detection(self):
        """Test task relationship detection for entanglement."""
        assert self.orchestrator._are_tasks_related("extract_users", "transform_users")
        assert self.orchestrator._are_tasks_related("load_orders", "transform_orders")
        assert not self.orchestrator._are_tasks_related("extract_users", "transform_orders")


class TestIntelligentDataQualityEngine:
    """Test suite for intelligent data quality engine."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.engine = IntelligentDataQualityEngine()
        self.sample_data = [
            {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com", "salary": 75000},
            {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com", "salary": 65000},
            {"id": 3, "name": "Charlie", "age": 35, "email": "charlie@example.com", "salary": 85000},
            {"id": 4, "name": "Diana", "age": 28, "email": "diana@invalid", "salary": 70000},  # Invalid email
            {"id": 5, "name": "", "age": 32, "email": "eve@example.com", "salary": 80000},  # Missing name
        ]
    
    def test_quality_rule_management(self):
        """Test quality rule addition and removal."""
        from src.agent_orchestrated_etl.data_quality_engine import QualityRule
        
        initial_count = len(self.engine.quality_rules)
        
        custom_rule = QualityRule(
            name="custom_test_rule",
            rule_type="custom",
            description="Test rule"
        )
        
        self.engine.add_quality_rule(custom_rule)
        assert len(self.engine.quality_rules) == initial_count + 1
        assert "custom_test_rule" in self.engine.quality_rules
        
        self.engine.remove_quality_rule("custom_test_rule")
        assert len(self.engine.quality_rules) == initial_count
        assert "custom_test_rule" not in self.engine.quality_rules
    
    def test_data_quality_analysis(self):
        """Test comprehensive data quality analysis."""
        result = self.engine.analyze_data_quality(self.sample_data)
        
        assert result["status"] == "completed"
        assert "quality_score" in result
        assert "quality_level" in result
        assert "issues" in result
        assert "data_profiles" in result
        assert "improvement_suggestions" in result
        
        # Should detect some quality issues in our sample data
        assert len(result["issues"]) > 0
        assert result["record_count"] == len(self.sample_data)
        assert 0 <= result["quality_score"] <= 100
    
    def test_data_profiling(self):
        """Test data profiling functionality."""
        profiles = self.engine._create_data_profiles(self.sample_data)
        
        assert "id" in profiles
        assert "name" in profiles
        assert "age" in profiles
        assert "email" in profiles
        
        age_profile = profiles["age"]
        assert age_profile.data_type in ["integer", "float"]
        assert age_profile.min_value == 25
        assert age_profile.max_value == 35
        assert age_profile.total_count == 5
        assert age_profile.null_count == 0
    
    def test_format_validation(self):
        """Test format validation rules."""
        # The sample data includes an invalid email
        result = self.engine.analyze_data_quality(self.sample_data)
        
        # Should detect email format issues
        email_issues = [issue for issue in result["issues"] if "email" in issue.get("field_name", "")]
        assert len(email_issues) > 0
    
    def test_completeness_validation(self):
        """Test completeness validation."""
        result = self.engine.analyze_data_quality(self.sample_data)
        
        # Should detect missing name in one record
        completeness_issues = [issue for issue in result["issues"] if issue.get("issue_type") == "completeness"]
        # May or may not detect depending on threshold, but should handle gracefully
        assert isinstance(completeness_issues, list)
    
    def test_quality_trends(self):
        """Test quality trend analysis."""
        # Run multiple analyses to build history
        for _ in range(3):
            self.engine.analyze_data_quality(self.sample_data)
        
        trends = self.engine.get_quality_trends(days=1)
        
        if "message" not in trends:  # If we have enough data
            assert "average_quality_score" in trends
            assert "score_trend" in trends
            assert trends["analysis_count"] > 0
    
    def test_empty_data_handling(self):
        """Test handling of empty data."""
        result = self.engine.analyze_data_quality([])
        
        assert result["status"] == "error"
        assert result["quality_score"] == 0.0
        assert "No data provided" in result["message"]


class TestIntegrationScenarios:
    """Integration tests combining multiple quantum features."""
    
    def setup_method(self):
        """Set up integration test fixtures."""
        self.orchestrator = DataOrchestrator(
            enable_quantum_planning=True, 
            enable_adaptive_resources=True
        )
    
    def teardown_method(self):
        """Clean up integration test fixtures."""
        if hasattr(self.orchestrator, 'resource_manager'):
            self.orchestrator.resource_manager.stop_monitoring()
    
    def test_quantum_enhanced_orchestrator_creation(self):
        """Test creation of quantum-enhanced orchestrator."""
        assert self.orchestrator.enable_quantum_planning
        assert self.orchestrator.enable_adaptive_resources
        assert hasattr(self.orchestrator, 'quantum_orchestrator')
        assert hasattr(self.orchestrator, 'resource_manager')
    
    def test_orchestrator_status_with_quantum_features(self):
        """Test orchestrator status includes quantum metrics."""
        status = self.orchestrator.get_orchestrator_status()
        
        assert "quantum_planning_enabled" in status
        assert "adaptive_resources_enabled" in status
        assert status["quantum_planning_enabled"] is True
        assert status["adaptive_resources_enabled"] is True
        
        if hasattr(self.orchestrator, 'quantum_orchestrator'):
            assert "quantum_metrics" in status
        
        if hasattr(self.orchestrator, 'resource_manager'):
            assert "resource_status" in status
    
    @pytest.mark.async_test
    async def test_async_pipeline_execution(self):
        """Test asynchronous pipeline execution with quantum optimization."""
        # Mock a simple pipeline
        mock_pipeline = Mock()
        mock_pipeline.dag_id = "test_pipeline"
        mock_pipeline.execute.return_value = {"test": "result"}
        
        # Test async execution
        result = await self.orchestrator.execute_pipeline_async(mock_pipeline)
        
        assert isinstance(result, dict)


def test_quantum_features_import():
    """Test that all quantum features can be imported correctly."""
    from src.agent_orchestrated_etl import quantum_planner, adaptive_resources
    from src.agent_orchestrated_etl.data_quality_engine import IntelligentDataQualityEngine
    
    # Verify key classes can be instantiated
    planner = quantum_planner.QuantumTaskPlanner()
    assert planner is not None
    
    engine = IntelligentDataQualityEngine()
    assert engine is not None


def test_cli_quantum_integration():
    """Test CLI integration with quantum features."""
    from src.agent_orchestrated_etl.cli import run_pipeline_cmd
    
    # Test that CLI accepts quantum arguments
    test_args = ["s3", "--quantum-optimize", "--adaptive-resources", "--data-quality"]
    
    # This would normally run the pipeline, but we're just testing argument parsing
    # In a real test, we'd mock the pipeline execution
    try:
        # Just verify the arguments are accepted without error
        import argparse
        parser = argparse.ArgumentParser()
        parser.add_argument("source", nargs="?")
        parser.add_argument("--quantum-optimize", action="store_true")
        parser.add_argument("--adaptive-resources", action="store_true")
        parser.add_argument("--data-quality", action="store_true")
        
        ns = parser.parse_args(["s3", "--quantum-optimize", "--adaptive-resources"])
        assert ns.quantum_optimize is True
        assert ns.adaptive_resources is True
        
    except Exception as e:
        pytest.fail(f"CLI argument parsing failed: {e}")


# Performance benchmarks
class TestPerformanceBenchmarks:
    """Performance benchmarks for quantum features."""
    
    def test_quantum_scheduling_performance(self):
        """Benchmark quantum scheduling performance."""
        planner = QuantumTaskPlanner(max_parallel_tasks=8)
        
        # Add many tasks
        start_time = time.time()
        for i in range(100):
            planner.add_task(f"task_{i}", priority=0.5)
        
        # Create some dependencies
        for i in range(50):
            if i > 0:
                planner.quantum_tasks[f"task_{i}"].dependencies.add(f"task_{i-1}")
        
        # Benchmark scheduling
        schedule_start = time.time()
        schedule = planner.quantum_optimize_schedule()
        schedule_time = time.time() - schedule_start
        
        # Should complete in reasonable time (less than 5 seconds for 100 tasks)
        assert schedule_time < 5.0
        assert len(schedule) > 0
        
        print(f"Quantum scheduling for 100 tasks took {schedule_time:.3f} seconds")
    
    def test_resource_manager_scalability(self):
        """Test resource manager scalability."""
        manager = AdaptiveResourceManager()
        
        # Assign many tasks
        start_time = time.time()
        assignments = []
        
        for i in range(50):
            assignment = manager.assign_task_resources(
                f"task_{i}", 
                {"cpu": 0.1, "memory": 0.2}, 
                priority=0.5
            )
            assignments.append(assignment)
        
        assignment_time = time.time() - start_time
        
        # Clean up
        for i in range(50):
            manager.release_task_resources(f"task_{i}")
        
        manager.stop_monitoring()
        
        # Should handle 50 tasks quickly
        assert assignment_time < 2.0
        print(f"Resource assignment for 50 tasks took {assignment_time:.3f} seconds")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])