#!/usr/bin/env python3
"""Simple test runner for quantum-inspired ETL features without pytest dependency."""

import sys
import time
import traceback
from unittest.mock import Mock

# Add src to path
sys.path.insert(0, 'src')

def run_test(test_name, test_func):
    """Run a single test function."""  
    print(f"Running {test_name}...", end=" ")
    try:
        test_func()
        print("✓ PASS")
        return True
    except Exception as e:
        print(f"✗ FAIL: {e}")
        traceback.print_exc()
        return False

def test_quantum_task_planner():
    """Test basic quantum task planner functionality."""
    from agent_orchestrated_etl.quantum_planner import QuantumTaskPlanner, QuantumState
    
    planner = QuantumTaskPlanner(max_parallel_tasks=4)
    
    # Test adding tasks
    planner.add_task("extract_users", priority=0.8)
    assert "extract_users" in planner.quantum_tasks
    
    task = planner.quantum_tasks["extract_users"]
    assert task.task_id == "extract_users"
    assert task.priority == 0.8
    assert task.quantum_state == QuantumState.SUPERPOSITION
    
    # Test entanglement
    planner.add_task("transform_users")
    planner.create_entanglement("extract_users", "transform_users")
    
    task1 = planner.quantum_tasks["extract_users"]
    task2 = planner.quantum_tasks["transform_users"]
    assert "transform_users" in task1.entangled_tasks
    assert "extract_users" in task2.entangled_tasks
    
    print("  - Task addition and entanglement work correctly")

def test_adaptive_resource_manager():
    """Test adaptive resource manager functionality."""
    from agent_orchestrated_etl.adaptive_resources import AdaptiveResourceManager, ResourceType
    
    manager = AdaptiveResourceManager(scaling_interval=0.1)
    
    # Test resource pools
    assert len(manager.resource_pools) > 0
    assert "cpu_pool" in manager.resource_pools
    
    # Test task assignment
    resource_req = {"cpu": 0.5, "memory": 1.0}
    assignments = manager.assign_task_resources("test_task", resource_req, priority=0.7)
    
    # Test resource release
    manager.release_task_resources("test_task")
    
    # Test status
    status = manager.get_resource_status()
    assert "resource_pools" in status
    assert "total_active_tasks" in status
    
    manager.stop_monitoring()
    print("  - Resource assignment and management work correctly")

def test_data_quality_engine():
    """Test intelligent data quality engine."""
    from agent_orchestrated_etl.data_quality_engine import IntelligentDataQualityEngine
    
    engine = IntelligentDataQualityEngine()
    
    # Test with sample data
    sample_data = [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"},
        {"id": 3, "name": "", "age": 35, "email": "invalid-email"},  # Issues
    ]
    
    result = engine.analyze_data_quality(sample_data)
    
    assert result["status"] == "completed"
    assert "quality_score" in result
    assert "issues" in result
    assert result["record_count"] == 3
    assert 0 <= result["quality_score"] <= 100
    
    print("  - Data quality analysis works correctly")

def test_orchestrator_integration():
    """Test orchestrator with quantum features."""
    from agent_orchestrated_etl.orchestrator import DataOrchestrator
    
    # Test creation with quantum features
    orchestrator = DataOrchestrator(
        enable_quantum_planning=True,
        enable_adaptive_resources=True
    )
    
    assert orchestrator.enable_quantum_planning
    assert orchestrator.enable_adaptive_resources
    assert hasattr(orchestrator, 'quantum_orchestrator')
    assert hasattr(orchestrator, 'resource_manager')
    
    # Test status
    status = orchestrator.get_orchestrator_status()
    assert "quantum_planning_enabled" in status
    assert "adaptive_resources_enabled" in status
    
    # Cleanup
    if hasattr(orchestrator, 'resource_manager'):
        orchestrator.resource_manager.stop_monitoring()
    
    print("  - Orchestrator integration works correctly")

def test_quantum_scheduling():
    """Test quantum scheduling algorithm."""
    from agent_orchestrated_etl.quantum_planner import QuantumTaskPlanner
    
    planner = QuantumTaskPlanner(max_parallel_tasks=3)
    
    # Add tasks with dependencies
    planner.add_task("extract_users", priority=0.8)
    planner.add_task("extract_orders", priority=0.7) 
    planner.add_task("transform_users", dependencies={"extract_users"}, priority=0.6)
    planner.add_task("transform_orders", dependencies={"extract_orders"}, priority=0.6)
    planner.add_task("load_data", dependencies={"transform_users", "transform_orders"}, priority=0.4)
    
    # Test scheduling
    schedule = planner.quantum_optimize_schedule()
    
    assert isinstance(schedule, list)
    assert len(schedule) > 0
    
    # Verify dependencies are respected
    completed_tasks = set()
    for wave in schedule:
        for task_id in wave:
            task = planner.quantum_tasks[task_id]
            assert task.dependencies.issubset(completed_tasks)
        completed_tasks.update(wave)
    
    # Verify all tasks are scheduled
    all_scheduled = set()
    for wave in schedule:
        all_scheduled.update(wave)
    assert all_scheduled == set(planner.quantum_tasks.keys())
    
    print("  - Quantum scheduling respects dependencies and schedules all tasks")

def test_performance_benchmarks():
    """Test performance of quantum features."""
    from agent_orchestrated_etl.quantum_planner import QuantumTaskPlanner
    from agent_orchestrated_etl.adaptive_resources import AdaptiveResourceManager
    
    # Test quantum planner performance
    planner = QuantumTaskPlanner(max_parallel_tasks=8)
    
    start_time = time.time()
    for i in range(50):  # Reduced from 100 for faster testing
        planner.add_task(f"task_{i}", priority=0.5)
    
    # Create dependencies
    for i in range(25):
        if i > 0:
            planner.quantum_tasks[f"task_{i}"].dependencies.add(f"task_{i-1}")
    
    # Benchmark scheduling
    schedule_start = time.time()
    schedule = planner.quantum_optimize_schedule()
    schedule_time = time.time() - schedule_start
    
    assert schedule_time < 3.0  # Should be fast
    assert len(schedule) > 0
    
    print(f"  - Quantum scheduling for 50 tasks: {schedule_time:.3f}s")
    
    # Test resource manager performance
    manager = AdaptiveResourceManager()
    
    start_time = time.time()
    for i in range(20):  # Reduced for faster testing
        manager.assign_task_resources(f"task_{i}", {"cpu": 0.1, "memory": 0.2}, priority=0.5)
    
    assignment_time = time.time() - start_time
    
    # Cleanup
    for i in range(20):
        manager.release_task_resources(f"task_{i}")
    manager.stop_monitoring()
    
    assert assignment_time < 1.0
    print(f"  - Resource assignment for 20 tasks: {assignment_time:.3f}s")

def test_imports():
    """Test that all modules can be imported."""
    try:
        from agent_orchestrated_etl import quantum_planner, adaptive_resources
        from agent_orchestrated_etl.data_quality_engine import IntelligentDataQualityEngine
        from agent_orchestrated_etl.orchestrator import DataOrchestrator
        
        # Verify instantiation
        planner = quantum_planner.QuantumTaskPlanner()
        engine = IntelligentDataQualityEngine()
        orchestrator = DataOrchestrator()
        
        print("  - All modules import and instantiate correctly")
        
        # Cleanup
        if hasattr(orchestrator, 'resource_manager'):
            orchestrator.resource_manager.stop_monitoring()
            
    except ImportError as e:
        raise AssertionError(f"Import failed: {e}")

def main():
    """Run all tests."""
    print("🧪 Testing Quantum-Inspired ETL Features")
    print("=" * 50)
    
    tests = [
        ("Module Imports", test_imports),
        ("Quantum Task Planner", test_quantum_task_planner),
        ("Adaptive Resource Manager", test_adaptive_resource_manager),
        ("Data Quality Engine", test_data_quality_engine),
        ("Orchestrator Integration", test_orchestrator_integration),
        ("Quantum Scheduling Algorithm", test_quantum_scheduling),
        ("Performance Benchmarks", test_performance_benchmarks),
    ]
    
    passed = 0
    failed = 0
    
    for test_name, test_func in tests:
        if run_test(test_name, test_func):
            passed += 1
        else:
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"Tests completed: {passed} passed, {failed} failed")
    
    if failed > 0:
        print("\n❌ Some tests failed. Check the output above for details.")
        return 1
    else:
        print("\n✅ All tests passed! Quantum features are working correctly.")
        return 0

if __name__ == "__main__":
    sys.exit(main())