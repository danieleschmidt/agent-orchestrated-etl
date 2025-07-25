#!/usr/bin/env python3
"""Test core monitoring functionality without WebSocket dependencies."""

import asyncio
import time
import tempfile
import os

from src.agent_orchestrated_etl.monitoring.realtime_monitor import RealtimeMonitor
from src.agent_orchestrated_etl.monitoring.pipeline_monitor import (
    PipelineMonitor, PipelineStatus, TaskStatus
)


async def test_metrics_collection():
    """Test metrics collection functionality."""
    print("📊 Testing Metrics Collection...")
    
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
        db_path = temp_db.name
    
    try:
        # Initialize monitoring system
        monitor = RealtimeMonitor(db_path=db_path)
        
        # Test metric registration
        monitor.metrics_collector.register_metric(
            "test.response_time", "API response time", "ms", "histogram"
        )
        
        # Test metric collection
        monitor.metrics_collector.collect_metric("test.response_time", 150.5)
        monitor.metrics_collector.collect_metric("test.response_time", 200.3)
        monitor.metrics_collector.collect_metric("test.response_time", 125.8)
        
        # Test pipeline-specific metrics
        monitor.collect_pipeline_metric("pipeline.execution_time", 45.2, "test_pipeline_1")
        monitor.collect_pipeline_metric("pipeline.task_count", 5.0, "test_pipeline_1")
        
        # Get current metrics
        current_metrics = monitor.metrics_collector.get_current_metrics()
        print(f"✅ Collected {len(current_metrics)} metric types")
        
        # Test metric history
        history = monitor.metrics_collector.get_metric_history("test.response_time", hours=1)
        print(f"✅ Retrieved {len(history)} historical data points")
        
        assert len(current_metrics) > 0, "No metrics collected"
        assert len(history) == 3, f"Expected 3 history entries, got {len(history)}"
        
        print("🎉 Metrics collection tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Metrics collection test failed: {e}")
        return False
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)


async def test_alert_system():
    """Test alert system functionality."""
    print("🚨 Testing Alert System...")
    
    try:
        monitor = RealtimeMonitor()
        
        # Register custom alert rule
        monitor.alert_manager.register_alert_rule(
            name="test_high_response_time",
            metric="api.response_time",
            condition="greater_than",
            threshold=500.0,
            severity="critical",
            description="API response time too high"
        )
        
        # Collect metrics that should trigger alert
        monitor.metrics_collector.collect_metric("api.response_time", 750.0)
        monitor.metrics_collector.collect_metric("api.response_time", 800.0)
        
        # Check alerts
        triggered_alerts = await monitor.alert_manager.check_alerts()
        print(f"✅ Triggered {len(triggered_alerts)} alerts")
        
        # Test alert history
        alert_history = monitor.alert_manager.alert_history
        print(f"✅ Alert history contains {len(alert_history)} alerts")
        
        # Test active alerts
        active_alerts = monitor.alert_manager.active_alerts
        print(f"✅ Active alerts: {len(active_alerts)}")
        
        assert len(triggered_alerts) > 0, "No alerts triggered"
        
        print("🎉 Alert system tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Alert system test failed: {e}")
        return False


async def test_sla_monitoring():
    """Test SLA monitoring functionality."""
    print("📋 Testing SLA Monitoring...")
    
    try:
        monitor = RealtimeMonitor()
        
        # Register custom SLA
        monitor.sla_monitor.register_sla(
            name="api_response_time_sla",
            description="API responses should complete within 200ms",
            target_value=200.0,
            metric="api.response_time",
            measurement_period="per_execution"
        )
        
        # Collect metrics for SLA evaluation
        monitor.metrics_collector.collect_metric("api.response_time", 150.0, pipeline_id="test_api")
        monitor.metrics_collector.collect_metric("api.response_time", 180.0, pipeline_id="test_api")
        monitor.metrics_collector.collect_metric("api.response_time", 250.0, pipeline_id="test_api")  # SLA violation
        
        # Check SLA compliance
        sla_results = await monitor.sla_monitor.check_sla_compliance("test_api")
        print(f"✅ Checked {len(sla_results)} SLA definitions")
        
        # Verify SLA results
        for result in sla_results:
            print(f"   - {result['sla_name']}: {result['status']}")
        
        assert len(sla_results) > 0, "No SLA results"
        
        print("🎉 SLA monitoring tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ SLA monitoring test failed: {e}")
        return False


async def test_pipeline_monitoring():
    """Test pipeline monitoring functionality."""
    print("🔍 Testing Pipeline Monitoring...")
    
    try:
        realtime_monitor = RealtimeMonitor()
        pipeline_monitor = PipelineMonitor(realtime_monitor)
        
        # Track events for verification
        events = []
        
        def track_pipeline_start(pipeline):
            events.append(f"pipeline_start:{pipeline.pipeline_id}")
        
        def track_pipeline_end(pipeline):
            events.append(f"pipeline_end:{pipeline.pipeline_id}:{pipeline.status.value}")
        
        def track_task_start(task):
            events.append(f"task_start:{task.task_id}")
        
        def track_task_end(task):
            events.append(f"task_end:{task.task_id}:{task.status.value}")
        
        # Register callbacks
        pipeline_monitor.register_pipeline_start_callback(track_pipeline_start)
        pipeline_monitor.register_pipeline_end_callback(track_pipeline_end)
        pipeline_monitor.register_task_start_callback(track_task_start)
        pipeline_monitor.register_task_end_callback(track_task_end)
        
        # Test pipeline execution monitoring
        pipeline_id = "test_monitoring_pipeline"
        
        # Start pipeline
        pipeline_monitor.start_pipeline(
            pipeline_id=pipeline_id,
            dag_id="monitoring_test_dag",
            total_tasks=3,
            metadata={"test": "monitoring"}
        )
        
        # Execute tasks
        tasks = [
            ("extract_task", "ExtractOperator", TaskStatus.COMPLETED, 2.5),
            ("transform_task", "TransformOperator", TaskStatus.COMPLETED, 3.2),
            ("load_task", "LoadOperator", TaskStatus.FAILED, 1.8)
        ]
        
        for task_id, operator_type, final_status, duration in tasks:
            pipeline_monitor.start_task(pipeline_id, task_id, operator_type)
            time.sleep(0.01)  # Brief simulation
            
            if final_status == TaskStatus.FAILED:
                pipeline_monitor.end_task(
                    pipeline_id, task_id, final_status, 
                    duration=duration, error_message="Simulated failure"
                )
            else:
                pipeline_monitor.end_task(
                    pipeline_id, task_id, final_status,
                    duration=duration, result_data={"records_processed": 100}
                )
        
        # End pipeline
        pipeline_monitor.end_pipeline(pipeline_id, PipelineStatus.FAILED)
        
        # Verify tracking
        print(f"✅ Tracked {len(events)} events")
        
        # Test pipeline status retrieval
        status = pipeline_monitor.get_pipeline_status(pipeline_id)
        assert status is not None, "Pipeline status not found"
        assert status["status"] == "failed", f"Expected failed status, got {status['status']}"
        print("✅ Pipeline status correctly tracked")
        
        # Test statistics
        stats = pipeline_monitor.get_pipeline_statistics(hours=1)
        assert stats["total_pipelines"] == 1, f"Expected 1 pipeline, got {stats['total_pipelines']}"
        assert stats["failed_pipelines"] == 1, f"Expected 1 failed pipeline, got {stats['failed_pipelines']}"
        print("✅ Pipeline statistics calculated correctly")
        
        # Test monitoring summary
        summary = pipeline_monitor.get_monitoring_summary()
        assert summary["completed_pipelines_tracked"] == 1, "Pipeline not tracked in summary"
        print("✅ Monitoring summary generated")
        
        print("🎉 Pipeline monitoring tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline monitoring test failed: {e}")
        return False


async def test_performance_baselines():
    """Test performance baseline tracking."""
    print("📈 Testing Performance Baselines...")
    
    try:
        pipeline_monitor = PipelineMonitor()
        dag_id = "baseline_test_dag"
        
        # Run multiple executions to establish baseline
        execution_times = [10.0, 12.0, 9.5, 11.0, 10.5]
        
        for i, exec_time in enumerate(execution_times):
            pipeline_id = f"baseline_test_{i}"
            
            # Start pipeline
            pipeline_monitor.start_pipeline(pipeline_id, dag_id, total_tasks=1)
            
            # Execute single task
            pipeline_monitor.start_task(pipeline_id, f"task_{i}")
            time.sleep(0.01)
            pipeline_monitor.end_task(pipeline_id, f"task_{i}", TaskStatus.COMPLETED, duration=exec_time)
            
            # End pipeline
            pipeline_monitor.end_pipeline(pipeline_id, PipelineStatus.COMPLETED)
        
        # Check baseline establishment
        baselines = pipeline_monitor.get_performance_baselines()
        assert dag_id in baselines, f"Baseline not established for {dag_id}"
        
        baseline = baselines[dag_id]
        print(f"✅ Baseline established - Avg duration: {baseline['average_duration']:.2f}s")
        print(f"✅ Success rate: {baseline['success_rate']:.1f}%")
        print(f"✅ Sample count: {baseline['sample_count']}")
        
        # Test performance degradation detection
        pipeline_monitor.start_pipeline("slow_pipeline", dag_id, total_tasks=1)
        pipeline_monitor.start_task("slow_pipeline", "slow_task")
        time.sleep(0.01)
        # This should trigger degradation warning (20s vs ~10.4s baseline)
        pipeline_monitor.end_task("slow_pipeline", "slow_task", TaskStatus.COMPLETED, duration=20.0)
        pipeline_monitor.end_pipeline("slow_pipeline", PipelineStatus.COMPLETED)
        
        print("🎉 Performance baseline tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Performance baseline test failed: {e}")
        return False


async def test_dashboard_data():
    """Test dashboard data generation."""
    print("📊 Testing Dashboard Data Generation...")
    
    try:
        monitor = RealtimeMonitor()
        
        # Collect some sample data
        monitor.metrics_collector.collect_metric("system.cpu_percent", 45.5)
        monitor.metrics_collector.collect_metric("system.memory_percent", 62.3)
        monitor.collect_pipeline_metric("pipeline.execution_time", 25.8, "sample_pipeline")
        
        # Generate dashboard data
        dashboard_data = monitor.get_monitoring_dashboard_data()
        
        # Verify dashboard structure
        required_sections = ["system_status", "current_metrics", "active_alerts", "recent_alerts"]
        for section in required_sections:
            assert section in dashboard_data, f"Missing dashboard section: {section}"
        
        print(f"✅ Dashboard data contains {len(dashboard_data)} sections")
        print(f"✅ Current metrics: {len(dashboard_data['current_metrics'])}")
        print(f"✅ System status: {dashboard_data['system_status']['monitoring_active']}")
        
        print("🎉 Dashboard data tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Dashboard data test failed: {e}")
        return False


async def main():
    """Run all core monitoring tests."""
    print("🚀 Starting Core Monitoring System Tests...\n")
    
    tests = [
        ("Metrics Collection", test_metrics_collection()),
        ("Alert System", test_alert_system()),
        ("SLA Monitoring", test_sla_monitoring()),
        ("Pipeline Monitoring", test_pipeline_monitoring()),
        ("Performance Baselines", test_performance_baselines()),
        ("Dashboard Data", test_dashboard_data())
    ]
    
    results = []
    
    for test_name, test_coro in tests:
        print(f"\n{'='*60}")
        print(f"Running {test_name} Tests")
        print('='*60)
        
        try:
            result = await test_coro
            results.append((test_name, result))
            
            if result:
                print(f"✅ {test_name} - PASSED")
            else:
                print(f"❌ {test_name} - FAILED")
                
        except Exception as e:
            print(f"❌ {test_name} - ERROR: {e}")
            results.append((test_name, False))
        
        time.sleep(0.1)  # Brief pause between tests
    
    # Summary
    print(f"\n{'='*60}")
    print("Test Results Summary")
    print('='*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<25} {status}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All core monitoring tests passed!")
        return True
    else:
        print("❌ Some monitoring tests failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)