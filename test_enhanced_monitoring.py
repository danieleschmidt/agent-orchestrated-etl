#!/usr/bin/env python3
"""Test enhanced pipeline monitoring system."""

import asyncio
import time
import tempfile
import os
from pathlib import Path

from src.agent_orchestrated_etl.monitoring.realtime_monitor import RealtimeMonitor
from src.agent_orchestrated_etl.monitoring.pipeline_monitor import (
    PipelineMonitor, PipelineStatus, TaskStatus
)
from src.agent_orchestrated_etl.monitoring.websocket_server import start_monitoring_server


async def test_realtime_monitoring():
    """Test real-time monitoring functionality."""
    print("🔍 Testing Real-time Monitoring System...")
    
    # Create temporary database
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
        db_path = temp_db.name
    
    try:
        # Initialize monitoring system
        monitor = RealtimeMonitor(db_path=db_path)
        
        # Test metrics collection
        print("📊 Testing metrics collection...")
        monitor.metrics_collector.collect_metric("test.cpu_usage", 65.5)
        monitor.metrics_collector.collect_metric("test.memory_usage", 78.2)
        monitor.metrics_collector.collect_metric("pipeline.execution_time", 120.5, "test_pipeline_1")
        
        # Get current metrics
        current_metrics = monitor.metrics_collector.get_current_metrics()
        print(f"✅ Collected {len(current_metrics)} metrics")
        
        # Test alert system
        print("🚨 Testing alert system...")
        monitor.alert_manager.register_alert_rule(
            name="test_high_cpu",
            metric="test.cpu_usage", 
            condition="greater_than",
            threshold=60.0,
            severity="warning"
        )
        
        # Trigger alert by collecting high CPU metric
        monitor.metrics_collector.collect_metric("test.cpu_usage", 85.0)
        
        # Check alerts
        triggered_alerts = await monitor.alert_manager.check_alerts()
        print(f"✅ Triggered {len(triggered_alerts)} alerts")
        
        # Test SLA monitoring
        print("📋 Testing SLA monitoring...")
        sla_results = await monitor.sla_monitor.check_sla_compliance("test_pipeline_1")
        print(f"✅ Checked {len(sla_results)} SLA definitions")
        
        # Test dashboard data
        dashboard_data = monitor.get_monitoring_dashboard_data()
        print(f"✅ Dashboard data contains {len(dashboard_data)} sections")
        
        print("🎉 Real-time monitoring tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Real-time monitoring test failed: {e}")
        return False
    finally:
        # Cleanup
        if os.path.exists(db_path):
            os.unlink(db_path)


async def test_pipeline_monitoring():
    """Test pipeline monitoring functionality."""
    print("🔍 Testing Pipeline Monitoring System...")
    
    try:
        # Initialize monitoring
        realtime_monitor = RealtimeMonitor()
        pipeline_monitor = PipelineMonitor(realtime_monitor)
        
        # Set up callbacks for testing
        pipeline_events = []
        task_events = []
        
        def on_pipeline_start(pipeline):
            pipeline_events.append(f"Started: {pipeline.pipeline_id}")
            print(f"📦 Pipeline started: {pipeline.pipeline_id}")
        
        def on_pipeline_end(pipeline):
            pipeline_events.append(f"Ended: {pipeline.pipeline_id} ({pipeline.status.value})")
            print(f"📦 Pipeline ended: {pipeline.pipeline_id} - {pipeline.status.value}")
        
        def on_task_start(task):
            task_events.append(f"Task started: {task.task_id}")
            print(f"⚙️ Task started: {task.task_id}")
        
        def on_task_end(task):
            task_events.append(f"Task ended: {task.task_id} ({task.status.value})")
            print(f"⚙️ Task ended: {task.task_id} - {task.status.value}")
        
        pipeline_monitor.register_pipeline_start_callback(on_pipeline_start)
        pipeline_monitor.register_pipeline_end_callback(on_pipeline_end)
        pipeline_monitor.register_task_start_callback(on_task_start)
        pipeline_monitor.register_task_end_callback(on_task_end)
        
        # Test pipeline execution monitoring
        print("📊 Testing pipeline execution monitoring...")
        
        # Start pipeline
        pipeline_monitor.start_pipeline(
            pipeline_id="test_pipeline_1",
            dag_id="test_dag",
            total_tasks=3
        )
        
        # Start tasks
        pipeline_monitor.start_task("test_pipeline_1", "task_1", "ExtractOperator")
        time.sleep(0.1)  # Simulate task execution
        pipeline_monitor.end_task("test_pipeline_1", "task_1", TaskStatus.COMPLETED, 
                                 duration=2.5, result_data={"records": 100})
        
        pipeline_monitor.start_task("test_pipeline_1", "task_2", "TransformOperator")
        time.sleep(0.1)
        pipeline_monitor.end_task("test_pipeline_1", "task_2", TaskStatus.COMPLETED,
                                 duration=3.2, result_data={"records": 95})
        
        pipeline_monitor.start_task("test_pipeline_1", "task_3", "LoadOperator")
        time.sleep(0.1)
        pipeline_monitor.end_task("test_pipeline_1", "task_3", TaskStatus.FAILED,
                                 duration=1.8, error_message="Connection timeout")
        
        # End pipeline
        pipeline_monitor.end_pipeline("test_pipeline_1", PipelineStatus.FAILED)
        
        # Test statistics
        stats = pipeline_monitor.get_pipeline_statistics(hours=1)
        print(f"✅ Pipeline statistics: {stats['total_pipelines']} pipelines")
        
        # Test monitoring summary
        summary = pipeline_monitor.get_monitoring_summary()
        print(f"✅ Monitoring summary: {summary['completed_pipelines_tracked']} completed pipelines tracked")
        
        # Verify events were captured
        assert len(pipeline_events) == 2, f"Expected 2 pipeline events, got {len(pipeline_events)}"
        assert len(task_events) == 6, f"Expected 6 task events, got {len(task_events)}"
        
        print("🎉 Pipeline monitoring tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Pipeline monitoring test failed: {e}")
        return False


async def test_websocket_integration():
    """Test WebSocket server integration."""
    print("🔍 Testing WebSocket Integration...")
    
    try:
        # Initialize monitoring system
        monitor = RealtimeMonitor()
        
        # Start real-time monitoring
        await monitor.start_monitoring()
        print("✅ Real-time monitoring started")
        
        # Test server startup and shutdown
        server = await start_monitoring_server(monitor, host="localhost", port=8766)
        print("✅ WebSocket server started on port 8766")
        
        # Get server stats
        stats = server.get_server_stats()
        print(f"✅ Server stats: {stats['connected_clients']} clients")
        
        # Stop server
        await server.stop_server()
        print("✅ WebSocket server stopped")
        
        # Stop monitoring
        await monitor.stop_monitoring()
        print("✅ Real-time monitoring stopped")
        
        print("🎉 WebSocket integration tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ WebSocket integration test failed: {e}")
        return False


async def test_performance_monitoring():
    """Test performance monitoring and baselines."""
    print("🔍 Testing Performance Monitoring...")
    
    try:
        pipeline_monitor = PipelineMonitor()
        
        # Test multiple executions of the same DAG
        dag_id = "performance_test_dag"
        
        for i in range(5):
            pipeline_id = f"test_pipeline_{i}"
            
            # Start pipeline
            pipeline_monitor.start_pipeline(pipeline_id, dag_id, total_tasks=2)
            
            # Simulate variable execution times
            execution_time = 10.0 + (i * 2.0)  # Gradual slowdown
            
            # Start and end tasks
            pipeline_monitor.start_task(pipeline_id, f"task_1_{i}")
            time.sleep(0.05)
            pipeline_monitor.end_task(pipeline_id, f"task_1_{i}", TaskStatus.COMPLETED, duration=execution_time/2)
            
            pipeline_monitor.start_task(pipeline_id, f"task_2_{i}")
            time.sleep(0.05)
            pipeline_monitor.end_task(pipeline_id, f"task_2_{i}", TaskStatus.COMPLETED, duration=execution_time/2)
            
            # End pipeline
            pipeline_monitor.end_pipeline(pipeline_id, PipelineStatus.COMPLETED)
        
        # Check performance baselines
        baselines = pipeline_monitor.get_performance_baselines()
        print(f"✅ Performance baselines established for {len(baselines)} DAGs")
        
        if dag_id in baselines:
            baseline = baselines[dag_id]
            print(f"✅ DAG baseline - Average duration: {baseline['average_duration']:.2f}s, "
                  f"Success rate: {baseline['success_rate']:.1f}%")
        
        print("🎉 Performance monitoring tests passed!")
        return True
        
    except Exception as e:
        print(f"❌ Performance monitoring test failed: {e}")
        return False


async def main():
    """Run all monitoring tests."""
    print("🚀 Starting Enhanced Monitoring System Tests...\n")
    
    tests = [
        ("Real-time Monitoring", test_realtime_monitoring()),
        ("Pipeline Monitoring", test_pipeline_monitoring()),
        ("WebSocket Integration", test_websocket_integration()),
        ("Performance Monitoring", test_performance_monitoring())
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
    
    # Summary
    print(f"\n{'='*60}")
    print("Test Results Summary")
    print('='*60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{test_name:<30} {status}")
    
    print(f"\n🎯 Overall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All enhanced monitoring tests passed!")
        return True
    else:
        print("❌ Some monitoring tests failed")
        return False


if __name__ == "__main__":
    success = asyncio.run(main())
    exit(0 if success else 1)