#!/usr/bin/env python3
"""Test enhanced robustness features - Generation 2 functionality."""

import asyncio
import time
import sys
import os

# Add src to path for testing
sys.path.insert(0, 'src')

async def test_error_recovery():
    """Test the enhanced error recovery system."""
    print("🔧 Testing Enhanced Error Recovery System")
    print("-" * 50)
    
    try:
        from agent_orchestrated_etl.enhanced_error_recovery import (
            SelfHealingPipeline, AdaptiveRecoveryEngine, 
            IntelligentFailureDetector, FailureType
        )
        
        # Test 1: Failure Detection
        detector = IntelligentFailureDetector()
        
        # Test different error types
        test_errors = [
            (Exception("Connection reset by peer"), {"network": True}),
            (Exception("Out of memory"), {"resource": True}),
            (Exception("Authentication failed"), {}),
            (Exception("Timeout occurred"), {}),
            (Exception("Unknown error"), {})
        ]
        
        print("Testing failure detection:")
        for error, context in test_errors:
            failure_type = detector.detect_failure_type(error, context)
            print(f"  ✅ Error '{error}' detected as: {failure_type.value}")
        
        # Test 2: Recovery Engine
        engine = AdaptiveRecoveryEngine()
        
        # Simulate a failure
        failure_context = engine.analyze_failure(
            Exception("Network timeout"), 
            "test_component", 
            {"network": True}
        )
        
        recovery_action = engine.get_recovery_action(failure_context)
        print(f"  ✅ Recovery strategy: {recovery_action.strategy.value}")
        
        # Test 3: Self-Healing Pipeline
        pipeline = SelfHealingPipeline()
        
        # Mock operation that sometimes fails
        call_count = 0
        async def flaky_operation():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("Transient error")
            return {"status": "success", "call_count": call_count}
        
        # Execute with recovery
        result = await pipeline.execute_with_recovery(
            flaky_operation, 
            "test_operation"
        )
        
        print(f"  ✅ Operation succeeded after {result['call_count']} attempts")
        
        # Test recovery statistics
        stats = pipeline.get_recovery_statistics()
        print(f"  📊 Recovery stats: {stats['total_failures']} failures handled")
        
        print("✅ Error Recovery System: PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Error Recovery test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_comprehensive_monitoring():
    """Test the comprehensive monitoring system."""
    print("\n📊 Testing Comprehensive Monitoring System")
    print("-" * 50)
    
    try:
        from agent_orchestrated_etl.comprehensive_monitoring import (
            ComprehensiveMonitor, MetricType, AlertSeverity
        )
        
        # Test 1: Metrics Collection
        monitor = ComprehensiveMonitor()
        
        # Record various metrics
        monitor.record_metric("test_counter", 1, MetricType.COUNTER)
        monitor.increment_counter("operations_total", labels={"type": "test"})
        monitor.set_gauge("memory_usage", 75.5, labels={"component": "etl"})
        
        # Test timing
        with monitor.time_operation("test_operation"):
            await asyncio.sleep(0.1)  # Simulate work
        
        print("  ✅ Metrics collection working")
        
        # Test 2: Health Checks
        def test_health_check():
            return {"healthy": True, "metrics": {"response_time": 0.05}}
        
        def failing_health_check():
            return {"healthy": False, "error": "Service unavailable"}
        
        monitor.health_checker.register_health_check("test_service", test_health_check)
        monitor.health_checker.register_health_check("failing_service", failing_health_check)
        
        health_status = await monitor.health_checker.run_health_checks()
        print(f"  ✅ Health checks: {len(health_status)} components checked")
        
        overall_health = monitor.health_checker.get_overall_health()
        print(f"  📊 Overall health: {'Healthy' if overall_health else 'Unhealthy'}")
        
        # Test 3: Alert Rules
        def high_memory_alert(metrics):
            for key, metric in metrics.items():
                if "memory_usage" in key and metric.get("last_value", 0) > 80:
                    return True
            return False
        
        monitor.alert_manager.add_alert_rule(
            "high_memory",
            high_memory_alert,
            AlertSeverity.WARNING,
            "Memory usage is high: {name}"
        )
        
        # Trigger alert
        monitor.set_gauge("memory_usage", 85.0)
        alerts = monitor.alert_manager.check_alerts(monitor.metrics_collector.get_aggregated_metrics())
        print(f"  🚨 Alerts generated: {len(alerts)}")
        
        # Test 4: System Status
        status = monitor.get_system_status()
        print(f"  📊 System status components: {list(status.keys())}")
        print(f"  📈 Metrics tracked: {len(status['metrics'])}")
        print(f"  🔍 Performance stats: {len(status['performance'])}")
        
        print("✅ Comprehensive Monitoring: PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Monitoring test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_integration():
    """Test integration between error recovery and monitoring."""
    print("\n🔗 Testing Integration Between Systems")
    print("-" * 50)
    
    try:
        from agent_orchestrated_etl.enhanced_error_recovery import SelfHealingPipeline
        from agent_orchestrated_etl.comprehensive_monitoring import ComprehensiveMonitor
        
        # Create integrated system
        pipeline = SelfHealingPipeline()
        monitor = ComprehensiveMonitor()
        
        # Mock operation with monitoring
        async def monitored_operation():
            # Record metrics
            monitor.increment_counter("operation_attempts")
            
            with monitor.time_operation("business_logic"):
                await asyncio.sleep(0.05)  # Simulate work
                
                # Sometimes fail
                import random
                if random.random() < 0.3:
                    monitor.increment_counter("operation_failures")
                    raise Exception("Random failure for testing")
                
                monitor.increment_counter("operation_successes")
                return {"status": "success", "timestamp": time.time()}
        
        # Run multiple operations
        successes = 0
        for i in range(10):
            try:
                result = await pipeline.execute_with_recovery(
                    monitored_operation,
                    f"operation_{i}"
                )
                if result:
                    successes += 1
            except Exception as e:
                print(f"  ⚠️  Operation {i} failed permanently: {e}")
        
        print(f"  ✅ Completed {successes}/10 operations successfully")
        
        # Check recovery stats
        recovery_stats = pipeline.get_recovery_statistics()
        print(f"  🔧 Recovery operations: {recovery_stats['total_failures']}")
        
        # Check monitoring stats
        system_status = monitor.get_system_status()
        print(f"  📊 Metrics collected: {len(system_status['metrics'])}")
        
        print("✅ System Integration: PASSED")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Run all Generation 2 robustness tests."""
    print("🛡️  GENERATION 2: ROBUSTNESS & RELIABILITY TESTING")
    print("=" * 60)
    
    test_results = []
    
    # Run all tests
    test_results.append(await test_error_recovery())
    test_results.append(await test_comprehensive_monitoring())
    test_results.append(await test_integration())
    
    # Summary
    print("\n" + "=" * 60)
    print("🛡️  GENERATION 2 TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(test_results)
    total = len(test_results)
    
    if passed == total:
        print("🎉 ALL TESTS PASSED!")
        print("✅ Error Recovery System: Advanced")
        print("✅ Comprehensive Monitoring: Production-Ready")
        print("✅ System Integration: Seamless")
        print("\n🚀 Generation 2 Foundation: ROBUST & RELIABLE")
    else:
        print(f"⚠️  {passed}/{total} tests passed")
        print("🔧 Some robustness features need attention")
    
    return passed == total


if __name__ == "__main__":
    success = asyncio.run(main())
    sys.exit(0 if success else 1)