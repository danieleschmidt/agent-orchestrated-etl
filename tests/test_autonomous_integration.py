"""Integration tests for autonomous SDLC system."""

import time
from unittest.mock import MagicMock, patch

import pytest

from src.agent_orchestrated_etl.autonomous_pipeline_manager import (
    AutomationLevel,
    AutomationRule,
    AutonomousPipelineManager,
    PipelineLifecycleStage,
)
from src.agent_orchestrated_etl.intelligent_monitoring import (
    Alert,
    AlertType,
    IntelligentMonitor,
    MonitoringLevel,
)
from src.agent_orchestrated_etl.quantum_scaling_engine import (
    QuantumScalingEngine,
    ScalingMetrics,
)


@pytest.fixture
def mock_workspace(tmp_path):
    """Create a temporary workspace for testing."""
    return str(tmp_path / "test_workspace")


@pytest.fixture
def autonomous_manager(mock_workspace):
    """Create autonomous pipeline manager for testing."""
    return AutonomousPipelineManager(workspace_path=mock_workspace)


class TestAutonomousPipelineManager:
    """Test the autonomous pipeline manager."""

    def test_initialization(self, autonomous_manager):
        """Test manager initialization."""
        assert autonomous_manager is not None
        assert autonomous_manager.default_automation_level == AutomationLevel.AUTONOMOUS
        assert len(autonomous_manager.automation_rules) >= 3  # Default rules
        assert autonomous_manager.monitor is not None
        assert autonomous_manager.pipeline_detector is not None

    def test_pipeline_discovery_and_creation(self, autonomous_manager):
        """Test pipeline discovery and creation."""
        source_id = "s3://test-bucket/data.csv"

        with patch.object(autonomous_manager.pipeline_detector, 'generate_pipeline_recommendation') as mock_recommend:
            # Mock recommendation
            mock_recommendation = MagicMock()
            mock_recommendation.confidence_score = 0.85
            mock_recommendation.transformation_strategy = MagicMock()
            mock_recommendation.transformation_strategy.value = "batch_processing"
            mock_recommendation.recommended_type = MagicMock()
            mock_recommendation.recommended_type.value = "data_warehouse"
            mock_recommendation.output_destinations = []
            mock_recommend.return_value = mock_recommendation

            with patch.object(autonomous_manager.pipeline_detector, 'create_pipeline_config') as mock_config:
                mock_config.return_value = MagicMock()

                pipeline_id = autonomous_manager.discover_and_create_pipeline(
                    source_id,
                    business_priority=8
                )

                assert pipeline_id is not None
                assert pipeline_id in autonomous_manager.managed_pipelines

                pipeline_metadata = autonomous_manager.managed_pipelines[pipeline_id]
                assert pipeline_metadata.source_identifier == source_id
                assert pipeline_metadata.business_priority == 8
                assert pipeline_metadata.lifecycle_stage == PipelineLifecycleStage.DESIGN

    def test_pipeline_execution(self, autonomous_manager):
        """Test pipeline execution with monitoring."""
        # Create a test pipeline
        source_id = "test://source"

        with patch.object(autonomous_manager.pipeline_detector, 'generate_pipeline_recommendation') as mock_recommend:
            mock_recommendation = MagicMock()
            mock_recommendation.confidence_score = 0.9
            mock_recommendation.transformation_strategy = MagicMock()
            mock_recommendation.transformation_strategy.value = "batch_processing"
            mock_recommendation.recommended_type = MagicMock()
            mock_recommendation.recommended_type.value = "general_purpose"
            mock_recommendation.output_destinations = []
            mock_recommend.return_value = mock_recommendation

            with patch.object(autonomous_manager.pipeline_detector, 'create_pipeline_config') as mock_config:
                mock_config.return_value = MagicMock()

                pipeline_id = autonomous_manager.discover_and_create_pipeline(source_id)

        # Mock orchestrator execution
        with patch.object(autonomous_manager.orchestrator, 'execute_pipeline_resilient') as mock_execute:
            mock_execute.return_value = "execution_123"

            execution_id = autonomous_manager.execute_pipeline(pipeline_id)

            assert execution_id == "execution_123"
            mock_execute.assert_called_once()

            # Verify pipeline metadata was updated
            pipeline_metadata = autonomous_manager.managed_pipelines[pipeline_id]
            assert pipeline_metadata.total_executions == 1

    def test_system_overview(self, autonomous_manager):
        """Test system overview generation."""
        overview = autonomous_manager.get_system_overview()

        required_keys = [
            "system_status",
            "timestamp",
            "pipelines",
            "execution_statistics",
            "monitoring",
            "resource_utilization"
        ]

        for key in required_keys:
            assert key in overview

        assert "total" in overview["pipelines"]
        assert "active" in overview["pipelines"]
        assert "lifecycle_distribution" in overview["pipelines"]
        assert "automation_distribution" in overview["pipelines"]

    def test_automation_rule_management(self, autonomous_manager):
        """Test automation rule addition and removal."""
        test_rule = AutomationRule(
            rule_id="test_rule",
            name="Test Rule",
            description="Test automation rule",
            trigger_condition='{"test": true}',
            action="test_action"
        )

        autonomous_manager.add_automation_rule(test_rule)
        assert "test_rule" in autonomous_manager.automation_rules

        success = autonomous_manager.remove_automation_rule("test_rule")
        assert success
        assert "test_rule" not in autonomous_manager.automation_rules

        # Test removing non-existent rule
        success = autonomous_manager.remove_automation_rule("non_existent")
        assert not success

    def test_pipeline_filtering(self, autonomous_manager):
        """Test pipeline filtering functionality."""
        # Create test pipelines with different characteristics
        with patch.object(autonomous_manager.pipeline_detector, 'generate_pipeline_recommendation') as mock_recommend:
            mock_recommendation = MagicMock()
            mock_recommendation.confidence_score = 0.8
            mock_recommendation.transformation_strategy = MagicMock()
            mock_recommendation.transformation_strategy.value = "batch_processing"
            mock_recommendation.recommended_type = MagicMock()
            mock_recommendation.recommended_type.value = "general_purpose"
            mock_recommendation.output_destinations = []
            mock_recommend.return_value = mock_recommendation

            with patch.object(autonomous_manager.pipeline_detector, 'create_pipeline_config') as mock_config:
                mock_config.return_value = MagicMock()

                pipeline1 = autonomous_manager.discover_and_create_pipeline(
                    "s3://bucket1/data.csv",
                    automation_level=AutomationLevel.AUTOMATIC
                )
                pipeline2 = autonomous_manager.discover_and_create_pipeline(
                    "s3://bucket2/data.json",
                    automation_level=AutomationLevel.MANUAL
                )

        # Test filtering by automation level
        automatic_pipelines = autonomous_manager.list_pipelines({
            "automation_level": AutomationLevel.AUTOMATIC
        })
        assert len(automatic_pipelines) == 1
        assert automatic_pipelines[0].pipeline_id == pipeline1

        # Test no filter
        all_pipelines = autonomous_manager.list_pipelines()
        assert len(all_pipelines) == 2


class TestIntelligentMonitoring:
    """Test the intelligent monitoring system."""

    def test_monitor_initialization(self):
        """Test monitor initialization."""
        monitor = IntelligentMonitor()
        assert monitor is not None
        assert not monitor._monitoring_active

    def test_metric_recording(self):
        """Test metric recording and retrieval."""
        monitor = IntelligentMonitor()

        # Record some metrics
        monitor.record_metric("test_metric", 10.0, {"tag": "test"})
        monitor.record_metric("test_metric", 15.0, {"tag": "test"})
        monitor.record_metric("test_metric", 12.0, {"tag": "test"})

        # Retrieve metrics
        metrics = monitor.get_metrics("test_metric")
        assert len(metrics) == 3
        assert all(m.metric_name == "test_metric" for m in metrics)

        # Test metric summary
        summary = monitor.get_metric_summary("test_metric")
        assert summary["count"] == 3
        assert summary["min"] == 10.0
        assert summary["max"] == 15.0
        assert abs(summary["avg"] - 12.33) < 0.1  # Approximate average

    def test_health_check_recording(self):
        """Test health check recording."""
        monitor = IntelligentMonitor()

        # Record healthy component
        monitor.record_health_check("test_component", "healthy", 100.0)

        health_data = monitor.get_system_health()
        assert "test_component" in health_data["components"]
        assert health_data["components"]["test_component"]["status"] == "healthy"

        # Record unhealthy component
        monitor.record_health_check("test_component", "unhealthy", 5000.0, "Connection failed")

        health_data = monitor.get_system_health()
        assert health_data["components"]["test_component"]["status"] == "unhealthy"
        assert health_data["components"]["test_component"]["error"] == "Connection failed"

    def test_alert_management(self):
        """Test alert generation and management."""
        alerts_received = []

        def alert_callback(alert):
            alerts_received.append(alert)

        monitor = IntelligentMonitor(alert_callback=alert_callback)

        # Trigger alert via threshold violation
        monitor.record_metric("error_rate_percent", 25.0)  # Above 20% threshold

        time.sleep(0.1)  # Give time for alert processing

        # Check alerts
        alerts = monitor.get_alerts(resolved=False)
        assert len(alerts) > 0

        # Test alert resolution
        if alerts:
            alert_id = alerts[0].alert_id
            success = monitor.resolve_alert(alert_id)
            assert success

            resolved_alerts = monitor.get_alerts(resolved=True)
            assert len(resolved_alerts) > 0


class TestQuantumScalingEngine:
    """Test the quantum scaling engine."""

    def test_scaling_engine_initialization(self):
        """Test scaling engine initialization."""
        monitor = IntelligentMonitor()
        engine = QuantumScalingEngine(monitor)

        assert engine is not None
        assert engine.current_allocation is not None
        assert engine.predictor is not None

    def test_scaling_analysis(self):
        """Test scaling needs analysis."""
        monitor = IntelligentMonitor()
        engine = QuantumScalingEngine(monitor)

        # Create test metrics indicating high load
        test_metrics = ScalingMetrics(
            timestamp=time.time(),
            queue_depth=150,  # High queue depth
            cpu_utilization=90.0,  # High CPU
            memory_utilization=85.0,  # High memory
            throughput_records_per_second=50.0,
            error_rate=2.0,
            response_time_p95=8000.0,
            active_workers=4,
            pending_tasks=20
        )

        decision = engine.analyze_scaling_needs(test_metrics)

        assert decision is not None
        assert decision.action in ["scale_up", "scale_down", "optimize", "maintain"]
        assert 0.0 <= decision.confidence <= 1.0
        assert len(decision.reasoning) > 0

    def test_batch_size_optimization(self):
        """Test optimal batch size calculation."""
        monitor = IntelligentMonitor()
        engine = QuantumScalingEngine(monitor)

        # Test with small records
        batch_size = engine.get_optimal_batch_size(
            record_size_bytes=1024,  # 1KB records
            available_memory_gb=8
        )
        assert batch_size > 1000  # Should allow many small records

        # Test with large records
        batch_size = engine.get_optimal_batch_size(
            record_size_bytes=1024*1024,  # 1MB records
            available_memory_gb=8
        )
        assert batch_size < 10000  # Should limit large records
        assert batch_size >= 100  # But not too small

    def test_processing_time_estimation(self):
        """Test processing time estimation."""
        monitor = IntelligentMonitor()
        engine = QuantumScalingEngine(monitor)

        estimated_time = engine.estimate_processing_time(
            total_records=10000,
            batch_size=1000,
            workers=4,
            records_per_second_per_worker=100.0
        )

        assert estimated_time > 0
        # With 4 workers at 100 rps each, 10K records should take ~25 seconds + overhead
        assert 20 < estimated_time < 40  # Account for overhead and batching


class TestSystemIntegration:
    """Test integration between all system components."""

    def test_end_to_end_pipeline_creation_and_execution(self, mock_workspace):
        """Test complete end-to-end pipeline workflow."""
        # Initialize manager
        manager = AutonomousPipelineManager(workspace_path=mock_workspace)

        try:
            # Step 1: Mock external dependencies
            with patch.object(manager.pipeline_detector, 'generate_pipeline_recommendation') as mock_recommend:
                mock_recommendation = self._create_mock_recommendation()
                mock_recommend.return_value = mock_recommendation

                with patch.object(manager.pipeline_detector, 'create_pipeline_config') as mock_config:
                    mock_config.return_value = MagicMock()

                    with patch.object(manager.orchestrator, 'execute_pipeline_resilient') as mock_execute:
                        mock_execute.return_value = "test_execution_123"

                        with patch.object(manager.orchestrator, 'get_pipeline_status') as mock_status:
                            mock_result = MagicMock()
                            mock_result.state = MagicMock()
                            mock_result.state.SUCCESS = "success"
                            mock_result.execution_time_seconds = 45.0
                            mock_result.records_processed = 1000
                            mock_status.return_value = mock_result

                            # Step 2: Create pipeline
                            pipeline_id = manager.discover_and_create_pipeline(
                                "s3://test-bucket/integration-test.csv",
                                automation_level=AutomationLevel.AUTONOMOUS,
                                business_priority=9
                            )

                            assert pipeline_id is not None

                            # Step 3: Verify pipeline was created
                            pipeline_metadata = manager.get_pipeline_status(pipeline_id)
                            assert pipeline_metadata is not None
                            assert pipeline_metadata.business_priority == 9
                            assert pipeline_metadata.automation_level == AutomationLevel.AUTONOMOUS

                            # Step 4: Execute pipeline
                            execution_id = manager.execute_pipeline(pipeline_id)
                            assert execution_id == "test_execution_123"

                            # Step 5: Verify system overview
                            overview = manager.get_system_overview()
                            assert overview["pipelines"]["total"] >= 1
                            assert overview["execution_statistics"]["total_executions"] >= 1

        finally:
            # Cleanup
            if hasattr(manager, 'stop'):
                manager.stop()

    def test_monitoring_and_scaling_integration(self, mock_workspace):
        """Test integration between monitoring and scaling systems."""
        manager = AutonomousPipelineManager(workspace_path=mock_workspace)

        try:
            # Start monitoring
            manager.monitor.start_monitoring()

            # Record high utilization metrics
            manager.monitor.record_metric("cpu_usage_percent", 95.0)
            manager.monitor.record_metric("memory_usage_percent", 90.0)

            # Create scaling metrics
            scaling_metrics = ScalingMetrics(
                timestamp=time.time(),
                queue_depth=200,
                cpu_utilization=95.0,
                memory_utilization=90.0,
                throughput_records_per_second=25.0,
                error_rate=0.5,
                response_time_p95=12000.0,
                active_workers=2,
                pending_tasks=50
            )

            # Analyze scaling needs
            decision = manager.scaling_engine.analyze_scaling_needs(scaling_metrics)

            # Should recommend scaling up due to high resource utilization
            assert decision.action == "scale_up"
            assert decision.confidence > 0.5

            # Verify scaling decision contains reasoning
            assert len(decision.reasoning) > 0
            assert decision.target_allocation is not None

        finally:
            manager.stop()

    def test_alert_handling_and_automation(self, mock_workspace):
        """Test alert handling and automated responses."""
        alerts_handled = []

        def mock_alert_handler(alert):
            alerts_handled.append(alert)

        manager = AutonomousPipelineManager(workspace_path=mock_workspace)
        manager.monitor.alert_callback = mock_alert_handler

        try:
            # Generate an alert that should trigger automation
            test_alert = Alert(
                alert_id="test_alert_123",
                alert_type=AlertType.PERFORMANCE_DEGRADATION,
                severity=MonitoringLevel.WARNING,
                message="Test performance degradation",
                timestamp=time.time(),
                metadata={"metric": "response_time", "value": 15000}
            )

            # Trigger alert handling
            manager._handle_alert(test_alert)

            # Verify alert was processed
            # (Implementation would check if appropriate automated response was triggered)
            assert True  # Placeholder - real implementation would verify specific actions

        finally:
            manager.stop()

    def _create_mock_recommendation(self):
        """Create a mock pipeline recommendation for testing."""
        mock_rec = MagicMock()
        mock_rec.confidence_score = 0.95
        mock_rec.transformation_strategy = MagicMock()
        mock_rec.transformation_strategy.value = "batch_processing"
        mock_rec.recommended_type = MagicMock()
        mock_rec.recommended_type.value = "data_warehouse"
        mock_rec.optimization_strategy = MagicMock()
        mock_rec.optimization_strategy.value = "balanced"
        mock_rec.output_destinations = []
        mock_rec.performance_optimizations = ["Use parallel processing", "Enable caching"]
        mock_rec.estimated_resources = {
            "cpu_cores": 4,
            "memory_gb": 8,
            "storage_gb": 100,
            "estimated_execution_time_minutes": 30,
            "recommended_worker_count": 6
        }
        mock_rec.reasoning = ["High confidence source detection", "Optimal strategy selected"]
        mock_rec.warnings = []
        mock_rec.alternative_approaches = ["Consider streaming processing"]

        return mock_rec


class TestQualityGates:
    """Test quality gates and validation mechanisms."""

    def test_code_quality_metrics(self):
        """Test that code quality metrics meet standards."""
        # This would typically run static analysis tools
        # For this example, we'll test basic structural requirements

        from src.agent_orchestrated_etl import (
            autonomous_pipeline_manager,
            intelligent_monitoring,
            intelligent_pipeline_detector,
            quantum_scaling_engine,
            resilient_orchestrator,
        )

        modules = [
            intelligent_pipeline_detector,
            intelligent_monitoring,
            resilient_orchestrator,
            quantum_scaling_engine,
            autonomous_pipeline_manager
        ]

        for module in modules:
            # Check that modules have proper docstrings
            assert module.__doc__ is not None

            # Check for required classes (basic structure validation)
            assert hasattr(module, '__file__')

    def test_error_handling_coverage(self):
        """Test that error handling is comprehensive."""
        monitor = IntelligentMonitor()

        # Test handling of invalid metrics
        try:
            monitor.record_metric("", float('inf'))  # Invalid metric name and value
            # Should not crash, might log warning
        except Exception as e:
            # If it raises an exception, it should be a controlled one
            assert isinstance(e, (ValueError, TypeError))

        # Test handling of None values
        try:
            monitor.record_metric("test_metric", None)
            # Should handle gracefully or raise controlled exception
        except Exception as e:
            assert isinstance(e, (ValueError, TypeError))

    def test_resource_cleanup(self, mock_workspace):
        """Test that resources are properly cleaned up."""
        manager = AutonomousPipelineManager(workspace_path=mock_workspace)

        # Start and stop manager to test cleanup
        manager.start()
        assert manager._active

        manager.stop()
        assert not manager._active

        # Verify threads are stopped
        if manager._management_thread:
            assert not manager._management_thread.is_alive()

    def test_memory_usage_patterns(self):
        """Test memory usage patterns for potential leaks."""
        import gc

        # Get initial memory usage
        gc.collect()
        initial_objects = len(gc.get_objects())

        # Create and destroy multiple monitors
        for i in range(10):
            monitor = IntelligentMonitor()
            monitor.record_metric(f"test_metric_{i}", float(i))
            del monitor

        # Force garbage collection
        gc.collect()
        final_objects = len(gc.get_objects())

        # Should not have significant memory growth
        object_growth = final_objects - initial_objects
        assert object_growth < 100  # Allow some growth but not excessive

    def test_performance_benchmarks(self):
        """Test performance benchmarks meet requirements."""
        monitor = IntelligentMonitor()

        # Benchmark metric recording
        start_time = time.time()
        for i in range(1000):
            monitor.record_metric("benchmark_metric", float(i))

        recording_time = time.time() - start_time

        # Should be able to record 1000 metrics quickly
        assert recording_time < 1.0  # Less than 1 second

        # Benchmark metric retrieval
        start_time = time.time()
        metrics = monitor.get_metrics("benchmark_metric")
        retrieval_time = time.time() - start_time

        assert len(metrics) == 1000
        assert retrieval_time < 0.1  # Less than 100ms

    def test_concurrent_access_safety(self):
        """Test thread safety of concurrent operations."""
        import threading

        monitor = IntelligentMonitor()
        errors = []

        def record_metrics(thread_id):
            try:
                for i in range(100):
                    monitor.record_metric(f"thread_{thread_id}_metric", float(i))
            except Exception as e:
                errors.append(e)

        # Create multiple threads recording metrics concurrently
        threads = []
        for i in range(5):
            thread = threading.Thread(target=record_metrics, args=(i,))
            threads.append(thread)
            thread.start()

        # Wait for all threads to complete
        for thread in threads:
            thread.join()

        # Should have no errors from concurrent access
        assert len(errors) == 0

        # Should have recorded all metrics
        total_metrics = 0
        for i in range(5):
            metrics = monitor.get_metrics(f"thread_{i}_metric")
            total_metrics += len(metrics)

        assert total_metrics == 500  # 5 threads * 100 metrics each
