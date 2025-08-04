"""Unit tests for OptimizationService."""

import pytest
from datetime import datetime, timedelta
from typing import Dict, Any
from unittest.mock import AsyncMock, MagicMock, patch

from src.agent_orchestrated_etl.services.optimization_service import OptimizationService


class TestOptimizationService:
    """Test cases for OptimizationService."""

    def setup_method(self):
        """Set up test fixtures."""
        self.mock_db_manager = AsyncMock()
        self.optimization_service = OptimizationService(db_manager=self.mock_db_manager)

    @pytest.mark.asyncio
    async def test_optimize_pipeline_configuration_performance_goal(self):
        """Test pipeline optimization for performance goals."""
        pipeline_id = "perf-pipeline-001"
        current_config = {
            "batch_size": 1000,
            "parallel_workers": 2,
            "memory_limit_gb": 4,
            "timeout_minutes": 60
        }
        performance_data = {
            "avg_execution_time_minutes": 45,
            "avg_memory_usage_gb": 3.2,
            "avg_cpu_utilization": 0.4,
            "throughput_records_per_second": 800,
            "error_rate": 0.02
        }
        optimization_goals = {
            "primary": "reduce_execution_time",
            "target_reduction_percent": 30,
            "acceptable_resource_increase": 50
        }
        
        result = await self.optimization_service.optimize_pipeline_configuration(
            pipeline_id, current_config, performance_data, optimization_goals
        )
        
        assert "optimized_config" in result
        assert "expected_improvements" in result
        assert "confidence_score" in result
        
        optimized = result["optimized_config"]
        
        # Should increase parallelism due to low CPU utilization
        assert optimized["parallel_workers"] > current_config["parallel_workers"]
        
        # Should potentially increase batch size for better throughput
        assert optimized["batch_size"] >= current_config["batch_size"]
        
        # Should have improvement predictions
        improvements = result["expected_improvements"]
        assert "execution_time_reduction_percent" in improvements

    @pytest.mark.asyncio
    async def test_optimize_pipeline_configuration_memory_goal(self):
        """Test pipeline optimization for memory efficiency."""
        pipeline_id = "memory-pipeline-001"
        current_config = {
            "batch_size": 5000,
            "parallel_workers": 8,
            "memory_limit_gb": 16,
            "cache_enabled": True
        }
        performance_data = {
            "avg_memory_usage_gb": 14.5,
            "peak_memory_usage_gb": 15.8,
            "memory_pressure_events": 5,
            "gc_overhead_percent": 25
        }
        optimization_goals = {
            "primary": "reduce_memory_usage",
            "target_reduction_percent": 20,
            "maintain_performance": True
        }
        
        result = await self.optimization_service.optimize_pipeline_configuration(
            pipeline_id, current_config, performance_data, optimization_goals
        )
        
        optimized = result["optimized_config"]
        
        # Should reduce batch size to lower memory usage
        assert optimized["batch_size"] < current_config["batch_size"]
        
        # Should potentially reduce parallel workers
        assert optimized["parallel_workers"] <= current_config["parallel_workers"]
        
        # Should have memory-specific optimizations
        assert "streaming_enabled" in optimized or "incremental_processing" in optimized

    @pytest.mark.asyncio
    async def test_optimize_pipeline_configuration_cost_goal(self):
        """Test pipeline optimization for cost reduction."""
        pipeline_id = "cost-pipeline-001"
        current_config = {
            "instance_type": "large",
            "auto_scaling": True,
            "storage_tier": "premium",
            "retention_days": 365
        }
        performance_data = {
            "avg_resource_utilization": 0.3,
            "storage_usage_gb": 1000,
            "network_transfer_gb": 500,
            "compute_cost_per_hour": 2.50
        }
        optimization_goals = {
            "primary": "reduce_cost",
            "target_reduction_percent": 40,
            "performance_tolerance": 20
        }
        
        result = await self.optimization_service.optimize_pipeline_configuration(
            pipeline_id, current_config, performance_data, optimization_goals
        )
        
        optimized = result["optimized_config"]
        
        # Should recommend smaller instance due to low utilization
        assert optimized["instance_type"] in ["medium", "small"]
        
        # Should optimize storage tier
        assert optimized["storage_tier"] in ["standard", "cold"]
        
        # Should have cost projections
        improvements = result["expected_improvements"]
        assert "cost_reduction_percent" in improvements

    @pytest.mark.asyncio
    async def test_real_time_optimization_performance_degradation(self):
        """Test real-time optimization when performance degrades."""
        pipeline_id = "realtime-pipeline-001"
        live_metrics = {
            "current_throughput": 500,  # Down from normal 1000
            "memory_usage_percent": 85,
            "cpu_utilization": 0.9,
            "queue_depth": 10000,
            "error_rate": 0.08,  # Higher than normal
            "response_time_ms": 2500
        }
        adaptation_threshold = 0.3  # 30% performance degradation triggers optimization
        
        result = await self.optimization_service.real_time_optimization(
            pipeline_id, live_metrics, adaptation_threshold
        )
        
        assert result["optimization_triggered"] is True
        assert "immediate_actions" in result
        assert "configuration_changes" in result
        
        actions = result["immediate_actions"]
        
        # Should recommend immediate actions for performance issues
        assert any("scale" in action.lower() or "reduce" in action.lower() for action in actions)
        
        config_changes = result["configuration_changes"]
        assert len(config_changes) > 0

    @pytest.mark.asyncio
    async def test_real_time_optimization_within_threshold(self):
        """Test real-time optimization when metrics are within threshold."""
        pipeline_id = "stable-pipeline-001"
        live_metrics = {
            "current_throughput": 950,  # Close to normal 1000
            "memory_usage_percent": 65,
            "cpu_utilization": 0.7,
            "error_rate": 0.01,
            "response_time_ms": 800
        }
        adaptation_threshold = 0.3
        
        result = await self.optimization_service.real_time_optimization(
            pipeline_id, live_metrics, adaptation_threshold
        )
        
        assert result["optimization_triggered"] is False
        assert result["status"] == "stable"
        assert "monitoring_recommendations" in result

    @pytest.mark.asyncio
    async def test_calculate_optimization_score(self):
        """Test optimization score calculation."""
        current_metrics = {
            "execution_time_minutes": 30,
            "memory_usage_gb": 6.0,
            "cpu_utilization": 0.8,
            "cost_per_execution": 5.00,
            "success_rate": 0.95
        }
        baseline_metrics = {
            "execution_time_minutes": 45,
            "memory_usage_gb": 8.0,
            "cpu_utilization": 0.6,
            "cost_per_execution": 7.50,
            "success_rate": 0.90
        }
        weights = {
            "performance": 0.4,
            "resource_efficiency": 0.3,
            "cost": 0.2,
            "reliability": 0.1
        }
        
        score = await self.optimization_service._calculate_optimization_score(
            current_metrics, baseline_metrics, weights
        )
        
        assert 0.0 <= score <= 1.0
        assert score > 0.5  # Should be positive since current metrics are better

    @pytest.mark.asyncio
    async def test_apply_performance_optimizations(self):
        """Test application of performance-focused optimizations."""
        current_config = {
            "batch_size": 1000,
            "parallel_workers": 2,
            "connection_pool_size": 5,
            "cache_ttl_minutes": 60
        }
        performance_data = {
            "avg_cpu_utilization": 0.3,
            "avg_memory_usage_percent": 40,
            "io_wait_percent": 15,
            "cache_hit_rate": 0.6
        }
        
        result = await self.optimization_service._apply_performance_optimizations(
            current_config, performance_data
        )
        
        # Should increase parallelism due to low CPU usage
        assert result["parallel_workers"] > current_config["parallel_workers"]
        
        # Should increase batch size for better throughput
        assert result["batch_size"] >= current_config["batch_size"]
        
        # Should optimize connection pool
        assert result["connection_pool_size"] >= current_config["connection_pool_size"]

    @pytest.mark.asyncio
    async def test_apply_memory_optimizations(self):
        """Test application of memory-focused optimizations."""
        current_config = {
            "batch_size": 10000,
            "parallel_workers": 8,
            "buffer_size_mb": 512,
            "gc_strategy": "default"
        }
        memory_data = {
            "avg_memory_usage_percent": 90,
            "gc_overhead_percent": 30,
            "memory_pressure_events": 10,
            "swap_usage_mb": 1024
        }
        
        result = await self.optimization_service._apply_memory_optimizations(
            current_config, memory_data
        )
        
        # Should reduce batch size to lower memory pressure
        assert result["batch_size"] < current_config["batch_size"]
        
        # Should adjust GC strategy
        assert result["gc_strategy"] != current_config["gc_strategy"]
        
        # Should add streaming or incremental processing
        assert "streaming_enabled" in result or "incremental_processing" in result

    @pytest.mark.asyncio
    async def test_apply_cost_optimizations(self):
        """Test application of cost-focused optimizations."""
        current_config = {
            "instance_type": "xlarge",
            "storage_type": "ssd",
            "backup_frequency": "hourly",
            "log_retention_days": 90
        }
        cost_data = {
            "avg_resource_utilization": 0.25,
            "storage_utilization": 0.4,
            "network_costs_percent": 15,
            "idle_time_percent": 60
        }
        
        result = await self.optimization_service._apply_cost_optimizations(
            current_config, cost_data
        )
        
        # Should downsize instance due to low utilization
        assert "large" in result["instance_type"] or "medium" in result["instance_type"]
        
        # Should optimize storage type
        assert result["storage_type"] in ["standard", "hdd"]
        
        # Should reduce backup frequency
        assert result["backup_frequency"] in ["daily", "weekly"]

    @pytest.mark.asyncio
    async def test_predict_optimization_impact(self):
        """Test prediction of optimization impact."""
        current_config = {"batch_size": 1000, "parallel_workers": 2}
        optimized_config = {"batch_size": 2000, "parallel_workers": 4}
        historical_data = [
            {"config": {"batch_size": 1500, "parallel_workers": 3}, "execution_time": 25},
            {"config": {"batch_size": 2000, "parallel_workers": 4}, "execution_time": 20},
            {"config": {"batch_size": 500, "parallel_workers": 1}, "execution_time": 40}
        ]
        
        result = await self.optimization_service._predict_optimization_impact(
            current_config, optimized_config, historical_data
        )
        
        assert "predicted_execution_time_change" in result
        assert "confidence_score" in result
        assert "risk_assessment" in result
        assert 0.0 <= result["confidence_score"] <= 1.0

    @pytest.mark.asyncio
    async def test_adaptive_optimization_strategy(self):
        """Test adaptive optimization strategy selection."""
        pipeline_characteristics = {
            "data_volume": "large",
            "transformation_complexity": "high",
            "source_variability": "medium",
            "schedule_frequency": "hourly"
        }
        performance_history = {
            "success_rate": 0.85,
            "avg_execution_time": 45,
            "performance_variance": 0.3,
            "resource_efficiency": 0.6
        }
        
        strategy = await self.optimization_service._select_optimization_strategy(
            pipeline_characteristics, performance_history
        )
        
        assert strategy["primary_focus"] in ["performance", "reliability", "cost", "resource_efficiency"]
        assert "tactics" in strategy
        assert "monitoring_points" in strategy
        assert len(strategy["tactics"]) > 0

    @pytest.mark.asyncio
    async def test_continuous_learning_integration(self):
        """Test continuous learning from optimization results."""
        optimization_result = {
            "pipeline_id": "learn-opt-001",
            "original_config": {"batch_size": 1000},
            "optimized_config": {"batch_size": 2000},
            "predicted_improvement": 30,
            "actual_improvement": 25,
            "confidence_score": 0.8
        }
        actual_performance = {
            "execution_time_minutes": 22,
            "memory_usage_gb": 4.5,
            "success_rate": 0.98
        }
        
        await self.optimization_service.learn_from_optimization_result(
            optimization_result, actual_performance
        )
        
        # Should have updated learning database
        self.mock_db_manager.execute_query.assert_called()
        
        # Verify learning data structure
        call_args = self.mock_db_manager.execute_query.call_args
        query = call_args[0][0]
        assert "INSERT" in query or "UPDATE" in query

    @pytest.mark.asyncio
    async def test_get_optimization_recommendations(self):
        """Test getting optimization recommendations."""
        pipeline_id = "rec-pipeline-001"
        
        # Mock current performance data
        self.mock_db_manager.execute_query.return_value = [{
            "avg_execution_time": 35,
            "avg_memory_usage": 6.5,
            "avg_cpu_utilization": 0.4,
            "success_rate": 0.92,
            "cost_per_execution": 3.50
        }]
        
        result = await self.optimization_service.get_optimization_recommendations(pipeline_id)
        
        assert "recommendations" in result
        assert "priority_order" in result
        assert "impact_estimates" in result
        
        recommendations = result["recommendations"]
        assert len(recommendations) > 0
        assert all("type" in rec and "description" in rec for rec in recommendations)

    @pytest.mark.asyncio
    async def test_benchmark_configuration(self):
        """Test configuration benchmarking."""
        pipeline_id = "benchmark-pipeline-001"
        test_configs = [
            {"batch_size": 1000, "parallel_workers": 2},
            {"batch_size": 2000, "parallel_workers": 4},
            {"batch_size": 500, "parallel_workers": 1}
        ]
        
        # Mock benchmark results
        with patch.object(self.optimization_service, '_run_benchmark_test') as mock_benchmark:
            mock_benchmark.side_effect = [
                {"execution_time": 30, "memory_usage": 4.0, "success": True},
                {"execution_time": 20, "memory_usage": 6.0, "success": True},
                {"execution_time": 45, "memory_usage": 2.0, "success": True}
            ]
            
            result = await self.optimization_service.benchmark_configurations(
                pipeline_id, test_configs
            )
            
            assert "best_configuration" in result
            assert "benchmark_results" in result
            assert len(result["benchmark_results"]) == 3
            
            # Best config should be the one with shortest execution time
            best_config = result["best_configuration"]
            assert best_config["batch_size"] == 2000
            assert best_config["parallel_workers"] == 4

    @pytest.mark.asyncio
    async def test_error_handling_in_optimization(self):
        """Test error handling during optimization."""
        pipeline_id = "error-pipeline-001"
        current_config = {"batch_size": 1000}
        performance_data = {"avg_execution_time": 30}
        optimization_goals = {"primary": "reduce_execution_time"}
        
        # Mock database error
        self.mock_db_manager.execute_query.side_effect = Exception("Database connection failed")
        
        result = await self.optimization_service.optimize_pipeline_configuration(
            pipeline_id, current_config, performance_data, optimization_goals
        )
        
        # Should gracefully handle errors and provide fallback recommendations
        assert "optimized_config" in result
        assert "error" in result
        assert result["optimized_config"] is not None

    @pytest.mark.asyncio
    async def test_multi_objective_optimization(self):
        """Test optimization with multiple competing objectives."""
        pipeline_id = "multi-obj-pipeline-001"
        current_config = {
            "batch_size": 1000,
            "parallel_workers": 4,
            "cache_enabled": True
        }
        performance_data = {
            "execution_time_minutes": 30,
            "memory_usage_gb": 5.0,
            "cost_per_execution": 4.00,
            "success_rate": 0.95
        }
        optimization_goals = {
            "objectives": [
                {"type": "reduce_execution_time", "weight": 0.4},
                {"type": "reduce_cost", "weight": 0.3},
                {"type": "improve_reliability", "weight": 0.3}
            ],
            "constraints": {
                "max_memory_gb": 8,
                "max_cost_increase_percent": 20
            }
        }
        
        result = await self.optimization_service.optimize_pipeline_configuration(
            pipeline_id, current_config, performance_data, optimization_goals
        )
        
        assert "optimized_config" in result
        assert "trade_off_analysis" in result
        assert "pareto_solutions" in result
        
        # Should provide multiple solution options
        pareto_solutions = result["pareto_solutions"]
        assert len(pareto_solutions) > 1
        assert all("config" in sol and "objectives_score" in sol for sol in pareto_solutions)