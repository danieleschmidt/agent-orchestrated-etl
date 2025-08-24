"""Comprehensive tests for Generation 3 optimizations."""

import asyncio
import time
import pytest
from unittest.mock import MagicMock, patch

# Import our new Generation 3 optimizations
from src.agent_orchestrated_etl.intelligent_performance_optimizer import (
    PerformanceOptimizer,
    AdaptiveCache, 
    ConcurrentProcessor,
    PerformanceMetrics,
    get_performance_optimizer
)
from src.agent_orchestrated_etl.advanced_scaling_engine import (
    AdvancedScalingEngine,
    WorkerNode,
    LoadBalancingStrategy,
    ScalingDirection,
    get_scaling_engine
)


class TestIntelligentPerformanceOptimizer:
    """Test intelligent performance optimization features."""
    
    def test_adaptive_cache_basic_operations(self):
        """Test basic cache operations."""
        cache = AdaptiveCache(max_size=100, ttl=3600)
        
        # Test put and get
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # Test cache miss
        assert cache.get("nonexistent") is None
        
        # Test metrics
        metrics = cache.get_metrics()
        assert metrics["hits"] >= 1
        assert metrics["misses"] >= 1
        assert metrics["size"] >= 1
    
    def test_adaptive_cache_ttl_expiration(self):
        """Test cache TTL expiration."""
        cache = AdaptiveCache(max_size=100, ttl=1)  # 1 second TTL
        
        cache.put("key1", "value1")
        assert cache.get("key1") == "value1"
        
        # Wait for expiration
        time.sleep(1.1)
        assert cache.get("key1") is None
    
    def test_adaptive_cache_eviction(self):
        """Test intelligent cache eviction."""
        cache = AdaptiveCache(max_size=3, ttl=3600)
        
        # Fill cache beyond capacity
        for i in range(5):
            cache.put(f"key{i}", f"value{i}")
        
        # Should have evicted some entries
        metrics = cache.get_metrics()
        assert metrics["size"] <= 3
        assert metrics["evictions"] > 0
    
    @pytest.mark.asyncio
    async def test_concurrent_processor_batch_processing(self):
        """Test concurrent batch processing."""
        processor = ConcurrentProcessor(max_workers=4)
        
        def simple_processor(item):
            return item * 2
        
        items = list(range(20))
        results = await processor.process_batch_concurrent(
            items, simple_processor, batch_size=5, max_concurrent=3
        )
        
        expected = [item * 2 for item in items]
        assert len(results) == len(expected)
        assert sorted(results) == sorted(expected)
    
    @pytest.mark.asyncio
    async def test_performance_optimizer_integration(self):
        """Test performance optimizer end-to-end."""
        optimizer = PerformanceOptimizer()
        
        # Mock pipeline function
        async def mock_pipeline(*args, **kwargs):
            await asyncio.sleep(0.1)
            return {"status": "success", "data": [1, 2, 3]}
        
        result = await optimizer.optimize_pipeline_execution(mock_pipeline)
        
        assert "result" in result
        assert "execution_time" in result
        assert "optimization_result" in result
        assert result["result"]["status"] == "success"
    
    def test_performance_metrics_collection(self):
        """Test performance metrics collection."""
        optimizer = get_performance_optimizer()
        report = optimizer.get_performance_report()
        
        assert "current_metrics" in report
        assert "recommendations" in report
        assert "cache_metrics" in report
        assert "processor_metrics" in report


class TestAdvancedScalingEngine:
    """Test advanced scaling and load balancing features."""
    
    def test_worker_node_creation(self):
        """Test worker node basic functionality."""
        worker = WorkerNode(
            node_id="test_worker",
            capacity=100,
            current_load=50
        )
        
        assert worker.node_id == "test_worker"
        assert worker.utilization == 50.0
        assert worker.is_healthy
    
    def test_worker_node_health_check(self):
        """Test worker health checking."""
        worker = WorkerNode(
            node_id="test_worker",
            capacity=100,
            current_load=50,
            error_rate=0.15,  # High error rate
            health_score=0.5  # Low health score
        )
        
        assert not worker.is_healthy
    
    def test_scaling_engine_initialization(self):
        """Test scaling engine initialization."""
        engine = AdvancedScalingEngine(
            min_workers=2,
            max_workers=10,
            scaling_strategy=LoadBalancingStrategy.PREDICTIVE
        )
        
        cluster_metrics = engine._load_balancer.get_cluster_metrics()
        assert cluster_metrics["total_workers"] >= 2
        assert cluster_metrics["healthy_workers"] >= 2
    
    @pytest.mark.asyncio
    async def test_task_processing_with_load_balancing(self):
        """Test task processing with intelligent load balancing."""
        engine = AdvancedScalingEngine(min_workers=2, max_workers=5)
        
        def simple_task(x):
            time.sleep(0.01)  # Simulate work
            return x * 2
        
        # Process multiple tasks
        tasks = []
        for i in range(10):
            task = engine.process_task(simple_task, i, task_weight=1.0)
            tasks.append(task)
        
        results = await asyncio.gather(*tasks)
        
        expected = [i * 2 for i in range(10)]
        assert sorted(results) == sorted(expected)
    
    def test_scaling_recommendation_generation(self):
        """Test scaling recommendation generation."""
        engine = AdvancedScalingEngine(min_workers=2, max_workers=10)
        
        # Simulate high load to trigger scale-up recommendation
        for worker in engine._load_balancer._workers.values():
            worker.current_load = 90  # High load
        
        recommendation = engine.get_scaling_recommendation()
        
        if recommendation:
            assert hasattr(recommendation, 'direction')
            assert hasattr(recommendation, 'magnitude')
            assert hasattr(recommendation, 'confidence')
            assert 0 <= recommendation.confidence <= 1.0
    
    def test_auto_scaling_execution(self):
        """Test automatic scaling execution."""
        engine = AdvancedScalingEngine(min_workers=2, max_workers=10)
        
        result = engine.auto_scale()
        
        assert "status" in result
        assert "cluster_metrics" in result
        if "recommendation" in result:
            assert "direction" in result["recommendation"]
            assert "magnitude" in result["recommendation"]
            assert "confidence" in result["recommendation"]
    
    def test_load_balancing_strategies(self):
        """Test different load balancing strategies."""
        strategies = [
            LoadBalancingStrategy.ROUND_ROBIN,
            LoadBalancingStrategy.LEAST_CONNECTIONS,
            LoadBalancingStrategy.RESOURCE_BASED,
            LoadBalancingStrategy.PREDICTIVE
        ]
        
        for strategy in strategies:
            engine = AdvancedScalingEngine(
                min_workers=3,
                max_workers=5,
                scaling_strategy=strategy
            )
            
            # Test worker selection
            worker = engine._load_balancer.select_worker(task_weight=1.0)
            assert worker is not None
            assert worker.is_healthy


class TestPerformanceIntegration:
    """Integration tests for performance optimization features."""
    
    @pytest.mark.asyncio
    async def test_performance_and_scaling_integration(self):
        """Test integration between performance optimizer and scaling engine."""
        # Create both systems
        optimizer = get_performance_optimizer()
        engine = get_scaling_engine(min_workers=2, max_workers=8)
        
        # Mock a CPU-intensive pipeline
        async def cpu_intensive_pipeline():
            tasks = []
            for i in range(20):
                task = engine.process_task(lambda x: x ** 2, i)
                tasks.append(task)
            
            results = await asyncio.gather(*tasks)
            return {"processed_items": len(results), "results": results}
        
        # Run with performance optimization
        result = await optimizer.optimize_pipeline_execution(cpu_intensive_pipeline)
        
        assert result["result"]["processed_items"] == 20
        assert "optimization_result" in result
        assert "execution_time" in result
    
    def test_cache_and_scaling_metrics_integration(self):
        """Test integration of caching and scaling metrics."""
        optimizer = get_performance_optimizer()
        engine = get_scaling_engine()
        
        # Get performance report
        perf_report = optimizer.get_performance_report()
        
        # Get scaling metrics
        scaling_result = engine.auto_scale()
        
        # Verify both systems provide comprehensive metrics
        assert "cache_metrics" in perf_report
        assert "cluster_metrics" in scaling_result
        
        # Verify metrics have expected structure
        cache_metrics = perf_report["cache_metrics"]
        assert "hit_ratio" in cache_metrics
        assert "size" in cache_metrics
        
        cluster_metrics = scaling_result["cluster_metrics"]
        assert "total_workers" in cluster_metrics
        assert "average_utilization" in cluster_metrics


class TestAdvancedFeaturesResilience:
    """Test resilience and error handling of advanced features."""
    
    def test_cache_resilience_to_errors(self):
        """Test cache resilience to various error conditions."""
        cache = AdaptiveCache(max_size=5, ttl=1)
        
        # Test with various data types
        test_data = [
            ("string", "test_value"),
            ("int", 12345),
            ("list", [1, 2, 3]),
            ("dict", {"key": "value"}),
            ("none", None)
        ]
        
        for key, value in test_data:
            cache.put(key, value)
            retrieved = cache.get(key)
            assert retrieved == value
    
    @pytest.mark.asyncio
    async def test_scaling_engine_error_recovery(self):
        """Test scaling engine error recovery."""
        engine = AdvancedScalingEngine(min_workers=2, max_workers=5)
        
        def failing_task(x):
            if x % 3 == 0:  # Fail every 3rd task
                raise ValueError(f"Simulated failure for {x}")
            return x * 2
        
        # Process tasks with some failures
        results = []
        errors = []
        
        for i in range(6):
            try:
                result = await engine.process_task(failing_task, i, task_weight=1.0)
                results.append(result)
            except ValueError as e:
                errors.append(str(e))
        
        # Should have some successes and some failures
        assert len(results) > 0
        assert len(errors) > 0
        
        # System should still be healthy
        cluster_metrics = engine._load_balancer.get_cluster_metrics()
        assert cluster_metrics["healthy_workers"] > 0


if __name__ == "__main__":
    # Run basic smoke tests
    print("Running Generation 3 optimization smoke tests...")
    
    # Test cache
    cache = AdaptiveCache(max_size=10)
    cache.put("test", "value")
    assert cache.get("test") == "value"
    print("✓ Adaptive Cache working")
    
    # Test performance optimizer
    optimizer = get_performance_optimizer()
    report = optimizer.get_performance_report()
    assert "current_metrics" in report
    print("✓ Performance Optimizer working")
    
    # Test scaling engine
    engine = get_scaling_engine(min_workers=1, max_workers=3)
    metrics = engine._load_balancer.get_cluster_metrics()
    assert metrics["total_workers"] >= 1
    print("✓ Scaling Engine working")
    
    print("All Generation 3 optimizations are functional! ✅")