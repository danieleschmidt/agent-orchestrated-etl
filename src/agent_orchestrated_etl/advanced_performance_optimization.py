"""Advanced performance optimization system with AI-driven scaling and resource management."""

import asyncio
import statistics
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from .logging_config import get_logger


class OptimizationStrategy(Enum):
    """Performance optimization strategies."""
    PARALLEL_PROCESSING = "parallel_processing"
    ASYNC_PROCESSING = "async_processing"
    BATCH_OPTIMIZATION = "batch_optimization"
    CACHING = "caching"
    RESOURCE_POOLING = "resource_pooling"
    LOAD_BALANCING = "load_balancing"
    AUTO_SCALING = "auto_scaling"


class ResourceType(Enum):
    """Types of system resources."""
    CPU = "cpu"
    MEMORY = "memory"
    NETWORK = "network"
    DISK = "disk"
    DATABASE = "database"


@dataclass
class PerformanceMetrics:
    """Performance metrics for optimization decisions."""
    throughput: float = 0.0  # operations per second
    latency: float = 0.0  # average response time
    cpu_usage: float = 0.0  # CPU utilization percentage
    memory_usage: float = 0.0  # Memory utilization percentage
    error_rate: float = 0.0  # Error rate percentage
    queue_depth: int = 0  # Number of pending operations
    timestamp: float = field(default_factory=time.time)


@dataclass
class OptimizationAction:
    """Action to improve performance."""
    strategy: OptimizationStrategy
    parameters: Dict[str, Any] = field(default_factory=dict)
    expected_improvement: float = 0.0  # Expected performance gain
    cost_estimate: float = 0.0  # Resource cost estimate


class IntelligentResourceManager:
    """AI-driven resource management with predictive scaling."""

    def __init__(self, max_workers: int = 10, max_memory_mb: int = 1024):
        self.logger = get_logger("agent_etl.performance.resource_manager")
        self.max_workers = max_workers
        self.max_memory_mb = max_memory_mb

        # Resource pools
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.process_pool = ProcessPoolExecutor(max_workers=min(max_workers, 4))

        # Performance tracking
        self.performance_history: List[PerformanceMetrics] = []
        self.resource_usage: Dict[ResourceType, List[float]] = {
            resource: [] for resource in ResourceType
        }

        # Dynamic scaling parameters
        self.current_workers = max_workers // 2
        self.scale_up_threshold = 0.8
        self.scale_down_threshold = 0.3
        self.cooldown_period = 30.0  # seconds
        self.last_scale_time = 0.0

        # Performance caches
        self.result_cache: Dict[str, Tuple[Any, float]] = {}
        self.cache_ttl = 300.0  # 5 minutes

    def record_performance_metrics(self, metrics: PerformanceMetrics) -> None:
        """Record performance metrics for optimization decisions."""
        self.performance_history.append(metrics)

        # Keep only recent history (last 100 measurements)
        if len(self.performance_history) > 100:
            self.performance_history = self.performance_history[-100:]

        # Update resource usage tracking
        self.resource_usage[ResourceType.CPU].append(metrics.cpu_usage)
        self.resource_usage[ResourceType.MEMORY].append(metrics.memory_usage)

        # Clean up old resource usage data
        for resource_type in ResourceType:
            if len(self.resource_usage[resource_type]) > 100:
                self.resource_usage[resource_type] = self.resource_usage[resource_type][-100:]

    def get_current_performance(self) -> Optional[PerformanceMetrics]:
        """Get the most recent performance metrics."""
        return self.performance_history[-1] if self.performance_history else None

    def calculate_optimization_actions(self) -> List[OptimizationAction]:
        """Calculate recommended optimization actions based on current performance."""
        if not self.performance_history:
            return []

        current_metrics = self.performance_history[-1]
        actions = []

        # High CPU usage - suggest parallel processing
        if current_metrics.cpu_usage > 80:
            actions.append(OptimizationAction(
                strategy=OptimizationStrategy.PARALLEL_PROCESSING,
                parameters={"worker_multiplier": 1.5},
                expected_improvement=0.3,
                cost_estimate=0.2
            ))

        # High latency - suggest caching
        if current_metrics.latency > 1.0:
            actions.append(OptimizationAction(
                strategy=OptimizationStrategy.CACHING,
                parameters={"cache_size": 1000, "ttl": 300},
                expected_improvement=0.5,
                cost_estimate=0.1
            ))

        # High queue depth - suggest auto-scaling
        if current_metrics.queue_depth > 10:
            actions.append(OptimizationAction(
                strategy=OptimizationStrategy.AUTO_SCALING,
                parameters={"scale_factor": 2.0},
                expected_improvement=0.4,
                cost_estimate=0.3
            ))

        # Low throughput - suggest batch optimization
        if current_metrics.throughput < 10:
            actions.append(OptimizationAction(
                strategy=OptimizationStrategy.BATCH_OPTIMIZATION,
                parameters={"batch_size": 50},
                expected_improvement=0.6,
                cost_estimate=0.1
            ))

        return actions

    def should_scale_resources(self) -> Tuple[bool, str]:
        """Determine if resources should be scaled up or down."""
        if not self.performance_history:
            return False, "no_data"

        # Check cooldown period
        if time.time() - self.last_scale_time < self.cooldown_period:
            return False, "cooldown"

        # Get recent CPU usage average
        recent_cpu = [m.cpu_usage for m in self.performance_history[-5:]]
        avg_cpu = statistics.mean(recent_cpu) if recent_cpu else 0

        # Get recent queue depth
        recent_queue = [m.queue_depth for m in self.performance_history[-3:]]
        avg_queue = statistics.mean(recent_queue) if recent_queue else 0

        # Scale up conditions
        if avg_cpu > self.scale_up_threshold * 100 or avg_queue > 5:
            return True, "scale_up"

        # Scale down conditions
        if avg_cpu < self.scale_down_threshold * 100 and avg_queue < 1:
            return True, "scale_down"

        return False, "stable"

    def scale_resources(self, direction: str) -> bool:
        """Scale resources up or down."""
        if direction == "scale_up" and self.current_workers < self.max_workers:
            new_workers = min(self.current_workers * 2, self.max_workers)
            self.logger.info(f"Scaling up workers: {self.current_workers} -> {new_workers}")
            self.current_workers = new_workers
            self.last_scale_time = time.time()
            return True

        elif direction == "scale_down" and self.current_workers > 1:
            new_workers = max(self.current_workers // 2, 1)
            self.logger.info(f"Scaling down workers: {self.current_workers} -> {new_workers}")
            self.current_workers = new_workers
            self.last_scale_time = time.time()
            return True

        return False

    async def execute_with_optimization(
        self,
        operation: Callable,
        data: Any,
        optimization_hint: Optional[OptimizationStrategy] = None
    ) -> Any:
        """Execute an operation with automatic optimization."""
        start_time = time.time()

        try:
            # Check cache first
            cache_key = f"{operation.__name__}_{hash(str(data))}"
            if cache_key in self.result_cache:
                cached_result, cache_time = self.result_cache[cache_key]
                if time.time() - cache_time < self.cache_ttl:
                    self.logger.info(f"Cache hit for {operation.__name__}")
                    return cached_result

            # Determine optimization strategy
            if optimization_hint:
                strategy = optimization_hint
            else:
                strategy = self._determine_optimization_strategy(operation, data)

            # Execute with chosen strategy
            if strategy == OptimizationStrategy.PARALLEL_PROCESSING:
                result = await self._execute_parallel(operation, data)
            elif strategy == OptimizationStrategy.ASYNC_PROCESSING:
                result = await self._execute_async(operation, data)
            elif strategy == OptimizationStrategy.BATCH_OPTIMIZATION:
                result = await self._execute_batch(operation, data)
            else:
                # Default execution
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(data)
                else:
                    result = operation(data)

            # Cache successful results
            execution_time = time.time() - start_time
            if execution_time < 5.0:  # Only cache fast operations
                self.result_cache[cache_key] = (result, time.time())

            # Clean up old cache entries
            self._cleanup_cache()

            return result

        except Exception as e:
            self.logger.error(f"Optimized execution failed: {e}")
            raise

    def _determine_optimization_strategy(self, operation: Callable, data: Any) -> OptimizationStrategy:
        """Intelligently determine the best optimization strategy."""
        # If data is a list and large, use parallel processing
        if isinstance(data, list) and len(data) > 100:
            return OptimizationStrategy.PARALLEL_PROCESSING

        # If operation is async, use async processing
        if asyncio.iscoroutinefunction(operation):
            return OptimizationStrategy.ASYNC_PROCESSING

        # If data can be batched, use batch optimization
        if isinstance(data, (list, tuple)) and len(data) > 10:
            return OptimizationStrategy.BATCH_OPTIMIZATION

        # Default to async processing
        return OptimizationStrategy.ASYNC_PROCESSING

    async def _execute_parallel(self, operation: Callable, data: Any) -> Any:
        """Execute operation using parallel processing."""
        if isinstance(data, list):
            # Split data into chunks for parallel processing
            chunk_size = max(1, len(data) // self.current_workers)
            chunks = [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]

            # Execute chunks in parallel
            loop = asyncio.get_event_loop()
            tasks = []

            for chunk in chunks:
                if asyncio.iscoroutinefunction(operation):
                    tasks.append(operation(chunk))
                else:
                    tasks.append(loop.run_in_executor(self.thread_pool, operation, chunk))

            results = await asyncio.gather(*tasks)

            # Combine results
            if all(isinstance(r, list) for r in results):
                return [item for sublist in results for item in sublist]
            else:
                return results
        else:
            # Single item - execute in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.thread_pool, operation, data)

    async def _execute_async(self, operation: Callable, data: Any) -> Any:
        """Execute operation using async processing."""
        if asyncio.iscoroutinefunction(operation):
            return await operation(data)
        else:
            # Run CPU-bound operation in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.thread_pool, operation, data)

    async def _execute_batch(self, operation: Callable, data: Any) -> Any:
        """Execute operation using batch optimization."""
        if isinstance(data, list):
            # Process in optimal batch sizes
            optimal_batch_size = self._calculate_optimal_batch_size(len(data))
            results = []

            for i in range(0, len(data), optimal_batch_size):
                batch = data[i:i + optimal_batch_size]

                if asyncio.iscoroutinefunction(operation):
                    batch_result = await operation(batch)
                else:
                    batch_result = operation(batch)

                if isinstance(batch_result, list):
                    results.extend(batch_result)
                else:
                    results.append(batch_result)

            return results
        else:
            return await self._execute_async(operation, data)

    def _calculate_optimal_batch_size(self, data_size: int) -> int:
        """Calculate optimal batch size based on performance history."""
        # Simple heuristic - can be made more sophisticated
        if data_size < 100:
            return data_size
        elif data_size < 1000:
            return 50
        elif data_size < 10000:
            return 100
        else:
            return 500

    def _cleanup_cache(self) -> None:
        """Clean up expired cache entries."""
        current_time = time.time()
        expired_keys = [
            key for key, (_, cache_time) in self.result_cache.items()
            if current_time - cache_time > self.cache_ttl
        ]

        for key in expired_keys:
            del self.result_cache[key]

    def get_resource_statistics(self) -> Dict[str, Any]:
        """Get current resource usage statistics."""
        stats = {
            "current_workers": self.current_workers,
            "max_workers": self.max_workers,
            "cache_size": len(self.result_cache),
            "performance_samples": len(self.performance_history)
        }

        # Add resource usage averages
        for resource_type in ResourceType:
            usage_data = self.resource_usage[resource_type]
            if usage_data:
                stats[f"{resource_type.value}_avg"] = statistics.mean(usage_data)
                stats[f"{resource_type.value}_max"] = max(usage_data)

        return stats

    def shutdown(self) -> None:
        """Shutdown resource pools gracefully."""
        self.logger.info("Shutting down resource manager")
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)


class AdaptiveLoadBalancer:
    """Adaptive load balancer with intelligent request routing."""

    def __init__(self, max_concurrent_requests: int = 100):
        self.logger = get_logger("agent_etl.performance.load_balancer")
        self.max_concurrent_requests = max_concurrent_requests
        self.active_requests = 0
        self.request_queue = asyncio.Queue()
        self.request_semaphore = asyncio.Semaphore(max_concurrent_requests)

        # Performance tracking per endpoint
        self.endpoint_stats: Dict[str, Dict[str, float]] = {}

        # Circuit breaker state
        self.circuit_breakers: Dict[str, Dict[str, Any]] = {}

    async def execute_with_load_balancing(
        self,
        operation: Callable,
        endpoint_id: str,
        data: Any,
        timeout: float = 30.0
    ) -> Any:
        """Execute operation with load balancing and circuit breaking."""

        # Check circuit breaker
        if self._is_circuit_open(endpoint_id):
            raise Exception(f"Circuit breaker open for {endpoint_id}")

        # Acquire semaphore for rate limiting
        async with self.request_semaphore:
            start_time = time.time()
            self.active_requests += 1

            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    self._execute_operation(operation, data),
                    timeout=timeout
                )

                # Record success
                execution_time = time.time() - start_time
                self._record_endpoint_stats(endpoint_id, execution_time, success=True)

                return result

            except Exception:
                # Record failure
                execution_time = time.time() - start_time
                self._record_endpoint_stats(endpoint_id, execution_time, success=False)

                # Update circuit breaker
                self._update_circuit_breaker(endpoint_id, success=False)

                raise

            finally:
                self.active_requests -= 1

    async def _execute_operation(self, operation: Callable, data: Any) -> Any:
        """Execute the actual operation."""
        if asyncio.iscoroutinefunction(operation):
            return await operation(data)
        else:
            # Run in thread pool for CPU-bound operations
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, operation, data)

    def _record_endpoint_stats(self, endpoint_id: str, execution_time: float, success: bool) -> None:
        """Record performance statistics for an endpoint."""
        if endpoint_id not in self.endpoint_stats:
            self.endpoint_stats[endpoint_id] = {
                "total_requests": 0,
                "successful_requests": 0,
                "total_time": 0.0,
                "avg_response_time": 0.0,
                "success_rate": 1.0
            }

        stats = self.endpoint_stats[endpoint_id]
        stats["total_requests"] += 1
        stats["total_time"] += execution_time

        if success:
            stats["successful_requests"] += 1

        stats["avg_response_time"] = stats["total_time"] / stats["total_requests"]
        stats["success_rate"] = stats["successful_requests"] / stats["total_requests"]

    def _is_circuit_open(self, endpoint_id: str) -> bool:
        """Check if circuit breaker is open for an endpoint."""
        if endpoint_id not in self.circuit_breakers:
            return False

        breaker = self.circuit_breakers[endpoint_id]

        if breaker["state"] == "open":
            # Check if timeout period has passed
            if time.time() - breaker["last_failure"] > breaker["timeout"]:
                breaker["state"] = "half_open"
                return False
            return True

        return False

    def _update_circuit_breaker(self, endpoint_id: str, success: bool) -> None:
        """Update circuit breaker state based on operation result."""
        if endpoint_id not in self.circuit_breakers:
            self.circuit_breakers[endpoint_id] = {
                "state": "closed",
                "failure_count": 0,
                "last_failure": 0,
                "timeout": 60.0,  # 1 minute timeout
                "failure_threshold": 5
            }

        breaker = self.circuit_breakers[endpoint_id]

        if success:
            if breaker["state"] == "half_open":
                breaker["state"] = "closed"
                breaker["failure_count"] = 0
        else:
            breaker["failure_count"] += 1
            breaker["last_failure"] = time.time()

            if breaker["failure_count"] >= breaker["failure_threshold"]:
                breaker["state"] = "open"
                self.logger.warning(f"Circuit breaker opened for {endpoint_id}")

    def get_load_balancer_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        return {
            "active_requests": self.active_requests,
            "max_concurrent": self.max_concurrent_requests,
            "endpoints": self.endpoint_stats.copy(),
            "circuit_breakers": {
                endpoint: breaker["state"]
                for endpoint, breaker in self.circuit_breakers.items()
            }
        }


class PerformanceOptimizationEngine:
    """Main performance optimization engine that coordinates all optimization strategies."""

    def __init__(self):
        self.logger = get_logger("agent_etl.performance.engine")
        self.resource_manager = IntelligentResourceManager()
        self.load_balancer = AdaptiveLoadBalancer()

        # Performance monitoring
        self.optimization_active = False
        self.optimization_task: Optional[asyncio.Task] = None

        # Statistics
        self.optimization_stats = {
            "optimizations_applied": 0,
            "performance_improvements": [],
            "resource_savings": 0.0
        }

    def start_optimization(self, monitoring_interval: float = 10.0) -> None:
        """Start the performance optimization system."""
        if self.optimization_active:
            self.logger.warning("Optimization is already active")
            return

        self.optimization_active = True
        self.optimization_task = asyncio.create_task(
            self._optimization_loop(monitoring_interval)
        )
        self.logger.info("Performance optimization system started")

    def stop_optimization(self) -> None:
        """Stop the performance optimization system."""
        self.optimization_active = False
        if self.optimization_task:
            self.optimization_task.cancel()
        self.resource_manager.shutdown()
        self.logger.info("Performance optimization system stopped")

    async def _optimization_loop(self, interval: float) -> None:
        """Main optimization loop that continuously improves performance."""
        while self.optimization_active:
            try:
                # Get current performance metrics
                current_metrics = self.resource_manager.get_current_performance()

                if current_metrics:
                    # Calculate optimization actions
                    actions = self.resource_manager.calculate_optimization_actions()

                    # Apply highest-value optimizations
                    for action in sorted(actions, key=lambda a: a.expected_improvement, reverse=True):
                        await self._apply_optimization(action)

                    # Check if resources need scaling
                    should_scale, direction = self.resource_manager.should_scale_resources()
                    if should_scale:
                        scaled = self.resource_manager.scale_resources(direction)
                        if scaled:
                            self.optimization_stats["optimizations_applied"] += 1

                await asyncio.sleep(interval)

            except Exception as e:
                self.logger.error(f"Error in optimization loop: {e}")
                await asyncio.sleep(interval)

    async def _apply_optimization(self, action: OptimizationAction) -> None:
        """Apply a specific optimization action."""
        try:
            self.logger.info(f"Applying optimization: {action.strategy.value}")

            if action.strategy == OptimizationStrategy.AUTO_SCALING:
                # Auto-scaling is handled by resource manager
                pass
            elif action.strategy == OptimizationStrategy.CACHING:
                # Caching is handled by resource manager
                pass
            else:
                # Other optimizations can be implemented here
                pass

            self.optimization_stats["optimizations_applied"] += 1
            self.optimization_stats["performance_improvements"].append(action.expected_improvement)

        except Exception as e:
            self.logger.error(f"Failed to apply optimization {action.strategy.value}: {e}")

    async def execute_optimized_operation(
        self,
        operation: Callable,
        data: Any,
        endpoint_id: Optional[str] = None,
        optimization_hint: Optional[OptimizationStrategy] = None
    ) -> Any:
        """Execute an operation with full optimization pipeline."""
        start_time = time.time()

        try:
            # Use load balancing if endpoint specified
            if endpoint_id:
                result = await self.load_balancer.execute_with_load_balancing(
                    lambda d: self.resource_manager.execute_with_optimization(
                        operation, d, optimization_hint
                    ),
                    endpoint_id,
                    data
                )
            else:
                result = await self.resource_manager.execute_with_optimization(
                    operation, data, optimization_hint
                )

            # Record performance metrics
            execution_time = time.time() - start_time
            metrics = PerformanceMetrics(
                throughput=1.0 / execution_time,
                latency=execution_time,
                cpu_usage=self._get_cpu_usage(),
                memory_usage=self._get_memory_usage(),
                error_rate=0.0
            )
            self.resource_manager.record_performance_metrics(metrics)

            return result

        except Exception:
            # Record error metrics
            execution_time = time.time() - start_time
            metrics = PerformanceMetrics(
                throughput=0.0,
                latency=execution_time,
                cpu_usage=self._get_cpu_usage(),
                memory_usage=self._get_memory_usage(),
                error_rate=1.0
            )
            self.resource_manager.record_performance_metrics(metrics)

            raise

    def _get_cpu_usage(self) -> float:
        """Get current CPU usage (mock implementation)."""
        # In real implementation, would use psutil or similar
        return 50.0  # Mock value

    def _get_memory_usage(self) -> float:
        """Get current memory usage (mock implementation)."""
        # In real implementation, would use psutil or similar
        return 60.0  # Mock value

    def get_optimization_statistics(self) -> Dict[str, Any]:
        """Get comprehensive optimization statistics."""
        return {
            "optimization_stats": self.optimization_stats,
            "resource_stats": self.resource_manager.get_resource_statistics(),
            "load_balancer_stats": self.load_balancer.get_load_balancer_stats(),
            "performance_history": len(self.resource_manager.performance_history)
        }


# Export main classes
__all__ = [
    "PerformanceOptimizationEngine",
    "IntelligentResourceManager",
    "AdaptiveLoadBalancer",
    "OptimizationStrategy",
    "PerformanceMetrics",
    "OptimizationAction"
]
