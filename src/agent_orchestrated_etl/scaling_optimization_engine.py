"""
Scaling and Optimization Engine for Autonomous SDLC

This module implements advanced scaling, performance optimization, caching strategies,
and resource management for the autonomous SDLC system, enabling it to handle 
enterprise-scale workloads with optimal performance.
"""

from __future__ import annotations

import asyncio
import statistics
import threading
import time
from collections import defaultdict, deque
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

from .logging_config import get_logger


class ScalingStrategy(Enum):
    """Scaling strategies available."""
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"
    AUTO_SCALING = "auto_scaling"
    PREDICTIVE = "predictive"
    QUANTUM_INSPIRED = "quantum_inspired"


class CacheStrategy(Enum):
    """Cache strategies for optimization."""
    LRU = "lru"
    LFU = "lfu"
    ADAPTIVE = "adaptive"
    WRITE_THROUGH = "write_through"
    WRITE_BACK = "write_back"
    MULTILEVEL = "multilevel"


class OptimizationLevel(Enum):
    """Optimization levels."""
    BASIC = "basic"
    STANDARD = "standard"
    AGGRESSIVE = "aggressive"
    MAXIMUM = "maximum"
    QUANTUM_ENHANCED = "quantum_enhanced"


@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics."""

    # Throughput metrics
    requests_per_second: float = 0.0
    tasks_completed_per_minute: float = 0.0
    data_processed_per_second: float = 0.0  # MB/s

    # Latency metrics
    avg_response_time: float = 0.0
    p95_response_time: float = 0.0
    p99_response_time: float = 0.0

    # Resource utilization
    cpu_utilization: float = 0.0
    memory_utilization: float = 0.0
    disk_io_utilization: float = 0.0
    network_utilization: float = 0.0

    # Concurrency metrics
    active_connections: int = 0
    queued_requests: int = 0
    concurrent_tasks: int = 0

    # Cache metrics
    cache_hit_rate: float = 0.0
    cache_miss_rate: float = 0.0
    cache_size_mb: float = 0.0

    # Error metrics
    error_rate: float = 0.0
    timeout_rate: float = 0.0
    retry_rate: float = 0.0

    # Business metrics
    cost_per_request: float = 0.0
    energy_efficiency: float = 0.0

    def calculate_performance_score(self) -> float:
        """Calculate overall performance score."""
        # Weighted performance score (0.0 to 1.0)
        throughput_score = min(1.0, self.requests_per_second / 1000.0)  # Normalize to 1000 RPS
        latency_score = max(0.0, 1.0 - self.avg_response_time / 1000.0)  # Penalize high latency
        resource_score = 1.0 - max(self.cpu_utilization, self.memory_utilization)
        cache_score = self.cache_hit_rate
        reliability_score = 1.0 - self.error_rate

        return (
            throughput_score * 0.25 +
            latency_score * 0.25 +
            resource_score * 0.20 +
            cache_score * 0.15 +
            reliability_score * 0.15
        )


@dataclass
class ScalingConfiguration:
    """Configuration for scaling operations."""

    strategy: ScalingStrategy = ScalingStrategy.AUTO_SCALING
    min_instances: int = 1
    max_instances: int = 10
    target_cpu_utilization: float = 0.70
    target_memory_utilization: float = 0.75
    scale_up_threshold: float = 0.80
    scale_down_threshold: float = 0.30
    scale_up_cooldown: float = 300.0  # seconds
    scale_down_cooldown: float = 600.0  # seconds

    # Predictive scaling
    prediction_window: float = 3600.0  # 1 hour
    seasonality_detection: bool = True

    # Resource limits
    max_cpu_cores: int = 16
    max_memory_gb: int = 64
    max_disk_gb: int = 1000

    # Cost optimization
    cost_optimization_enabled: bool = True
    spot_instance_preference: bool = False


class IntelligentCache:
    """Multi-level intelligent caching system."""

    def __init__(
        self,
        strategy: CacheStrategy = CacheStrategy.ADAPTIVE,
        max_size_mb: int = 1024,
        ttl_seconds: int = 3600
    ):
        self.strategy = strategy
        self.max_size_bytes = max_size_mb * 1024 * 1024
        self.ttl_seconds = ttl_seconds

        # Cache storage layers
        self.l1_cache: Dict[str, Any] = {}  # In-memory
        self.l2_cache: Dict[str, Any] = {}  # Compressed
        self.cache_metadata: Dict[str, Dict[str, Any]] = {}

        # Access patterns
        self.access_frequency: Dict[str, int] = defaultdict(int)
        self.access_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))

        # Performance metrics
        self.hits = 0
        self.misses = 0
        self.evictions = 0

        # Thread safety
        self.lock = threading.RLock()

        self.logger = get_logger("scaling.cache")

    async def get(self, key: str) -> Optional[Any]:
        """Get item from cache with intelligent retrieval."""
        with self.lock:
            current_time = time.time()

            # Check L1 cache first
            if key in self.l1_cache:
                metadata = self.cache_metadata.get(key, {})
                if current_time - metadata.get('timestamp', 0) < self.ttl_seconds:
                    self._record_access(key)
                    self.hits += 1
                    return self.l1_cache[key]
                else:
                    # Expired, remove from cache
                    self._remove_key(key)

            # Check L2 cache
            if key in self.l2_cache:
                metadata = self.cache_metadata.get(key, {})
                if current_time - metadata.get('timestamp', 0) < self.ttl_seconds:
                    # Promote to L1 if frequently accessed
                    if self.access_frequency[key] > 5:
                        self._promote_to_l1(key)

                    self._record_access(key)
                    self.hits += 1
                    return self.l2_cache[key]
                else:
                    self._remove_key(key)

            # Cache miss
            self.misses += 1
            return None

    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        """Set item in cache with intelligent placement."""
        with self.lock:
            current_time = time.time()
            effective_ttl = ttl or self.ttl_seconds

            # Calculate value size (approximate)
            value_size = self._estimate_size(value)

            # Ensure cache capacity
            await self._ensure_capacity(value_size)

            # Determine cache level based on access patterns
            if self._should_use_l1(key, value_size):
                self.l1_cache[key] = value
                cache_level = "L1"
            else:
                self.l2_cache[key] = value
                cache_level = "L2"

            # Update metadata
            self.cache_metadata[key] = {
                'timestamp': current_time,
                'ttl': effective_ttl,
                'size': value_size,
                'level': cache_level,
                'access_count': self.access_frequency[key]
            }

            self.logger.debug(f"Cached {key} in {cache_level} (size: {value_size} bytes)")

    async def invalidate(self, pattern: str) -> int:
        """Invalidate cache entries matching pattern."""
        with self.lock:
            invalidated = 0
            keys_to_remove = []

            for key in list(self.l1_cache.keys()) + list(self.l2_cache.keys()):
                if pattern in key or key.startswith(pattern):
                    keys_to_remove.append(key)

            for key in keys_to_remove:
                self._remove_key(key)
                invalidated += 1

            return invalidated

    def _should_use_l1(self, key: str, size: int) -> bool:
        """Determine if key should be placed in L1 cache."""
        # Frequently accessed items go to L1
        if self.access_frequency[key] > 3:
            return True

        # Small items prefer L1
        if size < 1024:  # < 1KB
            return True

        # Recent access patterns
        if key in self.access_times:
            recent_accesses = len([t for t in self.access_times[key]
                                 if time.time() - t < 300])  # Last 5 minutes
            if recent_accesses > 2:
                return True

        return False

    def _promote_to_l1(self, key: str) -> None:
        """Promote item from L2 to L1 cache."""
        if key in self.l2_cache:
            value = self.l2_cache.pop(key)
            self.l1_cache[key] = value
            if key in self.cache_metadata:
                self.cache_metadata[key]['level'] = 'L1'

    def _record_access(self, key: str) -> None:
        """Record access for adaptive caching."""
        self.access_frequency[key] += 1
        self.access_times[key].append(time.time())

    def _remove_key(self, key: str) -> None:
        """Remove key from all cache levels."""
        self.l1_cache.pop(key, None)
        self.l2_cache.pop(key, None)
        self.cache_metadata.pop(key, None)

    async def _ensure_capacity(self, new_item_size: int) -> None:
        """Ensure cache has capacity for new item."""
        current_size = sum(
            meta.get('size', 0) for meta in self.cache_metadata.values()
        )

        if current_size + new_item_size > self.max_size_bytes:
            await self._evict_items(current_size + new_item_size - self.max_size_bytes)

    async def _evict_items(self, bytes_to_free: int) -> None:
        """Evict items based on caching strategy."""
        freed_bytes = 0

        if self.strategy == CacheStrategy.LRU:
            # Evict least recently used
            items = sorted(
                self.cache_metadata.items(),
                key=lambda x: x[1].get('timestamp', 0)
            )
        elif self.strategy == CacheStrategy.LFU:
            # Evict least frequently used
            items = sorted(
                self.cache_metadata.items(),
                key=lambda x: self.access_frequency[x[0]]
            )
        else:  # ADAPTIVE
            # Combine recency and frequency
            current_time = time.time()
            items = sorted(
                self.cache_metadata.items(),
                key=lambda x: (
                    self.access_frequency[x[0]] /
                    max(1, current_time - x[1].get('timestamp', 0))
                )
            )

        for key, metadata in items:
            if freed_bytes >= bytes_to_free:
                break

            freed_bytes += metadata.get('size', 0)
            self._remove_key(key)
            self.evictions += 1

    def _estimate_size(self, value: Any) -> int:
        """Estimate memory size of value."""
        try:
            if isinstance(value, (str, bytes)):
                return len(value)
            elif isinstance(value, (list, tuple)):
                return sum(self._estimate_size(item) for item in value)
            elif isinstance(value, dict):
                return sum(
                    self._estimate_size(k) + self._estimate_size(v)
                    for k, v in value.items()
                )
            else:
                # Rough estimate for other objects
                return len(str(value)) * 2
        except:
            return 1024  # Default estimate

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        total_requests = self.hits + self.misses
        hit_rate = self.hits / max(1, total_requests)

        current_size = sum(
            meta.get('size', 0) for meta in self.cache_metadata.values()
        )

        return {
            "strategy": self.strategy.value,
            "hit_rate": hit_rate,
            "miss_rate": 1.0 - hit_rate,
            "total_requests": total_requests,
            "hits": self.hits,
            "misses": self.misses,
            "evictions": self.evictions,
            "current_size_mb": current_size / (1024 * 1024),
            "max_size_mb": self.max_size_bytes / (1024 * 1024),
            "utilization": current_size / self.max_size_bytes,
            "l1_items": len(self.l1_cache),
            "l2_items": len(self.l2_cache),
            "total_items": len(self.cache_metadata)
        }


class AdaptiveLoadBalancer:
    """Intelligent load balancer with adaptive algorithms."""

    def __init__(self):
        self.instances: List[Dict[str, Any]] = []
        self.performance_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.current_loads: Dict[str, float] = defaultdict(float)

        # Load balancing algorithms
        self.algorithms = {
            "round_robin": self._round_robin,
            "least_connections": self._least_connections,
            "weighted_round_robin": self._weighted_round_robin,
            "adaptive": self._adaptive_selection,
            "performance_based": self._performance_based
        }

        self.current_algorithm = "adaptive"
        self.round_robin_index = 0

        self.logger = get_logger("scaling.load_balancer")

    def add_instance(self, instance_id: str, capacity: float = 1.0, metadata: Optional[Dict] = None) -> None:
        """Add instance to load balancer."""
        instance = {
            "id": instance_id,
            "capacity": capacity,
            "current_load": 0.0,
            "active_requests": 0,
            "total_requests": 0,
            "avg_response_time": 0.0,
            "error_rate": 0.0,
            "health_score": 1.0,
            "metadata": metadata or {},
            "last_used": 0.0
        }

        self.instances.append(instance)
        self.logger.info(f"Added instance {instance_id} with capacity {capacity}")

    def remove_instance(self, instance_id: str) -> bool:
        """Remove instance from load balancer."""
        for i, instance in enumerate(self.instances):
            if instance["id"] == instance_id:
                self.instances.pop(i)
                self.performance_history.pop(instance_id, None)
                self.current_loads.pop(instance_id, None)
                self.logger.info(f"Removed instance {instance_id}")
                return True
        return False

    async def select_instance(self, request_metadata: Optional[Dict] = None) -> Optional[str]:
        """Select best instance using current algorithm."""
        if not self.instances:
            return None

        # Filter healthy instances
        healthy_instances = [
            inst for inst in self.instances
            if inst["health_score"] > 0.5 and inst["current_load"] < 0.95
        ]

        if not healthy_instances:
            # Fallback to least loaded instance
            healthy_instances = [min(self.instances, key=lambda x: x["current_load"])]

        # Select using current algorithm
        algorithm_func = self.algorithms.get(self.current_algorithm, self._adaptive_selection)
        selected = algorithm_func(healthy_instances, request_metadata)

        if selected:
            # Update instance load
            for instance in self.instances:
                if instance["id"] == selected:
                    instance["active_requests"] += 1
                    instance["total_requests"] += 1
                    instance["last_used"] = time.time()
                    break

        return selected

    def _round_robin(self, instances: List[Dict], metadata: Optional[Dict] = None) -> str:
        """Round-robin selection."""
        selected = instances[self.round_robin_index % len(instances)]
        self.round_robin_index += 1
        return selected["id"]

    def _least_connections(self, instances: List[Dict], metadata: Optional[Dict] = None) -> str:
        """Select instance with least active connections."""
        selected = min(instances, key=lambda x: x["active_requests"])
        return selected["id"]

    def _weighted_round_robin(self, instances: List[Dict], metadata: Optional[Dict] = None) -> str:
        """Weighted round-robin based on capacity."""
        # Calculate weights based on capacity and current load
        weights = []
        for instance in instances:
            available_capacity = instance["capacity"] - instance["current_load"]
            weight = max(0.1, available_capacity * instance["health_score"])
            weights.append(weight)

        # Weighted random selection
        total_weight = sum(weights)
        if total_weight == 0:
            return instances[0]["id"]

        import random
        selection_point = random.uniform(0, total_weight)
        current_weight = 0

        for i, weight in enumerate(weights):
            current_weight += weight
            if current_weight >= selection_point:
                return instances[i]["id"]

        return instances[-1]["id"]

    def _adaptive_selection(self, instances: List[Dict], metadata: Optional[Dict] = None) -> str:
        """Adaptive selection based on multiple factors."""
        scores = []

        for instance in instances:
            # Calculate composite score
            load_score = 1.0 - instance["current_load"]
            performance_score = 1.0 / max(0.001, instance["avg_response_time"])
            reliability_score = 1.0 - instance["error_rate"]
            health_score = instance["health_score"]

            # Weighted composite score
            composite_score = (
                load_score * 0.3 +
                performance_score * 0.25 +
                reliability_score * 0.25 +
                health_score * 0.2
            )

            scores.append(composite_score)

        # Select instance with highest score
        best_index = scores.index(max(scores))
        return instances[best_index]["id"]

    def _performance_based(self, instances: List[Dict], metadata: Optional[Dict] = None) -> str:
        """Select based on recent performance history."""
        best_instance = None
        best_score = -1

        for instance in instances:
            instance_id = instance["id"]
            recent_performance = self.performance_history[instance_id]

            if recent_performance:
                avg_response_time = statistics.mean(recent_performance)
                # Lower response time = better score
                score = 1.0 / max(0.001, avg_response_time)
            else:
                score = 1.0  # Default score for new instances

            # Adjust for current load
            score *= (1.0 - instance["current_load"])

            if score > best_score:
                best_score = score
                best_instance = instance

        return best_instance["id"] if best_instance else instances[0]["id"]

    async def record_response(self, instance_id: str, response_time: float, success: bool) -> None:
        """Record response metrics for an instance."""
        for instance in self.instances:
            if instance["id"] == instance_id:
                instance["active_requests"] = max(0, instance["active_requests"] - 1)

                # Update performance metrics
                current_avg = instance["avg_response_time"]
                total_requests = instance["total_requests"]

                # Rolling average
                instance["avg_response_time"] = (
                    (current_avg * (total_requests - 1) + response_time) / total_requests
                )

                # Update error rate
                if not success:
                    current_errors = instance["error_rate"] * (total_requests - 1)
                    instance["error_rate"] = (current_errors + 1) / total_requests
                else:
                    current_errors = instance["error_rate"] * (total_requests - 1)
                    instance["error_rate"] = current_errors / total_requests

                # Update health score
                instance["health_score"] = max(0.1, 1.0 - instance["error_rate"])

                # Record in performance history
                self.performance_history[instance_id].append(response_time)

                # Update current load
                self.current_loads[instance_id] = (
                    instance["active_requests"] / max(1, instance["capacity"])
                )

                break

    def get_load_balancer_stats(self) -> Dict[str, Any]:
        """Get load balancer statistics."""
        total_requests = sum(inst["total_requests"] for inst in self.instances)

        instance_stats = []
        for instance in self.instances:
            instance_stats.append({
                "id": instance["id"],
                "capacity": instance["capacity"],
                "current_load": instance["current_load"],
                "active_requests": instance["active_requests"],
                "total_requests": instance["total_requests"],
                "avg_response_time": instance["avg_response_time"],
                "error_rate": instance["error_rate"],
                "health_score": instance["health_score"],
                "request_percentage": instance["total_requests"] / max(1, total_requests)
            })

        return {
            "algorithm": self.current_algorithm,
            "total_instances": len(self.instances),
            "healthy_instances": len([i for i in self.instances if i["health_score"] > 0.5]),
            "total_requests": total_requests,
            "instance_stats": instance_stats
        }


class AutoScalingEngine:
    """Intelligent auto-scaling engine with predictive capabilities."""

    def __init__(self, config: ScalingConfiguration):
        self.config = config
        self.current_instances = config.min_instances

        # Performance monitoring
        self.metrics_history: deque = deque(maxlen=1000)
        self.load_predictor = LoadPredictor()

        # Scaling state
        self.last_scale_up = 0.0
        self.last_scale_down = 0.0
        self.scaling_in_progress = False

        # Event callbacks
        self.scale_up_callbacks: List[Callable] = []
        self.scale_down_callbacks: List[Callable] = []

        self.logger = get_logger("scaling.auto_scaler")

    async def evaluate_scaling(self, current_metrics: PerformanceMetrics) -> Dict[str, Any]:
        """Evaluate if scaling is needed."""

        self.metrics_history.append({
            "timestamp": time.time(),
            "metrics": current_metrics,
            "instances": self.current_instances
        })

        scaling_decision = {
            "action": "none",
            "current_instances": self.current_instances,
            "target_instances": self.current_instances,
            "reason": "no_scaling_needed",
            "confidence": 0.0
        }

        if self.scaling_in_progress:
            scaling_decision["reason"] = "scaling_in_progress"
            return scaling_decision

        current_time = time.time()

        # Check scale-up conditions
        should_scale_up, scale_up_confidence = await self._should_scale_up(current_metrics, current_time)
        if should_scale_up:
            new_instances = min(
                self.current_instances + self._calculate_scale_up_amount(current_metrics),
                self.config.max_instances
            )

            scaling_decision.update({
                "action": "scale_up",
                "target_instances": new_instances,
                "reason": "high_load_detected",
                "confidence": scale_up_confidence
            })

            return scaling_decision

        # Check scale-down conditions
        should_scale_down, scale_down_confidence = await self._should_scale_down(current_metrics, current_time)
        if should_scale_down:
            new_instances = max(
                self.current_instances - self._calculate_scale_down_amount(current_metrics),
                self.config.min_instances
            )

            scaling_decision.update({
                "action": "scale_down",
                "target_instances": new_instances,
                "reason": "low_load_detected",
                "confidence": scale_down_confidence
            })

        return scaling_decision

    async def _should_scale_up(self, metrics: PerformanceMetrics, current_time: float) -> Tuple[bool, float]:
        """Determine if scale-up is needed."""

        # Check cooldown period
        if current_time - self.last_scale_up < self.config.scale_up_cooldown:
            return False, 0.0

        confidence = 0.0
        reasons = []

        # CPU utilization check
        if metrics.cpu_utilization > self.config.scale_up_threshold:
            confidence += 0.3
            reasons.append("high_cpu")

        # Memory utilization check
        if metrics.memory_utilization > self.config.scale_up_threshold:
            confidence += 0.3
            reasons.append("high_memory")

        # Response time check
        if metrics.avg_response_time > 1000.0:  # 1 second threshold
            confidence += 0.2
            reasons.append("high_latency")

        # Queue length check
        if metrics.queued_requests > self.current_instances * 10:
            confidence += 0.2
            reasons.append("long_queue")

        # Predictive scaling
        if self.config.strategy == ScalingStrategy.PREDICTIVE:
            predicted_load = await self.load_predictor.predict_load(self.metrics_history)
            if predicted_load > self.config.scale_up_threshold:
                confidence += 0.1
                reasons.append("predicted_load_increase")

        should_scale = confidence > 0.5

        if should_scale:
            self.logger.info(f"Scale-up decision: confidence={confidence:.2f}, reasons={reasons}")

        return should_scale, confidence

    async def _should_scale_down(self, metrics: PerformanceMetrics, current_time: float) -> Tuple[bool, float]:
        """Determine if scale-down is needed."""

        # Check cooldown period
        if current_time - self.last_scale_down < self.config.scale_down_cooldown:
            return False, 0.0

        # Don't scale below minimum
        if self.current_instances <= self.config.min_instances:
            return False, 0.0

        confidence = 0.0
        reasons = []

        # CPU utilization check
        if metrics.cpu_utilization < self.config.scale_down_threshold:
            confidence += 0.3
            reasons.append("low_cpu")

        # Memory utilization check
        if metrics.memory_utilization < self.config.scale_down_threshold:
            confidence += 0.3
            reasons.append("low_memory")

        # Response time check (consistently low)
        if len(self.metrics_history) >= 5:
            recent_response_times = [
                entry["metrics"].avg_response_time
                for entry in list(self.metrics_history)[-5:]
            ]
            if all(rt < 200.0 for rt in recent_response_times):  # All under 200ms
                confidence += 0.2
                reasons.append("consistently_low_latency")

        # Queue length check
        if metrics.queued_requests < self.current_instances * 2:
            confidence += 0.1
            reasons.append("short_queue")

        # Cost optimization
        if self.config.cost_optimization_enabled:
            # Scale down more aggressively during off-peak hours
            current_hour = time.localtime().tm_hour
            if current_hour < 6 or current_hour > 22:  # Off-peak hours
                confidence += 0.1
                reasons.append("off_peak_optimization")

        should_scale = confidence > 0.6  # Higher threshold for scale-down

        if should_scale:
            self.logger.info(f"Scale-down decision: confidence={confidence:.2f}, reasons={reasons}")

        return should_scale, confidence

    def _calculate_scale_up_amount(self, metrics: PerformanceMetrics) -> int:
        """Calculate number of instances to add."""
        if metrics.cpu_utilization > 0.9 or metrics.memory_utilization > 0.9:
            return max(2, int(self.current_instances * 0.5))  # Emergency scaling
        elif metrics.avg_response_time > 2000:  # 2 seconds
            return max(1, int(self.current_instances * 0.3))
        else:
            return 1  # Conservative scaling

    def _calculate_scale_down_amount(self, metrics: PerformanceMetrics) -> int:
        """Calculate number of instances to remove."""
        # Conservative scale-down
        if self.current_instances > self.config.min_instances * 2:
            return max(1, int(self.current_instances * 0.2))
        else:
            return 1

    async def execute_scaling(self, scaling_decision: Dict[str, Any]) -> bool:
        """Execute scaling decision."""

        if scaling_decision["action"] == "none":
            return True

        self.scaling_in_progress = True

        try:
            current_instances = self.current_instances
            target_instances = scaling_decision["target_instances"]

            if scaling_decision["action"] == "scale_up":
                # Execute scale-up callbacks
                for callback in self.scale_up_callbacks:
                    await callback(target_instances - current_instances)

                self.current_instances = target_instances
                self.last_scale_up = time.time()

                self.logger.info(
                    f"Scaled up from {current_instances} to {target_instances} instances"
                )

            elif scaling_decision["action"] == "scale_down":
                # Execute scale-down callbacks
                for callback in self.scale_down_callbacks:
                    await callback(current_instances - target_instances)

                self.current_instances = target_instances
                self.last_scale_down = time.time()

                self.logger.info(
                    f"Scaled down from {current_instances} to {target_instances} instances"
                )

            return True

        except Exception as e:
            self.logger.error(f"Scaling execution failed: {e}")
            return False

        finally:
            self.scaling_in_progress = False

    def add_scale_up_callback(self, callback: Callable) -> None:
        """Add callback for scale-up events."""
        self.scale_up_callbacks.append(callback)

    def add_scale_down_callback(self, callback: Callable) -> None:
        """Add callback for scale-down events."""
        self.scale_down_callbacks.append(callback)


class LoadPredictor:
    """ML-based load prediction for predictive scaling."""

    def __init__(self):
        self.logger = get_logger("scaling.load_predictor")

        # Simple moving average predictor (can be enhanced with ML models)
        self.prediction_window = 10
        self.seasonal_patterns = {}

    async def predict_load(self, metrics_history: deque) -> float:
        """Predict future load based on historical data."""

        if len(metrics_history) < self.prediction_window:
            return 0.5  # Default prediction

        recent_metrics = list(metrics_history)[-self.prediction_window:]

        # Extract load indicators
        cpu_loads = [m["metrics"].cpu_utilization for m in recent_metrics]
        memory_loads = [m["metrics"].memory_utilization for m in recent_metrics]
        response_times = [m["metrics"].avg_response_time for m in recent_metrics]

        # Simple trend analysis
        cpu_trend = self._calculate_trend(cpu_loads)
        memory_trend = self._calculate_trend(memory_loads)
        latency_trend = self._calculate_trend(response_times)

        # Combine trends for load prediction
        predicted_cpu = cpu_loads[-1] + (cpu_trend * 5)  # 5-minute prediction
        predicted_memory = memory_loads[-1] + (memory_trend * 5)

        # Return the higher of the two as predicted load
        predicted_load = max(predicted_cpu, predicted_memory)

        # Apply seasonal adjustments
        predicted_load = self._apply_seasonal_adjustment(predicted_load)

        return min(1.0, max(0.0, predicted_load))

    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate trend slope using simple linear regression."""
        if len(values) < 2:
            return 0.0

        n = len(values)
        x_values = list(range(n))

        # Calculate slope
        x_mean = sum(x_values) / n
        y_mean = sum(values) / n

        numerator = sum((x_values[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x_values[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0

        slope = numerator / denominator
        return slope

    def _apply_seasonal_adjustment(self, base_prediction: float) -> float:
        """Apply seasonal adjustments to prediction."""
        current_hour = time.localtime().tm_hour
        day_of_week = time.localtime().tm_wday

        # Peak hours adjustment
        if 9 <= current_hour <= 17:  # Business hours
            base_prediction *= 1.2
        elif current_hour < 6 or current_hour > 22:  # Off-peak
            base_prediction *= 0.8

        # Weekend adjustment
        if day_of_week >= 5:  # Weekend
            base_prediction *= 0.7

        return base_prediction


class ScalingOptimizationEngine:
    """Main engine orchestrating all scaling and optimization components."""

    def __init__(self, config: ScalingConfiguration):
        self.config = config
        self.logger = get_logger("scaling.optimization_engine")

        # Core components
        self.cache = IntelligentCache(CacheStrategy.ADAPTIVE, max_size_mb=512)
        self.load_balancer = AdaptiveLoadBalancer()
        self.auto_scaler = AutoScalingEngine(config)

        # Performance monitoring
        self.current_metrics = PerformanceMetrics()
        self.metrics_collectors: List[Callable] = []

        # Optimization state
        self.optimization_level = OptimizationLevel.STANDARD
        self.is_running = False
        self.monitoring_task: Optional[asyncio.Task] = None

        # Thread pools for CPU/IO intensive operations
        self.cpu_executor = ThreadPoolExecutor(max_workers=4)
        self.io_executor = ThreadPoolExecutor(max_workers=8)
        self.process_executor = ProcessPoolExecutor(max_workers=2)

    async def start_optimization(self) -> None:
        """Start the optimization engine."""
        if self.is_running:
            return

        self.is_running = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())

        self.logger.info("Scaling optimization engine started")

    async def stop_optimization(self) -> None:
        """Stop the optimization engine."""
        self.is_running = False

        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass

        # Shutdown executors
        self.cpu_executor.shutdown(wait=True)
        self.io_executor.shutdown(wait=True)
        self.process_executor.shutdown(wait=True)

        self.logger.info("Scaling optimization engine stopped")

    async def _monitoring_loop(self) -> None:
        """Main monitoring and optimization loop."""
        while self.is_running:
            try:
                # Collect current metrics
                await self._collect_metrics()

                # Evaluate auto-scaling
                scaling_decision = await self.auto_scaler.evaluate_scaling(self.current_metrics)

                # Execute scaling if needed
                if scaling_decision["action"] != "none":
                    await self.auto_scaler.execute_scaling(scaling_decision)

                # Optimize cache
                await self._optimize_cache()

                # Update load balancer
                await self._update_load_balancer()

                # Sleep before next iteration
                await asyncio.sleep(30)  # 30-second monitoring interval

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(10)  # Shorter sleep on error

    async def _collect_metrics(self) -> None:
        """Collect performance metrics from all sources."""

        # Collect from registered collectors
        collected_metrics = []
        for collector in self.metrics_collectors:
            try:
                metrics = await collector()
                collected_metrics.append(metrics)
            except Exception as e:
                self.logger.warning(f"Metrics collector failed: {e}")

        # Aggregate metrics
        if collected_metrics:
            self.current_metrics = self._aggregate_metrics(collected_metrics)

        # Add cache metrics
        cache_stats = self.cache.get_cache_stats()
        self.current_metrics.cache_hit_rate = cache_stats["hit_rate"]
        self.current_metrics.cache_miss_rate = cache_stats["miss_rate"]
        self.current_metrics.cache_size_mb = cache_stats["current_size_mb"]

        # Add load balancer metrics
        lb_stats = self.load_balancer.get_load_balancer_stats()
        self.current_metrics.active_connections = sum(
            inst["active_requests"] for inst in lb_stats["instance_stats"]
        )

    def _aggregate_metrics(self, metrics_list: List[PerformanceMetrics]) -> PerformanceMetrics:
        """Aggregate multiple performance metrics."""
        if not metrics_list:
            return PerformanceMetrics()

        # Calculate averages and sums as appropriate
        aggregated = PerformanceMetrics()

        aggregated.requests_per_second = sum(m.requests_per_second for m in metrics_list)
        aggregated.tasks_completed_per_minute = sum(m.tasks_completed_per_minute for m in metrics_list)
        aggregated.data_processed_per_second = sum(m.data_processed_per_second for m in metrics_list)

        # Average latency metrics
        aggregated.avg_response_time = statistics.mean(m.avg_response_time for m in metrics_list)
        aggregated.p95_response_time = statistics.mean(m.p95_response_time for m in metrics_list)
        aggregated.p99_response_time = statistics.mean(m.p99_response_time for m in metrics_list)

        # Average utilization
        aggregated.cpu_utilization = statistics.mean(m.cpu_utilization for m in metrics_list)
        aggregated.memory_utilization = statistics.mean(m.memory_utilization for m in metrics_list)
        aggregated.disk_io_utilization = statistics.mean(m.disk_io_utilization for m in metrics_list)
        aggregated.network_utilization = statistics.mean(m.network_utilization for m in metrics_list)

        # Sum concurrency metrics
        aggregated.active_connections = sum(m.active_connections for m in metrics_list)
        aggregated.queued_requests = sum(m.queued_requests for m in metrics_list)
        aggregated.concurrent_tasks = sum(m.concurrent_tasks for m in metrics_list)

        # Average error rates
        aggregated.error_rate = statistics.mean(m.error_rate for m in metrics_list)
        aggregated.timeout_rate = statistics.mean(m.timeout_rate for m in metrics_list)
        aggregated.retry_rate = statistics.mean(m.retry_rate for m in metrics_list)

        return aggregated

    async def _optimize_cache(self) -> None:
        """Optimize cache performance."""

        cache_stats = self.cache.get_cache_stats()

        # If hit rate is too low, consider cache warming
        if cache_stats["hit_rate"] < 0.6:
            await self._warm_cache()

        # If cache is nearly full, trigger eviction
        if cache_stats["utilization"] > 0.9:
            await self.cache.invalidate("temp_")  # Remove temporary entries

    async def _warm_cache(self) -> None:
        """Warm cache with frequently accessed data."""
        # This would implement cache warming logic in a real system
        self.logger.info("Cache warming triggered")

    async def _update_load_balancer(self) -> None:
        """Update load balancer configuration."""

        # Dynamically adjust algorithm based on current conditions
        current_algorithm = self.load_balancer.current_algorithm

        if self.current_metrics.cpu_utilization > 0.8:
            # High CPU - use performance-based balancing
            self.load_balancer.current_algorithm = "performance_based"
        elif self.current_metrics.error_rate > 0.05:
            # High error rate - use adaptive balancing
            self.load_balancer.current_algorithm = "adaptive"
        else:
            # Normal conditions - use weighted round robin
            self.load_balancer.current_algorithm = "weighted_round_robin"

        if self.load_balancer.current_algorithm != current_algorithm:
            self.logger.info(f"Load balancer algorithm changed to {self.load_balancer.current_algorithm}")

    def add_metrics_collector(self, collector: Callable) -> None:
        """Add a metrics collector function."""
        self.metrics_collectors.append(collector)

    async def get_optimization_status(self) -> Dict[str, Any]:
        """Get comprehensive optimization status."""

        return {
            "engine_status": "running" if self.is_running else "stopped",
            "optimization_level": self.optimization_level.value,
            "current_instances": self.auto_scaler.current_instances,
            "scaling_strategy": self.config.strategy.value,
            "performance_metrics": {
                "requests_per_second": self.current_metrics.requests_per_second,
                "avg_response_time": self.current_metrics.avg_response_time,
                "cpu_utilization": self.current_metrics.cpu_utilization,
                "memory_utilization": self.current_metrics.memory_utilization,
                "error_rate": self.current_metrics.error_rate,
                "performance_score": self.current_metrics.calculate_performance_score()
            },
            "cache_stats": self.cache.get_cache_stats(),
            "load_balancer_stats": self.load_balancer.get_load_balancer_stats(),
            "auto_scaler_stats": {
                "min_instances": self.config.min_instances,
                "max_instances": self.config.max_instances,
                "current_instances": self.auto_scaler.current_instances,
                "scaling_in_progress": self.auto_scaler.scaling_in_progress
            }
        }


# Global optimization engine instance
_optimization_engine: Optional[ScalingOptimizationEngine] = None


def get_optimization_engine(config: Optional[ScalingConfiguration] = None) -> ScalingOptimizationEngine:
    """Get the global optimization engine instance."""
    global _optimization_engine
    if _optimization_engine is None:
        default_config = ScalingConfiguration()
        _optimization_engine = ScalingOptimizationEngine(config or default_config)
    return _optimization_engine


async def optimize_with_scaling(
    func: Callable,
    cache_key: Optional[str] = None,
    use_load_balancer: bool = True,
    *args,
    **kwargs
) -> Any:
    """Execute function with scaling optimization."""

    engine = get_optimization_engine()

    # Try cache first if key provided
    if cache_key:
        cached_result = await engine.cache.get(cache_key)
        if cached_result is not None:
            return cached_result

    # Select instance via load balancer
    if use_load_balancer:
        instance_id = await engine.load_balancer.select_instance()
        if instance_id:
            start_time = time.time()

            try:
                # Execute function
                result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

                # Record successful response
                response_time = time.time() - start_time
                await engine.load_balancer.record_response(instance_id, response_time, True)

                # Cache result if key provided
                if cache_key:
                    await engine.cache.set(cache_key, result)

                return result

            except Exception:
                # Record failed response
                response_time = time.time() - start_time
                await engine.load_balancer.record_response(instance_id, response_time, False)
                raise

    else:
        # Execute without load balancing
        result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

        # Cache result if key provided
        if cache_key:
            await engine.cache.set(cache_key, result)

        return result
