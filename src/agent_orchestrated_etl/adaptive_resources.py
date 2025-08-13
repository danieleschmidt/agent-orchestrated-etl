"""Adaptive Resource Management for Intelligent ETL Orchestration.

This module implements AI-driven resource management with predictive scaling,
dynamic load balancing, and intelligent resource allocation optimization.
"""

from __future__ import annotations

import statistics
import threading
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from .logging_config import get_logger


class ResourceType(Enum):
    """Types of computational resources."""
    CPU = "cpu"
    MEMORY = "memory"
    IO = "io"
    NETWORK = "network"
    GPU = "gpu"
    STORAGE = "storage"


class ScalingDirection(Enum):
    """Resource scaling directions."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


@dataclass
class ResourceMetrics:
    """Resource utilization metrics."""
    timestamp: float
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    io_usage: float = 0.0
    network_usage: float = 0.0
    gpu_usage: float = 0.0
    storage_usage: float = 0.0
    active_tasks: int = 0
    queue_length: int = 0
    throughput: float = 0.0
    latency: float = 0.0


@dataclass
class ResourcePool:
    """Managed resource pool with dynamic scaling."""
    pool_id: str
    resource_type: ResourceType
    current_capacity: float = 1.0
    max_capacity: float = 10.0
    min_capacity: float = 0.1
    utilization: float = 0.0
    efficiency_score: float = 1.0
    scaling_factor: float = 1.2
    active_tasks: List[str] = field(default_factory=list)
    reserved_capacity: float = 0.0
    health_score: float = 1.0


class PredictiveScaler:
    """AI-driven predictive resource scaling."""

    def __init__(self, prediction_window: int = 10):
        self.logger = get_logger("agent_etl.adaptive_resources.scaler")
        self.prediction_window = prediction_window
        self.historical_metrics: deque = deque(maxlen=100)
        self.usage_patterns: Dict[str, List[float]] = defaultdict(list)
        self.learning_rate = 0.1
        self.trend_weights = [0.4, 0.3, 0.2, 0.1]  # Recent to old importance

    def predict_resource_demand(
        self,
        resource_type: ResourceType,
        upcoming_tasks: List[str],
        time_horizon: int = 5
    ) -> float:
        """Predict future resource demand using historical patterns."""
        if not self.historical_metrics:
            return 1.0  # Default demand

        # Analyze historical patterns
        historical_demand = self._analyze_historical_demand(resource_type)

        # Analyze upcoming task patterns
        task_demand = self._analyze_task_demand(resource_type, upcoming_tasks)

        # Trend analysis
        trend_demand = self._analyze_trend(resource_type, time_horizon)

        # Weighted prediction
        predicted_demand = (
            historical_demand * 0.4 +
            task_demand * 0.4 +
            trend_demand * 0.2
        )

        self.logger.debug(
            f"Predicted demand for {resource_type.value}: {predicted_demand:.3f}",
            extra={
                "resource_type": resource_type.value,
                "historical": historical_demand,
                "task_based": task_demand,
                "trend_based": trend_demand
            }
        )

        return max(0.1, min(10.0, predicted_demand))

    def _analyze_historical_demand(self, resource_type: ResourceType) -> float:
        """Analyze historical resource demand patterns."""
        if not self.historical_metrics:
            return 1.0

        # Extract resource usage from recent metrics
        usage_values = []
        for metrics in list(self.historical_metrics)[-self.prediction_window:]:
            usage = getattr(metrics, f"{resource_type.value}_usage", 0.0)
            usage_values.append(usage)

        if not usage_values:
            return 1.0

        # Apply weighted average with higher weight to recent values
        weighted_sum = 0.0
        total_weight = 0.0

        for i, usage in enumerate(reversed(usage_values)):
            weight = self.trend_weights[min(i, len(self.trend_weights) - 1)]
            weighted_sum += usage * weight
            total_weight += weight

        return weighted_sum / total_weight if total_weight > 0 else 1.0

    def _analyze_task_demand(self, resource_type: ResourceType, upcoming_tasks: List[str]) -> float:
        """Analyze expected demand from upcoming tasks."""
        if not upcoming_tasks:
            return 0.5

        # Estimate demand based on task types and patterns
        total_demand = 0.0

        for task_id in upcoming_tasks:
            task_demand = self._estimate_task_resource_demand(task_id, resource_type)
            total_demand += task_demand

        # Normalize by task count and apply concurrency factor
        avg_demand = total_demand / len(upcoming_tasks)
        concurrency_factor = min(2.0, len(upcoming_tasks) / 4.0)  # More tasks = higher concurrency

        return avg_demand * concurrency_factor

    def _estimate_task_resource_demand(self, task_id: str, resource_type: ResourceType) -> float:
        """Estimate resource demand for a specific task."""
        # Task type-based estimation
        base_demand = 0.5

        if task_id.startswith("extract"):
            if resource_type == ResourceType.IO:
                base_demand = 0.8
            elif resource_type == ResourceType.NETWORK:
                base_demand = 0.7
            elif resource_type == ResourceType.CPU:
                base_demand = 0.4
        elif task_id.startswith("transform"):
            if resource_type == ResourceType.CPU:
                base_demand = 0.9
            elif resource_type == ResourceType.MEMORY:
                base_demand = 0.7
            elif resource_type == ResourceType.IO:
                base_demand = 0.3
        elif task_id.startswith("load"):
            if resource_type == ResourceType.IO:
                base_demand = 0.8
            elif resource_type == ResourceType.NETWORK:
                base_demand = 0.6
            elif resource_type == ResourceType.CPU:
                base_demand = 0.5

        # Use historical data if available
        if task_id in self.usage_patterns:
            historical_data = self.usage_patterns[task_id]
            if historical_data:
                historical_avg = statistics.mean(historical_data[-5:])  # Last 5 executions
                # Blend with task-type estimation
                base_demand = base_demand * 0.6 + historical_avg * 0.4

        return base_demand

    def _analyze_trend(self, resource_type: ResourceType, time_horizon: int) -> float:
        """Analyze resource usage trends for prediction."""
        if len(self.historical_metrics) < 3:
            return 1.0

        # Extract recent usage values
        recent_usage = []
        for metrics in list(self.historical_metrics)[-time_horizon:]:
            usage = getattr(metrics, f"{resource_type.value}_usage", 0.0)
            recent_usage.append(usage)

        if len(recent_usage) < 2:
            return 1.0

        # Calculate trend (simple linear regression slope)
        n = len(recent_usage)
        sum_x = sum(range(n))
        sum_y = sum(recent_usage)
        sum_xy = sum(i * usage for i, usage in enumerate(recent_usage))
        sum_x2 = sum(i * i for i in range(n))

        if n * sum_x2 - sum_x * sum_x == 0:
            trend_slope = 0
        else:
            trend_slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)

        # Project trend forward
        current_usage = recent_usage[-1]
        projected_usage = current_usage + trend_slope * time_horizon

        return max(0.1, min(10.0, projected_usage))

    def record_usage(self, task_id: str, resource_type: ResourceType, usage: float) -> None:
        """Record resource usage for learning."""
        self.usage_patterns[task_id].append(usage)

        # Keep only recent data
        if len(self.usage_patterns[task_id]) > 20:
            self.usage_patterns[task_id] = self.usage_patterns[task_id][-10:]

    def add_metrics(self, metrics: ResourceMetrics) -> None:
        """Add new metrics for trend analysis."""
        self.historical_metrics.append(metrics)


class LoadBalancer:
    """Intelligent load balancer for task distribution."""

    def __init__(self):
        self.logger = get_logger("agent_etl.adaptive_resources.load_balancer")
        self.resource_pools: Dict[str, ResourcePool] = {}
        self.task_assignments: Dict[str, str] = {}  # task_id -> pool_id
        self.load_history: deque = deque(maxlen=50)

    def add_resource_pool(self, pool: ResourcePool) -> None:
        """Add a resource pool to the load balancer."""
        self.resource_pools[pool.pool_id] = pool
        self.logger.info(f"Added resource pool: {pool.pool_id}")

    def assign_task(
        self,
        task_id: str,
        resource_requirements: Dict[ResourceType, float],
        priority: float = 0.5
    ) -> Optional[str]:
        """Assign task to optimal resource pool."""
        best_pool = self._find_optimal_pool(resource_requirements, priority)

        if best_pool:
            self.task_assignments[task_id] = best_pool.pool_id
            best_pool.active_tasks.append(task_id)

            # Reserve resources
            for resource_type, requirement in resource_requirements.items():
                if resource_type == best_pool.resource_type:
                    best_pool.reserved_capacity += requirement
                    best_pool.utilization = min(1.0, best_pool.reserved_capacity / best_pool.current_capacity)

            self.logger.info(
                f"Assigned task {task_id} to pool {best_pool.pool_id}",
                extra={
                    "pool_utilization": best_pool.utilization,
                    "pool_efficiency": best_pool.efficiency_score
                }
            )

            return best_pool.pool_id

        return None

    def _find_optimal_pool(
        self,
        resource_requirements: Dict[ResourceType, float],
        priority: float
    ) -> Optional[ResourcePool]:
        """Find the optimal resource pool for task assignment."""
        if not self.resource_pools:
            return None

        candidate_pools = []

        for pool in self.resource_pools.values():
            if pool.resource_type not in resource_requirements:
                continue

            required_capacity = resource_requirements[pool.resource_type]
            available_capacity = pool.current_capacity - pool.reserved_capacity

            # Check if pool can accommodate the task
            if available_capacity >= required_capacity:
                score = self._calculate_pool_score(pool, required_capacity, priority)
                candidate_pools.append((score, pool))

        if not candidate_pools:
            return None

        # Return pool with highest score
        candidate_pools.sort(reverse=True, key=lambda x: x[0])
        return candidate_pools[0][1]

    def _calculate_pool_score(
        self,
        pool: ResourcePool,
        required_capacity: float,
        priority: float
    ) -> float:
        """Calculate assignment score for a resource pool."""
        # Base score from efficiency and health
        base_score = pool.efficiency_score * pool.health_score

        # Utilization factor (prefer moderate utilization)
        future_utilization = (pool.reserved_capacity + required_capacity) / pool.current_capacity
        utilization_score = 1.0 - abs(future_utilization - 0.7)  # Optimal around 70%

        # Priority factor
        priority_factor = 1.0 + priority * 0.5

        # Load balancing factor (prefer less loaded pools)
        load_factor = 1.0 - (len(pool.active_tasks) / 10.0)  # Normalize by max expected tasks

        total_score = base_score * utilization_score * priority_factor * load_factor

        return max(0.0, total_score)

    def release_task(self, task_id: str) -> None:
        """Release task resources from assigned pool."""
        if task_id not in self.task_assignments:
            return

        pool_id = self.task_assignments[task_id]
        pool = self.resource_pools.get(pool_id)

        if pool and task_id in pool.active_tasks:
            pool.active_tasks.remove(task_id)
            # Note: Reserved capacity should be updated based on actual usage
            del self.task_assignments[task_id]

            self.logger.debug(f"Released task {task_id} from pool {pool_id}")

    def rebalance_load(self) -> None:
        """Rebalance load across resource pools."""
        if len(self.resource_pools) < 2:
            return

        # Calculate load imbalance
        utilizations = [pool.utilization for pool in self.resource_pools.values()]
        if not utilizations:
            return

        avg_utilization = statistics.mean(utilizations)
        max_utilization = max(utilizations)

        # Trigger rebalancing if imbalance is significant
        if max_utilization - avg_utilization > 0.3:
            self._perform_rebalancing()

    def _perform_rebalancing(self) -> None:
        """Perform load rebalancing between pools."""
        # Find overloaded and underloaded pools
        overloaded = [p for p in self.resource_pools.values() if p.utilization > 0.8]
        underloaded = [p for p in self.resource_pools.values() if p.utilization < 0.4]

        # Move tasks from overloaded to underloaded pools
        for overloaded_pool in overloaded:
            if not underloaded:
                break

            # Try to move some tasks
            tasks_to_move = overloaded_pool.active_tasks[:2]  # Move up to 2 tasks

            for task_id in tasks_to_move:
                target_pool = underloaded[0]  # Simple selection

                # Move task
                overloaded_pool.active_tasks.remove(task_id)
                target_pool.active_tasks.append(task_id)
                self.task_assignments[task_id] = target_pool.pool_id

                # Update utilizations (simplified)
                overloaded_pool.utilization *= 0.9
                target_pool.utilization *= 1.1

                self.logger.info(
                    f"Rebalanced task {task_id} from {overloaded_pool.pool_id} to {target_pool.pool_id}"
                )


class AdaptiveResourceManager:
    """Main adaptive resource management system."""

    def __init__(self, scaling_interval: float = 30.0):
        self.logger = get_logger("agent_etl.adaptive_resources.manager")
        self.scaling_interval = scaling_interval
        self.predictive_scaler = PredictiveScaler()
        self.load_balancer = LoadBalancer()
        self.resource_pools: Dict[str, ResourcePool] = {}
        self.monitoring_active = False
        self.monitoring_thread: Optional[threading.Thread] = None
        self.scaling_history: deque = deque(maxlen=100)
        self.performance_metrics: Dict[str, float] = {}

        # Initialize default resource pools
        self._initialize_default_pools()

    def _initialize_default_pools(self) -> None:
        """Initialize default resource pools."""
        default_pools = [
            ResourcePool("cpu_pool", ResourceType.CPU, 4.0, 16.0, 0.5),
            ResourcePool("memory_pool", ResourceType.MEMORY, 8.0, 32.0, 1.0),
            ResourcePool("io_pool", ResourceType.IO, 2.0, 8.0, 0.5),
            ResourcePool("network_pool", ResourceType.NETWORK, 1.0, 4.0, 0.2),
        ]

        for pool in default_pools:
            self.resource_pools[pool.pool_id] = pool
            self.load_balancer.add_resource_pool(pool)

    def start_monitoring(self) -> None:
        """Start resource monitoring and adaptive scaling."""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        self.monitoring_thread = threading.Thread(target=self._monitoring_loop, daemon=True)
        self.monitoring_thread.start()

        self.logger.info("Started adaptive resource monitoring")

    def stop_monitoring(self) -> None:
        """Stop resource monitoring."""
        self.monitoring_active = False
        if self.monitoring_thread:
            self.monitoring_thread.join(timeout=5.0)

        self.logger.info("Stopped adaptive resource monitoring")

    def _monitoring_loop(self) -> None:
        """Main monitoring loop for adaptive scaling."""
        while self.monitoring_active:
            try:
                # Collect current metrics
                metrics = self._collect_resource_metrics()
                self.predictive_scaler.add_metrics(metrics)

                # Perform scaling decisions
                self._evaluate_scaling_needs()

                # Rebalance load if needed
                self.load_balancer.rebalance_load()

                # Update performance metrics
                self._update_performance_metrics(metrics)

                time.sleep(self.scaling_interval)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}", exc_info=e)
                time.sleep(self.scaling_interval)

    def _collect_resource_metrics(self) -> ResourceMetrics:
        """Collect current resource utilization metrics."""
        # In a real implementation, this would collect actual system metrics
        # For this example, we'll simulate based on current pool states

        total_cpu_usage = sum(p.utilization for p in self.resource_pools.values()
                             if p.resource_type == ResourceType.CPU)
        total_memory_usage = sum(p.utilization for p in self.resource_pools.values()
                                if p.resource_type == ResourceType.MEMORY)
        total_io_usage = sum(p.utilization for p in self.resource_pools.values()
                            if p.resource_type == ResourceType.IO)
        total_network_usage = sum(p.utilization for p in self.resource_pools.values()
                                 if p.resource_type == ResourceType.NETWORK)

        total_active_tasks = sum(len(p.active_tasks) for p in self.resource_pools.values())

        return ResourceMetrics(
            timestamp=time.time(),
            cpu_usage=min(1.0, total_cpu_usage),
            memory_usage=min(1.0, total_memory_usage),
            io_usage=min(1.0, total_io_usage),
            network_usage=min(1.0, total_network_usage),
            active_tasks=total_active_tasks,
            throughput=self.performance_metrics.get('throughput', 0.0),
            latency=self.performance_metrics.get('latency', 1.0)
        )

    def _evaluate_scaling_needs(self) -> None:
        """Evaluate and execute scaling decisions."""
        for pool in self.resource_pools.values():
            scaling_decision = self._make_scaling_decision(pool)

            if scaling_decision != ScalingDirection.STABLE:
                self._execute_scaling(pool, scaling_decision)

    def _make_scaling_decision(self, pool: ResourcePool) -> ScalingDirection:
        """Make scaling decision for a resource pool."""
        current_utilization = pool.utilization

        # Predict future demand
        upcoming_tasks = pool.active_tasks  # Simplified
        predicted_demand = self.predictive_scaler.predict_resource_demand(
            pool.resource_type, upcoming_tasks
        )

        # Calculate future utilization
        future_utilization = predicted_demand / pool.current_capacity

        # Scaling thresholds
        scale_up_threshold = 0.8
        scale_down_threshold = 0.3

        # Decision logic
        if future_utilization > scale_up_threshold and pool.current_capacity < pool.max_capacity:
            return ScalingDirection.UP
        elif current_utilization < scale_down_threshold and pool.current_capacity > pool.min_capacity:
            return ScalingDirection.DOWN
        else:
            return ScalingDirection.STABLE

    def _execute_scaling(self, pool: ResourcePool, direction: ScalingDirection) -> None:
        """Execute scaling decision for a resource pool."""
        old_capacity = pool.current_capacity

        if direction == ScalingDirection.UP:
            new_capacity = min(pool.max_capacity, pool.current_capacity * pool.scaling_factor)
        else:  # ScalingDirection.DOWN
            new_capacity = max(pool.min_capacity, pool.current_capacity / pool.scaling_factor)

        pool.current_capacity = new_capacity
        pool.utilization = pool.reserved_capacity / new_capacity if new_capacity > 0 else 0

        # Record scaling event
        self.scaling_history.append({
            "timestamp": time.time(),
            "pool_id": pool.pool_id,
            "direction": direction.value,
            "old_capacity": old_capacity,
            "new_capacity": new_capacity,
            "utilization": pool.utilization
        })

        self.logger.info(
            f"Scaled {pool.pool_id} {direction.value}",
            extra={
                "old_capacity": old_capacity,
                "new_capacity": new_capacity,
                "utilization": pool.utilization,
                "resource_type": pool.resource_type.value
            }
        )

    def _update_performance_metrics(self, metrics: ResourceMetrics) -> None:
        """Update performance metrics for decision making."""
        # Calculate throughput (tasks completed per unit time)
        if hasattr(self, '_last_metrics'):
            time_delta = metrics.timestamp - self._last_metrics.timestamp
            if time_delta > 0:
                task_delta = metrics.active_tasks - self._last_metrics.active_tasks
                self.performance_metrics['throughput'] = task_delta / time_delta

        # Update latency estimation
        self.performance_metrics['latency'] = metrics.latency

        # Store current metrics for next iteration
        self._last_metrics = metrics

    def assign_task_resources(
        self,
        task_id: str,
        resource_requirements: Dict[str, float],
        priority: float = 0.5
    ) -> Dict[str, str]:
        """Assign resources to a task across multiple pools."""
        assignments = {}

        # Convert string keys to ResourceType enum
        typed_requirements = {}
        for resource_name, requirement in resource_requirements.items():
            try:
                resource_type = ResourceType(resource_name.lower())
                typed_requirements[resource_type] = requirement
            except ValueError:
                self.logger.warning(f"Unknown resource type: {resource_name}")

        # Assign task to appropriate pools
        for resource_type, requirement in typed_requirements.items():
            pool_id = self.load_balancer.assign_task(
                f"{task_id}_{resource_type.value}",
                {resource_type: requirement},
                priority
            )
            if pool_id:
                assignments[resource_type.value] = pool_id

        return assignments

    def release_task_resources(self, task_id: str) -> None:
        """Release all resources assigned to a task."""
        # Release from all resource types
        for resource_type in ResourceType:
            resource_task_id = f"{task_id}_{resource_type.value}"
            self.load_balancer.release_task(resource_task_id)

    def get_resource_status(self) -> Dict[str, Any]:
        """Get current resource status and metrics."""
        pool_status = {}
        for pool_id, pool in self.resource_pools.items():
            pool_status[pool_id] = {
                "resource_type": pool.resource_type.value,
                "current_capacity": pool.current_capacity,
                "utilization": pool.utilization,
                "efficiency_score": pool.efficiency_score,
                "active_tasks": len(pool.active_tasks),
                "health_score": pool.health_score
            }

        return {
            "resource_pools": pool_status,
            "total_active_tasks": sum(len(p.active_tasks) for p in self.resource_pools.values()),
            "monitoring_active": self.monitoring_active,
            "recent_scaling_events": len(self.scaling_history),
            "performance_metrics": self.performance_metrics.copy()
        }

    def optimize_for_workload(self, workload_pattern: str) -> None:
        """Optimize resource allocation for specific workload patterns."""
        if workload_pattern == "cpu_intensive":
            # Boost CPU pools
            for pool in self.resource_pools.values():
                if pool.resource_type == ResourceType.CPU:
                    pool.scaling_factor = 1.5
                    pool.efficiency_score *= 1.2

        elif workload_pattern == "io_intensive":
            # Boost I/O pools
            for pool in self.resource_pools.values():
                if pool.resource_type == ResourceType.IO:
                    pool.scaling_factor = 1.4
                    pool.efficiency_score *= 1.2

        elif workload_pattern == "balanced":
            # Reset to balanced configuration
            for pool in self.resource_pools.values():
                pool.scaling_factor = 1.2
                pool.efficiency_score = 1.0

        self.logger.info(f"Optimized resource allocation for {workload_pattern} workload")

    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_monitoring()
