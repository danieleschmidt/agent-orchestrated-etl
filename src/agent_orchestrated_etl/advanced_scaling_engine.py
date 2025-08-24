"""Generation 3: Advanced Scaling and Load Balancing Engine.

Enterprise-grade auto-scaling with predictive algorithms, intelligent load balancing,
and multi-tier resource optimization.
"""

from __future__ import annotations

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple
from threading import Lock, RLock

try:
    import numpy as np
    from scipy import stats
except ImportError:
    np = None
    stats = None

from .logging_config import get_logger


class ScalingDirection(Enum):
    """Scaling direction enumeration."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


class LoadBalancingStrategy(Enum):
    """Load balancing strategy enumeration."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    RESOURCE_BASED = "resource_based"
    PREDICTIVE = "predictive"


@dataclass
class WorkerNode:
    """Represents a worker node in the system."""
    node_id: str
    capacity: int = 100
    current_load: int = 0
    response_time: float = 0.0
    error_rate: float = 0.0
    health_score: float = 1.0
    last_heartbeat: float = field(default_factory=time.time)
    active_tasks: int = 0
    completed_tasks: int = 0
    
    @property
    def utilization(self) -> float:
        """Get current utilization percentage."""
        return min(100.0, (self.current_load / max(self.capacity, 1)) * 100)
    
    @property
    def is_healthy(self) -> bool:
        """Check if node is healthy."""
        return (
            self.health_score > 0.7 and
            time.time() - self.last_heartbeat < 60 and
            self.error_rate < 0.1
        )


@dataclass 
class ScalingMetrics:
    """Metrics for scaling decisions."""
    timestamp: float
    total_requests: int
    average_response_time: float
    error_rate: float
    cpu_utilization: float
    memory_utilization: float
    active_workers: int
    queue_length: int
    throughput: float


@dataclass
class ScalingRecommendation:
    """Scaling recommendation from predictive algorithms."""
    direction: ScalingDirection
    magnitude: int  # How many workers to add/remove
    confidence: float
    reason: str
    expected_improvement: float
    time_horizon: int  # Minutes into future


class PredictiveScaler:
    """ML-powered predictive scaling engine."""
    
    def __init__(self, min_workers: int = 2, max_workers: int = 100):
        self._min_workers = min_workers
        self._max_workers = max_workers
        self._metrics_history: List[ScalingMetrics] = []
        self._scaling_history: List[Tuple[float, ScalingDirection, int]] = []
        self._logger = get_logger("agent_etl.scaling.predictive")
        
    def add_metrics(self, metrics: ScalingMetrics) -> None:
        """Add metrics to history for analysis."""
        self._metrics_history.append(metrics)
        
        # Keep only recent history to prevent memory bloat
        if len(self._metrics_history) > 1000:
            self._metrics_history = self._metrics_history[-500:]
    
    def predict_load(self, time_horizon_minutes: int = 5) -> Tuple[float, float]:
        """Predict future load using time series analysis."""
        if len(self._metrics_history) < 10:
            # Not enough data, return current load with high uncertainty
            current_metrics = self._metrics_history[-1] if self._metrics_history else None
            current_load = current_metrics.total_requests if current_metrics else 100
            return float(current_load), 0.5
        
        # Use simple trend analysis if scipy not available
        if not stats:
            recent_requests = [m.total_requests for m in self._metrics_history[-20:]]
            if len(recent_requests) >= 2:
                trend = (recent_requests[-1] - recent_requests[0]) / len(recent_requests)
                predicted_load = recent_requests[-1] + (trend * time_horizon_minutes)
                confidence = 0.7
            else:
                predicted_load = float(recent_requests[-1] if recent_requests else 100)
                confidence = 0.5
        else:
            # Advanced time series prediction with scipy
            timestamps = [m.timestamp for m in self._metrics_history[-50:]]
            requests = [m.total_requests for m in self._metrics_history[-50:]]
            
            if len(timestamps) >= 3:
                # Linear regression for trend
                slope, intercept, r_value, _, _ = stats.linregress(timestamps, requests)
                future_timestamp = time.time() + (time_horizon_minutes * 60)
                predicted_load = slope * future_timestamp + intercept
                confidence = abs(r_value) * 0.9  # R-value as confidence proxy
            else:
                predicted_load = float(requests[-1] if requests else 100)
                confidence = 0.5
        
        return max(0, predicted_load), min(1.0, max(0.1, confidence))
    
    def generate_scaling_recommendation(
        self, 
        current_workers: int,
        current_metrics: ScalingMetrics
    ) -> ScalingRecommendation:
        """Generate intelligent scaling recommendation."""
        
        # Predict future load
        predicted_load, confidence = self.predict_load(time_horizon_minutes=5)
        current_load = current_metrics.total_requests
        
        # Calculate capacity and utilization
        total_capacity = current_workers * 100  # Assume 100 requests per worker capacity
        current_utilization = (current_load / max(total_capacity, 1)) * 100
        predicted_utilization = (predicted_load / max(total_capacity, 1)) * 100
        
        # Scaling thresholds (configurable)
        scale_up_threshold = 75.0
        scale_down_threshold = 30.0
        
        # Factor in response time and error rate
        performance_factor = 1.0
        if current_metrics.average_response_time > 1000:  # >1s response time
            performance_factor += 0.3
        if current_metrics.error_rate > 0.05:  # >5% error rate
            performance_factor += 0.2
        
        adjusted_utilization = predicted_utilization * performance_factor
        
        # Generate recommendation
        if adjusted_utilization > scale_up_threshold:
            # Scale up
            workers_needed = max(1, int((adjusted_utilization - scale_up_threshold) / 25))
            workers_needed = min(workers_needed, self._max_workers - current_workers)
            
            return ScalingRecommendation(
                direction=ScalingDirection.UP,
                magnitude=workers_needed,
                confidence=confidence,
                reason=f"Predicted utilization {adjusted_utilization:.1f}% exceeds threshold {scale_up_threshold}%",
                expected_improvement=min(30.0, workers_needed * 10),
                time_horizon=5
            )
        
        elif current_utilization < scale_down_threshold and current_workers > self._min_workers:
            # Scale down
            excess_capacity = (scale_down_threshold - current_utilization) / 25
            workers_to_remove = min(
                max(1, int(excess_capacity)),
                current_workers - self._min_workers
            )
            
            return ScalingRecommendation(
                direction=ScalingDirection.DOWN,
                magnitude=workers_to_remove,
                confidence=confidence * 0.8,  # Be more conservative with scale down
                reason=f"Current utilization {current_utilization:.1f}% below threshold {scale_down_threshold}%",
                expected_improvement=workers_to_remove * 5,  # Cost savings
                time_horizon=5
            )
        
        else:
            # Stay stable
            return ScalingRecommendation(
                direction=ScalingDirection.STABLE,
                magnitude=0,
                confidence=confidence,
                reason=f"Utilization {current_utilization:.1f}% within optimal range",
                expected_improvement=0,
                time_horizon=5
            )


class IntelligentLoadBalancer:
    """Advanced load balancer with multiple strategies and health monitoring."""
    
    def __init__(self, strategy: LoadBalancingStrategy = LoadBalancingStrategy.PREDICTIVE):
        self._workers: Dict[str, WorkerNode] = {}
        self._strategy = strategy
        self._lock = RLock()
        self._request_counter = 0
        self._logger = get_logger("agent_etl.scaling.load_balancer")
        
    def register_worker(self, worker: WorkerNode) -> None:
        """Register a new worker node."""
        with self._lock:
            self._workers[worker.node_id] = worker
            self._logger.info(f"Registered worker {worker.node_id} with capacity {worker.capacity}")
    
    def unregister_worker(self, node_id: str) -> bool:
        """Unregister a worker node."""
        with self._lock:
            if node_id in self._workers:
                del self._workers[node_id]
                self._logger.info(f"Unregistered worker {node_id}")
                return True
            return False
    
    def update_worker_metrics(
        self, 
        node_id: str, 
        current_load: int = None,
        response_time: float = None,
        error_rate: float = None,
        health_score: float = None
    ) -> None:
        """Update worker metrics."""
        with self._lock:
            if node_id in self._workers:
                worker = self._workers[node_id]
                if current_load is not None:
                    worker.current_load = current_load
                if response_time is not None:
                    worker.response_time = response_time
                if error_rate is not None:
                    worker.error_rate = error_rate
                if health_score is not None:
                    worker.health_score = health_score
                worker.last_heartbeat = time.time()
    
    def get_healthy_workers(self) -> List[WorkerNode]:
        """Get list of healthy workers."""
        with self._lock:
            return [worker for worker in self._workers.values() if worker.is_healthy]
    
    def select_worker(self, task_weight: float = 1.0) -> Optional[WorkerNode]:
        """Select optimal worker based on current strategy."""
        healthy_workers = self.get_healthy_workers()
        
        if not healthy_workers:
            return None
        
        if self._strategy == LoadBalancingStrategy.ROUND_ROBIN:
            return self._round_robin_selection(healthy_workers)
        elif self._strategy == LoadBalancingStrategy.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_selection(healthy_workers, task_weight)
        elif self._strategy == LoadBalancingStrategy.LEAST_CONNECTIONS:
            return self._least_connections_selection(healthy_workers)
        elif self._strategy == LoadBalancingStrategy.LEAST_RESPONSE_TIME:
            return self._least_response_time_selection(healthy_workers)
        elif self._strategy == LoadBalancingStrategy.RESOURCE_BASED:
            return self._resource_based_selection(healthy_workers, task_weight)
        elif self._strategy == LoadBalancingStrategy.PREDICTIVE:
            return self._predictive_selection(healthy_workers, task_weight)
        else:
            return healthy_workers[0]  # Fallback
    
    def _round_robin_selection(self, workers: List[WorkerNode]) -> WorkerNode:
        """Simple round-robin selection."""
        self._request_counter += 1
        return workers[self._request_counter % len(workers)]
    
    def _weighted_round_robin_selection(self, workers: List[WorkerNode], task_weight: float) -> WorkerNode:
        """Weighted round-robin based on capacity."""
        # Calculate weights based on available capacity
        weights = []
        for worker in workers:
            available_capacity = max(1, worker.capacity - worker.current_load)
            weights.append(available_capacity)
        
        # Select based on weighted probability
        total_weight = sum(weights)
        if total_weight == 0:
            return workers[0]
        
        r = (self._request_counter * 31) % total_weight  # Pseudo-random selection
        self._request_counter += 1
        
        cumulative = 0
        for i, weight in enumerate(weights):
            cumulative += weight
            if r < cumulative:
                return workers[i]
        
        return workers[-1]
    
    def _least_connections_selection(self, workers: List[WorkerNode]) -> WorkerNode:
        """Select worker with least active connections."""
        return min(workers, key=lambda w: w.active_tasks)
    
    def _least_response_time_selection(self, workers: List[WorkerNode]) -> WorkerNode:
        """Select worker with lowest response time."""
        return min(workers, key=lambda w: w.response_time + (w.utilization / 100) * 0.1)
    
    def _resource_based_selection(self, workers: List[WorkerNode], task_weight: float) -> WorkerNode:
        """Select worker based on resource availability and task requirements."""
        scores = []
        for worker in workers:
            # Score based on multiple factors
            utilization_score = 1.0 - (worker.utilization / 100)  # Lower utilization = higher score
            response_score = 1.0 / (1.0 + worker.response_time / 1000)  # Lower response time = higher score
            error_score = 1.0 - min(0.9, worker.error_rate * 10)  # Lower error rate = higher score
            health_score = worker.health_score
            
            # Weighted composite score
            composite_score = (
                utilization_score * 0.4 +
                response_score * 0.3 +
                error_score * 0.2 +
                health_score * 0.1
            )
            
            scores.append((worker, composite_score))
        
        # Select worker with highest score
        return max(scores, key=lambda x: x[1])[0]
    
    def _predictive_selection(self, workers: List[WorkerNode], task_weight: float) -> WorkerNode:
        """Advanced predictive selection using ML insights."""
        # Start with resource-based selection
        best_worker = self._resource_based_selection(workers, task_weight)
        
        # Apply predictive adjustments
        for worker in workers:
            # Predict worker performance based on historical data
            predicted_response = worker.response_time
            
            # Adjust for current load trend
            if worker.current_load > worker.capacity * 0.8:
                predicted_response *= 1.5
            elif worker.current_load < worker.capacity * 0.3:
                predicted_response *= 0.8
            
            # Consider task weight impact
            if task_weight > 1.5:  # Heavy task
                if worker.utilization > 60:
                    predicted_response *= 1.3
            
            # Update selection if this worker is predicted to perform better
            if (predicted_response < best_worker.response_time * 1.2 and 
                worker.health_score > best_worker.health_score * 0.9):
                best_worker = worker
        
        return best_worker
    
    def get_cluster_metrics(self) -> Dict[str, Any]:
        """Get comprehensive cluster metrics."""
        with self._lock:
            total_workers = len(self._workers)
            healthy_workers = len(self.get_healthy_workers())
            
            if not self._workers:
                return {
                    "total_workers": 0,
                    "healthy_workers": 0,
                    "average_utilization": 0,
                    "average_response_time": 0,
                    "total_capacity": 0,
                    "total_load": 0,
                }
            
            total_capacity = sum(w.capacity for w in self._workers.values())
            total_load = sum(w.current_load for w in self._workers.values())
            avg_utilization = sum(w.utilization for w in self._workers.values()) / total_workers
            avg_response_time = sum(w.response_time for w in self._workers.values()) / total_workers
            
            return {
                "total_workers": total_workers,
                "healthy_workers": healthy_workers,
                "average_utilization": avg_utilization,
                "average_response_time": avg_response_time,
                "total_capacity": total_capacity,
                "total_load": total_load,
                "cluster_utilization": (total_load / max(total_capacity, 1)) * 100,
            }


class AdvancedScalingEngine:
    """Main scaling engine that orchestrates predictive scaling and load balancing."""
    
    def __init__(
        self, 
        min_workers: int = 2,
        max_workers: int = 100,
        scaling_strategy: LoadBalancingStrategy = LoadBalancingStrategy.PREDICTIVE
    ):
        self._scaler = PredictiveScaler(min_workers, max_workers)
        self._load_balancer = IntelligentLoadBalancer(scaling_strategy)
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._logger = get_logger("agent_etl.scaling.engine")
        
        # Initialize with minimum workers
        for i in range(min_workers):
            worker = WorkerNode(
                node_id=f"worker_{i}",
                capacity=100,
                current_load=0
            )
            self._load_balancer.register_worker(worker)
    
    async def process_task(self, task_func: Callable, *args, task_weight: float = 1.0, **kwargs) -> Any:
        """Process task using intelligent load balancing."""
        # Select optimal worker
        worker = self._load_balancer.select_worker(task_weight)
        
        if not worker:
            raise RuntimeError("No healthy workers available")
        
        # Update worker load
        worker.active_tasks += 1
        worker.current_load += int(task_weight * 10)
        
        start_time = time.time()
        try:
            # Execute task
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(self._executor, task_func, *args, **kwargs)
            
            # Update worker metrics
            execution_time = (time.time() - start_time) * 1000  # ms
            self._load_balancer.update_worker_metrics(
                worker.node_id,
                current_load=max(0, worker.current_load - int(task_weight * 10)),
                response_time=execution_time,
                error_rate=max(0, worker.error_rate * 0.95)  # Decay error rate on success
            )
            
            worker.active_tasks -= 1
            worker.completed_tasks += 1
            
            return result
            
        except Exception as e:
            # Update error metrics
            execution_time = (time.time() - start_time) * 1000
            self._load_balancer.update_worker_metrics(
                worker.node_id,
                current_load=max(0, worker.current_load - int(task_weight * 10)),
                response_time=execution_time,
                error_rate=min(1.0, worker.error_rate + 0.1)
            )
            
            worker.active_tasks -= 1
            raise
    
    def get_scaling_recommendation(self) -> Optional[ScalingRecommendation]:
        """Get current scaling recommendation."""
        cluster_metrics = self._load_balancer.get_cluster_metrics()
        current_workers = cluster_metrics["healthy_workers"]
        
        if not current_workers:
            return None
        
        # Create scaling metrics from cluster state
        scaling_metrics = ScalingMetrics(
            timestamp=time.time(),
            total_requests=cluster_metrics["total_load"],
            average_response_time=cluster_metrics["average_response_time"],
            error_rate=0.01,  # Default low error rate
            cpu_utilization=cluster_metrics["average_utilization"],
            memory_utilization=50.0,  # Default
            active_workers=current_workers,
            queue_length=0,  # To be implemented
            throughput=cluster_metrics["total_load"] / max(1, current_workers)
        )
        
        self._scaler.add_metrics(scaling_metrics)
        return self._scaler.generate_scaling_recommendation(current_workers, scaling_metrics)
    
    def apply_scaling_recommendation(self, recommendation: ScalingRecommendation) -> bool:
        """Apply scaling recommendation by adding/removing workers."""
        try:
            if recommendation.direction == ScalingDirection.UP:
                # Add workers
                current_workers = len(self._load_balancer._workers)
                for i in range(recommendation.magnitude):
                    worker_id = f"worker_{current_workers + i}"
                    worker = WorkerNode(
                        node_id=worker_id,
                        capacity=100,
                        current_load=0
                    )
                    self._load_balancer.register_worker(worker)
                
                self._logger.info(f"Scaled up by {recommendation.magnitude} workers")
                
            elif recommendation.direction == ScalingDirection.DOWN:
                # Remove workers (remove least loaded workers)
                workers = list(self._load_balancer._workers.values())
                workers.sort(key=lambda w: w.current_load)
                
                for i in range(min(recommendation.magnitude, len(workers) - self._scaler._min_workers)):
                    worker = workers[i]
                    if worker.active_tasks == 0:  # Only remove idle workers
                        self._load_balancer.unregister_worker(worker.node_id)
                
                self._logger.info(f"Scaled down by {recommendation.magnitude} workers")
            
            return True
            
        except Exception as e:
            self._logger.error(f"Failed to apply scaling recommendation: {e}")
            return False
    
    def auto_scale(self) -> Dict[str, Any]:
        """Perform automatic scaling based on current conditions."""
        recommendation = self.get_scaling_recommendation()
        
        if not recommendation:
            return {"status": "no_recommendation", "cluster_metrics": self._load_balancer.get_cluster_metrics()}
        
        # Apply high-confidence recommendations automatically
        applied = False
        if recommendation.confidence > 0.8:
            applied = self.apply_scaling_recommendation(recommendation)
        
        return {
            "status": "completed",
            "recommendation": {
                "direction": recommendation.direction.value,
                "magnitude": recommendation.magnitude,
                "confidence": recommendation.confidence,
                "reason": recommendation.reason,
                "expected_improvement": recommendation.expected_improvement,
            },
            "applied": applied,
            "cluster_metrics": self._load_balancer.get_cluster_metrics(),
        }
    
    async def shutdown(self) -> None:
        """Gracefully shutdown the scaling engine."""
        self._executor.shutdown(wait=True)
        self._logger.info("Scaling engine shutdown completed")


# Global scaling engine instance
_scaling_engine = None


def get_scaling_engine(**kwargs) -> AdvancedScalingEngine:
    """Get global scaling engine instance."""
    global _scaling_engine
    if _scaling_engine is None:
        _scaling_engine = AdvancedScalingEngine(**kwargs)
    return _scaling_engine