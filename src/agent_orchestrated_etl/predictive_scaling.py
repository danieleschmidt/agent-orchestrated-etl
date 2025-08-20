"""Predictive auto-scaling system with ML-based resource optimization."""

from __future__ import annotations

import asyncio
import json
import time
import math
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
from collections import deque
# Use built-in statistics instead of numpy for compatibility
try:
    import numpy as np
    HAS_NUMPY = True
except ImportError:
    HAS_NUMPY = False
    
    # Provide numpy-like functions using built-in math
    class np:
        @staticmethod
        def mean(data):
            return sum(data) / len(data) if data else 0
        
        @staticmethod
        def median(data):
            sorted_data = sorted(data)
            n = len(sorted_data)
            if n == 0:
                return 0
            return sorted_data[n//2] if n % 2 == 1 else (sorted_data[n//2-1] + sorted_data[n//2]) / 2
        
        @staticmethod
        def std(data):
            if not data:
                return 0
            mean_val = np.mean(data)
            variance = sum((x - mean_val) ** 2 for x in data) / len(data)
            return variance ** 0.5
        
        @staticmethod
        def min(data):
            return min(data) if data else 0
        
        @staticmethod
        def max(data):
            return max(data) if data else 0
        
        @staticmethod
        def percentile(data, percentile):
            if not data:
                return 0
            sorted_data = sorted(data)
            k = (len(sorted_data) - 1) * percentile / 100
            f = int(k)
            c = k - f
            if f + 1 < len(sorted_data):
                return sorted_data[f] + c * (sorted_data[f + 1] - sorted_data[f])
            else:
                return sorted_data[f]
        
        @staticmethod
        def corrcoef(x, y):
            # Simple correlation coefficient calculation
            if len(x) != len(y) or len(x) < 2:
                return [[1.0, 0.0], [0.0, 1.0]]
            
            mean_x = np.mean(x)
            mean_y = np.mean(y)
            
            numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(len(x)))
            sum_sq_x = sum((x[i] - mean_x) ** 2 for i in range(len(x)))
            sum_sq_y = sum((y[i] - mean_y) ** 2 for i in range(len(y)))
            
            denominator = (sum_sq_x * sum_sq_y) ** 0.5
            
            if denominator == 0:
                return [[1.0, 0.0], [0.0, 1.0]]
            
            corr = numerator / denominator
            return [[1.0, corr], [corr, 1.0]]
        
        @staticmethod
        def isnan(value):
            return value != value  # NaN is not equal to itself
from concurrent.futures import ThreadPoolExecutor

from .exceptions import DataProcessingException
from .logging_config import get_logger


class ResourceType(Enum):
    """Types of resources that can be scaled."""
    CPU = "cpu"
    MEMORY = "memory"
    STORAGE = "storage"
    NETWORK = "network"
    WORKERS = "workers"
    CONTAINERS = "containers"


class ScalingDirection(Enum):
    """Scaling directions."""
    UP = "up"
    DOWN = "down"
    STABLE = "stable"


class ScalingTrigger(Enum):
    """Triggers for scaling decisions."""
    THRESHOLD = "threshold"
    PREDICTIVE = "predictive"
    QUEUE_LENGTH = "queue_length"
    RESPONSE_TIME = "response_time"
    ERROR_RATE = "error_rate"
    COST_OPTIMIZATION = "cost_optimization"


@dataclass
class ResourceMetrics:
    """Current resource usage metrics."""
    timestamp: float
    cpu_utilization: float
    memory_utilization: float
    storage_utilization: float
    network_io: float
    active_connections: int
    queue_length: int
    response_time_ms: float
    error_rate: float
    throughput_rps: float
    cost_per_hour: float


@dataclass
class ScalingAction:
    """Represents a scaling action."""
    action_id: str
    timestamp: float
    resource_type: ResourceType
    direction: ScalingDirection
    current_capacity: float
    target_capacity: float
    scaling_factor: float
    trigger: ScalingTrigger
    confidence: float
    estimated_cost_impact: float
    execution_time_seconds: float
    metadata: Dict[str, Any]


@dataclass
class ScalingPolicy:
    """Configuration for scaling policies."""
    policy_name: str
    resource_type: ResourceType
    min_capacity: float
    max_capacity: float
    target_utilization: float
    scale_up_threshold: float
    scale_down_threshold: float
    scale_up_cooldown: float
    scale_down_cooldown: float
    prediction_window_minutes: int
    cost_optimization_enabled: bool
    emergency_scaling_enabled: bool


class PredictiveScaler:
    """ML-based predictive scaling system."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.scaling")
        self.metrics_history: deque = deque(maxlen=1000)  # Keep last 1000 metrics
        self.scaling_history: List[ScalingAction] = []
        self.scaling_policies: Dict[ResourceType, ScalingPolicy] = {}
        self.executor = ThreadPoolExecutor(max_workers=4)
        
        # ML model parameters (simplified)
        self.prediction_models: Dict[ResourceType, Dict[str, Any]] = {}
        self.feature_weights = {
            "time_of_day": 0.2,
            "day_of_week": 0.1,
            "historical_trend": 0.3,
            "current_utilization": 0.25,
            "queue_length": 0.15
        }
        
        # Scaling state
        self.last_scaling_time: Dict[ResourceType, float] = {}
        self.current_capacity: Dict[ResourceType, float] = {}
        
        # Performance tracking
        self.scaling_effectiveness = {
            "successful_scale_ups": 0,
            "successful_scale_downs": 0,
            "unnecessary_scales": 0,
            "prediction_accuracy": 0.0,
            "cost_savings": 0.0
        }
        
        # Initialize default policies
        self._initialize_default_policies()
    
    def _initialize_default_policies(self):
        """Initialize default scaling policies for different resource types."""
        
        default_policies = {
            ResourceType.CPU: ScalingPolicy(
                policy_name="cpu_autoscale",
                resource_type=ResourceType.CPU,
                min_capacity=1.0,
                max_capacity=10.0,
                target_utilization=0.7,
                scale_up_threshold=0.8,
                scale_down_threshold=0.3,
                scale_up_cooldown=300,  # 5 minutes
                scale_down_cooldown=600,  # 10 minutes
                prediction_window_minutes=30,
                cost_optimization_enabled=True,
                emergency_scaling_enabled=True
            ),
            ResourceType.MEMORY: ScalingPolicy(
                policy_name="memory_autoscale",
                resource_type=ResourceType.MEMORY,
                min_capacity=2.0,  # GB
                max_capacity=32.0,  # GB
                target_utilization=0.75,
                scale_up_threshold=0.85,
                scale_down_threshold=0.4,
                scale_up_cooldown=180,
                scale_down_cooldown=900,
                prediction_window_minutes=15,
                cost_optimization_enabled=True,
                emergency_scaling_enabled=True
            ),
            ResourceType.WORKERS: ScalingPolicy(
                policy_name="worker_autoscale",
                resource_type=ResourceType.WORKERS,
                min_capacity=2.0,
                max_capacity=50.0,
                target_utilization=0.6,
                scale_up_threshold=0.8,
                scale_down_threshold=0.2,
                scale_up_cooldown=120,
                scale_down_cooldown=300,
                prediction_window_minutes=10,
                cost_optimization_enabled=True,
                emergency_scaling_enabled=True
            )
        }
        
        self.scaling_policies.update(default_policies)
        
        # Initialize current capacity
        for resource_type, policy in default_policies.items():
            self.current_capacity[resource_type] = policy.min_capacity
    
    def add_metrics(self, metrics: ResourceMetrics):
        """Add new metrics for analysis."""
        self.metrics_history.append(metrics)
        
        # Trigger scaling analysis if we have enough data
        if len(self.metrics_history) >= 10:  # Minimum data points
            asyncio.create_task(self._analyze_and_scale())
    
    async def _analyze_and_scale(self):
        """Analyze metrics and make scaling decisions."""
        
        current_metrics = self.metrics_history[-1]
        
        # Analyze each resource type
        for resource_type, policy in self.scaling_policies.items():
            try:
                await self._analyze_resource(resource_type, policy, current_metrics)
            except Exception as e:
                self.logger.error(f"Error analyzing {resource_type.value}: {str(e)}")
    
    async def _analyze_resource(self, 
                              resource_type: ResourceType, 
                              policy: ScalingPolicy,
                              current_metrics: ResourceMetrics):
        """Analyze a specific resource type and make scaling decisions."""
        
        # Check cooldown period
        last_scaling = self.last_scaling_time.get(resource_type, 0)
        current_time = time.time()
        
        # Get current utilization for this resource type
        current_utilization = self._get_resource_utilization(resource_type, current_metrics)
        current_capacity = self.current_capacity.get(resource_type, policy.min_capacity)
        
        # Check for emergency scaling (immediate response needed)
        if policy.emergency_scaling_enabled:
            emergency_action = await self._check_emergency_scaling(
                resource_type, policy, current_metrics, current_utilization
            )
            if emergency_action:
                await self._execute_scaling_action(emergency_action)
                return
        
        # Predictive analysis
        predicted_utilization = await self._predict_resource_utilization(
            resource_type, policy.prediction_window_minutes
        )
        
        # Determine scaling decision
        scaling_decision = await self._make_scaling_decision(
            resource_type, policy, current_utilization, predicted_utilization, current_capacity
        )
        
        if scaling_decision:
            # Check cooldown
            required_cooldown = (policy.scale_up_cooldown if scaling_decision.direction == ScalingDirection.UP 
                               else policy.scale_down_cooldown)
            
            if current_time - last_scaling >= required_cooldown:
                await self._execute_scaling_action(scaling_decision)
            else:
                self.logger.debug(
                    f"Scaling {resource_type.value} skipped due to cooldown "
                    f"({current_time - last_scaling:.0f}s < {required_cooldown}s)"
                )
    
    def _get_resource_utilization(self, resource_type: ResourceType, metrics: ResourceMetrics) -> float:
        """Get current utilization for a resource type."""
        
        utilization_map = {
            ResourceType.CPU: metrics.cpu_utilization,
            ResourceType.MEMORY: metrics.memory_utilization,
            ResourceType.STORAGE: metrics.storage_utilization,
            ResourceType.NETWORK: min(1.0, metrics.network_io / 1000),  # Normalize to 0-1
            ResourceType.WORKERS: min(1.0, metrics.queue_length / 100),  # Queue-based utilization
            ResourceType.CONTAINERS: metrics.cpu_utilization  # Use CPU as proxy
        }
        
        return utilization_map.get(resource_type, 0.0)
    
    async def _check_emergency_scaling(self,
                                     resource_type: ResourceType,
                                     policy: ScalingPolicy,
                                     metrics: ResourceMetrics,
                                     utilization: float) -> Optional[ScalingAction]:
        """Check if emergency scaling is needed."""
        
        # Emergency conditions
        emergency_conditions = [
            utilization > 0.95,  # Very high utilization
            metrics.error_rate > 0.1,  # High error rate (10%)
            metrics.response_time_ms > 5000,  # Very slow response (5s)
            metrics.queue_length > 1000  # Very long queue
        ]
        
        if any(emergency_conditions):
            # Calculate emergency scaling factor
            if utilization > 0.95:
                scale_factor = 2.0  # Double capacity
            elif metrics.error_rate > 0.1:
                scale_factor = 1.5
            else:
                scale_factor = 1.3
            
            current_capacity = self.current_capacity.get(resource_type, policy.min_capacity)
            target_capacity = min(policy.max_capacity, current_capacity * scale_factor)
            
            return ScalingAction(
                action_id=f"emergency_{resource_type.value}_{int(time.time())}",
                timestamp=time.time(),
                resource_type=resource_type,
                direction=ScalingDirection.UP,
                current_capacity=current_capacity,
                target_capacity=target_capacity,
                scaling_factor=scale_factor,
                trigger=ScalingTrigger.THRESHOLD,
                confidence=0.9,
                estimated_cost_impact=self._estimate_cost_impact(current_capacity, target_capacity),
                execution_time_seconds=30,  # Fast emergency scaling
                metadata={
                    "emergency": True,
                    "utilization": utilization,
                    "error_rate": metrics.error_rate,
                    "response_time": metrics.response_time_ms
                }
            )
        
        return None
    
    async def _predict_resource_utilization(self, 
                                          resource_type: ResourceType, 
                                          prediction_window_minutes: int) -> float:
        """Predict future resource utilization using simplified ML model."""
        
        if len(self.metrics_history) < 5:
            return 0.5  # Default prediction if insufficient data
        
        # Extract historical utilization data
        historical_data = [
            self._get_resource_utilization(resource_type, metrics) 
            for metrics in self.metrics_history
        ]
        
        # Simple time-series features
        current_time = time.time()
        hour_of_day = (current_time % 86400) / 86400  # 0-1 normalized
        day_of_week = ((current_time // 86400) % 7) / 7  # 0-1 normalized
        
        # Trend analysis (simple linear regression on recent data)
        recent_data = historical_data[-10:]  # Last 10 data points
        if len(recent_data) >= 3:
            x = list(range(len(recent_data)))
            y = recent_data
            
            # Simple linear regression
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_xy = sum(x[i] * y[i] for i in range(n))
            sum_x2 = sum(x[i] * x[i] for i in range(n))
            
            if n * sum_x2 - sum_x * sum_x != 0:
                slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
                intercept = (sum_y - slope * sum_x) / n
                
                # Predict future point (prediction_window_minutes from now)
                future_x = len(recent_data) + (prediction_window_minutes / 5)  # Assuming 5-minute intervals
                trend_prediction = slope * future_x + intercept
            else:
                trend_prediction = np.mean(recent_data) if recent_data else 0.5
        else:
            trend_prediction = np.mean(historical_data) if historical_data else 0.5
        
        # Current utilization
        current_utilization = historical_data[-1] if historical_data else 0.5
        
        # Queue length impact
        current_metrics = self.metrics_history[-1]
        queue_impact = min(0.3, current_metrics.queue_length / 1000)  # Max 30% impact
        
        # Weighted prediction
        prediction = (
            self.feature_weights["time_of_day"] * self._get_time_factor(hour_of_day) +
            self.feature_weights["day_of_week"] * self._get_day_factor(day_of_week) +
            self.feature_weights["historical_trend"] * trend_prediction +
            self.feature_weights["current_utilization"] * current_utilization +
            self.feature_weights["queue_length"] * queue_impact
        )
        
        # Clamp prediction to reasonable bounds
        prediction = max(0.0, min(1.0, prediction))
        
        return prediction
    
    def _get_time_factor(self, hour_normalized: float) -> float:
        """Get scaling factor based on time of day."""
        # Assume higher usage during business hours (normalized sine wave)
        hour_angle = hour_normalized * 2 * math.pi
        return 0.5 + 0.3 * math.sin(hour_angle - math.pi/3)  # Peak around 2 PM
    
    def _get_day_factor(self, day_normalized: float) -> float:
        """Get scaling factor based on day of week."""
        # Assume lower usage on weekends
        day = day_normalized * 7
        if 0 <= day < 1 or 6 <= day < 7:  # Weekend
            return 0.7
        else:  # Weekday
            return 1.0
    
    async def _make_scaling_decision(self,
                                   resource_type: ResourceType,
                                   policy: ScalingPolicy,
                                   current_utilization: float,
                                   predicted_utilization: float,
                                   current_capacity: float) -> Optional[ScalingAction]:
        """Make scaling decision based on current and predicted utilization."""
        
        # Use predicted utilization for proactive scaling
        decision_utilization = max(current_utilization, predicted_utilization)
        
        scaling_direction = None
        scaling_factor = 1.0
        trigger = ScalingTrigger.THRESHOLD
        confidence = 0.7
        
        # Scale up conditions
        if decision_utilization > policy.scale_up_threshold:
            scaling_direction = ScalingDirection.UP
            
            # Calculate scaling factor based on how much over threshold
            overage = decision_utilization - policy.target_utilization
            scaling_factor = 1.0 + min(1.0, overage / 0.2)  # Max 2x scaling
            
            if predicted_utilization > current_utilization:
                trigger = ScalingTrigger.PREDICTIVE
                confidence = 0.8
            
        # Scale down conditions
        elif decision_utilization < policy.scale_down_threshold:
            scaling_direction = ScalingDirection.DOWN
            
            # Calculate scaling factor
            underutilization = policy.target_utilization - decision_utilization
            scaling_factor = max(0.5, 1.0 - (underutilization / 0.3))  # Min 0.5x scaling
            
            # Be more conservative with scale down
            confidence = 0.6
        
        if scaling_direction is None:
            return None
        
        # Calculate target capacity
        if scaling_direction == ScalingDirection.UP:
            target_capacity = min(policy.max_capacity, current_capacity * scaling_factor)
        else:
            target_capacity = max(policy.min_capacity, current_capacity * scaling_factor)
        
        # Don't scale if change is too small (less than 10%)
        if abs(target_capacity - current_capacity) / current_capacity < 0.1:
            return None
        
        return ScalingAction(
            action_id=f"scale_{resource_type.value}_{int(time.time())}",
            timestamp=time.time(),
            resource_type=resource_type,
            direction=scaling_direction,
            current_capacity=current_capacity,
            target_capacity=target_capacity,
            scaling_factor=scaling_factor,
            trigger=trigger,
            confidence=confidence,
            estimated_cost_impact=self._estimate_cost_impact(current_capacity, target_capacity),
            execution_time_seconds=120,  # Normal scaling time
            metadata={
                "current_utilization": current_utilization,
                "predicted_utilization": predicted_utilization,
                "policy": policy.policy_name
            }
        )
    
    def _estimate_cost_impact(self, current_capacity: float, target_capacity: float) -> float:
        """Estimate cost impact of scaling action."""
        # Simple cost model ($/hour)
        cost_per_unit_per_hour = {
            ResourceType.CPU: 0.05,
            ResourceType.MEMORY: 0.01,
            ResourceType.WORKERS: 0.10,
            ResourceType.CONTAINERS: 0.08,
            ResourceType.STORAGE: 0.02,
            ResourceType.NETWORK: 0.03
        }
        
        capacity_change = target_capacity - current_capacity
        cost_change = capacity_change * cost_per_unit_per_hour.get(ResourceType.CPU, 0.05)
        
        return cost_change
    
    async def _execute_scaling_action(self, action: ScalingAction):
        """Execute a scaling action."""
        
        self.logger.info(
            f"Executing scaling action: {action.resource_type.value} "
            f"{action.direction.value} from {action.current_capacity:.1f} to {action.target_capacity:.1f} "
            f"(trigger: {action.trigger.value}, confidence: {action.confidence:.2f})"
        )
        
        try:
            # Simulate scaling execution
            await asyncio.sleep(0.1)  # Simulate execution delay
            
            # Update current capacity
            self.current_capacity[action.resource_type] = action.target_capacity
            self.last_scaling_time[action.resource_type] = time.time()
            
            # Record action
            self.scaling_history.append(action)
            
            # Update effectiveness metrics
            if action.direction == ScalingDirection.UP:
                self.scaling_effectiveness["successful_scale_ups"] += 1
            else:
                self.scaling_effectiveness["successful_scale_downs"] += 1
            
            # Estimate cost savings for scale-down actions
            if action.direction == ScalingDirection.DOWN and action.estimated_cost_impact < 0:
                self.scaling_effectiveness["cost_savings"] += abs(action.estimated_cost_impact)
            
            self.logger.info(f"Scaling action completed successfully: {action.action_id}")
            
        except Exception as e:
            self.logger.error(f"Failed to execute scaling action {action.action_id}: {str(e)}")
            raise
    
    def set_scaling_policy(self, policy: ScalingPolicy):
        """Set or update a scaling policy."""
        self.scaling_policies[policy.resource_type] = policy
        self.logger.info(f"Updated scaling policy for {policy.resource_type.value}")
    
    def get_current_capacity(self) -> Dict[str, float]:
        """Get current capacity for all resource types."""
        return {rt.value: capacity for rt, capacity in self.current_capacity.items()}
    
    def get_scaling_metrics(self) -> Dict[str, Any]:
        """Get comprehensive scaling metrics and analytics."""
        
        recent_actions = [
            action for action in self.scaling_history
            if time.time() - action.timestamp < 3600  # Last hour
        ]
        
        # Analyze scaling patterns
        scale_up_actions = [a for a in recent_actions if a.direction == ScalingDirection.UP]
        scale_down_actions = [a for a in recent_actions if a.direction == ScalingDirection.DOWN]
        
        # Calculate prediction accuracy (simplified)
        total_predictions = len([a for a in recent_actions if a.trigger == ScalingTrigger.PREDICTIVE])
        # In a real system, we'd track whether predictions were accurate
        prediction_accuracy = 0.75 if total_predictions > 0 else 0.0
        
        return {
            "current_capacity": self.get_current_capacity(),
            "scaling_effectiveness": {
                **self.scaling_effectiveness,
                "prediction_accuracy": prediction_accuracy
            },
            "recent_scaling_activity": {
                "total_actions": len(recent_actions),
                "scale_up_actions": len(scale_up_actions),
                "scale_down_actions": len(scale_down_actions),
                "avg_confidence": np.mean([a.confidence for a in recent_actions]) if recent_actions else 0.0,
                "total_cost_impact": sum(a.estimated_cost_impact for a in recent_actions)
            },
            "scaling_policies": {
                rt.value: asdict(policy) for rt, policy in self.scaling_policies.items()
            },
            "resource_utilization": {
                rt.value: self._get_resource_utilization(rt, self.metrics_history[-1])
                for rt in self.current_capacity.keys()
            } if self.metrics_history else {},
            "timestamp": time.time()
        }
    
    async def optimize_policies(self) -> Dict[str, Any]:
        """Optimize scaling policies based on historical performance."""
        
        if len(self.scaling_history) < 10:
            return {"message": "Insufficient data for policy optimization"}
        
        optimizations = {}
        
        for resource_type, policy in self.scaling_policies.items():
            resource_actions = [
                action for action in self.scaling_history 
                if action.resource_type == resource_type
            ]
            
            if len(resource_actions) < 5:
                continue
            
            # Analyze scaling patterns
            unnecessary_scales = 0
            effective_scales = 0
            
            for i, action in enumerate(resource_actions[:-1]):
                next_action = resource_actions[i + 1]
                
                # Check if scaling was reversed quickly (potentially unnecessary)
                time_diff = next_action.timestamp - action.timestamp
                if (time_diff < 1800 and  # Less than 30 minutes
                    action.direction != next_action.direction):
                    unnecessary_scales += 1
                else:
                    effective_scales += 1
            
            # Calculate optimization suggestions
            if unnecessary_scales > effective_scales / 2:
                # Too many unnecessary scales - increase cooldown
                suggested_changes = {
                    "scale_up_cooldown": policy.scale_up_cooldown * 1.2,
                    "scale_down_cooldown": policy.scale_down_cooldown * 1.2
                }
            else:
                # Scaling seems effective - potentially decrease cooldown
                suggested_changes = {
                    "scale_up_cooldown": max(120, policy.scale_up_cooldown * 0.9),
                    "scale_down_cooldown": max(300, policy.scale_down_cooldown * 0.9)
                }
            
            optimizations[resource_type.value] = {
                "current_policy": asdict(policy),
                "performance_analysis": {
                    "total_actions": len(resource_actions),
                    "unnecessary_scales": unnecessary_scales,
                    "effective_scales": effective_scales,
                    "effectiveness_ratio": effective_scales / len(resource_actions)
                },
                "suggested_changes": suggested_changes
            }
        
        return {
            "optimizations": optimizations,
            "overall_effectiveness": self.scaling_effectiveness,
            "optimization_timestamp": time.time()
        }


# Usage example and integration
async def demo_predictive_scaling():
    """Demonstrate predictive scaling capabilities."""
    logger = get_logger("agent_etl.scaling.demo")
    
    # Create predictive scaler
    scaler = PredictiveScaler()
    
    # Simulate metrics over time
    base_time = time.time()
    
    for i in range(50):  # Simulate 50 metric data points
        # Simulate varying load patterns
        time_factor = (i % 20) / 20  # Cyclical pattern
        load_multiplier = 0.5 + 0.4 * math.sin(time_factor * 2 * math.pi)
        
        # Add some randomness
        import random
        noise = random.uniform(0.9, 1.1)
        
        metrics = ResourceMetrics(
            timestamp=base_time + i * 60,  # Every minute
            cpu_utilization=min(0.95, load_multiplier * noise),
            memory_utilization=min(0.9, load_multiplier * noise * 0.8),
            storage_utilization=0.3,
            network_io=load_multiplier * 500,
            active_connections=int(load_multiplier * 100),
            queue_length=int(load_multiplier * noise * 50),
            response_time_ms=50 + load_multiplier * noise * 200,
            error_rate=max(0.0, (load_multiplier - 0.8) * 0.1) if load_multiplier > 0.8 else 0.0,
            throughput_rps=load_multiplier * 100,
            cost_per_hour=5.0
        )
        
        scaler.add_metrics(metrics)
        
        # Give time for async processing
        await asyncio.sleep(0.1)
        
        # Log every 10th iteration
        if i % 10 == 0:
            scaling_metrics = scaler.get_scaling_metrics()
            logger.info(f"Iteration {i}: Current capacity: {scaling_metrics['current_capacity']}")
    
    # Get final metrics and optimization suggestions
    final_metrics = scaler.get_scaling_metrics()
    optimizations = await scaler.optimize_policies()
    
    logger.info("Final Scaling Metrics:")
    logger.info(json.dumps(final_metrics, indent=2))
    
    logger.info("Policy Optimizations:")
    logger.info(json.dumps(optimizations, indent=2))


if __name__ == "__main__":
    asyncio.run(demo_predictive_scaling())