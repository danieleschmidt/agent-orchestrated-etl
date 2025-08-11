"""Self-Healing Infrastructure for Autonomous ETL Systems.

This module implements an advanced self-healing infrastructure that automatically
detects, diagnoses, and recovers from system failures. It provides:

- Automatic failure detection using ML-based anomaly detection
- Predictive maintenance with time-series forecasting
- Automated recovery mechanisms and rollback strategies
- Adaptive healing strategies that learn from past incidents
- Circuit breaker patterns for cascading failure prevention
- Health monitoring and system observability
"""

from __future__ import annotations

import asyncio
import json
import math
import random
import time
import traceback
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union, Tuple
import uuid

from .logging_config import get_logger, LogContext, TimedOperation, get_performance_logger
from .exceptions import (
    AgentETLException, PipelineExecutionException, ResourceExhaustedException,
    ErrorCategory, ErrorSeverity
)
from .ai_resource_allocation import LSTMResourcePredictor


class FailureType(Enum):
    """Types of failures that can occur in the system."""
    
    TASK_FAILURE = "task_failure"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    NETWORK_FAILURE = "network_failure"
    DATA_CORRUPTION = "data_corruption"
    DEPENDENCY_FAILURE = "dependency_failure"
    TIMEOUT = "timeout"
    CONFIGURATION_ERROR = "configuration_error"
    EXTERNAL_SERVICE_FAILURE = "external_service_failure"
    SECURITY_BREACH = "security_breach"
    CASCADING_FAILURE = "cascading_failure"
    PERFORMANCE_DEGRADATION = "performance_degradation"
    MEMORY_LEAK = "memory_leak"
    DISK_FULL = "disk_full"


class HealthStatus(Enum):
    """Health status levels for system components."""
    
    HEALTHY = "healthy"
    WARNING = "warning"
    DEGRADED = "degraded"
    CRITICAL = "critical"
    FAILED = "failed"
    RECOVERING = "recovering"
    MAINTENANCE = "maintenance"


class HealingStrategy(Enum):
    """Available healing strategies."""
    
    RESTART = "restart"
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    ROLLBACK = "rollback"
    FAILOVER = "failover"
    CIRCUIT_BREAKER = "circuit_breaker"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    DATA_RECOVERY = "data_recovery"
    CONFIGURATION_RESET = "configuration_reset"
    QUARANTINE = "quarantine"
    LOAD_BALANCE = "load_balance"
    RESOURCE_REALLOCATION = "resource_reallocation"


@dataclass
class FailureEvent:
    """Represents a failure event in the system."""
    
    event_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    failure_type: FailureType = FailureType.TASK_FAILURE
    severity: ErrorSeverity = ErrorSeverity.MEDIUM
    component: str = ""
    description: str = ""
    timestamp: float = field(default_factory=time.time)
    
    # Contextual information
    stack_trace: Optional[str] = None
    error_data: Dict[str, Any] = field(default_factory=dict)
    affected_resources: List[str] = field(default_factory=list)
    upstream_dependencies: List[str] = field(default_factory=list)
    downstream_dependencies: List[str] = field(default_factory=list)
    
    # Recovery information
    recovery_attempted: bool = False
    healing_strategy: Optional[HealingStrategy] = None
    recovery_time: Optional[float] = None
    recovery_success: bool = False
    
    # Learning data
    similar_failures: List[str] = field(default_factory=list)
    prediction_accuracy: Optional[float] = None
    prevention_possible: bool = False


@dataclass
class ComponentHealth:
    """Health metrics for a system component."""
    
    component_id: str
    health_status: HealthStatus = HealthStatus.HEALTHY
    last_check: float = field(default_factory=time.time)
    
    # Performance metrics
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    disk_usage: float = 0.0
    network_io: float = 0.0
    error_rate: float = 0.0
    response_time: float = 0.0
    throughput: float = 0.0
    
    # Reliability metrics
    uptime: float = 1.0
    availability: float = 1.0
    failure_count: int = 0
    last_failure: Optional[float] = None
    
    # Predictive metrics
    degradation_trend: float = 0.0  # -1 to 1, negative means improving
    predicted_failure_time: Optional[float] = None
    maintenance_due: Optional[float] = None
    
    # Thresholds
    warning_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "cpu_usage": 0.7,
        "memory_usage": 0.8,
        "disk_usage": 0.85,
        "error_rate": 0.05,
        "response_time": 1000.0  # milliseconds
    })
    
    critical_thresholds: Dict[str, float] = field(default_factory=lambda: {
        "cpu_usage": 0.9,
        "memory_usage": 0.95,
        "disk_usage": 0.95,
        "error_rate": 0.1,
        "response_time": 5000.0  # milliseconds
    })


@dataclass
class HealingAction:
    """Represents a healing action taken by the system."""
    
    action_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    strategy: HealingStrategy = HealingStrategy.RESTART
    target_component: str = ""
    triggered_by: str = ""  # failure_event_id
    timestamp: float = field(default_factory=time.time)
    
    # Action parameters
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout: float = 300.0  # seconds
    
    # Execution tracking
    status: str = "pending"  # pending, executing, completed, failed
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    error_message: Optional[str] = None
    
    # Impact assessment
    affected_components: List[str] = field(default_factory=list)
    expected_downtime: float = 0.0
    actual_downtime: Optional[float] = None
    success_probability: float = 0.8
    risk_level: str = "low"  # low, medium, high
    
    # Learning feedback
    effectiveness_score: Optional[float] = None
    side_effects: List[str] = field(default_factory=list)
    lessons_learned: List[str] = field(default_factory=list)


class AnomalyDetector:
    """ML-based anomaly detection for system health monitoring."""
    
    def __init__(self, window_size: int = 100, sensitivity: float = 0.8):
        self.logger = get_logger("self_healing.anomaly_detector")
        self.window_size = window_size
        self.sensitivity = sensitivity
        
        # Historical data storage
        self.metric_history: Dict[str, deque] = defaultdict(lambda: deque(maxlen=window_size))
        self.baseline_models: Dict[str, Dict[str, float]] = {}
        
        # Detection parameters
        self.std_dev_threshold = 2.5  # Standard deviations for anomaly
        self.trend_threshold = 0.1    # Significant trend change
        self.min_samples = 10         # Minimum samples for detection
        
        # Learning parameters
        self.learning_rate = 0.1
        self.adaptation_period = 50   # Samples before model update
        
    def add_metric(self, component_id: str, metric_name: str, value: float, timestamp: float) -> None:
        """Add a new metric value for anomaly detection."""
        
        metric_key = f"{component_id}:{metric_name}"
        
        data_point = {
            "value": value,
            "timestamp": timestamp,
            "component": component_id,
            "metric": metric_name
        }
        
        self.metric_history[metric_key].append(data_point)
        
        # Update baseline model if we have enough data
        if len(self.metric_history[metric_key]) >= self.min_samples:
            self._update_baseline_model(metric_key)
    
    def detect_anomalies(self, component_id: str, metric_name: str) -> Dict[str, Any]:
        """Detect anomalies in the specified metric."""
        
        metric_key = f"{component_id}:{metric_name}"
        history = list(self.metric_history[metric_key])
        
        if len(history) < self.min_samples:
            return {"anomaly_detected": False, "reason": "insufficient_data"}
        
        latest_value = history[-1]["value"]
        
        # Statistical anomaly detection
        statistical_anomaly = self._detect_statistical_anomaly(metric_key, latest_value)
        
        # Trend anomaly detection
        trend_anomaly = self._detect_trend_anomaly(metric_key)
        
        # Pattern anomaly detection
        pattern_anomaly = self._detect_pattern_anomaly(metric_key)
        
        # Combine detection results
        anomaly_detected = any([
            statistical_anomaly["anomaly"],
            trend_anomaly["anomaly"],
            pattern_anomaly["anomaly"]
        ])
        
        anomaly_score = max([
            statistical_anomaly["score"],
            trend_anomaly["score"],
            pattern_anomaly["score"]
        ])
        
        result = {
            "anomaly_detected": anomaly_detected,
            "anomaly_score": anomaly_score,
            "detection_methods": {
                "statistical": statistical_anomaly,
                "trend": trend_anomaly,
                "pattern": pattern_anomaly
            },
            "severity": self._calculate_anomaly_severity(anomaly_score),
            "recommendation": self._get_anomaly_recommendation(anomaly_score, metric_name)
        }
        
        if anomaly_detected:
            self.logger.warning(
                f"Anomaly detected in {metric_key}",
                extra={
                    "component": component_id,
                    "metric": metric_name,
                    "value": latest_value,
                    "anomaly_score": anomaly_score,
                    "detection_result": result
                }
            )
        
        return result
    
    def _detect_statistical_anomaly(self, metric_key: str, value: float) -> Dict[str, Any]:
        """Detect statistical anomalies using z-score."""
        
        if metric_key not in self.baseline_models:
            return {"anomaly": False, "score": 0.0, "method": "z_score"}
        
        baseline = self.baseline_models[metric_key]
        mean = baseline.get("mean", value)
        std_dev = baseline.get("std_dev", 0.1)
        
        # Calculate z-score
        if std_dev > 0:
            z_score = abs(value - mean) / std_dev
        else:
            z_score = 0.0
        
        anomaly_detected = z_score > self.std_dev_threshold
        anomaly_score = min(1.0, z_score / (self.std_dev_threshold * 2))
        
        return {
            "anomaly": anomaly_detected,
            "score": anomaly_score,
            "z_score": z_score,
            "threshold": self.std_dev_threshold,
            "method": "z_score"
        }
    
    def _detect_trend_anomaly(self, metric_key: str) -> Dict[str, Any]:
        """Detect anomalies in metric trends."""
        
        history = list(self.metric_history[metric_key])
        if len(history) < 5:  # Need at least 5 points for trend analysis
            return {"anomaly": False, "score": 0.0, "method": "trend"}
        
        # Calculate recent trend
        recent_values = [dp["value"] for dp in history[-5:]]
        recent_trend = self._calculate_trend(recent_values)
        
        # Calculate baseline trend
        if len(history) >= 10:
            baseline_values = [dp["value"] for dp in history[-20:-5]]
            baseline_trend = self._calculate_trend(baseline_values)
        else:
            baseline_trend = 0.0
        
        # Check for significant trend change
        trend_change = abs(recent_trend - baseline_trend)
        anomaly_detected = trend_change > self.trend_threshold
        anomaly_score = min(1.0, trend_change / (self.trend_threshold * 2))
        
        return {
            "anomaly": anomaly_detected,
            "score": anomaly_score,
            "recent_trend": recent_trend,
            "baseline_trend": baseline_trend,
            "trend_change": trend_change,
            "method": "trend"
        }
    
    def _detect_pattern_anomaly(self, metric_key: str) -> Dict[str, Any]:
        """Detect anomalies in metric patterns (simplified)."""
        
        history = list(self.metric_history[metric_key])
        if len(history) < 20:
            return {"anomaly": False, "score": 0.0, "method": "pattern"}
        
        # Analyze recent pattern vs historical pattern
        recent_values = [dp["value"] for dp in history[-10:]]
        historical_values = [dp["value"] for dp in history[-50:-10]]
        
        # Simple pattern comparison using coefficient of variation
        recent_cv = self._coefficient_of_variation(recent_values)
        historical_cv = self._coefficient_of_variation(historical_values)
        
        # Check for significant pattern change
        pattern_change = abs(recent_cv - historical_cv)
        anomaly_detected = pattern_change > 0.5  # Threshold for pattern change
        anomaly_score = min(1.0, pattern_change)
        
        return {
            "anomaly": anomaly_detected,
            "score": anomaly_score,
            "recent_cv": recent_cv,
            "historical_cv": historical_cv,
            "pattern_change": pattern_change,
            "method": "pattern"
        }
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate linear trend of values."""
        if len(values) < 2:
            return 0.0
        
        n = len(values)
        x = list(range(n))
        y = values
        
        # Simple linear regression slope
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(x[i] * y[i] for i in range(n))
        sum_x2 = sum(x[i] * x[i] for i in range(n))
        
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return 0.0
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        return slope
    
    def _coefficient_of_variation(self, values: List[float]) -> float:
        """Calculate coefficient of variation."""
        if not values:
            return 0.0
        
        mean_val = sum(values) / len(values)
        if mean_val == 0:
            return 0.0
        
        variance = sum((x - mean_val) ** 2 for x in values) / len(values)
        std_dev = math.sqrt(variance)
        
        return std_dev / mean_val
    
    def _update_baseline_model(self, metric_key: str) -> None:
        """Update baseline model for the metric."""
        
        history = list(self.metric_history[metric_key])
        values = [dp["value"] for dp in history]
        
        # Calculate statistical properties
        mean_val = sum(values) / len(values)
        variance = sum((x - mean_val) ** 2 for x in values) / len(values)
        std_dev = math.sqrt(variance)
        
        # Update baseline model with exponential moving average
        if metric_key in self.baseline_models:
            old_model = self.baseline_models[metric_key]
            alpha = self.learning_rate
            
            new_mean = alpha * mean_val + (1 - alpha) * old_model.get("mean", mean_val)
            new_std_dev = alpha * std_dev + (1 - alpha) * old_model.get("std_dev", std_dev)
        else:
            new_mean = mean_val
            new_std_dev = std_dev
        
        self.baseline_models[metric_key] = {
            "mean": new_mean,
            "std_dev": max(0.01, new_std_dev),  # Minimum std_dev to avoid division by zero
            "last_update": time.time(),
            "sample_count": len(values)
        }
    
    def _calculate_anomaly_severity(self, anomaly_score: float) -> ErrorSeverity:
        """Calculate severity level based on anomaly score."""
        if anomaly_score >= 0.8:
            return ErrorSeverity.CRITICAL
        elif anomaly_score >= 0.6:
            return ErrorSeverity.HIGH
        elif anomaly_score >= 0.3:
            return ErrorSeverity.MEDIUM
        else:
            return ErrorSeverity.LOW
    
    def _get_anomaly_recommendation(self, anomaly_score: float, metric_name: str) -> str:
        """Get recommendation based on anomaly score and metric type."""
        
        if anomaly_score < 0.3:
            return "monitor"
        
        recommendations = {
            "cpu_usage": "Consider scaling up or optimizing CPU-intensive tasks",
            "memory_usage": "Check for memory leaks or consider increasing memory allocation",
            "disk_usage": "Clean up temporary files or increase disk space",
            "error_rate": "Investigate error causes and implement fixes",
            "response_time": "Check for performance bottlenecks and optimize",
            "network_io": "Investigate network issues or reduce network traffic"
        }
        
        return recommendations.get(metric_name, "Investigate the anomaly and take appropriate action")


class PredictiveMaintenanceEngine:
    """Predictive maintenance using time-series forecasting."""
    
    def __init__(self):
        self.logger = get_logger("self_healing.predictive_maintenance")
        
        # Time series predictor for different metrics
        self.predictors: Dict[str, LSTMResourcePredictor] = {}
        
        # Maintenance models
        self.failure_probability_models: Dict[str, Dict[str, Any]] = {}
        self.maintenance_schedules: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        
        # Prediction parameters
        self.prediction_horizon = 24  # hours
        self.maintenance_threshold = 0.7  # probability threshold for maintenance
        self.early_warning_threshold = 0.4  # early warning threshold
        
    def add_metric_data(
        self, 
        component_id: str, 
        metric_name: str, 
        value: float, 
        timestamp: float
    ) -> None:
        """Add metric data for predictive analysis."""
        
        predictor_key = f"{component_id}:{metric_name}"
        
        if predictor_key not in self.predictors:
            self.predictors[predictor_key] = LSTMResourcePredictor(sequence_length=24)
        
        self.predictors[predictor_key].add_historical_data(
            predictor_key, timestamp, value, {"component": component_id, "metric": metric_name}
        )
    
    def predict_failures(self, component_id: str) -> Dict[str, Any]:
        """Predict potential failures for a component."""
        
        predictions = {}
        failure_probabilities = {}
        
        # Predict each metric
        for predictor_key, predictor in self.predictors.items():
            if predictor_key.startswith(f"{component_id}:"):
                metric_name = predictor_key.split(":", 1)[1]
                
                try:
                    forecast = predictor.predict_demand(
                        predictor_key, forecast_horizon=self.prediction_horizon
                    )
                    
                    predictions[metric_name] = forecast
                    
                    # Calculate failure probability based on forecast
                    failure_prob = self._calculate_failure_probability(metric_name, forecast)
                    failure_probabilities[metric_name] = failure_prob
                    
                except Exception as e:
                    self.logger.warning(f"Failed to predict {predictor_key}: {e}")
        
        # Overall component failure probability
        overall_failure_probability = max(failure_probabilities.values()) if failure_probabilities else 0.0
        
        # Generate maintenance recommendations
        maintenance_recommendations = self._generate_maintenance_recommendations(
            component_id, predictions, failure_probabilities
        )
        
        result = {
            "component_id": component_id,
            "overall_failure_probability": overall_failure_probability,
            "metric_predictions": predictions,
            "failure_probabilities": failure_probabilities,
            "maintenance_recommendations": maintenance_recommendations,
            "prediction_timestamp": time.time(),
            "prediction_horizon_hours": self.prediction_horizon
        }
        
        # Log high-risk predictions
        if overall_failure_probability > self.early_warning_threshold:
            self.logger.warning(
                f"High failure risk predicted for {component_id}",
                extra={
                    "component": component_id,
                    "failure_probability": overall_failure_probability,
                    "recommendations": maintenance_recommendations
                }
            )
        
        return result
    
    def _calculate_failure_probability(
        self, 
        metric_name: str, 
        forecast: Any  # ResourceDemandForecast type from ai_resource_allocation
    ) -> float:
        """Calculate failure probability based on metric forecast."""
        
        # Define critical thresholds for different metrics
        critical_thresholds = {
            "cpu_usage": 0.95,
            "memory_usage": 0.98,
            "disk_usage": 0.98,
            "error_rate": 0.15,
            "response_time": 10000.0  # 10 seconds
        }
        
        threshold = critical_thresholds.get(metric_name, 1.0)
        
        # Calculate probability based on predicted values
        predicted_values = forecast.predicted_demands
        if not predicted_values:
            return 0.0
        
        # Count how many predicted values exceed threshold
        threshold_violations = sum(1 for value in predicted_values if value > threshold)
        violation_ratio = threshold_violations / len(predicted_values)
        
        # Factor in trend and anomaly score
        trend_factor = 1.0
        if forecast.trend == "increasing":
            trend_factor = 1.2
        elif forecast.trend == "decreasing":
            trend_factor = 0.8
        
        anomaly_factor = 1 + forecast.anomaly_score
        
        # Calculate final probability
        base_probability = violation_ratio
        adjusted_probability = base_probability * trend_factor * anomaly_factor
        
        return min(1.0, adjusted_probability)
    
    def _generate_maintenance_recommendations(
        self,
        component_id: str,
        predictions: Dict[str, Any],
        failure_probabilities: Dict[str, float]
    ) -> List[Dict[str, Any]]:
        """Generate maintenance recommendations based on predictions."""
        
        recommendations = []
        
        for metric_name, probability in failure_probabilities.items():
            if probability > self.maintenance_threshold:
                # High-priority maintenance needed
                recommendations.append({
                    "priority": "high",
                    "metric": metric_name,
                    "failure_probability": probability,
                    "recommended_action": self._get_maintenance_action(metric_name),
                    "urgency": "immediate" if probability > 0.9 else "within_24_hours",
                    "estimated_downtime": self._estimate_maintenance_downtime(metric_name),
                    "preventive": True
                })
            elif probability > self.early_warning_threshold:
                # Preventive maintenance recommended
                recommendations.append({
                    "priority": "medium",
                    "metric": metric_name,
                    "failure_probability": probability,
                    "recommended_action": self._get_preventive_action(metric_name),
                    "urgency": "within_week",
                    "estimated_downtime": self._estimate_maintenance_downtime(metric_name) * 0.5,
                    "preventive": True
                })
        
        # Sort by priority and probability
        recommendations.sort(
            key=lambda x: (x["priority"] == "high", x["failure_probability"]), 
            reverse=True
        )
        
        return recommendations
    
    def _get_maintenance_action(self, metric_name: str) -> str:
        """Get maintenance action for high-risk metric."""
        actions = {
            "cpu_usage": "Scale up CPU resources or optimize CPU-intensive processes",
            "memory_usage": "Increase memory allocation or investigate memory leaks",
            "disk_usage": "Clean up disk space or add additional storage",
            "error_rate": "Investigate and fix error sources",
            "response_time": "Optimize performance bottlenecks"
        }
        return actions.get(metric_name, f"Investigate and address {metric_name} issues")
    
    def _get_preventive_action(self, metric_name: str) -> str:
        """Get preventive action for medium-risk metric."""
        actions = {
            "cpu_usage": "Monitor CPU usage and prepare for scaling",
            "memory_usage": "Review memory usage patterns and optimize if needed",
            "disk_usage": "Schedule disk cleanup and monitor growth",
            "error_rate": "Review error logs and implement preventive measures",
            "response_time": "Review performance metrics and identify optimization opportunities"
        }
        return actions.get(metric_name, f"Monitor {metric_name} closely and prepare for intervention")
    
    def _estimate_maintenance_downtime(self, metric_name: str) -> float:
        """Estimate maintenance downtime in minutes."""
        downtimes = {
            "cpu_usage": 15.0,    # CPU scaling
            "memory_usage": 20.0,  # Memory optimization
            "disk_usage": 30.0,    # Disk cleanup
            "error_rate": 45.0,    # Error investigation and fixing
            "response_time": 30.0  # Performance optimization
        }
        return downtimes.get(metric_name, 30.0)
    
    def schedule_maintenance(
        self, 
        component_id: str, 
        maintenance_action: str, 
        priority: str,
        estimated_downtime: float
    ) -> str:
        """Schedule a maintenance action."""
        
        maintenance_id = str(uuid.uuid4())
        
        maintenance_item = {
            "maintenance_id": maintenance_id,
            "component_id": component_id,
            "action": maintenance_action,
            "priority": priority,
            "estimated_downtime": estimated_downtime,
            "scheduled_at": time.time(),
            "status": "scheduled",
            "execution_window": self._calculate_execution_window(priority)
        }
        
        self.maintenance_schedules[component_id].append(maintenance_item)
        
        self.logger.info(
            f"Scheduled maintenance for {component_id}",
            extra={
                "maintenance_id": maintenance_id,
                "action": maintenance_action,
                "priority": priority,
                "estimated_downtime": estimated_downtime
            }
        )
        
        return maintenance_id
    
    def _calculate_execution_window(self, priority: str) -> Dict[str, float]:
        """Calculate optimal execution window for maintenance."""
        current_time = time.time()
        
        if priority == "high":
            # Execute within next 4 hours
            start_time = current_time + 1800  # 30 minutes from now
            end_time = current_time + 14400   # 4 hours from now
        elif priority == "medium":
            # Execute within next 24 hours, prefer low-usage periods
            start_time = current_time + 3600   # 1 hour from now
            end_time = current_time + 86400    # 24 hours from now
        else:
            # Execute within next week
            start_time = current_time + 86400  # 24 hours from now
            end_time = current_time + 604800   # 1 week from now
        
        return {
            "earliest_start": start_time,
            "latest_end": end_time,
            "preferred_start": self._find_preferred_maintenance_time(start_time, end_time)
        }
    
    def _find_preferred_maintenance_time(self, start_time: float, end_time: float) -> float:
        """Find preferred maintenance time based on usage patterns."""
        # Simplified: prefer early morning hours (2-6 AM)
        # In production, this would analyze historical usage patterns
        
        current_time = time.time()
        preferred_hour = 3  # 3 AM
        
        # Find next occurrence of preferred hour within window
        import datetime
        
        start_dt = datetime.datetime.fromtimestamp(start_time)
        preferred_dt = start_dt.replace(hour=preferred_hour, minute=0, second=0, microsecond=0)
        
        # If preferred time is in the past or too early, move to next day
        if preferred_dt.timestamp() < start_time:
            preferred_dt += datetime.timedelta(days=1)
        
        # If preferred time is beyond window, use start of window
        if preferred_dt.timestamp() > end_time:
            return start_time
        
        return preferred_dt.timestamp()


class CircuitBreaker:
    """Circuit breaker pattern implementation for failure isolation."""
    
    def __init__(
        self, 
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 3
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        # Circuit state
        self.state = "closed"  # closed, open, half_open
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        
        # Monitoring
        self.total_requests = 0
        self.total_failures = 0
        self.state_changes = []
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker."""
        
        self.total_requests += 1
        
        # Check circuit state
        if self.state == "open":
            if time.time() - self.last_failure_time > self.recovery_timeout:
                self.state = "half_open"
                self.success_count = 0
                self._log_state_change("half_open")
            else:
                raise Exception("Circuit breaker is OPEN - request rejected")
        
        try:
            # Execute the function
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)
            
            # Success handling
            self._handle_success()
            return result
            
        except Exception as e:
            # Failure handling
            self._handle_failure()
            raise e
    
    def _handle_success(self):
        """Handle successful execution."""
        
        if self.state == "half_open":
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = "closed"
                self.failure_count = 0
                self._log_state_change("closed")
        else:
            self.failure_count = max(0, self.failure_count - 1)
    
    def _handle_failure(self):
        """Handle failed execution."""
        
        self.failure_count += 1
        self.total_failures += 1
        self.last_failure_time = time.time()
        
        if self.state == "closed" or self.state == "half_open":
            if self.failure_count >= self.failure_threshold:
                self.state = "open"
                self._log_state_change("open")
    
    def _log_state_change(self, new_state: str):
        """Log circuit breaker state change."""
        self.state_changes.append({
            "timestamp": time.time(),
            "old_state": self.state,
            "new_state": new_state,
            "failure_count": self.failure_count,
            "success_count": self.success_count
        })
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get circuit breaker metrics."""
        return {
            "state": self.state,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "total_requests": self.total_requests,
            "total_failures": self.total_failures,
            "failure_rate": self.total_failures / self.total_requests if self.total_requests > 0 else 0.0,
            "last_failure_time": self.last_failure_time,
            "state_changes": len(self.state_changes)
        }


class SelfHealingOrchestrator:
    """Main orchestrator for self-healing infrastructure."""
    
    def __init__(self):
        self.logger = get_logger("self_healing.orchestrator")
        self.performance_logger = get_performance_logger()
        
        # Core components
        self.anomaly_detector = AnomalyDetector()
        self.predictive_maintenance = PredictiveMaintenanceEngine()
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        
        # System state
        self.component_health: Dict[str, ComponentHealth] = {}
        self.failure_history: deque = deque(maxlen=1000)
        self.healing_actions: Dict[str, HealingAction] = {}
        
        # Configuration
        self.monitoring_interval = 60.0  # seconds
        self.health_check_interval = 30.0  # seconds
        self.healing_enabled = True
        self.learning_enabled = True
        
        # Learning and adaptation
        self.healing_effectiveness: Dict[HealingStrategy, deque] = defaultdict(lambda: deque(maxlen=100))
        self.failure_patterns: Dict[str, int] = defaultdict(int)
        self.component_failure_rates: Dict[str, float] = defaultdict(float)
        
        # Background tasks
        self.monitoring_task: Optional[asyncio.Task] = None
        self.health_check_task: Optional[asyncio.Task] = None
        
    async def start_self_healing(self) -> None:
        """Start the self-healing system."""
        
        if self.monitoring_task is not None:
            return  # Already running
        
        self.healing_enabled = True
        
        # Start background monitoring tasks
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.health_check_task = asyncio.create_task(self._health_check_loop())
        
        self.logger.info("Self-healing system started")
    
    async def stop_self_healing(self) -> None:
        """Stop the self-healing system."""
        
        self.healing_enabled = False
        
        # Cancel background tasks
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
            self.monitoring_task = None
        
        if self.health_check_task:
            self.health_check_task.cancel()
            try:
                await self.health_check_task
            except asyncio.CancelledError:
                pass
            self.health_check_task = None
        
        self.logger.info("Self-healing system stopped")
    
    def register_component(self, component_id: str, **health_config) -> None:
        """Register a component for health monitoring."""
        
        health = ComponentHealth(component_id=component_id)
        
        # Override default thresholds if provided
        if "warning_thresholds" in health_config:
            health.warning_thresholds.update(health_config["warning_thresholds"])
        
        if "critical_thresholds" in health_config:
            health.critical_thresholds.update(health_config["critical_thresholds"])
        
        self.component_health[component_id] = health
        
        # Create circuit breaker for component
        self.circuit_breakers[component_id] = CircuitBreaker()
        
        self.logger.info(f"Registered component for health monitoring: {component_id}")
    
    def report_metric(
        self, 
        component_id: str, 
        metric_name: str, 
        value: float,
        timestamp: Optional[float] = None
    ) -> None:
        """Report a metric value for a component."""
        
        if timestamp is None:
            timestamp = time.time()
        
        # Update component health
        if component_id in self.component_health:
            health = self.component_health[component_id]
            setattr(health, metric_name, value)
            health.last_check = timestamp
        
        # Add to anomaly detector
        self.anomaly_detector.add_metric(component_id, metric_name, value, timestamp)
        
        # Add to predictive maintenance
        self.predictive_maintenance.add_metric_data(component_id, metric_name, value, timestamp)
        
        # Check for immediate anomalies
        anomaly_result = self.anomaly_detector.detect_anomalies(component_id, metric_name)
        if anomaly_result["anomaly_detected"]:
            asyncio.create_task(self._handle_anomaly_detection(component_id, metric_name, anomaly_result))
    
    async def report_failure(
        self,
        component_id: str,
        failure_type: FailureType,
        description: str,
        error_data: Optional[Dict[str, Any]] = None,
        stack_trace: Optional[str] = None
    ) -> str:
        """Report a failure event."""
        
        failure_event = FailureEvent(
            failure_type=failure_type,
            component=component_id,
            description=description,
            error_data=error_data or {},
            stack_trace=stack_trace
        )
        
        # Determine severity based on failure type
        failure_event.severity = self._determine_failure_severity(failure_type, error_data)
        
        # Add to failure history
        self.failure_history.append(failure_event)
        
        # Update failure patterns for learning
        self.failure_patterns[f"{component_id}:{failure_type.value}"] += 1
        
        # Update component health status
        if component_id in self.component_health:
            health = self.component_health[component_id]
            health.failure_count += 1
            health.last_failure = time.time()
            
            # Update health status based on severity
            if failure_event.severity == ErrorSeverity.CRITICAL:
                health.health_status = HealthStatus.FAILED
            elif failure_event.severity == ErrorSeverity.HIGH:
                health.health_status = HealthStatus.CRITICAL
            else:
                health.health_status = HealthStatus.DEGRADED
        
        # Log the failure
        self.logger.error(
            f"Failure reported: {component_id} - {failure_type.value}",
            extra={
                "event_id": failure_event.event_id,
                "component": component_id,
                "failure_type": failure_type.value,
                "severity": failure_event.severity.value,
                "description": description,
                "error_data": error_data
            }
        )
        
        # Trigger healing if enabled
        if self.healing_enabled:
            await self._trigger_healing(failure_event)
        
        return failure_event.event_id
    
    async def _monitoring_loop(self) -> None:
        """Background monitoring loop."""
        
        while self.healing_enabled:
            try:
                # Check component health
                await self._check_all_components()
                
                # Run predictive maintenance
                await self._run_predictive_maintenance()
                
                # Clean up old data
                self._cleanup_old_data()
                
                await asyncio.sleep(self.monitoring_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                await asyncio.sleep(self.monitoring_interval)
    
    async def _health_check_loop(self) -> None:
        """Background health check loop."""
        
        while self.healing_enabled:
            try:
                # Perform active health checks
                await self._perform_health_checks()
                
                await asyncio.sleep(self.health_check_interval)
                
            except Exception as e:
                self.logger.error(f"Error in health check loop: {e}", exc_info=True)
                await asyncio.sleep(self.health_check_interval)
    
    async def _check_all_components(self) -> None:
        """Check health of all registered components."""
        
        for component_id, health in self.component_health.items():
            # Check metric thresholds
            await self._check_component_thresholds(component_id, health)
            
            # Update health status
            self._update_health_status(health)
    
    async def _check_component_thresholds(self, component_id: str, health: ComponentHealth) -> None:
        """Check if component metrics exceed thresholds."""
        
        metrics = ["cpu_usage", "memory_usage", "disk_usage", "error_rate", "response_time"]
        
        for metric in metrics:
            value = getattr(health, metric, 0.0)
            
            # Check critical threshold
            if value > health.critical_thresholds.get(metric, 1.0):
                await self._handle_threshold_violation(
                    component_id, metric, value, "critical", health.critical_thresholds[metric]
                )
            
            # Check warning threshold
            elif value > health.warning_thresholds.get(metric, 1.0):
                await self._handle_threshold_violation(
                    component_id, metric, value, "warning", health.warning_thresholds[metric]
                )
    
    async def _handle_threshold_violation(
        self,
        component_id: str,
        metric: str,
        value: float,
        severity: str,
        threshold: float
    ) -> None:
        """Handle metric threshold violation."""
        
        self.logger.warning(
            f"Threshold violation: {component_id}.{metric} = {value} exceeds {severity} threshold {threshold}"
        )
        
        # Create synthetic failure event for threshold violation
        failure_type = FailureType.PERFORMANCE_DEGRADATION
        if metric == "memory_usage" and severity == "critical":
            failure_type = FailureType.RESOURCE_EXHAUSTION
        elif metric == "disk_usage" and severity == "critical":
            failure_type = FailureType.DISK_FULL
        
        description = f"{metric} threshold violation: {value} > {threshold}"
        
        await self.report_failure(
            component_id=component_id,
            failure_type=failure_type,
            description=description,
            error_data={
                "metric": metric,
                "value": value,
                "threshold": threshold,
                "severity": severity
            }
        )
    
    def _update_health_status(self, health: ComponentHealth) -> None:
        """Update overall health status based on metrics."""
        
        current_time = time.time()
        
        # Check if component is stale (no recent updates)
        if current_time - health.last_check > 300:  # 5 minutes
            health.health_status = HealthStatus.FAILED
            return
        
        # Calculate health score based on metrics
        health_score = 1.0
        
        # CPU health
        cpu_ratio = health.cpu_usage / health.critical_thresholds.get("cpu_usage", 1.0)
        health_score *= max(0.0, 1.0 - cpu_ratio)
        
        # Memory health
        memory_ratio = health.memory_usage / health.critical_thresholds.get("memory_usage", 1.0)
        health_score *= max(0.0, 1.0 - memory_ratio)
        
        # Error rate health
        error_ratio = health.error_rate / health.critical_thresholds.get("error_rate", 1.0)
        health_score *= max(0.0, 1.0 - error_ratio)
        
        # Update status based on health score
        if health_score >= 0.8:
            health.health_status = HealthStatus.HEALTHY
        elif health_score >= 0.6:
            health.health_status = HealthStatus.WARNING
        elif health_score >= 0.3:
            health.health_status = HealthStatus.DEGRADED
        else:
            health.health_status = HealthStatus.CRITICAL
    
    async def _run_predictive_maintenance(self) -> None:
        """Run predictive maintenance analysis."""
        
        for component_id in self.component_health.keys():
            try:
                predictions = self.predictive_maintenance.predict_failures(component_id)
                
                # Schedule maintenance if needed
                for recommendation in predictions["maintenance_recommendations"]:
                    if recommendation["priority"] == "high":
                        maintenance_id = self.predictive_maintenance.schedule_maintenance(
                            component_id=component_id,
                            maintenance_action=recommendation["recommended_action"],
                            priority=recommendation["priority"],
                            estimated_downtime=recommendation["estimated_downtime"]
                        )
                        
                        self.logger.info(f"Scheduled high-priority maintenance: {maintenance_id}")
                
            except Exception as e:
                self.logger.warning(f"Predictive maintenance failed for {component_id}: {e}")
    
    async def _perform_health_checks(self) -> None:
        """Perform active health checks on components."""
        
        # This would perform actual health checks
        # For now, we'll simulate by checking component responsiveness
        
        for component_id, health in self.component_health.items():
            try:
                # Simulate health check
                health_check_success = await self._simulate_health_check(component_id)
                
                if health_check_success:
                    # Update uptime
                    health.uptime = min(1.0, health.uptime + 0.01)
                    
                    # If component was failed, mark as recovering
                    if health.health_status == HealthStatus.FAILED:
                        health.health_status = HealthStatus.RECOVERING
                
                else:
                    # Health check failed
                    health.uptime = max(0.0, health.uptime - 0.05)
                    
                    if health.health_status in [HealthStatus.HEALTHY, HealthStatus.WARNING]:
                        health.health_status = HealthStatus.DEGRADED
                
            except Exception as e:
                self.logger.warning(f"Health check failed for {component_id}: {e}")
    
    async def _simulate_health_check(self, component_id: str) -> bool:
        """Simulate a health check (replace with actual implementation)."""
        
        # Simulate network latency
        await asyncio.sleep(0.1)
        
        # Simulate health check response (90% success rate for simulation)
        return random.random() > 0.1
    
    async def _handle_anomaly_detection(
        self,
        component_id: str,
        metric_name: str,
        anomaly_result: Dict[str, Any]
    ) -> None:
        """Handle detected anomaly."""
        
        severity = anomaly_result["severity"]
        
        # Create failure event for anomaly
        failure_event = FailureEvent(
            failure_type=FailureType.PERFORMANCE_DEGRADATION,
            severity=severity,
            component=component_id,
            description=f"Anomaly detected in {metric_name}",
            error_data={
                "metric": metric_name,
                "anomaly_result": anomaly_result
            }
        )
        
        self.failure_history.append(failure_event)
        
        # Trigger healing for high-severity anomalies
        if severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            await self._trigger_healing(failure_event)
    
    async def _trigger_healing(self, failure_event: FailureEvent) -> None:
        """Trigger healing actions for a failure event."""
        
        if not self.healing_enabled:
            return
        
        # Determine healing strategy
        healing_strategy = self._select_healing_strategy(failure_event)
        
        if healing_strategy is None:
            self.logger.info(f"No healing strategy available for failure: {failure_event.event_id}")
            return
        
        # Create healing action
        healing_action = HealingAction(
            strategy=healing_strategy,
            target_component=failure_event.component,
            triggered_by=failure_event.event_id
        )
        
        # Configure healing parameters
        self._configure_healing_parameters(healing_action, failure_event)
        
        # Execute healing action
        try:
            await self._execute_healing_action(healing_action)
            failure_event.recovery_attempted = True
            failure_event.healing_strategy = healing_strategy
            
        except Exception as e:
            self.logger.error(f"Healing action failed: {e}", exc_info=True)
    
    def _select_healing_strategy(self, failure_event: FailureEvent) -> Optional[HealingStrategy]:
        """Select appropriate healing strategy for failure."""
        
        failure_type = failure_event.failure_type
        severity = failure_event.severity
        
        # Strategy selection logic
        strategy_map = {
            FailureType.TASK_FAILURE: HealingStrategy.RETRY_WITH_BACKOFF,
            FailureType.RESOURCE_EXHAUSTION: HealingStrategy.SCALE_UP,
            FailureType.MEMORY_LEAK: HealingStrategy.RESTART,
            FailureType.DISK_FULL: HealingStrategy.RESOURCE_REALLOCATION,
            FailureType.NETWORK_FAILURE: HealingStrategy.FAILOVER,
            FailureType.EXTERNAL_SERVICE_FAILURE: HealingStrategy.CIRCUIT_BREAKER,
            FailureType.PERFORMANCE_DEGRADATION: HealingStrategy.SCALE_UP,
            FailureType.TIMEOUT: HealingStrategy.RETRY_WITH_BACKOFF,
            FailureType.CONFIGURATION_ERROR: HealingStrategy.CONFIGURATION_RESET,
            FailureType.CASCADING_FAILURE: HealingStrategy.CIRCUIT_BREAKER,
        }
        
        base_strategy = strategy_map.get(failure_type)
        
        # Adjust strategy based on severity
        if severity == ErrorSeverity.CRITICAL:
            if base_strategy == HealingStrategy.RETRY_WITH_BACKOFF:
                return HealingStrategy.RESTART
            elif base_strategy == HealingStrategy.SCALE_UP:
                return HealingStrategy.FAILOVER
        
        # Check historical effectiveness
        if base_strategy and self.learning_enabled:
            effectiveness_scores = list(self.healing_effectiveness[base_strategy])
            if effectiveness_scores and sum(effectiveness_scores) / len(effectiveness_scores) < 0.5:
                # Try alternative strategy if base strategy has low effectiveness
                alternatives = {
                    HealingStrategy.RETRY_WITH_BACKOFF: HealingStrategy.RESTART,
                    HealingStrategy.SCALE_UP: HealingStrategy.RESOURCE_REALLOCATION,
                    HealingStrategy.RESTART: HealingStrategy.FAILOVER,
                }
                return alternatives.get(base_strategy, base_strategy)
        
        return base_strategy
    
    def _configure_healing_parameters(self, healing_action: HealingAction, failure_event: FailureEvent) -> None:
        """Configure parameters for healing action."""
        
        strategy = healing_action.strategy
        
        if strategy == HealingStrategy.RESTART:
            healing_action.parameters = {
                "graceful_shutdown": True,
                "timeout": 60.0,
                "backup_data": True
            }
            healing_action.expected_downtime = 30.0
            healing_action.risk_level = "medium"
            
        elif strategy == HealingStrategy.SCALE_UP:
            healing_action.parameters = {
                "scale_factor": 1.5,
                "min_instances": 1,
                "max_instances": 10
            }
            healing_action.expected_downtime = 0.0
            healing_action.risk_level = "low"
            
        elif strategy == HealingStrategy.RETRY_WITH_BACKOFF:
            healing_action.parameters = {
                "max_retries": 3,
                "initial_delay": 1.0,
                "backoff_multiplier": 2.0,
                "max_delay": 60.0
            }
            healing_action.expected_downtime = 0.0
            healing_action.risk_level = "low"
            
        elif strategy == HealingStrategy.FAILOVER:
            healing_action.parameters = {
                "backup_component": f"{failure_event.component}_backup",
                "data_sync_required": True
            }
            healing_action.expected_downtime = 10.0
            healing_action.risk_level = "medium"
            
        elif strategy == HealingStrategy.CIRCUIT_BREAKER:
            healing_action.parameters = {
                "failure_threshold": 5,
                "recovery_timeout": 60.0
            }
            healing_action.expected_downtime = 0.0
            healing_action.risk_level = "low"
            
        # Set success probability based on historical data
        healing_action.success_probability = self._calculate_success_probability(strategy, failure_event)
    
    def _calculate_success_probability(
        self, 
        strategy: HealingStrategy, 
        failure_event: FailureEvent
    ) -> float:
        """Calculate success probability for healing strategy."""
        
        # Base probabilities
        base_probabilities = {
            HealingStrategy.RESTART: 0.9,
            HealingStrategy.SCALE_UP: 0.8,
            HealingStrategy.RETRY_WITH_BACKOFF: 0.7,
            HealingStrategy.FAILOVER: 0.95,
            HealingStrategy.CIRCUIT_BREAKER: 0.85,
            HealingStrategy.RESOURCE_REALLOCATION: 0.75,
            HealingStrategy.CONFIGURATION_RESET: 0.8,
        }
        
        base_prob = base_probabilities.get(strategy, 0.6)
        
        # Adjust based on historical effectiveness
        if self.learning_enabled and strategy in self.healing_effectiveness:
            effectiveness_scores = list(self.healing_effectiveness[strategy])
            if effectiveness_scores:
                historical_effectiveness = sum(effectiveness_scores) / len(effectiveness_scores)
                # Blend base probability with historical effectiveness
                base_prob = (base_prob + historical_effectiveness) / 2
        
        # Adjust based on failure severity
        if failure_event.severity == ErrorSeverity.CRITICAL:
            base_prob *= 0.8  # Harder to heal critical failures
        elif failure_event.severity == ErrorSeverity.LOW:
            base_prob *= 1.1  # Easier to heal minor failures
        
        return min(1.0, max(0.1, base_prob))
    
    async def _execute_healing_action(self, healing_action: HealingAction) -> None:
        """Execute a healing action."""
        
        healing_action.status = "executing"
        healing_action.started_at = time.time()
        
        self.healing_actions[healing_action.action_id] = healing_action
        
        try:
            with TimedOperation(f"healing_{healing_action.strategy.value}", self.performance_logger):
                success = await self._perform_healing_strategy(healing_action)
                
                healing_action.completed_at = time.time()
                
                if success:
                    healing_action.status = "completed"
                    healing_action.effectiveness_score = 1.0
                    
                    # Update component health status
                    if healing_action.target_component in self.component_health:
                        health = self.component_health[healing_action.target_component]
                        health.health_status = HealthStatus.RECOVERING
                    
                    self.logger.info(
                        f"Healing action completed successfully: {healing_action.action_id}",
                        extra={
                            "strategy": healing_action.strategy.value,
                            "component": healing_action.target_component,
                            "duration": healing_action.completed_at - healing_action.started_at
                        }
                    )
                    
                else:
                    healing_action.status = "failed"
                    healing_action.effectiveness_score = 0.0
                    
                    self.logger.error(
                        f"Healing action failed: {healing_action.action_id}",
                        extra={
                            "strategy": healing_action.strategy.value,
                            "component": healing_action.target_component
                        }
                    )
                
                # Record effectiveness for learning
                if self.learning_enabled:
                    self.healing_effectiveness[healing_action.strategy].append(
                        healing_action.effectiveness_score
                    )
                
        except Exception as e:
            healing_action.status = "failed"
            healing_action.error_message = str(e)
            healing_action.completed_at = time.time()
            healing_action.effectiveness_score = 0.0
            
            self.logger.error(f"Healing action execution failed: {e}", exc_info=True)
            
            # Record failure for learning
            if self.learning_enabled:
                self.healing_effectiveness[healing_action.strategy].append(0.0)
    
    async def _perform_healing_strategy(self, healing_action: HealingAction) -> bool:
        """Perform the actual healing strategy (implementation specific)."""
        
        strategy = healing_action.strategy
        component = healing_action.target_component
        parameters = healing_action.parameters
        
        self.logger.info(
            f"Executing healing strategy: {strategy.value} for {component}",
            extra={"parameters": parameters}
        )
        
        # Simulate healing actions (replace with actual implementations)
        if strategy == HealingStrategy.RESTART:
            return await self._perform_restart(component, parameters)
            
        elif strategy == HealingStrategy.SCALE_UP:
            return await self._perform_scale_up(component, parameters)
            
        elif strategy == HealingStrategy.RETRY_WITH_BACKOFF:
            return await self._perform_retry_with_backoff(component, parameters)
            
        elif strategy == HealingStrategy.FAILOVER:
            return await self._perform_failover(component, parameters)
            
        elif strategy == HealingStrategy.CIRCUIT_BREAKER:
            return await self._perform_circuit_breaker(component, parameters)
            
        elif strategy == HealingStrategy.RESOURCE_REALLOCATION:
            return await self._perform_resource_reallocation(component, parameters)
            
        elif strategy == HealingStrategy.CONFIGURATION_RESET:
            return await self._perform_configuration_reset(component, parameters)
            
        else:
            self.logger.warning(f"Unknown healing strategy: {strategy.value}")
            return False
    
    async def _perform_restart(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Perform component restart."""
        # Simulate restart process
        self.logger.info(f"Restarting component: {component}")
        
        graceful = parameters.get("graceful_shutdown", True)
        timeout = parameters.get("timeout", 60.0)
        
        if graceful:
            # Simulate graceful shutdown
            await asyncio.sleep(2.0)
        
        # Simulate restart
        await asyncio.sleep(3.0)
        
        # 90% success rate for restart
        success = random.random() > 0.1
        
        if success:
            self.logger.info(f"Component restart successful: {component}")
            
            # Update component health
            if component in self.component_health:
                health = self.component_health[component]
                health.error_rate = 0.0
                health.health_status = HealthStatus.HEALTHY
        
        return success
    
    async def _perform_scale_up(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Perform component scale up."""
        self.logger.info(f"Scaling up component: {component}")
        
        scale_factor = parameters.get("scale_factor", 1.5)
        
        # Simulate scaling process
        await asyncio.sleep(1.0)
        
        # 85% success rate for scaling
        success = random.random() > 0.15
        
        if success:
            self.logger.info(f"Component scale up successful: {component} (factor: {scale_factor})")
            
            # Update component health
            if component in self.component_health:
                health = self.component_health[component]
                health.cpu_usage *= 0.7  # Reduce CPU usage due to scaling
                health.memory_usage *= 0.8  # Reduce memory pressure
        
        return success
    
    async def _perform_retry_with_backoff(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Perform retry with exponential backoff."""
        max_retries = parameters.get("max_retries", 3)
        initial_delay = parameters.get("initial_delay", 1.0)
        backoff_multiplier = parameters.get("backoff_multiplier", 2.0)
        
        delay = initial_delay
        
        for retry in range(max_retries):
            self.logger.info(f"Retry attempt {retry + 1}/{max_retries} for {component}")
            
            # Simulate operation
            await asyncio.sleep(0.5)
            
            # Increasing success probability with each retry
            success_prob = 0.3 + (retry * 0.2)
            if random.random() < success_prob:
                self.logger.info(f"Retry successful for {component}")
                return True
            
            if retry < max_retries - 1:
                await asyncio.sleep(delay)
                delay *= backoff_multiplier
        
        self.logger.warning(f"All retry attempts failed for {component}")
        return False
    
    async def _perform_failover(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Perform failover to backup component."""
        backup_component = parameters.get("backup_component", f"{component}_backup")
        
        self.logger.info(f"Performing failover from {component} to {backup_component}")
        
        # Simulate failover process
        await asyncio.sleep(2.0)
        
        # 95% success rate for failover
        success = random.random() > 0.05
        
        if success:
            self.logger.info(f"Failover successful: {component} -> {backup_component}")
            
            # Update health status
            if component in self.component_health:
                self.component_health[component].health_status = HealthStatus.MAINTENANCE
            
            # Add backup component if not exists
            if backup_component not in self.component_health:
                self.register_component(backup_component)
                self.component_health[backup_component].health_status = HealthStatus.HEALTHY
        
        return success
    
    async def _perform_circuit_breaker(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Activate circuit breaker for component."""
        failure_threshold = parameters.get("failure_threshold", 5)
        recovery_timeout = parameters.get("recovery_timeout", 60.0)
        
        self.logger.info(f"Activating circuit breaker for {component}")
        
        # Update circuit breaker configuration
        if component in self.circuit_breakers:
            circuit_breaker = self.circuit_breakers[component]
            circuit_breaker.failure_threshold = failure_threshold
            circuit_breaker.recovery_timeout = recovery_timeout
            circuit_breaker.state = "open"
        
        return True  # Circuit breaker activation always succeeds
    
    async def _perform_resource_reallocation(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Perform resource reallocation."""
        self.logger.info(f"Reallocating resources for {component}")
        
        # Simulate resource reallocation
        await asyncio.sleep(1.5)
        
        # 75% success rate for resource reallocation
        success = random.random() > 0.25
        
        if success:
            self.logger.info(f"Resource reallocation successful for {component}")
            
            # Update resource usage
            if component in self.component_health:
                health = self.component_health[component]
                health.disk_usage *= 0.6  # Simulate disk cleanup
                health.memory_usage *= 0.9  # Optimize memory usage
        
        return success
    
    async def _perform_configuration_reset(self, component: str, parameters: Dict[str, Any]) -> bool:
        """Reset component configuration to defaults."""
        self.logger.info(f"Resetting configuration for {component}")
        
        # Simulate configuration reset
        await asyncio.sleep(1.0)
        
        # 80% success rate for configuration reset
        success = random.random() > 0.2
        
        if success:
            self.logger.info(f"Configuration reset successful for {component}")
            
            # Reset error rate
            if component in self.component_health:
                self.component_health[component].error_rate = 0.0
        
        return success
    
    def _determine_failure_severity(
        self, 
        failure_type: FailureType, 
        error_data: Optional[Dict[str, Any]]
    ) -> ErrorSeverity:
        """Determine severity of failure based on type and context."""
        
        # Base severity mapping
        severity_map = {
            FailureType.TASK_FAILURE: ErrorSeverity.MEDIUM,
            FailureType.RESOURCE_EXHAUSTION: ErrorSeverity.HIGH,
            FailureType.NETWORK_FAILURE: ErrorSeverity.MEDIUM,
            FailureType.DATA_CORRUPTION: ErrorSeverity.CRITICAL,
            FailureType.DEPENDENCY_FAILURE: ErrorSeverity.HIGH,
            FailureType.TIMEOUT: ErrorSeverity.MEDIUM,
            FailureType.CONFIGURATION_ERROR: ErrorSeverity.MEDIUM,
            FailureType.EXTERNAL_SERVICE_FAILURE: ErrorSeverity.MEDIUM,
            FailureType.SECURITY_BREACH: ErrorSeverity.CRITICAL,
            FailureType.CASCADING_FAILURE: ErrorSeverity.CRITICAL,
            FailureType.PERFORMANCE_DEGRADATION: ErrorSeverity.LOW,
            FailureType.MEMORY_LEAK: ErrorSeverity.HIGH,
            FailureType.DISK_FULL: ErrorSeverity.HIGH,
        }
        
        base_severity = severity_map.get(failure_type, ErrorSeverity.MEDIUM)
        
        # Adjust based on error data
        if error_data:
            # Check for critical indicators
            if error_data.get("severity") == "critical":
                return ErrorSeverity.CRITICAL
            
            # Check metric values
            if "value" in error_data and "threshold" in error_data:
                ratio = error_data["value"] / error_data["threshold"]
                if ratio > 2.0:  # More than 2x threshold
                    return ErrorSeverity.CRITICAL
                elif ratio > 1.5:  # More than 1.5x threshold
                    return ErrorSeverity.HIGH
        
        return base_severity
    
    def _cleanup_old_data(self) -> None:
        """Clean up old monitoring and failure data."""
        
        current_time = time.time()
        cutoff_time = current_time - 86400 * 7  # 7 days
        
        # Clean up old healing actions
        old_actions = [
            action_id for action_id, action in self.healing_actions.items()
            if action.timestamp < cutoff_time
        ]
        
        for action_id in old_actions:
            del self.healing_actions[action_id]
        
        # Clean up old baseline models in anomaly detector
        for metric_key in list(self.anomaly_detector.baseline_models.keys()):
            model = self.anomaly_detector.baseline_models[metric_key]
            if model.get("last_update", 0) < cutoff_time:
                del self.anomaly_detector.baseline_models[metric_key]
    
    def get_system_health_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        
        total_components = len(self.component_health)
        if total_components == 0:
            return {"overall_status": "unknown", "components": {}}
        
        # Count components by health status
        status_counts = defaultdict(int)
        for health in self.component_health.values():
            status_counts[health.health_status.value] += 1
        
        # Determine overall system health
        if status_counts["failed"] > 0:
            overall_status = "critical"
        elif status_counts["critical"] > 0:
            overall_status = "degraded"
        elif status_counts["degraded"] > 0 or status_counts["warning"] > 0:
            overall_status = "warning"
        else:
            overall_status = "healthy"
        
        return {
            "overall_status": overall_status,
            "total_components": total_components,
            "status_distribution": dict(status_counts),
            "components": {
                comp_id: {
                    "health_status": health.health_status.value,
                    "uptime": health.uptime,
                    "last_check": health.last_check,
                    "failure_count": health.failure_count,
                    "key_metrics": {
                        "cpu_usage": health.cpu_usage,
                        "memory_usage": health.memory_usage,
                        "error_rate": health.error_rate
                    }
                }
                for comp_id, health in self.component_health.items()
            },
            "healing_enabled": self.healing_enabled,
            "total_failures": len(self.failure_history),
            "total_healing_actions": len(self.healing_actions),
            "timestamp": time.time()
        }
    
    def get_healing_statistics(self) -> Dict[str, Any]:
        """Get healing system statistics."""
        
        # Calculate strategy effectiveness
        strategy_effectiveness = {}
        for strategy, scores in self.healing_effectiveness.items():
            if scores:
                avg_effectiveness = sum(scores) / len(scores)
                strategy_effectiveness[strategy.value] = {
                    "average_effectiveness": avg_effectiveness,
                    "total_attempts": len(scores),
                    "success_rate": sum(1 for score in scores if score > 0.5) / len(scores)
                }
        
        # Calculate failure patterns
        top_failure_patterns = sorted(
            self.failure_patterns.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        # Calculate circuit breaker statistics
        circuit_breaker_stats = {}
        for component_id, cb in self.circuit_breakers.items():
            circuit_breaker_stats[component_id] = cb.get_metrics()
        
        return {
            "healing_enabled": self.healing_enabled,
            "total_failures": len(self.failure_history),
            "total_healing_actions": len(self.healing_actions),
            "strategy_effectiveness": strategy_effectiveness,
            "top_failure_patterns": top_failure_patterns,
            "circuit_breaker_stats": circuit_breaker_stats,
            "predictive_maintenance": {
                "components_monitored": len(self.predictive_maintenance.predictors),
                "maintenance_scheduled": sum(
                    len(schedules) for schedules in self.predictive_maintenance.maintenance_schedules.values()
                )
            },
            "anomaly_detection": {
                "metrics_monitored": len(self.anomaly_detector.metric_history),
                "baseline_models": len(self.anomaly_detector.baseline_models)
            }
        }


# Global self-healing orchestrator instance
_self_healing_orchestrator: Optional[SelfHealingOrchestrator] = None


def get_self_healing_orchestrator() -> SelfHealingOrchestrator:
    """Get the global self-healing orchestrator instance."""
    global _self_healing_orchestrator
    if _self_healing_orchestrator is None:
        _self_healing_orchestrator = SelfHealingOrchestrator()
    return _self_healing_orchestrator


async def start_self_healing() -> None:
    """Start the global self-healing system."""
    orchestrator = get_self_healing_orchestrator()
    await orchestrator.start_self_healing()


async def stop_self_healing() -> None:
    """Stop the global self-healing system."""
    orchestrator = get_self_healing_orchestrator()
    await orchestrator.stop_self_healing()


def register_component_for_healing(component_id: str, **health_config) -> None:
    """Register a component for self-healing monitoring."""
    orchestrator = get_self_healing_orchestrator()
    orchestrator.register_component(component_id, **health_config)


def report_component_metric(
    component_id: str, 
    metric_name: str, 
    value: float,
    timestamp: Optional[float] = None
) -> None:
    """Report a component metric for health monitoring."""
    orchestrator = get_self_healing_orchestrator()
    orchestrator.report_metric(component_id, metric_name, value, timestamp)


async def report_component_failure(
    component_id: str,
    failure_type: FailureType,
    description: str,
    error_data: Optional[Dict[str, Any]] = None,
    stack_trace: Optional[str] = None
) -> str:
    """Report a component failure for healing."""
    orchestrator = get_self_healing_orchestrator()
    return await orchestrator.report_failure(
        component_id, failure_type, description, error_data, stack_trace
    )


def get_system_health() -> Dict[str, Any]:
    """Get overall system health status."""
    orchestrator = get_self_healing_orchestrator()
    return orchestrator.get_system_health_status()


def get_healing_stats() -> Dict[str, Any]:
    """Get self-healing system statistics."""
    orchestrator = get_self_healing_orchestrator()
    return orchestrator.get_healing_statistics()