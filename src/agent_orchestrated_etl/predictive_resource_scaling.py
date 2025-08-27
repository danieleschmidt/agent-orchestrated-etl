"""
Predictive Resource Scaling System
ML-driven resource allocation with demand forecasting and autonomous scaling decisions.
"""
import asyncio
import json
import logging
import math
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple, Callable
from dataclasses import dataclass, asdict
import statistics
from pathlib import Path

logger = logging.getLogger(__name__)


class ScalingTrigger(Enum):
    CPU_THRESHOLD = "cpu_threshold"
    MEMORY_THRESHOLD = "memory_threshold"
    QUEUE_LENGTH = "queue_length"
    RESPONSE_TIME = "response_time"
    PREDICTIVE = "predictive"
    SEASONAL = "seasonal"
    ANOMALY = "anomaly"


class ScalingAction(Enum):
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    SCALE_OUT = "scale_out"  # Add instances
    SCALE_IN = "scale_in"   # Remove instances
    REBALANCE = "rebalance"
    NO_ACTION = "no_action"


class ResourceType(Enum):
    CPU = "cpu"
    MEMORY = "memory"
    DISK = "disk"
    NETWORK = "network"
    INSTANCES = "instances"


@dataclass
class ResourceMetrics:
    timestamp: datetime
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    network_io: float
    active_connections: int
    queue_length: int
    response_time: float
    throughput: float
    instance_count: int
    context: Dict[str, Any]


@dataclass
class ScalingDecision:
    trigger: ScalingTrigger
    action: ScalingAction
    resource_type: ResourceType
    current_value: float
    target_value: float
    confidence: float
    reasoning: str
    timestamp: datetime
    context: Dict[str, Any]


@dataclass
class PredictionModel:
    name: str
    accuracy: float
    last_trained: datetime
    training_samples: int
    parameters: Dict[str, Any]
    feature_importance: Dict[str, float]


class PredictiveResourceScaling:
    """ML-driven predictive resource scaling system."""
    
    def __init__(self, service_name: str, config: Optional[Dict[str, Any]] = None):
        self.service_name = service_name
        self.config = config or {}
        
        # Resource tracking
        self.metrics_history: List[ResourceMetrics] = []
        self.scaling_history: List[ScalingDecision] = []
        
        # Prediction models
        self.prediction_models: Dict[str, PredictionModel] = {}
        self.model_accuracy_threshold = self.config.get("model_accuracy_threshold", 0.7)
        
        # Scaling configuration
        self.min_instances = self.config.get("min_instances", 1)
        self.max_instances = self.config.get("max_instances", 20)
        self.scale_cooldown = self.config.get("scale_cooldown", 300)  # 5 minutes
        self.prediction_horizon = self.config.get("prediction_horizon", 1800)  # 30 minutes
        
        # Thresholds
        self.cpu_scale_up_threshold = self.config.get("cpu_scale_up", 0.7)
        self.cpu_scale_down_threshold = self.config.get("cpu_scale_down", 0.3)
        self.memory_scale_up_threshold = self.config.get("memory_scale_up", 0.8)
        self.memory_scale_down_threshold = self.config.get("memory_scale_down", 0.4)
        self.response_time_threshold = self.config.get("response_time_threshold", 2.0)
        
        # Learning parameters
        self.learning_window = self.config.get("learning_window", 1000)
        self.seasonal_patterns = {}
        self.anomaly_baseline = {}
        
        # State tracking
        self.last_scaling_action = None
        self.current_resources = {
            "cpu": 1.0,
            "memory": 1.0,
            "instances": self.min_instances
        }
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()
        
        # Load historical data
        self._load_historical_data()
        
        logger.info(f"Initialized predictive scaling for {service_name}")
    
    def _start_background_tasks(self) -> None:
        """Start background prediction and analysis tasks."""
        loop = asyncio.get_event_loop()
        
        # Model training and updating
        training_task = loop.create_task(self._background_model_training())
        self._background_tasks.append(training_task)
        
        # Prediction generation
        prediction_task = loop.create_task(self._background_prediction())
        self._background_tasks.append(prediction_task)
        
        # Anomaly detection
        anomaly_task = loop.create_task(self._background_anomaly_detection())
        self._background_tasks.append(anomaly_task)
    
    def _load_historical_data(self) -> None:
        """Load historical metrics and scaling data."""
        try:
            history_path = Path(f"reports/scaling_{self.service_name}_history.json")
            if history_path.exists():
                with open(history_path, 'r') as f:
                    data = json.load(f)
                
                # Load metrics history (keep recent data)
                metrics_data = data.get('metrics_history', [])[-500:]  # Last 500 entries
                self.metrics_history = [
                    ResourceMetrics(
                        timestamp=datetime.fromisoformat(m['timestamp']),
                        **{k: v for k, v in m.items() if k != 'timestamp'}
                    )
                    for m in metrics_data
                ]
                
                # Load seasonal patterns
                self.seasonal_patterns = data.get('seasonal_patterns', {})
                self.anomaly_baseline = data.get('anomaly_baseline', {})
                
                logger.info(f"Loaded {len(self.metrics_history)} historical metrics")
        
        except Exception as e:
            logger.warning(f"Failed to load historical scaling data: {e}")
    
    async def record_metrics(self, metrics: ResourceMetrics) -> None:
        """Record current resource metrics for analysis."""
        self.metrics_history.append(metrics)
        
        # Limit history size
        if len(self.metrics_history) > self.learning_window:
            self.metrics_history.pop(0)
        
        # Update seasonal patterns
        self._update_seasonal_patterns(metrics)
        
        # Update anomaly baseline
        self._update_anomaly_baseline(metrics)
        
        # Trigger scaling decision if needed
        scaling_decision = await self._make_scaling_decision(metrics)
        
        if scaling_decision.action != ScalingAction.NO_ACTION:
            await self._execute_scaling_decision(scaling_decision)
    
    async def _make_scaling_decision(self, current_metrics: ResourceMetrics) -> ScalingDecision:
        """Make intelligent scaling decision based on current and predicted metrics."""
        # Get predictions for the next period
        predictions = await self._generate_predictions(current_metrics)
        
        # Analyze current thresholds
        current_triggers = self._analyze_current_thresholds(current_metrics)
        
        # Analyze predictive triggers
        predictive_triggers = self._analyze_predictive_triggers(predictions)
        
        # Combine all triggers and prioritize
        all_triggers = current_triggers + predictive_triggers
        
        if not all_triggers:
            return ScalingDecision(
                trigger=ScalingTrigger.PREDICTIVE,
                action=ScalingAction.NO_ACTION,
                resource_type=ResourceType.CPU,
                current_value=current_metrics.cpu_usage,
                target_value=current_metrics.cpu_usage,
                confidence=0.9,
                reasoning="All metrics within acceptable ranges",
                timestamp=datetime.now(),
                context=current_metrics.context
            )
        
        # Select highest priority trigger
        primary_trigger = max(all_triggers, key=lambda t: t['priority'])
        
        return ScalingDecision(
            trigger=primary_trigger['trigger'],
            action=primary_trigger['action'],
            resource_type=primary_trigger['resource_type'],
            current_value=primary_trigger['current_value'],
            target_value=primary_trigger['target_value'],
            confidence=primary_trigger['confidence'],
            reasoning=primary_trigger['reasoning'],
            timestamp=datetime.now(),
            context=current_metrics.context
        )
    
    def _analyze_current_thresholds(self, metrics: ResourceMetrics) -> List[Dict[str, Any]]:
        """Analyze current metrics against thresholds."""
        triggers = []
        
        # CPU-based scaling
        if metrics.cpu_usage > self.cpu_scale_up_threshold:
            triggers.append({
                'trigger': ScalingTrigger.CPU_THRESHOLD,
                'action': ScalingAction.SCALE_UP,
                'resource_type': ResourceType.CPU,
                'current_value': metrics.cpu_usage,
                'target_value': self.cpu_scale_up_threshold * 0.8,  # Scale to 80% of threshold
                'confidence': min(0.9, (metrics.cpu_usage - self.cpu_scale_up_threshold) * 2),
                'reasoning': f"CPU usage {metrics.cpu_usage:.1%} exceeds scale-up threshold {self.cpu_scale_up_threshold:.1%}",
                'priority': 0.8
            })
        
        elif metrics.cpu_usage < self.cpu_scale_down_threshold:
            # Only scale down if we have multiple instances
            if self.current_resources["instances"] > self.min_instances:
                triggers.append({
                    'trigger': ScalingTrigger.CPU_THRESHOLD,
                    'action': ScalingAction.SCALE_DOWN,
                    'resource_type': ResourceType.CPU,
                    'current_value': metrics.cpu_usage,
                    'target_value': self.cpu_scale_down_threshold * 1.2,
                    'confidence': min(0.8, (self.cpu_scale_down_threshold - metrics.cpu_usage) * 2),
                    'reasoning': f"CPU usage {metrics.cpu_usage:.1%} below scale-down threshold {self.cpu_scale_down_threshold:.1%}",
                    'priority': 0.6
                })
        
        # Memory-based scaling
        if metrics.memory_usage > self.memory_scale_up_threshold:
            triggers.append({
                'trigger': ScalingTrigger.MEMORY_THRESHOLD,
                'action': ScalingAction.SCALE_UP,
                'resource_type': ResourceType.MEMORY,
                'current_value': metrics.memory_usage,
                'target_value': self.memory_scale_up_threshold * 0.8,
                'confidence': min(0.9, (metrics.memory_usage - self.memory_scale_up_threshold) * 2),
                'reasoning': f"Memory usage {metrics.memory_usage:.1%} exceeds threshold {self.memory_scale_up_threshold:.1%}",
                'priority': 0.9  # Memory is more critical
            })
        
        # Response time-based scaling
        if metrics.response_time > self.response_time_threshold:
            triggers.append({
                'trigger': ScalingTrigger.RESPONSE_TIME,
                'action': ScalingAction.SCALE_OUT,
                'resource_type': ResourceType.INSTANCES,
                'current_value': metrics.response_time,
                'target_value': self.response_time_threshold * 0.8,
                'confidence': min(0.9, (metrics.response_time / self.response_time_threshold) - 1),
                'reasoning': f"Response time {metrics.response_time:.2f}s exceeds threshold {self.response_time_threshold:.2f}s",
                'priority': 0.85
            })
        
        # Queue length-based scaling
        if metrics.queue_length > 10:  # Arbitrary threshold
            triggers.append({
                'trigger': ScalingTrigger.QUEUE_LENGTH,
                'action': ScalingAction.SCALE_OUT,
                'resource_type': ResourceType.INSTANCES,
                'current_value': float(metrics.queue_length),
                'target_value': 5.0,
                'confidence': min(0.9, metrics.queue_length / 20),
                'reasoning': f"Queue length {metrics.queue_length} indicates capacity pressure",
                'priority': 0.75
            })
        
        return triggers
    
    def _analyze_predictive_triggers(self, predictions: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Analyze predicted metrics for proactive scaling."""
        triggers = []
        
        if not predictions:
            return triggers
        
        # Predicted CPU scaling
        predicted_cpu = predictions.get('cpu_usage', {})
        if predicted_cpu.get('value', 0) > self.cpu_scale_up_threshold:
            confidence = predicted_cpu.get('confidence', 0.5)
            if confidence > 0.6:  # Only act on high-confidence predictions
                triggers.append({
                    'trigger': ScalingTrigger.PREDICTIVE,
                    'action': ScalingAction.SCALE_UP,
                    'resource_type': ResourceType.CPU,
                    'current_value': predictions.get('current_cpu', 0),
                    'target_value': predicted_cpu['value'] * 0.8,
                    'confidence': confidence,
                    'reasoning': f"Predicted CPU usage {predicted_cpu['value']:.1%} will exceed threshold in {self.prediction_horizon//60} minutes",
                    'priority': 0.7 * confidence
                })
        
        # Predicted memory scaling
        predicted_memory = predictions.get('memory_usage', {})
        if predicted_memory.get('value', 0) > self.memory_scale_up_threshold:
            confidence = predicted_memory.get('confidence', 0.5)
            if confidence > 0.6:
                triggers.append({
                    'trigger': ScalingTrigger.PREDICTIVE,
                    'action': ScalingAction.SCALE_UP,
                    'resource_type': ResourceType.MEMORY,
                    'current_value': predictions.get('current_memory', 0),
                    'target_value': predicted_memory['value'] * 0.8,
                    'confidence': confidence,
                    'reasoning': f"Predicted memory usage {predicted_memory['value']:.1%} will exceed threshold",
                    'priority': 0.8 * confidence
                })
        
        # Seasonal patterns
        seasonal_trigger = self._analyze_seasonal_patterns()
        if seasonal_trigger:
            triggers.append(seasonal_trigger)
        
        return triggers
    
    def _analyze_seasonal_patterns(self) -> Optional[Dict[str, Any]]:
        """Analyze seasonal patterns for proactive scaling."""
        if not self.seasonal_patterns:
            return None
        
        now = datetime.now()
        hour_key = f"hour_{now.hour}"
        weekday_key = f"weekday_{now.weekday()}"
        
        # Check if we're approaching a high-load period
        next_hour_key = f"hour_{(now.hour + 1) % 24}"
        
        current_load = self.seasonal_patterns.get(hour_key, {}).get('avg_cpu', 0.5)
        next_load = self.seasonal_patterns.get(next_hour_key, {}).get('avg_cpu', 0.5)
        
        # If next hour typically has significantly higher load
        if next_load > current_load * 1.3 and next_load > self.cpu_scale_up_threshold:
            return {
                'trigger': ScalingTrigger.SEASONAL,
                'action': ScalingAction.SCALE_UP,
                'resource_type': ResourceType.CPU,
                'current_value': current_load,
                'target_value': next_load * 0.8,
                'confidence': 0.75,
                'reasoning': f"Seasonal pattern indicates {next_load:.1%} CPU load in next hour",
                'priority': 0.65
            }
        
        return None
    
    async def _generate_predictions(self, current_metrics: ResourceMetrics) -> Dict[str, Any]:
        """Generate predictions for resource usage."""
        if len(self.metrics_history) < 10:
            return {}  # Not enough data for predictions
        
        predictions = {}
        
        # Simple trend-based prediction (in production, use actual ML models)
        recent_metrics = self.metrics_history[-10:]
        
        # CPU trend prediction
        cpu_values = [m.cpu_usage for m in recent_metrics]
        cpu_trend = self._calculate_trend(cpu_values)
        predicted_cpu = max(0, min(1, current_metrics.cpu_usage + cpu_trend))
        
        predictions['cpu_usage'] = {
            'value': predicted_cpu,
            'confidence': self._calculate_prediction_confidence(cpu_values, cpu_trend),
            'trend': cpu_trend
        }
        predictions['current_cpu'] = current_metrics.cpu_usage
        
        # Memory trend prediction
        memory_values = [m.memory_usage for m in recent_metrics]
        memory_trend = self._calculate_trend(memory_values)
        predicted_memory = max(0, min(1, current_metrics.memory_usage + memory_trend))
        
        predictions['memory_usage'] = {
            'value': predicted_memory,
            'confidence': self._calculate_prediction_confidence(memory_values, memory_trend),
            'trend': memory_trend
        }
        predictions['current_memory'] = current_metrics.memory_usage
        
        # Response time prediction
        response_values = [m.response_time for m in recent_metrics]
        response_trend = self._calculate_trend(response_values)
        predicted_response = max(0, current_metrics.response_time + response_trend)
        
        predictions['response_time'] = {
            'value': predicted_response,
            'confidence': self._calculate_prediction_confidence(response_values, response_trend),
            'trend': response_trend
        }
        
        return predictions
    
    def _calculate_trend(self, values: List[float]) -> float:
        """Calculate simple linear trend from values."""
        if len(values) < 2:
            return 0.0
        
        # Simple linear regression slope
        n = len(values)
        x = list(range(n))
        
        x_mean = sum(x) / n
        y_mean = sum(values) / n
        
        numerator = sum((x[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
        
        if denominator == 0:
            return 0.0
        
        slope = numerator / denominator
        
        # Project trend forward by prediction horizon (in time units)
        time_units = self.prediction_horizon / 300  # Assume 5-minute intervals
        return slope * time_units
    
    def _calculate_prediction_confidence(self, values: List[float], trend: float) -> float:
        """Calculate confidence in prediction based on data stability."""
        if len(values) < 3:
            return 0.5
        
        # Calculate coefficient of variation (stability measure)
        mean_val = statistics.mean(values)
        if mean_val == 0:
            return 0.5
        
        std_dev = statistics.stdev(values) if len(values) > 1 else 0
        cv = std_dev / mean_val
        
        # Lower CV = higher confidence
        stability_confidence = max(0.1, 1 - cv)
        
        # Trend strength affects confidence
        trend_strength = min(1, abs(trend) * 10)  # Normalize trend
        trend_confidence = 0.5 + (trend_strength * 0.4)
        
        # Combined confidence
        return min(0.95, (stability_confidence + trend_confidence) / 2)
    
    def _update_seasonal_patterns(self, metrics: ResourceMetrics) -> None:
        """Update seasonal pattern tracking."""
        hour_key = f"hour_{metrics.timestamp.hour}"
        weekday_key = f"weekday_{metrics.timestamp.weekday()}"
        
        # Update hourly patterns
        if hour_key not in self.seasonal_patterns:
            self.seasonal_patterns[hour_key] = {
                'count': 0,
                'total_cpu': 0.0,
                'total_memory': 0.0,
                'total_response_time': 0.0
            }
        
        pattern = self.seasonal_patterns[hour_key]
        pattern['count'] += 1
        pattern['total_cpu'] += metrics.cpu_usage
        pattern['total_memory'] += metrics.memory_usage
        pattern['total_response_time'] += metrics.response_time
        
        # Calculate averages
        pattern['avg_cpu'] = pattern['total_cpu'] / pattern['count']
        pattern['avg_memory'] = pattern['total_memory'] / pattern['count']
        pattern['avg_response_time'] = pattern['total_response_time'] / pattern['count']
        
        # Limit pattern data (moving window)
        if pattern['count'] > 100:
            # Reset with current averages
            pattern['count'] = 1
            pattern['total_cpu'] = pattern['avg_cpu']
            pattern['total_memory'] = pattern['avg_memory']
            pattern['total_response_time'] = pattern['avg_response_time']
    
    def _update_anomaly_baseline(self, metrics: ResourceMetrics) -> None:
        """Update anomaly detection baseline."""
        # Simple anomaly baseline using exponential moving average
        alpha = 0.1  # Smoothing factor
        
        for metric_name in ['cpu_usage', 'memory_usage', 'response_time']:
            value = getattr(metrics, metric_name, 0)
            
            if metric_name not in self.anomaly_baseline:
                self.anomaly_baseline[metric_name] = {
                    'mean': value,
                    'variance': 0.0,
                    'count': 1
                }
            else:
                baseline = self.anomaly_baseline[metric_name]
                
                # Update exponential moving average
                old_mean = baseline['mean']
                baseline['mean'] = alpha * value + (1 - alpha) * old_mean
                
                # Update variance estimate
                diff = value - old_mean
                baseline['variance'] = (1 - alpha) * baseline['variance'] + alpha * (diff ** 2)
                baseline['count'] += 1
    
    def _detect_anomalies(self, metrics: ResourceMetrics) -> List[Dict[str, Any]]:
        """Detect anomalies in current metrics."""
        anomalies = []
        
        for metric_name in ['cpu_usage', 'memory_usage', 'response_time']:
            if metric_name not in self.anomaly_baseline:
                continue
            
            baseline = self.anomaly_baseline[metric_name]
            if baseline['count'] < 10:  # Need sufficient baseline data
                continue
            
            value = getattr(metrics, metric_name, 0)
            mean = baseline['mean']
            std_dev = math.sqrt(baseline['variance'])
            
            if std_dev > 0:
                z_score = abs(value - mean) / std_dev
                
                # Anomaly if beyond 3 standard deviations
                if z_score > 3:
                    anomalies.append({
                        'metric': metric_name,
                        'value': value,
                        'expected': mean,
                        'z_score': z_score,
                        'severity': 'high' if z_score > 4 else 'medium'
                    })
        
        return anomalies
    
    async def _execute_scaling_decision(self, decision: ScalingDecision) -> None:
        """Execute scaling decision (simulated - integrate with actual infrastructure)."""
        # Check cooldown period
        if (self.last_scaling_action and 
            (datetime.now() - self.last_scaling_action).total_seconds() < self.scale_cooldown):
            logger.info(f"Scaling action suppressed due to cooldown period")
            return
        
        # Record the decision
        self.scaling_history.append(decision)
        self.last_scaling_action = datetime.now()
        
        # Simulate scaling action
        if decision.action == ScalingAction.SCALE_UP:
            if decision.resource_type == ResourceType.CPU:
                self.current_resources["cpu"] = min(4.0, self.current_resources["cpu"] * 1.5)
            elif decision.resource_type == ResourceType.MEMORY:
                self.current_resources["memory"] = min(4.0, self.current_resources["memory"] * 1.5)
        
        elif decision.action == ScalingAction.SCALE_DOWN:
            if decision.resource_type == ResourceType.CPU:
                self.current_resources["cpu"] = max(0.5, self.current_resources["cpu"] * 0.7)
            elif decision.resource_type == ResourceType.MEMORY:
                self.current_resources["memory"] = max(0.5, self.current_resources["memory"] * 0.7)
        
        elif decision.action == ScalingAction.SCALE_OUT:
            if self.current_resources["instances"] < self.max_instances:
                self.current_resources["instances"] += 1
        
        elif decision.action == ScalingAction.SCALE_IN:
            if self.current_resources["instances"] > self.min_instances:
                self.current_resources["instances"] -= 1
        
        logger.info(f"Executed scaling decision: {decision.action.value} for "
                   f"{decision.resource_type.value} (confidence: {decision.confidence:.2f})")
        logger.info(f"Reasoning: {decision.reasoning}")
        logger.info(f"Current resources: {self.current_resources}")
        
        # Save scaling history periodically
        if len(self.scaling_history) % 10 == 0:
            await self._save_historical_data()
    
    async def _background_model_training(self) -> None:
        """Background task for training and updating prediction models."""
        while True:
            try:
                await asyncio.sleep(1800)  # Train every 30 minutes
                
                if len(self.metrics_history) >= 50:
                    await self._train_prediction_models()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in model training: {e}")
    
    async def _train_prediction_models(self) -> None:
        """Train prediction models on historical data."""
        # Simplified model training (in production, use actual ML libraries)
        training_data = self.metrics_history[-200:]  # Use recent data
        
        if len(training_data) < 20:
            return
        
        # Train CPU prediction model
        cpu_model = await self._train_simple_model(
            [m.cpu_usage for m in training_data],
            "cpu_usage"
        )
        
        if cpu_model:
            self.prediction_models["cpu_usage"] = cpu_model
        
        # Train memory prediction model
        memory_model = await self._train_simple_model(
            [m.memory_usage for m in training_data],
            "memory_usage"
        )
        
        if memory_model:
            self.prediction_models["memory_usage"] = memory_model
        
        logger.info(f"Updated {len(self.prediction_models)} prediction models")
    
    async def _train_simple_model(self, values: List[float], metric_name: str) -> Optional[PredictionModel]:
        """Train a simple prediction model."""
        if len(values) < 10:
            return None
        
        # Calculate simple statistics
        mean_value = statistics.mean(values)
        std_dev = statistics.stdev(values) if len(values) > 1 else 0
        
        # Calculate trend
        trend = self._calculate_trend(values)
        
        # Estimate accuracy based on stability
        cv = std_dev / mean_value if mean_value > 0 else 1.0
        accuracy = max(0.1, 1 - cv)
        
        return PredictionModel(
            name=metric_name,
            accuracy=accuracy,
            last_trained=datetime.now(),
            training_samples=len(values),
            parameters={
                'mean': mean_value,
                'std_dev': std_dev,
                'trend': trend
            },
            feature_importance={
                'trend': 0.6,
                'seasonal': 0.3,
                'baseline': 0.1
            }
        )
    
    async def _background_prediction(self) -> None:
        """Background task for generating predictions."""
        while True:
            try:
                await asyncio.sleep(300)  # Generate predictions every 5 minutes
                
                if self.metrics_history:
                    current_metrics = self.metrics_history[-1]
                    predictions = await self._generate_predictions(current_metrics)
                    
                    # Log predictions for monitoring
                    if predictions:
                        logger.debug(f"Generated predictions: {predictions}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in prediction generation: {e}")
    
    async def _background_anomaly_detection(self) -> None:
        """Background task for anomaly detection."""
        while True:
            try:
                await asyncio.sleep(180)  # Check anomalies every 3 minutes
                
                if self.metrics_history:
                    current_metrics = self.metrics_history[-1]
                    anomalies = self._detect_anomalies(current_metrics)
                    
                    for anomaly in anomalies:
                        logger.warning(f"Anomaly detected in {anomaly['metric']}: "
                                     f"value={anomaly['value']:.3f}, "
                                     f"expected={anomaly['expected']:.3f}, "
                                     f"z_score={anomaly['z_score']:.2f}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in anomaly detection: {e}")
    
    async def _save_historical_data(self) -> None:
        """Save historical data for persistence."""
        try:
            # Convert metrics to serializable format
            metrics_data = []
            for metric in self.metrics_history[-500:]:  # Keep last 500
                metric_dict = asdict(metric)
                metric_dict['timestamp'] = metric.timestamp.isoformat()
                metrics_data.append(metric_dict)
            
            # Convert scaling history
            scaling_data = []
            for decision in self.scaling_history[-100:]:  # Keep last 100
                decision_dict = asdict(decision)
                decision_dict['timestamp'] = decision.timestamp.isoformat()
                decision_dict['trigger'] = decision.trigger.value
                decision_dict['action'] = decision.action.value
                decision_dict['resource_type'] = decision.resource_type.value
                scaling_data.append(decision_dict)
            
            data = {
                'service_name': self.service_name,
                'metrics_history': metrics_data,
                'scaling_history': scaling_data,
                'seasonal_patterns': self.seasonal_patterns,
                'anomaly_baseline': self.anomaly_baseline,
                'current_resources': self.current_resources,
                'last_updated': datetime.now().isoformat()
            }
            
            history_path = Path(f"reports/scaling_{self.service_name}_history.json")
            history_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(history_path, 'w') as f:
                json.dump(data, f, indent=2)
            
        except Exception as e:
            logger.warning(f"Failed to save scaling historical data: {e}")
    
    def get_scaling_stats(self) -> Dict[str, Any]:
        """Get comprehensive scaling statistics and insights."""
        if not self.metrics_history:
            return {"status": "no_data", "message": "No metrics recorded"}
        
        recent_metrics = self.metrics_history[-20:] if len(self.metrics_history) >= 20 else self.metrics_history
        
        # Calculate average metrics
        avg_cpu = statistics.mean(m.cpu_usage for m in recent_metrics)
        avg_memory = statistics.mean(m.memory_usage for m in recent_metrics)
        avg_response_time = statistics.mean(m.response_time for m in recent_metrics)
        
        # Scaling statistics
        total_scaling_actions = len(self.scaling_history)
        scale_up_actions = sum(1 for d in self.scaling_history if d.action in [ScalingAction.SCALE_UP, ScalingAction.SCALE_OUT])
        scale_down_actions = sum(1 for d in self.scaling_history if d.action in [ScalingAction.SCALE_DOWN, ScalingAction.SCALE_IN])
        
        return {
            "service_name": self.service_name,
            "current_resources": self.current_resources.copy(),
            "resource_utilization": {
                "avg_cpu_usage": avg_cpu,
                "avg_memory_usage": avg_memory,
                "avg_response_time": avg_response_time,
                "current_instances": self.current_resources["instances"]
            },
            "scaling_activity": {
                "total_scaling_actions": total_scaling_actions,
                "scale_up_actions": scale_up_actions,
                "scale_down_actions": scale_down_actions,
                "last_scaling_action": self.last_scaling_action.isoformat() if self.last_scaling_action else None
            },
            "prediction_models": {
                name: {
                    "accuracy": model.accuracy,
                    "last_trained": model.last_trained.isoformat(),
                    "training_samples": model.training_samples
                }
                for name, model in self.prediction_models.items()
            },
            "learning_insights": {
                "seasonal_patterns_learned": len(self.seasonal_patterns),
                "anomaly_baselines": len(self.anomaly_baseline),
                "metrics_history_size": len(self.metrics_history),
                "prediction_accuracy": statistics.mean(
                    m.accuracy for m in self.prediction_models.values()
                ) if self.prediction_models else 0.0
            },
            "configuration": {
                "min_instances": self.min_instances,
                "max_instances": self.max_instances,
                "prediction_horizon_minutes": self.prediction_horizon // 60,
                "scale_cooldown_minutes": self.scale_cooldown // 60
            }
        }
    
    async def close(self) -> None:
        """Close scaling system and clean up resources."""
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Save final state
        await self._save_historical_data()
        
        logger.info(f"Closed predictive scaling for {self.service_name}")


class ResourceScalingManager:
    """Manager for multiple predictive resource scaling instances."""
    
    def __init__(self):
        self.scaling_instances: Dict[str, PredictiveResourceScaling] = {}
    
    def get_scaling_instance(self, service_name: str, 
                           config: Optional[Dict[str, Any]] = None) -> PredictiveResourceScaling:
        """Get or create scaling instance for a service."""
        if service_name not in self.scaling_instances:
            self.scaling_instances[service_name] = PredictiveResourceScaling(service_name, config)
        return self.scaling_instances[service_name]
    
    async def get_all_scaling_stats(self) -> Dict[str, Any]:
        """Get scaling statistics for all services."""
        stats = {}
        total_instances = 0
        total_scaling_actions = 0
        
        for name, scaling in self.scaling_instances.items():
            service_stats = scaling.get_scaling_stats()
            stats[name] = service_stats
            
            if service_stats.get("status") != "no_data":
                total_instances += service_stats["current_resources"]["instances"]
                total_scaling_actions += service_stats["scaling_activity"]["total_scaling_actions"]
        
        # Add global summary
        stats["_global_summary"] = {
            "total_services": len(self.scaling_instances),
            "total_instances": total_instances,
            "total_scaling_actions": total_scaling_actions,
            "avg_prediction_accuracy": statistics.mean(
                stats[name]["learning_insights"]["prediction_accuracy"]
                for name in stats if name != "_global_summary" and stats[name].get("status") != "no_data"
            ) if len(stats) > 1 else 0.0
        }
        
        return stats
    
    async def close_all(self) -> None:
        """Close all scaling instances."""
        for scaling in self.scaling_instances.values():
            await scaling.close()
        self.scaling_instances.clear()


# Global scaling manager
scaling_manager = ResourceScalingManager()


# Convenience functions
def get_predictive_scaling(service_name: str, 
                         config: Optional[Dict[str, Any]] = None) -> PredictiveResourceScaling:
    """Get predictive resource scaling instance."""
    return scaling_manager.get_scaling_instance(service_name, config)


async def get_all_scaling_stats() -> Dict[str, Any]:
    """Get scaling statistics for all services."""
    return await scaling_manager.get_all_scaling_stats()