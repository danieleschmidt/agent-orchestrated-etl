"""
Intelligent auto-scaling system for agent-orchestrated-etl.
Uses machine learning to predict workload patterns and optimize resource allocation.
"""

import asyncio
import json
import logging
import pickle
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, NamedTuple
from dataclasses import dataclass
from enum import Enum

import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error
import boto3


class ScalingAction(Enum):
    """Types of scaling actions."""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"
    SCALE_OUT = "scale_out"  # Add instances
    SCALE_IN = "scale_in"    # Remove instances


@dataclass
class ResourceMetrics:
    """Resource utilization metrics."""
    timestamp: datetime
    cpu_utilization: float
    memory_utilization: float
    disk_io: float
    network_io: float
    queue_depth: int
    active_connections: int
    pipeline_throughput: float


@dataclass
class ScalingDecision:
    """Scaling decision with rationale."""
    action: ScalingAction
    resource_id: str
    current_capacity: int
    target_capacity: int
    confidence: float
    reasoning: str
    estimated_cost_impact: float
    implementation_time: timedelta


class WorkloadPredictor:
    """Predicts future workload based on historical patterns."""
    
    def __init__(self):
        """Initialize the workload predictor."""
        self.model = RandomForestRegressor(
            n_estimators=100,
            max_depth=10,
            random_state=42
        )
        self.scaler = StandardScaler()
        self.is_trained = False
        self.feature_names = [
            'hour_of_day', 'day_of_week', 'day_of_month',
            'cpu_utilization', 'memory_utilization', 'queue_depth',
            'pipeline_count', 'data_volume_gb'
        ]
        
        self.logger = logging.getLogger(__name__)
    
    def prepare_features(self, metrics: List[ResourceMetrics]) -> pd.DataFrame:
        """Prepare features for machine learning model."""
        data = []
        
        for metric in metrics:
            features = {
                'timestamp': metric.timestamp,
                'hour_of_day': metric.timestamp.hour,
                'day_of_week': metric.timestamp.weekday(),
                'day_of_month': metric.timestamp.day,
                'cpu_utilization': metric.cpu_utilization,
                'memory_utilization': metric.memory_utilization,
                'queue_depth': metric.queue_depth,
                'pipeline_count': metric.active_connections,  # Proxy for active pipelines
                'data_volume_gb': metric.pipeline_throughput,  # Proxy for data volume
                'target_cpu': metric.cpu_utilization  # Target variable
            }
            data.append(features)
        
        df = pd.DataFrame(data)
        
        # Create lag features for time series
        for lag in [1, 2, 3, 6, 12, 24]:  # Various lag periods
            df[f'cpu_lag_{lag}'] = df['cpu_utilization'].shift(lag)
            df[f'memory_lag_{lag}'] = df['memory_utilization'].shift(lag)
        
        # Create rolling averages
        for window in [3, 6, 12, 24]:
            df[f'cpu_rolling_{window}'] = df['cpu_utilization'].rolling(window).mean()
            df[f'memory_rolling_{window}'] = df['memory_utilization'].rolling(window).mean()
        
        # Drop rows with NaN values from lag/rolling features
        df = df.dropna()
        
        return df
    
    def train(self, historical_metrics: List[ResourceMetrics]) -> Dict[str, float]:
        """Train the prediction model on historical data."""
        df = self.prepare_features(historical_metrics)
        
        if len(df) < 100:  # Need sufficient data for training
            raise ValueError("Insufficient historical data for training")
        
        # Prepare features and target
        feature_cols = [col for col in df.columns if col not in ['timestamp', 'target_cpu']]
        X = df[feature_cols]
        y = df['target_cpu']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, shuffle=False
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Train model
        self.model.fit(X_train_scaled, y_train)
        
        # Evaluate model
        y_pred = self.model.predict(X_test_scaled)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        self.is_trained = True
        
        return {
            'mae': mae,
            'rmse': rmse,
            'training_samples': len(X_train),
            'test_samples': len(X_test)
        }
    
    def predict(
        self, 
        current_metrics: ResourceMetrics, 
        hours_ahead: int = 1
    ) -> Tuple[float, float]:
        """Predict future CPU utilization."""
        if not self.is_trained:
            raise ValueError("Model must be trained before prediction")
        
        # Create future timestamp
        future_time = current_metrics.timestamp + timedelta(hours=hours_ahead)
        
        # Prepare features for prediction
        features = {
            'hour_of_day': future_time.hour,
            'day_of_week': future_time.weekday(),
            'day_of_month': future_time.day,
            'cpu_utilization': current_metrics.cpu_utilization,
            'memory_utilization': current_metrics.memory_utilization,
            'queue_depth': current_metrics.queue_depth,
            'pipeline_count': current_metrics.active_connections,
            'data_volume_gb': current_metrics.pipeline_throughput,
        }
        
        # Add lag features (simplified - would use actual historical data)
        for lag in [1, 2, 3, 6, 12, 24]:
            features[f'cpu_lag_{lag}'] = current_metrics.cpu_utilization
            features[f'memory_lag_{lag}'] = current_metrics.memory_utilization
        
        # Add rolling averages (simplified)
        for window in [3, 6, 12, 24]:
            features[f'cpu_rolling_{window}'] = current_metrics.cpu_utilization
            features[f'memory_rolling_{window}'] = current_metrics.memory_utilization
        
        # Convert to DataFrame and scale
        feature_df = pd.DataFrame([features])
        feature_scaled = self.scaler.transform(feature_df)
        
        # Make prediction
        prediction = self.model.predict(feature_scaled)[0]
        
        # Estimate confidence (simplified)
        confidence = min(0.95, max(0.5, 1.0 - abs(prediction - current_metrics.cpu_utilization) / 100))
        
        return prediction, confidence
    
    def save_model(self, filepath: str):
        """Save trained model to disk."""
        model_data = {
            'model': self.model,
            'scaler': self.scaler,
            'is_trained': self.is_trained,
            'feature_names': self.feature_names
        }
        
        with open(filepath, 'wb') as f:
            pickle.dump(model_data, f)
    
    def load_model(self, filepath: str):
        """Load trained model from disk."""
        with open(filepath, 'rb') as f:
            model_data = pickle.load(f)
        
        self.model = model_data['model']
        self.scaler = model_data['scaler']
        self.is_trained = model_data['is_trained']
        self.feature_names = model_data['feature_names']


class IntelligentScaler:
    """Main intelligent scaling orchestrator."""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the intelligent scaler."""
        self.predictor = WorkloadPredictor()
        self.logger = logging.getLogger(__name__)
        
        # Default configuration
        self.config = {
            'scale_up_threshold': 70,      # CPU % threshold for scaling up
            'scale_down_threshold': 30,    # CPU % threshold for scaling down
            'prediction_horizon': 2,       # Hours to look ahead
            'min_confidence': 0.7,         # Minimum prediction confidence
            'cooldown_period': 300,        # Seconds between scaling actions
            'max_scale_factor': 2.0,       # Maximum scaling multiplier
            'min_instances': 1,            # Minimum number of instances
            'max_instances': 10,           # Maximum number of instances
        }
        
        if config:
            self.config.update(config)
        
        # Track recent scaling actions for cooldown
        self.recent_actions: Dict[str, datetime] = {}
    
    async def make_scaling_decision(
        self,
        resource_id: str,
        current_metrics: ResourceMetrics,
        current_capacity: int,
        historical_metrics: List[ResourceMetrics]
    ) -> Optional[ScalingDecision]:
        """Make an intelligent scaling decision."""
        
        # Check if we're in cooldown period
        if self._in_cooldown(resource_id):
            return None
        
        # Train predictor if not already trained
        if not self.predictor.is_trained and len(historical_metrics) >= 100:
            try:
                training_results = self.predictor.train(historical_metrics)
                self.logger.info(f"Model trained with MAE: {training_results['mae']:.2f}")
            except Exception as e:
                self.logger.error(f"Model training failed: {e}")
                return self._fallback_scaling_decision(resource_id, current_metrics, current_capacity)
        
        # Make prediction if model is trained
        if self.predictor.is_trained:
            try:
                predicted_cpu, confidence = self.predictor.predict(
                    current_metrics, 
                    self.config['prediction_horizon']
                )
                
                if confidence < self.config['min_confidence']:
                    self.logger.warning(f"Low prediction confidence: {confidence:.2f}")
                    return self._fallback_scaling_decision(resource_id, current_metrics, current_capacity)
                
                # Make scaling decision based on prediction
                return self._decide_scaling_action(
                    resource_id, current_metrics, current_capacity,
                    predicted_cpu, confidence
                )
                
            except Exception as e:
                self.logger.error(f"Prediction failed: {e}")
                return self._fallback_scaling_decision(resource_id, current_metrics, current_capacity)
        
        else:
            # Fallback to rule-based scaling
            return self._fallback_scaling_decision(resource_id, current_metrics, current_capacity)
    
    def _decide_scaling_action(
        self,
        resource_id: str,
        current_metrics: ResourceMetrics,
        current_capacity: int,
        predicted_cpu: float,
        confidence: float
    ) -> Optional[ScalingDecision]:
        """Decide scaling action based on predictions."""
        
        current_cpu = current_metrics.cpu_utilization
        scale_up_threshold = self.config['scale_up_threshold']
        scale_down_threshold = self.config['scale_down_threshold']
        
        # Determine scaling action
        if predicted_cpu > scale_up_threshold:
            if current_capacity >= self.config['max_instances']:
                return None  # Already at maximum
            
            # Calculate target capacity
            scale_factor = min(
                self.config['max_scale_factor'],
                predicted_cpu / 50.0  # More aggressive scaling for higher predicted load
            )
            target_capacity = min(
                self.config['max_instances'],
                int(current_capacity * scale_factor)
            )
            
            if target_capacity <= current_capacity:
                return None  # No meaningful scaling
            
            action = ScalingAction.SCALE_OUT
            reasoning = f"Predicted CPU {predicted_cpu:.1f}% > threshold {scale_up_threshold}%"
            cost_impact = (target_capacity - current_capacity) * 0.1  # Estimated hourly cost per instance
            
        elif predicted_cpu < scale_down_threshold:
            if current_capacity <= self.config['min_instances']:
                return None  # Already at minimum
            
            # Conservative scaling down
            target_capacity = max(
                self.config['min_instances'],
                int(current_capacity * 0.7)  # Scale down by 30%
            )
            
            if target_capacity >= current_capacity:
                return None  # No meaningful scaling
            
            action = ScalingAction.SCALE_IN
            reasoning = f"Predicted CPU {predicted_cpu:.1f}% < threshold {scale_down_threshold}%"
            cost_impact = -(current_capacity - target_capacity) * 0.1  # Negative cost = savings
            
        else:
            return None  # No scaling needed
        
        return ScalingDecision(
            action=action,
            resource_id=resource_id,
            current_capacity=current_capacity,
            target_capacity=target_capacity,
            confidence=confidence,
            reasoning=reasoning,
            estimated_cost_impact=cost_impact,
            implementation_time=timedelta(minutes=5)  # Estimated time to scale
        )
    
    def _fallback_scaling_decision(
        self,
        resource_id: str,
        current_metrics: ResourceMetrics,
        current_capacity: int
    ) -> Optional[ScalingDecision]:
        """Fallback rule-based scaling when ML prediction is unavailable."""
        
        current_cpu = current_metrics.cpu_utilization
        current_memory = current_metrics.memory_utilization
        
        # Simple rule-based scaling
        if current_cpu > 80 or current_memory > 85:
            if current_capacity < self.config['max_instances']:
                target_capacity = min(self.config['max_instances'], current_capacity + 1)
                
                return ScalingDecision(
                    action=ScalingAction.SCALE_OUT,
                    resource_id=resource_id,
                    current_capacity=current_capacity,
                    target_capacity=target_capacity,
                    confidence=0.6,  # Lower confidence for rule-based
                    reasoning=f"Rule-based: CPU {current_cpu:.1f}% or Memory {current_memory:.1f}% high",
                    estimated_cost_impact=0.1,
                    implementation_time=timedelta(minutes=5)
                )
        
        elif current_cpu < 20 and current_memory < 30:
            if current_capacity > self.config['min_instances']:
                target_capacity = max(self.config['min_instances'], current_capacity - 1)
                
                return ScalingDecision(
                    action=ScalingAction.SCALE_IN,
                    resource_id=resource_id,
                    current_capacity=current_capacity,
                    target_capacity=target_capacity,
                    confidence=0.6,
                    reasoning=f"Rule-based: CPU {current_cpu:.1f}% and Memory {current_memory:.1f}% low",
                    estimated_cost_impact=-0.1,
                    implementation_time=timedelta(minutes=5)
                )
        
        return None
    
    def _in_cooldown(self, resource_id: str) -> bool:
        """Check if resource is in cooldown period."""
        if resource_id not in self.recent_actions:
            return False
        
        last_action = self.recent_actions[resource_id]
        cooldown_end = last_action + timedelta(seconds=self.config['cooldown_period'])
        
        return datetime.now() < cooldown_end
    
    def record_scaling_action(self, resource_id: str):
        """Record that a scaling action was taken."""
        self.recent_actions[resource_id] = datetime.now()
    
    async def execute_scaling_decision(self, decision: ScalingDecision) -> bool:
        """Execute a scaling decision (placeholder for actual implementation)."""
        self.logger.info(f"Executing scaling decision: {decision}")
        
        # This would integrate with actual cloud provider APIs
        # For example, updating Auto Scaling Groups in AWS
        
        # Record the action
        self.record_scaling_action(decision.resource_id)
        
        return True


async def main():
    """Main entry point for intelligent scaling demonstration."""
    scaler = IntelligentScaler()
    
    # Simulate current metrics
    current_metrics = ResourceMetrics(
        timestamp=datetime.now(),
        cpu_utilization=75.0,
        memory_utilization=60.0,
        disk_io=1000.0,
        network_io=500.0,
        queue_depth=5,
        active_connections=10,
        pipeline_throughput=100.0
    )
    
    # Simulate historical metrics (would be loaded from monitoring system)
    historical_metrics = []
    base_time = datetime.now() - timedelta(days=7)
    
    for i in range(168):  # 1 week of hourly data
        timestamp = base_time + timedelta(hours=i)
        
        # Simulate realistic usage patterns
        hour = timestamp.hour
        day_of_week = timestamp.weekday()
        
        # Business hours pattern
        if 9 <= hour <= 17 and day_of_week < 5:
            base_cpu = 60 + np.random.normal(0, 15)
        else:
            base_cpu = 30 + np.random.normal(0, 10)
        
        base_cpu = max(5, min(95, base_cpu))  # Clamp between 5-95%
        
        historical_metrics.append(ResourceMetrics(
            timestamp=timestamp,
            cpu_utilization=base_cpu,
            memory_utilization=base_cpu * 0.8 + np.random.normal(0, 5),
            disk_io=base_cpu * 10 + np.random.normal(0, 100),
            network_io=base_cpu * 5 + np.random.normal(0, 50),
            queue_depth=int(base_cpu / 20) + np.random.randint(0, 3),
            active_connections=int(base_cpu / 10) + np.random.randint(1, 5),
            pipeline_throughput=base_cpu * 2 + np.random.normal(0, 20)
        ))
    
    # Make scaling decision
    decision = await scaler.make_scaling_decision(
        resource_id="etl-cluster-01",
        current_metrics=current_metrics,
        current_capacity=3,
        historical_metrics=historical_metrics
    )
    
    if decision:
        print(f"🤖 Scaling Decision Made:")
        print(f"   Action: {decision.action.value}")
        print(f"   Current Capacity: {decision.current_capacity}")
        print(f"   Target Capacity: {decision.target_capacity}")
        print(f"   Confidence: {decision.confidence:.2%}")
        print(f"   Reasoning: {decision.reasoning}")
        print(f"   Cost Impact: ${decision.estimated_cost_impact:.2f}/hour")
        
        # Execute the decision
        success = await scaler.execute_scaling_decision(decision)
        print(f"   Execution: {'✅ Success' if success else '❌ Failed'}")
    else:
        print("📊 No scaling action needed at this time")


if __name__ == "__main__":
    asyncio.run(main())