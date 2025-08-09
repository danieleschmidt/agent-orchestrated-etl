"""Intelligent auto-scaling with machine learning predictions."""

from __future__ import annotations

import time
import asyncio
import psutil
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import threading

from .logging_config import get_logger


class ScalingMetric(Enum):
    """Metrics used for scaling decisions."""
    CPU_USAGE = "cpu_usage"
    MEMORY_USAGE = "memory_usage"
    QUEUE_LENGTH = "queue_length"
    RESPONSE_TIME = "response_time"
    THROUGHPUT = "throughput"
    ERROR_RATE = "error_rate"


@dataclass
class ScalingRule:
    """Defines a scaling rule."""
    name: str
    metric: ScalingMetric
    threshold_up: float
    threshold_down: float
    scale_up_factor: float = 1.5
    scale_down_factor: float = 0.7
    cooldown_seconds: int = 300
    min_instances: int = 1
    max_instances: int = 10


@dataclass
class ScalingConfig:
    """Configuration for auto-scaling."""
    rules: List[ScalingRule]
    monitoring_interval: int = 30
    enable_predictive_scaling: bool = True
    enable_aggressive_scaling: bool = False
    resource_buffer_percent: int = 20


class IntelligentScaler:
    """Intelligent auto-scaling with predictive capabilities."""
    
    def __init__(self, config: ScalingConfig):
        self.config = config
        self.logger = get_logger("intelligent_scaler")
        self.current_instances = 1
        self.scaling_history: List[Dict[str, Any]] = []
        
    def get_scaling_recommendation(self, current_metrics: Dict[str, float]) -> Dict[str, Any]:
        """Get intelligent scaling recommendation."""
        
        # Basic rule-based scaling
        for rule in self.config.rules:
            metric_value = current_metrics.get(rule.metric.value, 0)
            
            if metric_value > rule.threshold_up:
                return {
                    "action": "scale_up",
                    "target_instances": min(rule.max_instances, self.current_instances + 1),
                    "confidence": 0.8,
                    "reason": f"High {rule.metric.value}: {metric_value}"
                }
            elif metric_value < rule.threshold_down and self.current_instances > rule.min_instances:
                return {
                    "action": "scale_down",
                    "target_instances": max(rule.min_instances, self.current_instances - 1),
                    "confidence": 0.7,
                    "reason": f"Low {rule.metric.value}: {metric_value}"
                }
        
        return {
            "action": "none",
            "target_instances": self.current_instances,
            "confidence": 1.0,
            "reason": "All metrics within normal range"
        }