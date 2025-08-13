"""Auto-scaling pipeline management with intelligent resource allocation."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from .logging_config import get_logger
from .orchestrator import Pipeline


class ScalingAction(Enum):
    """Types of scaling actions."""
    SCALE_UP = "scale_up"
    SCALE_DOWN = "scale_down"
    MAINTAIN = "maintain"


class ResourceType(Enum):
    """Types of resources to monitor and scale."""
    CPU = "cpu"
    MEMORY = "memory"
    PIPELINE_WORKERS = "pipeline_workers"
    CONCURRENT_TASKS = "concurrent_tasks"


@dataclass
class ScalingRule:
    """Configuration for auto-scaling behavior."""
    resource_type: ResourceType
    scale_up_threshold: float = 80.0  # Percentage
    scale_down_threshold: float = 30.0  # Percentage
    scale_up_factor: float = 1.5  # Multiply current capacity
    scale_down_factor: float = 0.7  # Multiply current capacity
    min_capacity: int = 1
    max_capacity: int = 10
    cooldown_minutes: int = 5  # Minimum time between scaling actions
    evaluation_window_minutes: int = 10  # Time window for metric evaluation
    enabled: bool = True


@dataclass
class ResourceMetrics:
    """Current resource usage metrics."""
    timestamp: float = field(default_factory=time.time)
    cpu_usage_percent: float = 0.0
    memory_usage_percent: float = 0.0
    active_pipelines: int = 0
    concurrent_tasks: int = 0
    queue_length: int = 0
    throughput_per_minute: float = 0.0
    error_rate_percent: float = 0.0


@dataclass
class ScalingEvent:
    """Record of a scaling action taken."""
    action: ScalingAction
    resource_type: ResourceType
    old_capacity: int
    new_capacity: int
    trigger_metric: str
    metric_value: float
    reason: str
    timestamp: float = field(default_factory=time.time)


class AutoScalingManager:
    """Intelligent auto-scaling manager for ETL pipelines."""

    def __init__(self):
        self.logger = get_logger("agent_etl.auto_scaling")
        self.scaling_rules: Dict[ResourceType, ScalingRule] = {}
        self.metrics_history: List[ResourceMetrics] = []
        self.scaling_events: List[ScalingEvent] = []
        self.is_running = False
        self.scaling_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()

        # Current resource capacities
        self.current_capacities: Dict[ResourceType, int] = {
            ResourceType.PIPELINE_WORKERS: 2,
            ResourceType.CONCURRENT_TASKS: 5
        }

        # Pipeline management
        self.active_pipelines: Dict[str, Pipeline] = {}
        self.pipeline_queue: List[Dict[str, Any]] = []
        self.task_executor = None

        # Initialize default scaling rules
        self._initialize_default_rules()

    def _initialize_default_rules(self):
        """Initialize default auto-scaling rules."""
        # CPU-based scaling rule
        self.scaling_rules[ResourceType.CPU] = ScalingRule(
            resource_type=ResourceType.CPU,
            scale_up_threshold=75.0,
            scale_down_threshold=25.0,
            scale_up_factor=1.3,
            scale_down_factor=0.8,
            min_capacity=1,
            max_capacity=8,
            cooldown_minutes=3
        )

        # Memory-based scaling rule
        self.scaling_rules[ResourceType.MEMORY] = ScalingRule(
            resource_type=ResourceType.MEMORY,
            scale_up_threshold=80.0,
            scale_down_threshold=40.0,
            scale_up_factor=1.4,
            scale_down_factor=0.7,
            min_capacity=1,
            max_capacity=6,
            cooldown_minutes=5
        )

        # Pipeline worker scaling rule
        self.scaling_rules[ResourceType.PIPELINE_WORKERS] = ScalingRule(
            resource_type=ResourceType.PIPELINE_WORKERS,
            scale_up_threshold=90.0,  # Based on active pipelines vs capacity
            scale_down_threshold=30.0,
            scale_up_factor=1.5,
            scale_down_factor=0.8,
            min_capacity=1,
            max_capacity=10,
            cooldown_minutes=2
        )

        # Concurrent task scaling rule
        self.scaling_rules[ResourceType.CONCURRENT_TASKS] = ScalingRule(
            resource_type=ResourceType.CONCURRENT_TASKS,
            scale_up_threshold=85.0,
            scale_down_threshold=35.0,
            scale_up_factor=1.6,
            scale_down_factor=0.7,
            min_capacity=2,
            max_capacity=20,
            cooldown_minutes=1
        )

    def set_scaling_rule(self, resource_type: ResourceType, rule: ScalingRule):
        """Set or update a scaling rule."""
        self.scaling_rules[resource_type] = rule
        self.logger.info(f"Updated scaling rule for {resource_type.value}")

    def start_auto_scaling(self):
        """Start the auto-scaling background process."""
        if self.is_running:
            return

        self.is_running = True
        self.shutdown_event.clear()
        self.scaling_thread = threading.Thread(
            target=self._scaling_loop,
            name="AutoScaler",
            daemon=True
        )
        self.scaling_thread.start()
        self.logger.info("Auto-scaling started")

    def stop_auto_scaling(self):
        """Stop the auto-scaling background process."""
        if not self.is_running:
            return

        self.is_running = False
        self.shutdown_event.set()

        if self.scaling_thread and self.scaling_thread.is_alive():
            self.scaling_thread.join(timeout=5.0)

        self.logger.info("Auto-scaling stopped")

    def _scaling_loop(self):
        """Main auto-scaling loop."""
        while self.is_running and not self.shutdown_event.is_set():
            try:
                # Collect current metrics
                current_metrics = self._collect_resource_metrics()
                self.metrics_history.append(current_metrics)

                # Clean up old metrics
                self._cleanup_metrics_history()

                # Evaluate scaling rules
                self._evaluate_scaling_rules(current_metrics)

                # Process pipeline queue
                self._process_pipeline_queue()

                # Sleep for evaluation interval
                self.shutdown_event.wait(30.0)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in auto-scaling loop: {e}", exc_info=True)
                self.shutdown_event.wait(10.0)

    def _collect_resource_metrics(self) -> ResourceMetrics:
        """Collect current resource usage metrics."""
        try:
            import psutil

            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()

            # Calculate pipeline and task metrics
            active_pipeline_count = len(self.active_pipelines)
            pipeline_usage = (active_pipeline_count / max(1, self.current_capacities[ResourceType.PIPELINE_WORKERS])) * 100

            # Estimate concurrent tasks (simplified)
            estimated_concurrent_tasks = active_pipeline_count * 2  # Assume avg 2 tasks per pipeline
            task_usage = (estimated_concurrent_tasks / max(1, self.current_capacities[ResourceType.CONCURRENT_TASKS])) * 100

            return ResourceMetrics(
                cpu_usage_percent=cpu_percent,
                memory_usage_percent=memory.percent,
                active_pipelines=active_pipeline_count,
                concurrent_tasks=estimated_concurrent_tasks,
                queue_length=len(self.pipeline_queue),
                throughput_per_minute=self._calculate_throughput(),
                error_rate_percent=self._calculate_error_rate()
            )

        except ImportError:
            # Fallback when psutil is not available
            return ResourceMetrics(
                active_pipelines=len(self.active_pipelines),
                queue_length=len(self.pipeline_queue)
            )
        except Exception as e:
            self.logger.error(f"Failed to collect resource metrics: {e}")
            return ResourceMetrics()

    def _calculate_throughput(self) -> float:
        """Calculate current throughput (pipelines per minute)."""
        if len(self.scaling_events) < 2:
            return 0.0

        # Simple throughput calculation based on recent activity
        recent_events = [
            e for e in self.scaling_events
            if time.time() - e.timestamp < 300  # Last 5 minutes
        ]

        return len(recent_events) * 12  # Scale to per-minute

    def _calculate_error_rate(self) -> float:
        """Calculate current error rate percentage."""
        # This would be implemented with actual error tracking
        # For now, return a placeholder
        return 0.0

    def _evaluate_scaling_rules(self, current_metrics: ResourceMetrics):
        """Evaluate all scaling rules and take actions if needed."""
        for resource_type, rule in self.scaling_rules.items():
            if not rule.enabled:
                continue

            # Check cooldown period
            if not self._is_cooldown_expired(resource_type, rule.cooldown_minutes):
                continue

            # Get current usage percentage for this resource
            usage_percent = self._get_resource_usage_percent(resource_type, current_metrics)

            # Determine scaling action
            action = self._determine_scaling_action(rule, usage_percent)

            if action != ScalingAction.MAINTAIN:
                self._execute_scaling_action(resource_type, rule, action, usage_percent, current_metrics)

    def _get_resource_usage_percent(self, resource_type: ResourceType, metrics: ResourceMetrics) -> float:
        """Get current usage percentage for a specific resource type."""
        if resource_type == ResourceType.CPU:
            return metrics.cpu_usage_percent
        elif resource_type == ResourceType.MEMORY:
            return metrics.memory_usage_percent
        elif resource_type == ResourceType.PIPELINE_WORKERS:
            capacity = self.current_capacities.get(resource_type, 1)
            return (metrics.active_pipelines / max(1, capacity)) * 100
        elif resource_type == ResourceType.CONCURRENT_TASKS:
            capacity = self.current_capacities.get(resource_type, 1)
            return (metrics.concurrent_tasks / max(1, capacity)) * 100
        else:
            return 0.0

    def _determine_scaling_action(self, rule: ScalingRule, usage_percent: float) -> ScalingAction:
        """Determine what scaling action to take based on usage and rule."""
        current_capacity = self.current_capacities.get(rule.resource_type, 1)

        if usage_percent >= rule.scale_up_threshold and current_capacity < rule.max_capacity:
            return ScalingAction.SCALE_UP
        elif usage_percent <= rule.scale_down_threshold and current_capacity > rule.min_capacity:
            return ScalingAction.SCALE_DOWN
        else:
            return ScalingAction.MAINTAIN

    def _execute_scaling_action(
        self,
        resource_type: ResourceType,
        rule: ScalingRule,
        action: ScalingAction,
        usage_percent: float,
        metrics: ResourceMetrics
    ):
        """Execute a scaling action."""
        old_capacity = self.current_capacities.get(resource_type, 1)

        if action == ScalingAction.SCALE_UP:
            new_capacity = min(
                rule.max_capacity,
                max(old_capacity + 1, int(old_capacity * rule.scale_up_factor))
            )
            reason = f"Usage {usage_percent:.1f}% exceeds scale-up threshold {rule.scale_up_threshold}%"
        else:  # SCALE_DOWN
            new_capacity = max(
                rule.min_capacity,
                max(old_capacity - 1, int(old_capacity * rule.scale_down_factor))
            )
            reason = f"Usage {usage_percent:.1f}% below scale-down threshold {rule.scale_down_threshold}%"

        if new_capacity == old_capacity:
            return  # No actual change needed

        # Apply the scaling
        self.current_capacities[resource_type] = new_capacity

        # Record the scaling event
        event = ScalingEvent(
            action=action,
            resource_type=resource_type,
            old_capacity=old_capacity,
            new_capacity=new_capacity,
            trigger_metric=f"{resource_type.value}_usage",
            metric_value=usage_percent,
            reason=reason
        )
        self.scaling_events.append(event)

        self.logger.info(
            f"Auto-scaling: {action.value} {resource_type.value}",
            extra={
                "resource_type": resource_type.value,
                "action": action.value,
                "old_capacity": old_capacity,
                "new_capacity": new_capacity,
                "usage_percent": usage_percent,
                "reason": reason
            }
        )

        # Apply the actual scaling changes
        self._apply_scaling_changes(resource_type, new_capacity)

    def _apply_scaling_changes(self, resource_type: ResourceType, new_capacity: int):
        """Apply actual scaling changes to the system."""
        if resource_type == ResourceType.PIPELINE_WORKERS:
            # Adjust pipeline worker pool
            self._adjust_pipeline_workers(new_capacity)
        elif resource_type == ResourceType.CONCURRENT_TASKS:
            # Adjust concurrent task limit
            self._adjust_concurrent_tasks(new_capacity)
        # CPU and MEMORY scaling would typically involve container/VM scaling
        # which is beyond the scope of this implementation

    def _adjust_pipeline_workers(self, new_capacity: int):
        """Adjust the number of pipeline workers."""
        # This would integrate with an actual worker pool
        # For now, just log the change
        self.logger.info(f"Pipeline worker capacity adjusted to {new_capacity}")

    def _adjust_concurrent_tasks(self, new_capacity: int):
        """Adjust the concurrent task execution limit."""
        # This would integrate with a task executor
        # For now, just log the change
        self.logger.info(f"Concurrent task capacity adjusted to {new_capacity}")

    def _is_cooldown_expired(self, resource_type: ResourceType, cooldown_minutes: int) -> bool:
        """Check if the cooldown period has expired for a resource type."""
        cutoff_time = time.time() - (cooldown_minutes * 60)

        recent_events = [
            e for e in self.scaling_events
            if e.resource_type == resource_type and e.timestamp > cutoff_time
        ]

        return len(recent_events) == 0

    def _cleanup_metrics_history(self, max_history_size: int = 1000):
        """Clean up old metrics to prevent memory growth."""
        if len(self.metrics_history) > max_history_size:
            self.metrics_history = self.metrics_history[-max_history_size:]

        # Also clean up scaling events (keep last 100)
        if len(self.scaling_events) > 100:
            self.scaling_events = self.scaling_events[-100:]

    def _process_pipeline_queue(self):
        """Process queued pipelines based on current capacity."""
        if not self.pipeline_queue:
            return

        available_capacity = (
            self.current_capacities[ResourceType.PIPELINE_WORKERS] -
            len(self.active_pipelines)
        )

        if available_capacity <= 0:
            return

        # Process as many queued pipelines as capacity allows
        to_process = min(available_capacity, len(self.pipeline_queue))

        for _ in range(to_process):
            if not self.pipeline_queue:
                break

            pipeline_config = self.pipeline_queue.pop(0)
            self._start_pipeline(pipeline_config)

    def _start_pipeline(self, pipeline_config: Dict[str, Any]):
        """Start a pipeline from the queue."""
        try:
            pipeline_id = pipeline_config.get("id", f"pipeline_{int(time.time())}")

            # This would create and start an actual pipeline
            # For now, simulate pipeline execution
            self.active_pipelines[pipeline_id] = pipeline_config

            self.logger.info(f"Started pipeline {pipeline_id} from queue")

            # Simulate pipeline completion after some time
            def complete_pipeline():
                time.sleep(pipeline_config.get("duration", 30))  # Simulate work
                if pipeline_id in self.active_pipelines:
                    del self.active_pipelines[pipeline_id]
                    self.logger.info(f"Completed pipeline {pipeline_id}")

            threading.Thread(target=complete_pipeline, daemon=True).start()

        except Exception as e:
            self.logger.error(f"Failed to start pipeline: {e}")

    def queue_pipeline(self, pipeline_config: Dict[str, Any]) -> str:
        """Add a pipeline to the execution queue."""
        pipeline_id = pipeline_config.get("id", f"pipeline_{int(time.time())}")
        pipeline_config["id"] = pipeline_id
        pipeline_config["queued_at"] = time.time()

        self.pipeline_queue.append(pipeline_config)

        self.logger.info(
            f"Queued pipeline {pipeline_id}",
            extra={"pipeline_id": pipeline_id, "queue_length": len(self.pipeline_queue)}
        )

        return pipeline_id

    def get_scaling_status(self) -> Dict[str, Any]:
        """Get current auto-scaling status and metrics."""
        recent_metrics = self.metrics_history[-1] if self.metrics_history else ResourceMetrics()
        recent_events = self.scaling_events[-5:] if self.scaling_events else []

        return {
            "auto_scaling_active": self.is_running,
            "current_capacities": {rt.value: cap for rt, cap in self.current_capacities.items()},
            "current_metrics": {
                "cpu_usage_percent": recent_metrics.cpu_usage_percent,
                "memory_usage_percent": recent_metrics.memory_usage_percent,
                "active_pipelines": recent_metrics.active_pipelines,
                "queue_length": recent_metrics.queue_length,
                "throughput_per_minute": recent_metrics.throughput_per_minute
            },
            "scaling_rules": {
                rt.value: {
                    "scale_up_threshold": rule.scale_up_threshold,
                    "scale_down_threshold": rule.scale_down_threshold,
                    "min_capacity": rule.min_capacity,
                    "max_capacity": rule.max_capacity,
                    "enabled": rule.enabled
                } for rt, rule in self.scaling_rules.items()
            },
            "recent_scaling_events": [
                {
                    "timestamp": event.timestamp,
                    "action": event.action.value,
                    "resource_type": event.resource_type.value,
                    "old_capacity": event.old_capacity,
                    "new_capacity": event.new_capacity,
                    "reason": event.reason
                } for event in recent_events
            ],
            "timestamp": time.time()
        }

    def __enter__(self):
        """Context manager entry."""
        self.start_auto_scaling()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_auto_scaling()


# Global auto-scaling manager instance
_auto_scaler = None


def get_auto_scaler() -> AutoScalingManager:
    """Get the global auto-scaling manager instance."""
    global _auto_scaler
    if _auto_scaler is None:
        _auto_scaler = AutoScalingManager()
    return _auto_scaler


def start_auto_scaling():
    """Start global auto-scaling."""
    scaler = get_auto_scaler()
    scaler.start_auto_scaling()


def stop_auto_scaling():
    """Stop global auto-scaling."""
    scaler = get_auto_scaler()
    scaler.stop_auto_scaling()


def queue_pipeline_for_scaling(pipeline_config: Dict[str, Any]) -> str:
    """Queue a pipeline for auto-scaled execution."""
    scaler = get_auto_scaler()
    return scaler.queue_pipeline(pipeline_config)


def get_auto_scaling_status() -> Dict[str, Any]:
    """Get current auto-scaling status."""
    scaler = get_auto_scaler()
    return scaler.get_scaling_status()
