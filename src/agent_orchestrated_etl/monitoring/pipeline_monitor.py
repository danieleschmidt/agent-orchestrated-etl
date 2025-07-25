"""Enhanced pipeline monitoring integration."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum

from ..logging_config import get_logger
from .realtime_monitor import RealtimeMonitor


class TaskStatus(Enum):
    """Task execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    SKIPPED = "skipped"


class PipelineStatus(Enum):
    """Pipeline execution status enumeration."""
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"


@dataclass
class TaskExecution:
    """Task execution tracking data."""
    task_id: str
    pipeline_id: str
    status: TaskStatus
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    duration: Optional[float] = None
    operator_type: Optional[str] = None
    result_data: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None
    retry_count: int = 0
    resource_usage: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "task_id": self.task_id,
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "operator_type": self.operator_type,
            "result_data": self.result_data,
            "error_message": self.error_message,
            "retry_count": self.retry_count,
            "resource_usage": self.resource_usage,
            "metadata": self.metadata
        }


@dataclass
class PipelineExecution:
    """Pipeline execution tracking data."""
    pipeline_id: str
    dag_id: str
    status: PipelineStatus
    start_time: float
    end_time: Optional[float] = None
    duration: Optional[float] = None
    execution_mode: str = "sync"
    total_tasks: int = 0
    completed_tasks: int = 0
    failed_tasks: int = 0
    running_tasks: int = 0
    skipped_tasks: int = 0
    task_executions: Optional[List[TaskExecution]] = None
    resource_usage: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if self.task_executions is None:
            self.task_executions = []
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation."""
        return {
            "pipeline_id": self.pipeline_id,
            "dag_id": self.dag_id,
            "status": self.status.value,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration": self.duration,
            "execution_mode": self.execution_mode,
            "total_tasks": self.total_tasks,
            "completed_tasks": self.completed_tasks,
            "failed_tasks": self.failed_tasks,
            "running_tasks": self.running_tasks,
            "skipped_tasks": self.skipped_tasks,
            "task_executions": [task.to_dict() for task in self.task_executions],
            "resource_usage": self.resource_usage,
            "metadata": self.metadata
        }


class PipelineMonitor:
    """Enhanced pipeline monitoring system with real-time capabilities."""
    
    def __init__(self, realtime_monitor: Optional[RealtimeMonitor] = None):
        self.logger = get_logger("monitoring.pipeline_monitor")
        self.realtime_monitor = realtime_monitor
        
        # Active pipeline tracking
        self.active_pipelines: Dict[str, PipelineExecution] = {}
        self.completed_pipelines: List[PipelineExecution] = []
        
        # Event callbacks
        self.pipeline_start_callbacks: List[Callable] = []
        self.pipeline_end_callbacks: List[Callable] = []
        self.task_start_callbacks: List[Callable] = []
        self.task_end_callbacks: List[Callable] = []
        
        # Performance tracking
        self.performance_baselines: Dict[str, Dict[str, float]] = {}
        
        # Historical data retention (keep last 1000 completed pipelines)
        self.max_completed_pipelines = 1000
    
    def register_pipeline_start_callback(self, callback: Callable[[PipelineExecution], None]) -> None:
        """Register callback for pipeline start events."""
        self.pipeline_start_callbacks.append(callback)
        self.logger.info(f"Registered pipeline start callback: {callback.__name__}")
    
    def register_pipeline_end_callback(self, callback: Callable[[PipelineExecution], None]) -> None:
        """Register callback for pipeline end events."""
        self.pipeline_end_callbacks.append(callback)
        self.logger.info(f"Registered pipeline end callback: {callback.__name__}")
    
    def register_task_start_callback(self, callback: Callable[[TaskExecution], None]) -> None:
        """Register callback for task start events."""
        self.task_start_callbacks.append(callback)
        self.logger.info(f"Registered task start callback: {callback.__name__}")
    
    def register_task_end_callback(self, callback: Callable[[TaskExecution], None]) -> None:
        """Register callback for task end events."""
        self.task_end_callbacks.append(callback)
        self.logger.info(f"Registered task end callback: {callback.__name__}")
    
    def start_pipeline(self, pipeline_id: str, dag_id: str, total_tasks: int = 0, 
                      execution_mode: str = "sync", metadata: Optional[Dict[str, Any]] = None) -> None:
        """Start monitoring a pipeline execution."""
        start_time = time.time()
        
        pipeline_execution = PipelineExecution(
            pipeline_id=pipeline_id,
            dag_id=dag_id,
            status=PipelineStatus.INITIALIZING,
            start_time=start_time,
            execution_mode=execution_mode,
            total_tasks=total_tasks,
            metadata=metadata or {}
        )
        
        self.active_pipelines[pipeline_id] = pipeline_execution
        
        # Collect initial metrics
        if self.realtime_monitor:
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.started", 1.0, pipeline_id, metadata={"dag_id": dag_id}
            )
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.total_tasks", float(total_tasks), pipeline_id
            )
        
        # Execute callbacks
        for callback in self.pipeline_start_callbacks:
            try:
                callback(pipeline_execution)
            except Exception as e:
                self.logger.error(f"Error in pipeline start callback {callback.__name__}: {e}")
        
        self.logger.info(f"Started monitoring pipeline: {pipeline_id}")
    
    def update_pipeline_status(self, pipeline_id: str, status: PipelineStatus) -> None:
        """Update pipeline status."""
        if pipeline_id not in self.active_pipelines:
            self.logger.warning(f"Pipeline {pipeline_id} not found in active pipelines")
            return
        
        pipeline = self.active_pipelines[pipeline_id]
        old_status = pipeline.status
        pipeline.status = status
        
        # Collect status change metric
        if self.realtime_monitor:
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.status_change", 1.0, pipeline_id, 
                metadata={"from_status": old_status.value, "to_status": status.value}
            )
        
        self.logger.info(f"Pipeline {pipeline_id} status changed: {old_status.value} -> {status.value}")
    
    def end_pipeline(self, pipeline_id: str, status: PipelineStatus = PipelineStatus.COMPLETED,
                    resource_usage: Optional[Dict[str, Any]] = None) -> None:
        """End monitoring a pipeline execution."""
        if pipeline_id not in self.active_pipelines:
            self.logger.warning(f"Pipeline {pipeline_id} not found in active pipelines")
            return
        
        pipeline = self.active_pipelines[pipeline_id]
        end_time = time.time()
        
        pipeline.end_time = end_time
        pipeline.duration = end_time - pipeline.start_time
        pipeline.status = status
        pipeline.resource_usage = resource_usage
        
        # Calculate task statistics
        self._update_pipeline_task_statistics(pipeline)
        
        # Collect final metrics
        if self.realtime_monitor:
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.execution_time", pipeline.duration, pipeline_id
            )
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.completed", 1.0 if status == PipelineStatus.COMPLETED else 0.0, pipeline_id
            )
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.task_success_rate", 
                (pipeline.completed_tasks / max(pipeline.total_tasks, 1)) * 100, 
                pipeline_id
            )
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.task_failure_rate",
                (pipeline.failed_tasks / max(pipeline.total_tasks, 1)) * 100,
                pipeline_id
            )
        
        # Execute callbacks
        for callback in self.pipeline_end_callbacks:
            try:
                callback(pipeline)
            except Exception as e:
                self.logger.error(f"Error in pipeline end callback {callback.__name__}: {e}")
        
        # Move to completed pipelines
        self.completed_pipelines.append(pipeline)
        del self.active_pipelines[pipeline_id]
        
        # Maintain completed pipelines limit
        if len(self.completed_pipelines) > self.max_completed_pipelines:
            self.completed_pipelines = self.completed_pipelines[-self.max_completed_pipelines:]
        
        # Check performance against baselines
        self._check_performance_baselines(pipeline)
        
        self.logger.info(f"Ended monitoring pipeline: {pipeline_id} (status: {status.value}, duration: {pipeline.duration:.2f}s)")
    
    def start_task(self, pipeline_id: str, task_id: str, operator_type: Optional[str] = None,
                  metadata: Optional[Dict[str, Any]] = None) -> None:
        """Start monitoring a task execution."""
        if pipeline_id not in self.active_pipelines:
            self.logger.warning(f"Pipeline {pipeline_id} not found for task {task_id}")
            return
        
        start_time = time.time()
        pipeline = self.active_pipelines[pipeline_id]
        
        task_execution = TaskExecution(
            task_id=task_id,
            pipeline_id=pipeline_id,
            status=TaskStatus.RUNNING,
            start_time=start_time,
            operator_type=operator_type,
            metadata=metadata or {}
        )
        
        pipeline.task_executions.append(task_execution)
        pipeline.running_tasks += 1
        
        # Update pipeline status to running if it was initializing
        if pipeline.status == PipelineStatus.INITIALIZING:
            pipeline.status = PipelineStatus.RUNNING
        
        # Collect task metrics
        if self.realtime_monitor:
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.task_started", 1.0, pipeline_id, task_id, 
                metadata={"operator_type": operator_type}
            )
        
        # Execute callbacks
        for callback in self.task_start_callbacks:
            try:
                callback(task_execution)
            except Exception as e:
                self.logger.error(f"Error in task start callback {callback.__name__}: {e}")
        
        self.logger.info(f"Started monitoring task: {task_id} in pipeline {pipeline_id}")
    
    def end_task(self, pipeline_id: str, task_id: str, status: TaskStatus,
                duration: Optional[float] = None, result_data: Optional[Dict[str, Any]] = None,
                error_message: Optional[str] = None, resource_usage: Optional[Dict[str, Any]] = None) -> None:
        """End monitoring a task execution."""
        if pipeline_id not in self.active_pipelines:
            self.logger.warning(f"Pipeline {pipeline_id} not found for task {task_id}")
            return
        
        pipeline = self.active_pipelines[pipeline_id]
        task_execution = None
        
        # Find the task execution
        for task in pipeline.task_executions:
            if task.task_id == task_id and task.status == TaskStatus.RUNNING:
                task_execution = task
                break
        
        if not task_execution:
            self.logger.warning(f"Running task {task_id} not found in pipeline {pipeline_id}")
            return
        
        end_time = time.time()
        
        # Update task execution
        task_execution.status = status
        task_execution.end_time = end_time
        task_execution.duration = duration or (end_time - task_execution.start_time)
        task_execution.result_data = result_data
        task_execution.error_message = error_message
        task_execution.resource_usage = resource_usage
        
        # Update pipeline counters
        pipeline.running_tasks -= 1
        
        if status == TaskStatus.COMPLETED:
            pipeline.completed_tasks += 1
        elif status == TaskStatus.FAILED:
            pipeline.failed_tasks += 1
        elif status == TaskStatus.SKIPPED:
            pipeline.skipped_tasks += 1
        
        # Collect task metrics
        if self.realtime_monitor:
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.task_duration", task_execution.duration, pipeline_id, task_id
            )
            self.realtime_monitor.collect_pipeline_metric(
                "pipeline.task_completed", 1.0 if status == TaskStatus.COMPLETED else 0.0, 
                pipeline_id, task_id
            )
            
            if resource_usage:
                for metric, value in resource_usage.items():
                    self.realtime_monitor.collect_pipeline_metric(
                        f"pipeline.task_{metric}", value, pipeline_id, task_id
                    )
        
        # Execute callbacks
        for callback in self.task_end_callbacks:
            try:
                callback(task_execution)
            except Exception as e:
                self.logger.error(f"Error in task end callback {callback.__name__}: {e}")
        
        self.logger.info(f"Ended monitoring task: {task_id} (status: {status.value}, duration: {task_execution.duration:.2f}s)")
    
    def get_pipeline_status(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Get current status of a pipeline."""
        # Check active pipelines first
        if pipeline_id in self.active_pipelines:
            return self.active_pipelines[pipeline_id].to_dict()
        
        # Check completed pipelines
        for pipeline in self.completed_pipelines:
            if pipeline.pipeline_id == pipeline_id:
                return pipeline.to_dict()
        
        return None
    
    def get_active_pipelines(self) -> List[Dict[str, Any]]:
        """Get all active pipeline statuses."""
        return [pipeline.to_dict() for pipeline in self.active_pipelines.values()]
    
    def get_completed_pipelines(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent completed pipeline statuses."""
        return [pipeline.to_dict() for pipeline in self.completed_pipelines[-limit:]]
    
    def get_pipeline_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get pipeline execution statistics for the specified time range."""
        cutoff_time = time.time() - (hours * 3600)
        
        # Get relevant pipelines
        relevant_pipelines = [
            pipeline for pipeline in self.completed_pipelines
            if pipeline.start_time >= cutoff_time
        ] + list(self.active_pipelines.values())
        
        if not relevant_pipelines:
            return {
                "total_pipelines": 0,
                "completed_pipelines": 0,
                "failed_pipelines": 0,
                "average_duration": 0.0,
                "success_rate": 0.0,
                "total_tasks": 0,
                "task_success_rate": 0.0,
                "time_range_hours": hours
            }
        
        # Calculate statistics
        total_pipelines = len(relevant_pipelines)
        completed_pipelines = sum(1 for p in relevant_pipelines if p.status == PipelineStatus.COMPLETED)
        failed_pipelines = sum(1 for p in relevant_pipelines if p.status == PipelineStatus.FAILED)
        
        durations = [p.duration for p in relevant_pipelines if p.duration is not None]
        average_duration = sum(durations) / len(durations) if durations else 0.0
        
        success_rate = (completed_pipelines / total_pipelines) * 100 if total_pipelines > 0 else 0.0
        
        total_tasks = sum(p.total_tasks for p in relevant_pipelines)
        completed_tasks = sum(p.completed_tasks for p in relevant_pipelines)
        task_success_rate = (completed_tasks / total_tasks) * 100 if total_tasks > 0 else 0.0
        
        return {
            "total_pipelines": total_pipelines,
            "completed_pipelines": completed_pipelines,
            "failed_pipelines": failed_pipelines,
            "active_pipelines": len(self.active_pipelines),
            "average_duration": average_duration,
            "success_rate": success_rate,
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "task_success_rate": task_success_rate,
            "time_range_hours": hours
        }
    
    def _update_pipeline_task_statistics(self, pipeline: PipelineExecution) -> None:
        """Update pipeline task statistics based on task executions."""
        pipeline.total_tasks = len(pipeline.task_executions)
        pipeline.completed_tasks = sum(1 for t in pipeline.task_executions if t.status == TaskStatus.COMPLETED)
        pipeline.failed_tasks = sum(1 for t in pipeline.task_executions if t.status == TaskStatus.FAILED)
        pipeline.running_tasks = sum(1 for t in pipeline.task_executions if t.status == TaskStatus.RUNNING)
        pipeline.skipped_tasks = sum(1 for t in pipeline.task_executions if t.status == TaskStatus.SKIPPED)
    
    def _check_performance_baselines(self, pipeline: PipelineExecution) -> None:
        """Check pipeline performance against established baselines."""
        dag_id = pipeline.dag_id
        
        if dag_id not in self.performance_baselines:
            # Establish baseline
            self.performance_baselines[dag_id] = {
                "average_duration": pipeline.duration,
                "success_rate": 100.0 if pipeline.status == PipelineStatus.COMPLETED else 0.0,
                "sample_count": 1
            }
            return
        
        baseline = self.performance_baselines[dag_id]
        
        # Update baseline (exponential moving average)
        alpha = 0.1  # Smoothing factor
        baseline["average_duration"] = (
            alpha * pipeline.duration + (1 - alpha) * baseline["average_duration"]
        )
        
        current_success = 1.0 if pipeline.status == PipelineStatus.COMPLETED else 0.0
        baseline["success_rate"] = (
            alpha * (current_success * 100) + (1 - alpha) * baseline["success_rate"]
        )
        
        baseline["sample_count"] += 1
        
        # Check for performance degradation
        if pipeline.duration > baseline["average_duration"] * 1.5:  # 50% slower than baseline
            self.logger.warning(f"Pipeline {pipeline.pipeline_id} took {pipeline.duration:.2f}s, "
                              f"which is significantly slower than baseline {baseline['average_duration']:.2f}s")
            
            if self.realtime_monitor:
                self.realtime_monitor.collect_pipeline_metric(
                    "pipeline.performance_degradation", 1.0, pipeline.pipeline_id,
                    metadata={
                        "actual_duration": pipeline.duration,
                        "baseline_duration": baseline["average_duration"],
                        "degradation_factor": pipeline.duration / baseline["average_duration"]
                    }
                )
    
    def get_performance_baselines(self) -> Dict[str, Dict[str, float]]:
        """Get current performance baselines for all DAGs."""
        return self.performance_baselines.copy()
    
    def reset_performance_baseline(self, dag_id: str) -> None:
        """Reset performance baseline for a specific DAG."""
        if dag_id in self.performance_baselines:
            del self.performance_baselines[dag_id]
            self.logger.info(f"Reset performance baseline for DAG: {dag_id}")
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        return {
            "active_pipelines": len(self.active_pipelines),
            "completed_pipelines_tracked": len(self.completed_pipelines),
            "performance_baselines": len(self.performance_baselines),
            "callback_counts": {
                "pipeline_start": len(self.pipeline_start_callbacks),
                "pipeline_end": len(self.pipeline_end_callbacks),
                "task_start": len(self.task_start_callbacks),
                "task_end": len(self.task_end_callbacks)
            },
            "recent_statistics": self.get_pipeline_statistics(hours=24)
        }