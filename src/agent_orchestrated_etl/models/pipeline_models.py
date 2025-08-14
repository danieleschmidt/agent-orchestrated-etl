"""Pipeline-related data models with validation and schemas."""

from __future__ import annotations

import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class PipelineStatus(str, Enum):
    """Pipeline execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


class TaskStatus(str, Enum):
    """Task execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    RETRYING = "retrying"


class PipelineType(str, Enum):
    """Pipeline type enumeration."""
    BATCH_PROCESSING = "batch_processing"
    STREAMING = "streaming"
    DATA_WAREHOUSE = "data_warehouse"
    MACHINE_LEARNING = "machine_learning"
    DATA_LAKE = "data_lake"
    GENERAL_PURPOSE = "general_purpose"


class OptimizationStrategy(str, Enum):
    """Pipeline optimization strategy enumeration."""
    THROUGHPUT_OPTIMIZED = "throughput_optimized"
    LATENCY_OPTIMIZED = "latency_optimized"
    ANALYTICS_OPTIMIZED = "analytics_optimized"
    ML_OPTIMIZED = "ml_optimized"
    STORAGE_OPTIMIZED = "storage_optimized"
    BALANCED = "balanced"


class TaskDefinition(BaseModel):
    """Definition of a pipeline task."""
    
    task_id: str = Field(..., description="Unique task identifier")
    name: str = Field(..., description="Human-readable task name")
    task_type: str = Field(..., description="Type of task (extract, transform, load, etc.)")
    description: Optional[str] = Field(None, description="Task description")
    
    # Task configuration
    tool_name: Optional[str] = Field(None, description="Tool to execute the task")
    inputs: Dict[str, Any] = Field(default_factory=dict, description="Task input parameters")
    outputs: Dict[str, Any] = Field(default_factory=dict, description="Expected outputs")
    
    # Dependencies and ordering
    dependencies: List[str] = Field(default_factory=list, description="Task dependencies")
    prerequisites: List[str] = Field(default_factory=list, description="Required conditions")
    
    # Execution configuration
    timeout_seconds: Optional[float] = Field(None, description="Task timeout in seconds")
    retry_config: Dict[str, Any] = Field(default_factory=dict, description="Retry configuration")
    continue_on_failure: bool = Field(False, description="Whether to continue pipeline on task failure")
    
    # Resource requirements
    resource_requirements: Dict[str, Any] = Field(default_factory=dict, description="Resource requirements")
    parallelizable: bool = Field(True, description="Whether task can run in parallel")
    
    # Metadata
    created_at: float = Field(default_factory=time.time, description="Task creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Task tags")
    
    @validator('task_id')
    def validate_task_id(cls, v):
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError('task_id must be a non-empty string')
        return v.strip()
    
    @validator('dependencies')
    def validate_dependencies(cls, v):
        # Remove duplicates and empty strings
        return list(set(dep.strip() for dep in v if dep and dep.strip()))


class TaskExecution(BaseModel):
    """Runtime execution information for a task."""
    
    execution_id: str = Field(..., description="Unique execution identifier")
    task_id: str = Field(..., description="Associated task identifier")
    pipeline_execution_id: str = Field(..., description="Parent pipeline execution ID")
    
    # Execution state
    status: TaskStatus = Field(TaskStatus.PENDING, description="Current execution status")
    attempt_number: int = Field(1, description="Current attempt number")
    max_retries: int = Field(3, description="Maximum retry attempts")
    
    # Timing information
    scheduled_at: Optional[float] = Field(None, description="When task was scheduled")
    started_at: Optional[float] = Field(None, description="When task execution started")
    completed_at: Optional[float] = Field(None, description="When task completed")
    duration_seconds: Optional[float] = Field(None, description="Execution duration")
    
    # Resource usage
    resource_usage: Dict[str, Any] = Field(default_factory=dict, description="Resource usage metrics")
    
    # Results and errors
    result: Optional[Dict[str, Any]] = Field(None, description="Task execution result")
    error_message: Optional[str] = Field(None, description="Error message if failed")
    error_type: Optional[str] = Field(None, description="Error type classification")
    
    # Agent information
    executed_by_agent: Optional[str] = Field(None, description="Agent that executed the task")
    execution_node: Optional[str] = Field(None, description="Node where task was executed")
    
    # Metadata
    logs: List[str] = Field(default_factory=list, description="Task execution logs")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Execution metrics")
    
    def mark_started(self) -> None:
        """Mark task as started."""
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()
        if self.scheduled_at:
            self.metrics["queue_time"] = self.started_at - self.scheduled_at
    
    def mark_completed(self, result: Dict[str, Any]) -> None:
        """Mark task as completed with result."""
        self.status = TaskStatus.COMPLETED
        self.completed_at = time.time()
        self.result = result
        if self.started_at:
            self.duration_seconds = self.completed_at - self.started_at
    
    def mark_failed(self, error_message: str, error_type: Optional[str] = None) -> None:
        """Mark task as failed with error information."""
        self.status = TaskStatus.FAILED
        self.completed_at = time.time()
        self.error_message = error_message
        self.error_type = error_type or "UnknownError"
        if self.started_at:
            self.duration_seconds = self.completed_at - self.started_at


class ExecutionContext(BaseModel):
    """Context information for pipeline execution."""
    
    execution_id: str = Field(..., description="Unique execution identifier")
    pipeline_id: str = Field(..., description="Pipeline identifier")
    
    # Execution environment
    environment: str = Field("development", description="Execution environment")
    user_id: Optional[str] = Field(None, description="User who triggered execution")
    trigger_type: str = Field("manual", description="How execution was triggered")
    
    # Runtime context
    start_time: float = Field(default_factory=time.time, description="Execution start time")
    variables: Dict[str, Any] = Field(default_factory=dict, description="Runtime variables")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")
    
    # Configuration overrides
    config_overrides: Dict[str, Any] = Field(default_factory=dict, description="Configuration overrides")
    resource_constraints: Dict[str, Any] = Field(default_factory=dict, description="Resource constraints")
    
    @validator('execution_id')
    def validate_execution_id(cls, v):
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError('execution_id must be a non-empty string')
        return v.strip()
    
    @validator('pipeline_id')
    def validate_pipeline_id(cls, v):
        if not v or not isinstance(v, str) or len(v.strip()) == 0:
            raise ValueError('pipeline_id must be a non-empty string')
        return v.strip()


class TaskResult(BaseModel):
    """Result of a task execution."""
    
    task_id: str = Field(..., description="Task identifier")
    execution_id: str = Field(..., description="Execution identifier")
    status: TaskStatus = Field(..., description="Final task status")
    
    # Result data
    data: Optional[Dict[str, Any]] = Field(None, description="Task output data")
    metrics: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics")
    
    # Quality information
    data_quality_score: Optional[float] = Field(None, description="Data quality score (0-1)")
    validation_results: List[Dict[str, Any]] = Field(default_factory=list, description="Validation results")
    
    # Timing and performance
    execution_time: Optional[float] = Field(None, description="Execution time in seconds")
    records_processed: Optional[int] = Field(None, description="Number of records processed")
    data_size_bytes: Optional[int] = Field(None, description="Size of processed data")
    
    # Error information
    error_details: Optional[Dict[str, Any]] = Field(None, description="Detailed error information")
    warnings: List[str] = Field(default_factory=list, description="Non-fatal warnings")
    
    # Metadata
    created_at: float = Field(default_factory=time.time, description="Result creation timestamp")
    agent_metadata: Dict[str, Any] = Field(default_factory=dict, description="Agent-specific metadata")


class PipelineConfig(BaseModel):
    """Configuration for an ETL pipeline."""
    
    pipeline_id: str = Field(..., description="Unique pipeline identifier")
    name: str = Field(..., description="Human-readable pipeline name")
    description: Optional[str] = Field(None, description="Pipeline description")
    
    # Pipeline classification
    pipeline_type: PipelineType = Field(PipelineType.GENERAL_PURPOSE, description="Pipeline type")
    optimization_strategy: OptimizationStrategy = Field(OptimizationStrategy.BALANCED, description="Optimization strategy")
    
    # Data source configuration
    data_source: Dict[str, Any] = Field(..., description="Data source configuration")
    target_configuration: Optional[Dict[str, Any]] = Field(None, description="Target configuration")
    
    # Pipeline structure
    tasks: List[TaskDefinition] = Field(..., description="Pipeline tasks")
    dependencies: Dict[str, List[str]] = Field(default_factory=dict, description="Task dependencies")
    
    # Execution configuration
    max_parallel_tasks: int = Field(5, description="Maximum parallel task execution")
    timeout_minutes: Optional[int] = Field(None, description="Pipeline timeout in minutes")
    retry_strategy: str = Field("exponential_backoff", description="Retry strategy")
    
    # Resource configuration
    resource_allocation: Dict[str, Any] = Field(default_factory=dict, description="Resource allocation")
    execution_priority: str = Field("normal", description="Execution priority")
    
    # Quality and validation
    validation_rules: List[Dict[str, Any]] = Field(default_factory=list, description="Data validation rules")
    transformation_rules: List[Dict[str, Any]] = Field(default_factory=list, description="Data transformation rules")
    quality_thresholds: Dict[str, float] = Field(default_factory=dict, description="Quality thresholds")
    
    # Monitoring and alerting
    monitoring_config: Dict[str, Any] = Field(default_factory=dict, description="Monitoring configuration")
    alert_config: Dict[str, Any] = Field(default_factory=dict, description="Alert configuration")
    
    # Metadata
    created_by: Optional[str] = Field(None, description="Pipeline creator")
    created_at: float = Field(default_factory=time.time, description="Creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    version: str = Field("1.0", description="Pipeline version")
    tags: List[str] = Field(default_factory=list, description="Pipeline tags")
    
    @validator('tasks')
    def validate_tasks(cls, v):
        if not v:
            raise ValueError('Pipeline must have at least one task')
        
        # Check for duplicate task IDs
        task_ids = [task.task_id for task in v]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError('Duplicate task IDs found in pipeline')
        
        return v
    
    @validator('dependencies')
    def validate_dependencies(cls, v, values):
        if 'tasks' not in values:
            return v
        
        task_ids = {task.task_id for task in values['tasks']}
        
        # Validate that all dependencies reference valid tasks
        for task_id, deps in v.items():
            if task_id not in task_ids:
                raise ValueError(f'Dependency references unknown task: {task_id}')
            
            for dep in deps:
                if dep not in task_ids:
                    raise ValueError(f'Task {task_id} depends on unknown task: {dep}')
        
        return v


class PipelineExecution(BaseModel):
    """Runtime execution information for a pipeline."""
    
    execution_id: str = Field(..., description="Unique execution identifier")
    pipeline_id: str = Field(..., description="Associated pipeline identifier")
    
    # Execution state
    status: PipelineStatus = Field(PipelineStatus.PENDING, description="Current execution status")
    
    # Timing information
    scheduled_at: Optional[float] = Field(None, description="When pipeline was scheduled")
    started_at: Optional[float] = Field(None, description="When pipeline started")
    completed_at: Optional[float] = Field(None, description="When pipeline completed")
    duration_seconds: Optional[float] = Field(None, description="Total execution duration")
    
    # Task execution tracking
    task_executions: Dict[str, TaskExecution] = Field(default_factory=dict, description="Task executions")
    current_tasks: List[str] = Field(default_factory=list, description="Currently executing tasks")
    completed_tasks: List[str] = Field(default_factory=list, description="Completed tasks")
    failed_tasks: List[str] = Field(default_factory=list, description="Failed tasks")
    
    # Progress tracking
    total_tasks: int = Field(0, description="Total number of tasks")
    completed_task_count: int = Field(0, description="Number of completed tasks")
    failed_task_count: int = Field(0, description="Number of failed tasks")
    completion_percentage: float = Field(0.0, description="Completion percentage")
    
    # Resource usage
    resource_usage: Dict[str, Any] = Field(default_factory=dict, description="Resource usage metrics")
    
    # Configuration snapshot
    configuration_snapshot: Optional[Dict[str, Any]] = Field(None, description="Configuration at execution time")
    
    # Results and metadata
    execution_context: Dict[str, Any] = Field(default_factory=dict, description="Execution context")
    error_summary: Optional[Dict[str, Any]] = Field(None, description="Error summary if failed")
    
    # Agent and infrastructure
    orchestrated_by: Optional[str] = Field(None, description="Orchestrating agent ID")
    execution_environment: Dict[str, Any] = Field(default_factory=dict, description="Execution environment")
    
    def update_progress(self) -> None:
        """Update progress metrics based on task executions."""
        if self.total_tasks > 0:
            self.completion_percentage = (self.completed_task_count / self.total_tasks) * 100
        
        # Update status based on task states
        if self.failed_task_count > 0 and self.current_tasks == []:
            self.status = PipelineStatus.FAILED
        elif self.completed_task_count == self.total_tasks:
            self.status = PipelineStatus.COMPLETED
        elif self.current_tasks:
            self.status = PipelineStatus.RUNNING


class PipelineResult(BaseModel):
    """Final result of a pipeline execution."""
    
    pipeline_id: str = Field(..., description="Pipeline identifier")
    execution_id: str = Field(..., description="Execution identifier")
    status: PipelineStatus = Field(..., description="Final pipeline status")
    
    # Task results
    task_results: Dict[str, TaskResult] = Field(default_factory=dict, description="Individual task results")
    
    # Aggregated metrics
    total_execution_time: float = Field(0.0, description="Total execution time")
    total_records_processed: Optional[int] = Field(None, description="Total records processed")
    total_data_size_bytes: Optional[int] = Field(None, description="Total data size processed")
    
    # Quality metrics
    overall_quality_score: Optional[float] = Field(None, description="Overall data quality score")
    quality_metrics: Dict[str, Any] = Field(default_factory=dict, description="Quality metrics")
    
    # Performance metrics
    performance_metrics: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics")
    resource_efficiency: Dict[str, Any] = Field(default_factory=dict, description="Resource efficiency metrics")
    
    # Error and warning summary
    error_count: int = Field(0, description="Total number of errors")
    warning_count: int = Field(0, description="Total number of warnings")
    error_summary: List[str] = Field(default_factory=list, description="Summary of errors")
    warning_summary: List[str] = Field(default_factory=list, description="Summary of warnings")
    
    # Output information
    output_locations: Dict[str, str] = Field(default_factory=dict, description="Output data locations")
    generated_artifacts: List[str] = Field(default_factory=list, description="Generated artifacts")
    
    # Metadata
    completed_at: float = Field(default_factory=time.time, description="Completion timestamp")
    execution_summary: str = Field("", description="Human-readable execution summary")
    recommendations: List[str] = Field(default_factory=list, description="Optimization recommendations")
    
    def calculate_success_rate(self) -> float:
        """Calculate task success rate."""
        total_tasks = len(self.task_results)
        if total_tasks == 0:
            return 0.0
        
        successful_tasks = sum(
            1 for result in self.task_results.values() 
            if result.status == TaskStatus.COMPLETED
        )
        
        return successful_tasks / total_tasks
    
    def get_failed_tasks(self) -> List[str]:
        """Get list of failed task IDs."""
        return [
            task_id for task_id, result in self.task_results.items()
            if result.status == TaskStatus.FAILED
        ]
    
    def generate_summary(self) -> str:
        """Generate human-readable execution summary."""
        success_rate = self.calculate_success_rate()
        
        summary_parts = [
            f"Pipeline {self.status.value}",
            f"Success rate: {success_rate:.1%}",
            f"Execution time: {self.total_execution_time:.2f}s"
        ]
        
        if self.total_records_processed:
            summary_parts.append(f"Records processed: {self.total_records_processed:,}")
        
        if self.error_count > 0:
            summary_parts.append(f"Errors: {self.error_count}")
        
        if self.warning_count > 0:
            summary_parts.append(f"Warnings: {self.warning_count}")
        
        return " | ".join(summary_parts)