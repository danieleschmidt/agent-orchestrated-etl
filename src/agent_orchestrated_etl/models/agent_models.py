"""Agent-related models for configuration and communication."""

from __future__ import annotations

import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class AgentRole(str, Enum):
    """Agent role enumeration."""
    ORCHESTRATOR = "orchestrator"
    ETL_AGENT = "etl_agent"
    EXTRACTION_AGENT = "extraction_agent"
    TRANSFORMATION_AGENT = "transformation_agent"
    LOADING_AGENT = "loading_agent"
    MONITOR_AGENT = "monitor_agent"
    QUALITY_AGENT = "quality_agent"
    SECURITY_AGENT = "security_agent"
    GENERIC_AGENT = "generic_agent"


class AgentStatus(str, Enum):
    """Agent status enumeration."""
    INITIALIZING = "initializing"
    READY = "ready"
    BUSY = "busy"
    IDLE = "idle"
    ERROR = "error"
    MAINTENANCE = "maintenance"
    SHUTDOWN = "shutdown"


class MessageType(str, Enum):
    """Message type enumeration."""
    TASK_REQUEST = "task_request"
    TASK_RESPONSE = "task_response"
    STATUS_UPDATE = "status_update"
    COORDINATION = "coordination"
    BROADCAST = "broadcast"
    HEARTBEAT = "heartbeat"
    ERROR_NOTIFICATION = "error_notification"
    METRICS_REPORT = "metrics_report"


class MessagePriority(str, Enum):
    """Message priority enumeration."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class WorkflowStatus(str, Enum):
    """Workflow status enumeration."""
    PLANNED = "planned"
    QUEUED = "queued"
    EXECUTING = "executing"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class AgentCapability(BaseModel):
    """Definition of an agent capability."""
    
    name: str = Field(..., description="Capability name")
    description: str = Field(..., description="Capability description")
    category: str = Field("general", description="Capability category")
    
    # Input/output specifications
    input_types: List[str] = Field(default_factory=list, description="Supported input types")
    output_types: List[str] = Field(default_factory=list, description="Supported output types")
    
    # Performance characteristics
    confidence_level: float = Field(0.8, description="Confidence level (0-1)")
    average_execution_time: Optional[float] = Field(None, description="Average execution time in seconds")
    resource_requirements: Dict[str, Any] = Field(default_factory=dict, description="Resource requirements")
    
    # Constraints and limitations
    prerequisites: List[str] = Field(default_factory=list, description="Required prerequisites")
    limitations: List[str] = Field(default_factory=list, description="Known limitations")
    
    # Metadata
    version: str = Field("1.0", description="Capability version")
    enabled: bool = Field(True, description="Whether capability is enabled")
    tags: List[str] = Field(default_factory=list, description="Capability tags")


class AgentConfiguration(BaseModel):
    """Configuration for an agent."""
    
    agent_id: str = Field(..., description="Unique agent identifier")
    name: str = Field(..., description="Agent name")
    role: AgentRole = Field(..., description="Agent role")
    description: Optional[str] = Field(None, description="Agent description")
    
    # Agent capabilities
    capabilities: List[AgentCapability] = Field(default_factory=list, description="Agent capabilities")
    specializations: List[str] = Field(default_factory=list, description="Agent specializations")
    
    # Execution configuration
    max_concurrent_tasks: int = Field(1, description="Maximum concurrent task execution")
    task_timeout_seconds: float = Field(300.0, description="Default task timeout")
    heartbeat_interval_seconds: float = Field(30.0, description="Heartbeat interval")
    
    # Communication configuration
    communication_protocols: List[str] = Field(default_factory=list, description="Supported communication protocols")
    message_queue_size: int = Field(100, description="Message queue size")
    response_timeout_seconds: float = Field(60.0, description="Response timeout")
    
    # Resource configuration
    cpu_allocation: Optional[float] = Field(None, description="CPU allocation (cores)")
    memory_allocation: Optional[str] = Field(None, description="Memory allocation (e.g., '2GB')")
    storage_allocation: Optional[str] = Field(None, description="Storage allocation")
    
    # Monitoring and logging
    logging_level: str = Field("INFO", description="Logging level")
    metrics_enabled: bool = Field(True, description="Whether to collect metrics")
    health_check_enabled: bool = Field(True, description="Whether to enable health checks")
    
    # Security configuration
    authentication_required: bool = Field(True, description="Whether authentication is required")
    encryption_enabled: bool = Field(True, description="Whether encryption is enabled")
    allowed_networks: List[str] = Field(default_factory=list, description="Allowed network ranges")
    
    # Behavioral configuration
    retry_policy: Dict[str, Any] = Field(default_factory=dict, description="Retry policy configuration")
    circuit_breaker_config: Dict[str, Any] = Field(default_factory=dict, description="Circuit breaker configuration")
    graceful_degradation_config: Dict[str, Any] = Field(default_factory=dict, description="Graceful degradation configuration")
    
    # Environment configuration
    environment_variables: Dict[str, str] = Field(default_factory=dict, description="Environment variables")
    configuration_overrides: Dict[str, Any] = Field(default_factory=dict, description="Configuration overrides")
    
    # Metadata
    version: str = Field("1.0", description="Agent version")
    created_at: float = Field(default_factory=time.time, description="Creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Agent tags")
    
    @validator('agent_id')
    def validate_agent_id(cls, v):
        if not v or not v.strip():
            raise ValueError('Agent ID cannot be empty')
        return v.strip()
    
    @validator('max_concurrent_tasks')
    def validate_concurrent_tasks(cls, v):
        if v < 1:
            raise ValueError('Max concurrent tasks must be at least 1')
        return v
    
    def add_capability(self, capability: AgentCapability) -> None:
        """Add a capability to the agent."""
        # Check if capability already exists
        for existing in self.capabilities:
            if existing.name == capability.name:
                # Update existing capability
                existing = capability
                return
        
        # Add new capability
        self.capabilities.append(capability)
    
    def has_capability(self, capability_name: str) -> bool:
        """Check if agent has a specific capability."""
        return any(cap.name == capability_name for cap in self.capabilities)
    
    def get_capability(self, capability_name: str) -> Optional[AgentCapability]:
        """Get a specific capability by name."""
        for capability in self.capabilities:
            if capability.name == capability_name:
                return capability
        return None


class AgentPerformanceMetrics(BaseModel):
    """Performance metrics for an agent."""
    
    agent_id: str = Field(..., description="Agent identifier")
    measurement_period_start: float = Field(..., description="Measurement period start timestamp")
    measurement_period_end: float = Field(..., description="Measurement period end timestamp")
    
    # Task execution metrics
    tasks_started: int = Field(0, description="Number of tasks started")
    tasks_completed: int = Field(0, description="Number of tasks completed")
    tasks_failed: int = Field(0, description="Number of tasks failed")
    tasks_cancelled: int = Field(0, description="Number of tasks cancelled")
    
    # Timing metrics
    average_task_duration: float = Field(0.0, description="Average task duration in seconds")
    median_task_duration: float = Field(0.0, description="Median task duration in seconds")
    min_task_duration: float = Field(0.0, description="Minimum task duration in seconds")
    max_task_duration: float = Field(0.0, description="Maximum task duration in seconds")
    total_execution_time: float = Field(0.0, description="Total execution time in seconds")
    
    # Throughput metrics
    tasks_per_minute: float = Field(0.0, description="Tasks completed per minute")
    data_processed_bytes: int = Field(0, description="Total data processed in bytes")
    records_processed: int = Field(0, description="Total records processed")
    
    # Quality metrics
    success_rate: float = Field(0.0, description="Task success rate (0-1)")
    error_rate: float = Field(0.0, description="Task error rate (0-1)")
    quality_score: float = Field(0.0, description="Overall quality score (0-1)")
    
    # Resource utilization
    cpu_utilization: Dict[str, float] = Field(default_factory=dict, description="CPU utilization metrics")
    memory_utilization: Dict[str, float] = Field(default_factory=dict, description="Memory utilization metrics")
    network_utilization: Dict[str, float] = Field(default_factory=dict, description="Network utilization metrics")
    storage_utilization: Dict[str, float] = Field(default_factory=dict, description="Storage utilization metrics")
    
    # Communication metrics
    messages_sent: int = Field(0, description="Number of messages sent")
    messages_received: int = Field(0, description="Number of messages received")
    message_processing_time: float = Field(0.0, description="Average message processing time")
    
    # Error and reliability metrics
    error_types: Dict[str, int] = Field(default_factory=dict, description="Error counts by type")
    retry_attempts: int = Field(0, description="Total retry attempts")
    circuit_breaker_trips: int = Field(0, description="Circuit breaker trip count")
    
    # Health metrics
    uptime_percentage: float = Field(0.0, description="Uptime percentage")
    health_check_failures: int = Field(0, description="Health check failure count")
    
    def calculate_success_rate(self) -> float:
        """Calculate task success rate."""
        total_tasks = self.tasks_completed + self.tasks_failed
        if total_tasks == 0:
            return 0.0
        return self.tasks_completed / total_tasks
    
    def calculate_throughput(self) -> float:
        """Calculate tasks per minute."""
        duration_minutes = (self.measurement_period_end - self.measurement_period_start) / 60
        if duration_minutes == 0:
            return 0.0
        return self.tasks_completed / duration_minutes
    
    def get_performance_grade(self) -> str:
        """Get performance grade based on metrics."""
        success_rate = self.calculate_success_rate()
        
        if success_rate >= 0.95 and self.quality_score >= 0.9:
            return "A"
        elif success_rate >= 0.9 and self.quality_score >= 0.8:
            return "B"
        elif success_rate >= 0.8 and self.quality_score >= 0.7:
            return "C"
        elif success_rate >= 0.7 and self.quality_score >= 0.6:
            return "D"
        else:
            return "F"


class CommunicationMessage(BaseModel):
    """Message for inter-agent communication."""
    
    message_id: str = Field(..., description="Unique message identifier")
    message_type: MessageType = Field(..., description="Message type")
    sender_id: str = Field(..., description="Sender agent ID")
    recipient_id: Optional[str] = Field(None, description="Recipient agent ID (None for broadcast)")
    
    # Message content
    subject: Optional[str] = Field(None, description="Message subject")
    content: Dict[str, Any] = Field(..., description="Message content")
    attachments: List[Dict[str, Any]] = Field(default_factory=list, description="Message attachments")
    
    # Message metadata
    priority: MessagePriority = Field(MessagePriority.NORMAL, description="Message priority")
    correlation_id: Optional[str] = Field(None, description="Correlation ID for request/response")
    conversation_id: Optional[str] = Field(None, description="Conversation ID for threaded messages")
    
    # Timing and delivery
    created_at: float = Field(default_factory=time.time, description="Message creation timestamp")
    expires_at: Optional[float] = Field(None, description="Message expiration timestamp")
    delivery_attempts: int = Field(0, description="Number of delivery attempts")
    max_delivery_attempts: int = Field(3, description="Maximum delivery attempts")
    
    # Processing state
    delivered: bool = Field(False, description="Whether message was delivered")
    acknowledged: bool = Field(False, description="Whether message was acknowledged")
    processed: bool = Field(False, description="Whether message was processed")
    
    # Response handling
    requires_response: bool = Field(False, description="Whether message requires a response")
    response_timeout: Optional[float] = Field(None, description="Response timeout in seconds")
    response_received: bool = Field(False, description="Whether response was received")
    
    # Security and validation
    checksum: Optional[str] = Field(None, description="Message checksum for integrity")
    signature: Optional[str] = Field(None, description="Digital signature")
    encrypted: bool = Field(False, description="Whether message is encrypted")
    
    def is_expired(self) -> bool:
        """Check if message has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def should_retry_delivery(self) -> bool:
        """Check if delivery should be retried."""
        return (not self.delivered and 
                self.delivery_attempts < self.max_delivery_attempts and
                not self.is_expired())
    
    def mark_delivered(self) -> None:
        """Mark message as delivered."""
        self.delivered = True
        self.delivery_attempts += 1
    
    def mark_acknowledged(self) -> None:
        """Mark message as acknowledged."""
        self.acknowledged = True
    
    def mark_processed(self) -> None:
        """Mark message as processed."""
        self.processed = True


class WorkflowState(BaseModel):
    """State information for a workflow execution."""
    
    workflow_id: str = Field(..., description="Workflow identifier")
    execution_id: str = Field(..., description="Execution identifier")
    status: WorkflowStatus = Field(..., description="Current workflow status")
    
    # Agent assignments
    orchestrator_agent_id: Optional[str] = Field(None, description="Orchestrator agent ID")
    assigned_agents: Dict[str, str] = Field(default_factory=dict, description="Task to agent assignments")
    active_agents: List[str] = Field(default_factory=list, description="Currently active agents")
    
    # Execution state
    current_step: int = Field(0, description="Current execution step")
    total_steps: int = Field(0, description="Total number of steps")
    completion_percentage: float = Field(0.0, description="Completion percentage")
    
    # Task states
    pending_tasks: List[str] = Field(default_factory=list, description="Pending task IDs")
    running_tasks: List[str] = Field(default_factory=list, description="Running task IDs")
    completed_tasks: List[str] = Field(default_factory=list, description="Completed task IDs")
    failed_tasks: List[str] = Field(default_factory=list, description="Failed task IDs")
    
    # Timing information
    started_at: Optional[float] = Field(None, description="Workflow start timestamp")
    estimated_completion: Optional[float] = Field(None, description="Estimated completion timestamp")
    last_updated: float = Field(default_factory=time.time, description="Last update timestamp")
    
    # Resource tracking
    resource_allocations: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Resource allocations by agent")
    resource_usage: Dict[str, Any] = Field(default_factory=dict, description="Current resource usage")
    
    # Communication state
    message_queue: List[str] = Field(default_factory=list, description="Message IDs in queue")
    coordination_state: Dict[str, Any] = Field(default_factory=dict, description="Agent coordination state")
    
    # Error handling
    error_count: int = Field(0, description="Total error count")
    warning_count: int = Field(0, description="Total warning count")
    recovery_attempts: int = Field(0, description="Recovery attempt count")
    
    # Checkpoints and recovery
    checkpoints: List[Dict[str, Any]] = Field(default_factory=list, description="Workflow checkpoints")
    recovery_strategy: Optional[str] = Field(None, description="Current recovery strategy")
    
    # Metadata
    configuration_snapshot: Optional[Dict[str, Any]] = Field(None, description="Configuration snapshot")
    context_data: Dict[str, Any] = Field(default_factory=dict, description="Workflow context data")
    
    def update_progress(self) -> None:
        """Update workflow progress metrics."""
        total_tasks = len(self.pending_tasks) + len(self.running_tasks) + len(self.completed_tasks) + len(self.failed_tasks)
        
        if total_tasks > 0:
            self.completion_percentage = (len(self.completed_tasks) / total_tasks) * 100
            self.total_steps = total_tasks
            self.current_step = len(self.completed_tasks) + len(self.failed_tasks)
        
        self.last_updated = time.time()
    
    def add_checkpoint(self, checkpoint_data: Dict[str, Any]) -> None:
        """Add a workflow checkpoint."""
        checkpoint = {
            "timestamp": time.time(),
            "step": self.current_step,
            "status": self.status.value,
            "completion_percentage": self.completion_percentage,
            "data": checkpoint_data
        }
        self.checkpoints.append(checkpoint)
    
    def get_latest_checkpoint(self) -> Optional[Dict[str, Any]]:
        """Get the most recent checkpoint."""
        if not self.checkpoints:
            return None
        return max(self.checkpoints, key=lambda c: c["timestamp"])
    
    def estimate_remaining_time(self) -> Optional[float]:
        """Estimate remaining execution time based on current progress."""
        if not self.started_at or self.completion_percentage <= 0:
            return None
        
        elapsed_time = time.time() - self.started_at
        estimated_total_time = elapsed_time / (self.completion_percentage / 100)
        remaining_time = estimated_total_time - elapsed_time
        
        return max(0, remaining_time)
    
    def is_healthy(self) -> bool:
        """Check if workflow is in a healthy state."""
        # Basic health checks
        if self.status == WorkflowStatus.FAILED:
            return False
        
        # Check for excessive errors
        total_tasks = len(self.completed_tasks) + len(self.failed_tasks)
        if total_tasks > 0:
            error_rate = len(self.failed_tasks) / total_tasks
            if error_rate > 0.5:  # More than 50% failure rate
                return False
        
        # Check for stalled execution
        if self.status == WorkflowStatus.EXECUTING:
            time_since_update = time.time() - self.last_updated
            if time_since_update > 600:  # No updates for 10 minutes
                return False
        
        return True