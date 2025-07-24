"""Base agent class with common functionality for all agents."""

from __future__ import annotations

import asyncio
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .communication import AgentCommunicationHub

from langchain_core.language_models.base import BaseLanguageModel
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, Field

from ..exceptions import AgentException, ValidationException
from ..logging_config import get_logger, LogContext
from ..retry import retry, RetryConfigs
from ..circuit_breaker import circuit_breaker, CircuitBreakerConfigs


class AgentRole(Enum):
    """Enumeration of agent roles in the system."""
    
    ORCHESTRATOR = "orchestrator"
    ETL_SPECIALIST = "etl_specialist"
    MONITOR = "monitor"
    DATA_ANALYST = "data_analyst"
    QUALITY_CHECKER = "quality_checker"


class AgentState(Enum):
    """Enumeration of agent states."""
    
    INITIALIZING = "initializing"
    READY = "ready"
    WORKING = "working"
    WAITING = "waiting"
    ERROR = "error"
    SHUTTING_DOWN = "shutting_down"
    STOPPED = "stopped"


@dataclass
class AgentConfig:
    """Configuration for an agent."""
    
    # Identity
    agent_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "UnnamedAgent"
    role: AgentRole = AgentRole.ORCHESTRATOR
    
    # Behavior
    max_concurrent_tasks: int = 3
    task_timeout_seconds: float = 300.0
    memory_limit_mb: int = 100
    
    # Communication
    communication_timeout_seconds: float = 30.0
    max_retries: int = 3
    
    # LangChain integration
    model_name: Optional[str] = None
    temperature: float = 0.1
    max_tokens: Optional[int] = None
    
    # Agent-specific configuration
    extra_config: Dict[str, Any] = field(default_factory=dict)


class AgentCapability(BaseModel):
    """Represents a capability that an agent possesses with enhanced metadata."""
    
    name: str = Field(description="Name of the capability")
    description: str = Field(description="Description of what this capability does")
    input_types: List[str] = Field(description="Types of input this capability accepts")
    output_types: List[str] = Field(description="Types of output this capability produces")
    confidence_level: float = Field(ge=0.0, le=1.0, description="Agent's confidence in this capability")
    
    # Enhanced metadata for intelligent selection
    specialization_areas: List[str] = Field(default_factory=list, description="Areas of specialization within this capability")
    performance_metrics: Dict[str, Any] = Field(default_factory=dict, description="Performance metrics for this capability")
    resource_requirements: Dict[str, Any] = Field(default_factory=dict, description="Resource requirements for this capability")
    version: str = Field(default="1.0.0", description="Version of this capability implementation")
    last_updated: float = Field(default_factory=time.time, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Tags for capability categorization")
    prerequisites: List[str] = Field(default_factory=list, description="Prerequisites for using this capability")
    
    class Config:
        """Pydantic configuration."""
        validate_assignment = True


class AgentTask(BaseModel):
    """Represents a task assigned to an agent."""
    
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = Field(description="Type of task")
    description: str = Field(description="Human-readable task description")
    inputs: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=5, ge=1, le=10, description="Task priority (1=lowest, 10=highest)")
    deadline: Optional[float] = Field(None, description="Unix timestamp deadline")
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    created_at: float = Field(default_factory=time.time)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    status: str = "pending"  # pending, running, completed, failed, cancelled


class BaseAgent(ABC):
    """Base class for all agents in the system."""
    
    def __init__(
        self,
        config: AgentConfig,
        llm: Optional[BaseLanguageModel] = None,
        communication_hub: Optional['AgentCommunicationHub'] = None,
    ):
        self.config = config
        self.llm = llm
        self.communication_hub = communication_hub
        
        # Core state
        self.state = AgentState.INITIALIZING
        self.capabilities: List[AgentCapability] = []
        self.active_tasks: Dict[str, AgentTask] = {}
        self.task_history: List[AgentTask] = []
        
        # Performance tracking for enhanced selection
        self.performance_metrics = {
            "total_tasks": 0,
            "successful_tasks": 0,
            "failed_tasks": 0,
            "avg_execution_time": 0.0,
            "total_execution_time": 0.0,
            "success_rate": 1.0,
            "error_rate": 0.0,
            "throughput": 0.0,
            "last_activity": time.time(),
            "availability": 1.0
        }
        self.current_load = 0.0
        self.specialization = getattr(config, 'specialization', 'general')
        
        # Synchronization
        self._task_lock = asyncio.Lock()
        self._state_lock = asyncio.Lock()
        
        # Setup logging
        self.logger = get_logger(f"agent.{config.role.value}.{config.name}")
        
        # Performance tracking
        self.metrics = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_execution_time": 0.0,
            "average_task_time": 0.0,
            "memory_usage_mb": 0.0,
        }
        
        # Initialize agent-specific components
        self._initialize_agent()
    
    @abstractmethod
    def _initialize_agent(self) -> None:
        """Initialize agent-specific components. Must be implemented by subclasses."""
        pass
    
    @abstractmethod
    async def _process_task(self, task: AgentTask) -> Dict[str, Any]:
        """Process a specific task. Must be implemented by subclasses.
        
        Args:
            task: The task to process
            
        Returns:
            Dictionary containing task results
            
        Raises:
            AgentException: If task processing fails
        """
        pass
    
    @abstractmethod
    def get_system_prompt(self) -> str:
        """Get the system prompt for this agent's LLM interactions."""
        pass
    
    async def start(self) -> None:
        """Start the agent and transition to ready state."""
        async with self._state_lock:
            if self.state != AgentState.INITIALIZING:
                raise AgentException(f"Cannot start agent in state: {self.state}")
            
            try:
                with LogContext(agent_id=self.config.agent_id, agent_name=self.config.name):
                    self.logger.info("Starting agent")
                    
                    # Perform startup tasks
                    await self._startup_tasks()
                    
                    # Register with communication hub if available
                    if self.communication_hub:
                        await self.communication_hub.register_agent(self)
                    
                    self.state = AgentState.READY
                    self.logger.info("Agent started successfully")
                    
            except Exception as e:
                self.state = AgentState.ERROR
                self.logger.error(f"Failed to start agent: {e}", exc_info=e)
                raise AgentException(f"Agent startup failed: {e}") from e
    
    async def stop(self) -> None:
        """Stop the agent gracefully."""
        async with self._state_lock:
            if self.state in [AgentState.SHUTTING_DOWN, AgentState.STOPPED]:
                return
            
            try:
                with LogContext(agent_id=self.config.agent_id, agent_name=self.config.name):
                    self.logger.info("Stopping agent")
                    self.state = AgentState.SHUTTING_DOWN
                    
                    # Cancel active tasks
                    await self._cancel_active_tasks()
                    
                    # Unregister from communication hub
                    if self.communication_hub:
                        await self.communication_hub.unregister_agent(self.config.agent_id)
                    
                    # Perform cleanup
                    await self._cleanup_tasks()
                    
                    self.state = AgentState.STOPPED
                    self.logger.info("Agent stopped successfully")
                    
            except Exception as e:
                self.state = AgentState.ERROR
                self.logger.error(f"Error during agent shutdown: {e}", exc_info=e)
                raise
    
    @retry(RetryConfigs.STANDARD)
    @circuit_breaker("agent_task_execution", CircuitBreakerConfigs.STANDARD)
    async def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        """Execute a task with resilience patterns.
        
        Args:
            task: The task to execute
            
        Returns:
            Task execution results
            
        Raises:
            AgentException: If task execution fails
        """
        if self.state != AgentState.READY:
            raise AgentException(f"Agent not ready for task execution. State: {self.state}")
        
        async with self._task_lock:
            if len(self.active_tasks) >= self.config.max_concurrent_tasks:
                raise AgentException("Agent at maximum concurrent task capacity")
            
            # Add to active tasks
            self.active_tasks[task.task_id] = task
        
        try:
            with LogContext(
                agent_id=self.config.agent_id,
                agent_name=self.config.name,
                task_id=task.task_id,
                task_type=task.task_type,
            ):
                self.logger.info(f"Starting task execution: {task.description}")
                
                # Update task status
                task.started_at = time.time()
                task.status = "running"
                self.state = AgentState.WORKING
                
                # Check timeout
                if task.deadline and time.time() > task.deadline:
                    raise AgentException(f"Task {task.task_id} deadline exceeded")
                
                # Process the task
                start_time = time.time()
                result = await asyncio.wait_for(
                    self._process_task(task),
                    timeout=self.config.task_timeout_seconds
                )
                
                # Update metrics and task status
                execution_time = time.time() - start_time
                self._update_metrics(execution_time, success=True)
                
                task.completed_at = time.time()
                task.status = "completed"
                
                self.logger.info(
                    f"Task completed successfully in {execution_time:.2f}s",
                    extra={"execution_time": execution_time, "result_keys": list(result.keys())}
                )
                
                return result
                
        except asyncio.TimeoutError:
            task.status = "failed"
            self._update_metrics(0, success=False)
            error_msg = f"Task {task.task_id} timed out after {self.config.task_timeout_seconds}s"
            self.logger.error(error_msg)
            raise AgentException(error_msg)
            
        except Exception as e:
            task.status = "failed"
            self._update_metrics(0, success=False)
            self.logger.error(f"Task execution failed: {e}", exc_info=e)
            raise AgentException(f"Task execution failed: {e}") from e
            
        finally:
            # Remove from active tasks and add to history
            async with self._task_lock:
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]
                self.task_history.append(task)
                
                # Maintain history size
                if len(self.task_history) > 1000:
                    self.task_history = self.task_history[-500:]
            
            # Return to ready state if no other active tasks
            if not self.active_tasks:
                self.state = AgentState.READY
    
    async def send_message(self, recipient_id: str, message_content: Dict[str, Any]) -> bool:
        """Send a message to another agent.
        
        Args:
            recipient_id: ID of the recipient agent
            message_content: Message content
            
        Returns:
            True if message sent successfully, False otherwise
        """
        if not self.communication_hub:
            self.logger.warning("No communication hub available for message sending")
            return False
        
        try:
            from .communication import Message, MessageType
            
            message = Message(
                sender_id=self.config.agent_id,
                recipient_id=recipient_id,
                message_type=MessageType.TASK_REQUEST,
                content=message_content,
            )
            
            await self.communication_hub.send_message(message)
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}", exc_info=e)
            return False
    
    async def query_llm(
        self,
        prompt: str,
        context: Optional[Dict[str, Any]] = None,
        system_prompt_override: Optional[str] = None,
    ) -> str:
        """Query the language model with context.
        
        Args:
            prompt: The user prompt
            context: Additional context for the query
            system_prompt_override: Override the default system prompt
            
        Returns:
            LLM response
            
        Raises:
            AgentException: If LLM query fails
        """
        if not self.llm:
            raise AgentException("No language model configured for this agent")
        
        try:
            system_prompt = system_prompt_override or self.get_system_prompt()
            
            # Prepare context information
            context_str = ""
            if context:
                context_str = f"\nContext: {context}"
            
            # Create messages
            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"{prompt}{context_str}"),
            ]
            
            # Query LLM
            response = await self.llm.ainvoke(messages)
            
            if hasattr(response, 'content'):
                return response.content
            else:
                return str(response)
                
        except Exception as e:
            self.logger.error(f"LLM query failed: {e}", exc_info=e)
            raise AgentException(f"LLM query failed: {e}") from e
    
    def add_capability(self, capability: AgentCapability) -> None:
        """Add a capability to this agent."""
        self.capabilities.append(capability)
        self.logger.info(f"Added capability: {capability.name}")
    
    def get_capabilities(self) -> List[AgentCapability]:
        """Get list of agent capabilities."""
        return self.capabilities.copy()
    
    def has_capability(self, capability_name: str) -> bool:
        """Check if agent has a specific capability."""
        return any(cap.name == capability_name for cap in self.capabilities)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get current performance metrics for agent selection."""
        return self.performance_metrics.copy()
    
    def get_current_load(self) -> float:
        """Get current load factor (0.0 = idle, 1.0 = fully loaded)."""
        active_task_count = len(self.active_tasks)
        max_concurrent_tasks = self.config.extra_config.get('max_concurrent_tasks', 5)
        return min(active_task_count / max_concurrent_tasks, 1.0)
    
    def get_specialization_score(self, task_type: str) -> float:
        """Get specialization score for a specific task type."""
        # Base score based on agent specialization
        if self.specialization == task_type or self.specialization == 'general':
            base_score = 0.8 if self.specialization == task_type else 0.5
        else:
            base_score = 0.3
        
        # Adjust based on historical performance for this task type
        task_type_history = [t for t in self.task_history if t.task_type == task_type]
        if task_type_history:
            successful_tasks = len([t for t in task_type_history if t.status == 'completed'])
            success_rate = successful_tasks / len(task_type_history)
            base_score = (base_score + success_rate) / 2
        
        return min(base_score, 1.0)
    
    def update_performance_metrics(self, task: AgentTask, execution_time: float, success: bool):
        """Update performance metrics after task completion."""
        self.performance_metrics["total_tasks"] += 1
        self.performance_metrics["total_execution_time"] += execution_time
        self.performance_metrics["last_activity"] = time.time()
        
        if success:
            self.performance_metrics["successful_tasks"] += 1
        else:
            self.performance_metrics["failed_tasks"] += 1
        
        # Update calculated metrics
        total_tasks = self.performance_metrics["total_tasks"]
        self.performance_metrics["success_rate"] = self.performance_metrics["successful_tasks"] / total_tasks
        self.performance_metrics["error_rate"] = self.performance_metrics["failed_tasks"] / total_tasks
        self.performance_metrics["avg_execution_time"] = self.performance_metrics["total_execution_time"] / total_tasks
        
        # Calculate throughput (tasks per minute)
        if execution_time > 0:
            self.performance_metrics["throughput"] = 60.0 / execution_time
        
        # Update availability (simplified calculation)
        error_rate = self.performance_metrics["error_rate"]
        self.performance_metrics["availability"] = max(0.5, 1.0 - error_rate)
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status and metrics."""
        return {
            "agent_id": self.config.agent_id,
            "name": self.config.name,
            "role": self.config.role.value,
            "state": self.state.value,
            "active_tasks": len(self.active_tasks),
            "capabilities": [cap.name for cap in self.capabilities],
            "metrics": self.metrics.copy(),
            "uptime_seconds": time.time() - getattr(self, '_start_time', time.time()),
        }
    
    async def _startup_tasks(self) -> None:
        """Perform agent-specific startup tasks. Override in subclasses."""
        self._start_time = time.time()
        
        # Register default capabilities
        await self._register_default_capabilities()
    
    async def _cleanup_tasks(self) -> None:
        """Perform agent-specific cleanup tasks. Override in subclasses."""
        pass
    
    async def _cancel_active_tasks(self) -> None:
        """Cancel all active tasks."""
        for task in self.active_tasks.values():
            task.status = "cancelled"
            self.logger.info(f"Cancelled task: {task.task_id}")
        
        self.active_tasks.clear()
    
    async def _register_default_capabilities(self) -> None:
        """Register default capabilities for this agent. Override in subclasses."""
        base_capability = AgentCapability(
            name="basic_reasoning",
            description="Basic reasoning and problem-solving capabilities",
            input_types=["text", "json"],
            output_types=["text", "json"],
            confidence_level=0.8,
        )
        self.add_capability(base_capability)
    
    def _update_metrics(self, execution_time: float, success: bool) -> None:
        """Update agent performance metrics."""
        if success:
            self.metrics["tasks_completed"] += 1
            self.metrics["total_execution_time"] += execution_time
            
            # Update average
            if self.metrics["tasks_completed"] > 0:
                self.metrics["average_task_time"] = (
                    self.metrics["total_execution_time"] / self.metrics["tasks_completed"]
                )
        else:
            self.metrics["tasks_failed"] += 1
    
    def _validate_task(self, task: AgentTask) -> None:
        """Validate a task before execution."""
        if not task.task_id:
            raise ValidationException("Task must have an ID")
        
        if not task.task_type:
            raise ValidationException("Task must have a type")
        
        if task.priority < 1 or task.priority > 10:
            raise ValidationException("Task priority must be between 1 and 10")
        
        if task.deadline and task.deadline <= time.time():
            raise ValidationException("Task deadline has already passed")
    
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.config.agent_id}, name={self.config.name}, state={self.state.value})"