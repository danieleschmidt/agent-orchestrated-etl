"""Agent coordination patterns and multi-agent workflows."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .base_agent import BaseAgent, AgentTask, AgentCapability
from .communication import AgentCommunicationHub
from .orchestrator_agent import OrchestratorAgent
from ..exceptions import CoordinationException
from ..logging_config import get_logger, LogContext


class CoordinationPattern(Enum):
    """Types of agent coordination patterns."""
    
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"
    PIPELINE = "pipeline"
    HIERARCHICAL = "hierarchical"
    CONSENSUS = "consensus"
    BROADCAST = "broadcast"
    AUCTION = "auction"
    NEGOTIATION = "negotiation"


class WorkflowStatus(Enum):
    """Status of multi-agent workflows."""
    
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass
class CoordinationTask:
    """Represents a task in a coordination workflow."""
    
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    agent_id: str = ""
    task_type: str = ""
    task_data: Dict[str, Any] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    priority: int = 5
    
    status: str = "pending"
    assigned_at: Optional[float] = None
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class WorkflowDefinition:
    """Defines a multi-agent workflow."""
    
    workflow_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    name: str = "Unnamed Workflow"
    description: str = ""
    coordination_pattern: CoordinationPattern = CoordinationPattern.SEQUENTIAL
    
    tasks: List[CoordinationTask] = field(default_factory=list)
    agents: List[str] = field(default_factory=list)
    
    # Workflow configuration
    timeout_seconds: Optional[float] = None
    retry_policy: Dict[str, Any] = field(default_factory=dict)
    error_handling: str = "stop_on_error"  # stop_on_error, continue_on_error, retry
    
    # Metadata
    created_at: float = field(default_factory=time.time)
    created_by: str = ""
    tags: Set[str] = field(default_factory=set)


class AgentCoordinator:
    """Coordinates multi-agent workflows and interactions."""
    
    def __init__(
        self,
        communication_hub: AgentCommunicationHub,
        orchestrator_agent: Optional[OrchestratorAgent] = None,
    ):
        self.communication_hub = communication_hub
        self.orchestrator_agent = orchestrator_agent
        
        # Coordination state
        self.registered_agents: Dict[str, BaseAgent] = {}
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_history: List[Dict[str, Any]] = []
        
        # Coordination patterns
        self.pattern_handlers = {
            CoordinationPattern.SEQUENTIAL: self._execute_sequential,
            CoordinationPattern.PARALLEL: self._execute_parallel,
            CoordinationPattern.PIPELINE: self._execute_pipeline,
            CoordinationPattern.HIERARCHICAL: self._execute_hierarchical,
            CoordinationPattern.CONSENSUS: self._execute_consensus,
            CoordinationPattern.BROADCAST: self._execute_broadcast,
            CoordinationPattern.AUCTION: self._execute_auction,
            CoordinationPattern.NEGOTIATION: self._execute_negotiation,
        }
        
        # Performance tracking
        self.coordination_metrics = {
            "workflows_executed": 0,
            "workflows_completed": 0,
            "workflows_failed": 0,
            "average_execution_time": 0.0,
            "total_coordination_time": 0.0,
        }
        
        # Enhanced agent selection capabilities (ETL-013)
        self.selection_audit_trail = []
        self.load_balancing_metrics = {
            "agent_loads": {},
            "load_distribution": [],
            "balancing_effectiveness": 0.0,
            "last_updated": time.time()
        }
        
        self.logger = get_logger("agent.coordination")
        self.logger.info("Agent coordinator initialized with enhanced selection capabilities")
    
    async def register_agent(self, agent: BaseAgent) -> None:
        """Register an agent for coordination."""
        agent_id = agent.config.agent_id
        
        if agent_id in self.registered_agents:
            raise CoordinationException(f"Agent {agent_id} is already registered")
        
        self.registered_agents[agent_id] = agent
        
        # Register with communication hub if not already registered
        try:
            await self.communication_hub.register_agent(agent)
        except Exception:
            pass  # Agent may already be registered
        
        self.logger.info(f"Agent registered for coordination: {agent_id} ({agent.config.role.value})")
    
    async def unregister_agent(self, agent_id: str) -> None:
        """Unregister an agent from coordination."""
        if agent_id in self.registered_agents:
            del self.registered_agents[agent_id]
            self.logger.info(f"Agent unregistered from coordination: {agent_id}")
    
    async def execute_workflow(self, workflow_def: WorkflowDefinition) -> Dict[str, Any]:
        """Execute a multi-agent workflow."""
        workflow_id = workflow_def.workflow_id
        
        with LogContext(workflow_id=workflow_id, pattern=workflow_def.coordination_pattern.value):
            self.logger.info(f"Starting workflow execution: {workflow_def.name}")
            
            try:
                # Initialize workflow execution state
                workflow_state = await self._initialize_workflow(workflow_def)
                self.active_workflows[workflow_id] = workflow_state
                
                # Execute based on coordination pattern
                pattern_handler = self.pattern_handlers.get(workflow_def.coordination_pattern)
                if not pattern_handler:
                    raise CoordinationException(f"Unsupported coordination pattern: {workflow_def.coordination_pattern}")
                
                execution_result = await pattern_handler(workflow_def, workflow_state)
                
                # Finalize workflow
                workflow_state["status"] = WorkflowStatus.COMPLETED
                workflow_state["completed_at"] = time.time()
                workflow_state["execution_result"] = execution_result
                
                # Update metrics
                self._update_coordination_metrics(workflow_state, success=True)
                
                # Move to history
                self.workflow_history.append(workflow_state.copy())
                del self.active_workflows[workflow_id]
                
                self.logger.info(f"Workflow completed: {workflow_def.name}")
                
                return {
                    "workflow_id": workflow_id,
                    "status": "completed",
                    "execution_result": execution_result,
                    "execution_time": workflow_state["completed_at"] - workflow_state["started_at"],
                    "tasks_completed": len([t for t in workflow_def.tasks if t.status == "completed"]),
                    "tasks_failed": len([t for t in workflow_def.tasks if t.status == "failed"]),
                }
                
            except Exception as e:
                self.logger.error(f"Workflow execution failed: {e}", exc_info=e)
                
                # Update workflow state
                if workflow_id in self.active_workflows:
                    workflow_state = self.active_workflows[workflow_id]
                    workflow_state["status"] = WorkflowStatus.FAILED
                    workflow_state["error"] = str(e)
                    workflow_state["completed_at"] = time.time()
                    
                    # Update metrics
                    self._update_coordination_metrics(workflow_state, success=False)
                    
                    # Move to history
                    self.workflow_history.append(workflow_state.copy())
                    del self.active_workflows[workflow_id]
                
                raise CoordinationException(f"Workflow execution failed: {e}") from e
    
    async def cancel_workflow(self, workflow_id: str) -> bool:
        """Cancel an active workflow."""
        if workflow_id not in self.active_workflows:
            return False
        
        try:
            workflow_state = self.active_workflows[workflow_id]
            workflow_state["status"] = WorkflowStatus.CANCELLED
            workflow_state["completed_at"] = time.time()
            
            # Cancel active tasks
            for task in workflow_state["workflow_def"].tasks:
                if task.status in ["pending", "running"]:
                    task.status = "cancelled"
            
            # Move to history
            self.workflow_history.append(workflow_state.copy())
            del self.active_workflows[workflow_id]
            
            self.logger.info(f"Workflow cancelled: {workflow_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to cancel workflow {workflow_id}: {e}")
            return False
    
    async def get_workflow_status(self, workflow_id: str) -> Optional[Dict[str, Any]]:
        """Get the status of a workflow."""
        if workflow_id in self.active_workflows:
            return self._get_workflow_status_info(self.active_workflows[workflow_id])
        
        # Check workflow history
        for workflow in self.workflow_history:
            if workflow["workflow_id"] == workflow_id:
                return self._get_workflow_status_info(workflow)
        
        return None
    
    async def list_active_workflows(self) -> List[Dict[str, Any]]:
        """List all active workflows."""
        return [
            self._get_workflow_status_info(workflow_state)
            for workflow_state in self.active_workflows.values()
        ]
    
    async def _initialize_workflow(self, workflow_def: WorkflowDefinition) -> Dict[str, Any]:
        """Initialize workflow execution state."""
        # Validate workflow
        await self._validate_workflow(workflow_def)
        
        # Create workflow state
        workflow_state = {
            "workflow_id": workflow_def.workflow_id,
            "workflow_def": workflow_def,
            "status": WorkflowStatus.INITIALIZING,
            "started_at": time.time(),
            "task_results": {},
            "agent_assignments": {},
            "execution_log": [],
        }
        
        # Assign agents to tasks
        await self._assign_agents_to_tasks(workflow_def, workflow_state)
        
        workflow_state["status"] = WorkflowStatus.RUNNING
        return workflow_state
    
    async def _validate_workflow(self, workflow_def: WorkflowDefinition) -> None:
        """Validate workflow definition."""
        if not workflow_def.tasks:
            raise CoordinationException("Workflow must have at least one task")
        
        # Check if required agents are available
        for agent_id in workflow_def.agents:
            if agent_id not in self.registered_agents:
                raise CoordinationException(f"Required agent not registered: {agent_id}")
        
        # Validate task dependencies
        task_ids = {task.task_id for task in workflow_def.tasks}
        for task in workflow_def.tasks:
            for dep_id in task.dependencies:
                if dep_id not in task_ids:
                    raise CoordinationException(f"Task {task.task_id} has invalid dependency: {dep_id}")
        
        # Check for circular dependencies
        if self._has_circular_dependencies(workflow_def.tasks):
            raise CoordinationException("Workflow has circular dependencies")
    
    def _has_circular_dependencies(self, tasks: List[CoordinationTask]) -> bool:
        """Check if tasks have circular dependencies."""
        # Simple cycle detection using DFS
        task_graph = {task.task_id: task.dependencies for task in tasks}
        visited = set()
        rec_stack = set()
        
        def has_cycle(node: str) -> bool:
            if node in rec_stack:
                return True
            if node in visited:
                return False
            
            visited.add(node)
            rec_stack.add(node)
            
            for neighbor in task_graph.get(node, []):
                if has_cycle(neighbor):
                    return True
            
            rec_stack.remove(node)
            return False
        
        for task_id in task_graph:
            if task_id not in visited:
                if has_cycle(task_id):
                    return True
        
        return False
    
    async def _assign_agents_to_tasks(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> None:
        """Assign agents to workflow tasks."""
        for task in workflow_def.tasks:
            if task.agent_id:
                # Task has specific agent assignment
                if task.agent_id not in self.registered_agents:
                    raise CoordinationException(f"Assigned agent not available: {task.agent_id}")
                workflow_state["agent_assignments"][task.task_id] = task.agent_id
            else:
                # Find suitable agent for task
                suitable_agent = await self._find_suitable_agent(task, workflow_def)
                if not suitable_agent:
                    raise CoordinationException(f"No suitable agent found for task: {task.task_id}")
                task.agent_id = suitable_agent
                workflow_state["agent_assignments"][task.task_id] = suitable_agent
    
    async def _find_suitable_agent(self, task: CoordinationTask, workflow_def: WorkflowDefinition) -> Optional[str]:
        """Find a suitable agent for a task using sophisticated capability matching."""
        # Extract required capabilities from task
        required_capabilities = self._get_required_capabilities(task)
        
        # Score agents based on capability matching
        agent_scores = []
        for agent_id, agent in self.registered_agents.items():
            if agent_id in workflow_def.agents:
                score = self._calculate_agent_score(agent, required_capabilities, task)
                if score > 0:  # Only consider agents with some capability match
                    agent_scores.append((agent_id, score))
        
        # Sort by score (highest first) and return best match
        if agent_scores:
            agent_scores.sort(key=lambda x: x[1], reverse=True)
            return agent_scores[0][0]
        
        # Fallback: try role-based matching if no capability match
        for agent_id, agent in self.registered_agents.items():
            if agent_id in workflow_def.agents:
                if task.task_type in ["extract_data", "transform_data", "load_data"] and agent.config.role.value == "etl_specialist":
                    return agent_id
                elif task.task_type in ["monitor_pipeline", "check_health"] and agent.config.role.value == "monitor":
                    return agent_id
                elif task.task_type in ["create_workflow", "execute_workflow"] and agent.config.role.value == "orchestrator":
                    return agent_id
        
        # Final fallback to any available agent from the workflow
        return workflow_def.agents[0] if workflow_def.agents else None
    
    def _get_required_capabilities(self, task: CoordinationTask) -> List[str]:
        """Extract required capabilities from task type and metadata."""
        capability_mapping = {
            "extract_data": ["data_extraction"],
            "transform_data": ["data_transformation"],
            "load_data": ["data_loading"],
            "validate_data": ["data_validation"],
            "monitor_pipeline": ["pipeline_monitoring", "health_check"],
            "check_health": ["health_check", "system_monitoring"],
            "create_workflow": ["workflow_creation", "orchestration"],
            "execute_workflow": ["workflow_execution", "orchestration"],
            "optimize_sql": ["sql_optimization"],
            "analyze_data": ["data_analysis", "statistical_analysis"],
        }
        
        # Get base capabilities for task type
        required = capability_mapping.get(task.task_type, [])
        
        # Add capabilities based on task data
        if "data_source" in task.task_data:
            source_type = task.task_data["data_source"]
            if source_type == "database":
                required.extend(["sql_optimization", "database_connectivity"])
            elif source_type == "cloud":
                required.extend(["cloud_integration", "api_management"])
            elif source_type == "streaming":
                required.extend(["stream_processing", "real_time_analytics"])
        
        if "data_format" in task.task_data:
            data_format = task.task_data["data_format"]
            if data_format in ["json", "xml"]:
                required.extend(["data_parsing", "schema_inference"])
            elif data_format in ["parquet", "avro"]:
                required.extend(["columnar_processing", "compression_handling"])
        
        return list(set(required))  # Remove duplicates
    
    def _calculate_agent_score(self, agent: 'BaseAgent', required_capabilities: List[str], task: CoordinationTask) -> float:
        """Calculate agent suitability score based on capabilities."""
        if not required_capabilities:
            return 0.5  # Neutral score if no specific requirements
        
        agent_capabilities = agent.get_capabilities()
        capability_names = {cap.name for cap in agent_capabilities}
        
        # Check direct capability matches
        matches = 0
        confidence_sum = 0.0
        
        for required_cap in required_capabilities:
            if required_cap in capability_names:
                matches += 1
                # Find the capability to get confidence level
                cap = next((c for c in agent_capabilities if c.name == required_cap), None)
                if cap:
                    confidence_sum += cap.confidence_level
        
        if matches == 0:
            return 0.0  # No capability match
        
        # Calculate base score from capability matches
        match_ratio = matches / len(required_capabilities)
        avg_confidence = confidence_sum / matches
        base_score = match_ratio * avg_confidence
        
        # Apply priority bonus (higher priority tasks get better agents)
        priority_bonus = task.priority / 10.0 * 0.1  # Up to 10% bonus
        
        # Apply input/output type compatibility bonus
        io_bonus = self._calculate_io_compatibility(agent_capabilities, task)
        
        final_score = base_score + priority_bonus + io_bonus
        return min(final_score, 1.0)  # Cap at 1.0
    
    def _calculate_io_compatibility(self, agent_capabilities: List['AgentCapability'], task: CoordinationTask) -> float:
        """Calculate input/output type compatibility bonus."""
        # Extract expected input/output types from task
        task_inputs = set()
        task_outputs = set()
        
        # Infer types from task data
        if "source_type" in task.task_data:
            task_inputs.add("source_config")
        if "target_type" in task.task_data:
            task_outputs.add("load_results")
        if "data_format" in task.task_data:
            task_inputs.add("source_data")
            task_outputs.add("transformed_data")
        
        if not task_inputs and not task_outputs:
            return 0.0  # No I/O information available
        
        # Check compatibility with agent capabilities
        compatible_inputs = 0
        compatible_outputs = 0
        
        for cap in agent_capabilities:
            if task_inputs:
                compatible_inputs += len(set(cap.input_types) & task_inputs)
            if task_outputs:
                compatible_outputs += len(set(cap.output_types) & task_outputs)
        
        total_required = len(task_inputs) + len(task_outputs)
        total_compatible = compatible_inputs + compatible_outputs
        
        return (total_compatible / total_required * 0.2) if total_required > 0 else 0.0  # Up to 20% bonus
    
    # Enhanced agent selection methods for ETL-013
    
    def _get_enhanced_capabilities(self, task: CoordinationTask) -> List[str]:
        """Enhanced capability inference from task context."""
        required_capabilities = self._get_required_capabilities(task)
        
        # Add intelligent capability inference
        task_data = task.task_data
        
        # Infer from volume and complexity
        if task_data.get("volume") == "large":
            required_capabilities.extend(["high_throughput", "memory_optimization"])
        
        if task_data.get("complexity") == "high":
            required_capabilities.extend(["advanced_algorithms", "error_handling"])
        
        # Infer from real-time requirements
        if task_data.get("real_time"):
            required_capabilities.extend(["real_time_processing", "low_latency"])
        
        # Infer database-specific optimizations
        if "postgresql" in str(task_data.get("data_source", "")).lower():
            required_capabilities.extend(["postgresql_optimization", "database_optimization"])
        
        # Infer format-specific processing
        data_format = task_data.get("data_format", "").lower()
        if data_format == "json":
            required_capabilities.extend(["json_processing", "schema_validation"])
        
        return list(set(required_capabilities))
    
    def _calculate_performance_score(self, agent: 'BaseAgent', task: CoordinationTask) -> float:
        """Calculate performance-based score for agent selection."""
        try:
            metrics = agent.get_performance_metrics()
            
            # Extract performance requirements from task
            task_requirements = task.task_data.get("performance_requirement", "standard")
            
            # Weight different metrics based on task requirements
            if task_requirements == "high":
                # Prioritize speed and reliability for high-performance tasks
                score = (
                    metrics.get("success_rate", 0.5) * 0.4 +
                    (1.0 - min(metrics.get("avg_execution_time", 1000) / 1000, 1.0)) * 0.3 +
                    metrics.get("availability", 0.5) * 0.2 +
                    min(metrics.get("throughput", 0) / 1000, 1.0) * 0.1
                )
            else:
                # Standard weighting for normal tasks
                score = (
                    metrics.get("success_rate", 0.5) * 0.5 +
                    metrics.get("availability", 0.5) * 0.3 +
                    (1.0 - metrics.get("error_rate", 0.5)) * 0.2
                )
            
            return min(score, 1.0)
            
        except Exception as e:
            self.logger.warning(f"Could not calculate performance score for agent: {e}")
            return 0.5  # Default neutral score
    
    def _find_fallback_agent(self, task: CoordinationTask) -> Dict[str, Any]:
        """Find fallback agent when no exact capability match exists."""
        required_capabilities = self._get_enhanced_capabilities(task)
        
        best_partial_match = None
        best_match_score = 0.0
        
        for agent_id, agent in self.registered_agents.items():
            agent_capabilities = {cap.name for cap in agent.get_capabilities()}
            
            # Calculate partial match score
            matches = len(set(required_capabilities) & agent_capabilities)
            partial_score = matches / len(required_capabilities) if required_capabilities else 0
            
            if partial_score > best_match_score:
                best_match_score = partial_score
                best_partial_match = agent_id
        
        if best_partial_match and best_match_score >= 0.3:  # At least 30% capability match
            missing_capabilities = set(required_capabilities) - {cap.name for cap in self.registered_agents[best_partial_match].get_capabilities()}
            
            return {
                "strategy": "partial_match",
                "recommended_agent": best_partial_match,
                "match_score": best_match_score,
                "capability_gap": list(missing_capabilities),
                "mitigation_steps": [
                    f"Agent lacks {len(missing_capabilities)} required capabilities",
                    "Consider task decomposition or capability enhancement",
                    "Monitor execution closely for potential issues"
                ]
            }
        else:
            # No suitable agent found - suggest decomposition
            return {
                "strategy": "decompose",
                "recommended_agent": None,
                "capability_gap": required_capabilities,
                "mitigation_steps": [
                    "Break task into smaller components",
                    "Assign each component to specialized agents",
                    "Use workflow orchestration for coordination"
                ]
            }
    
    async def _find_suitable_agent_with_audit(self, task: CoordinationTask, workflow_def: Optional[WorkflowDefinition]) -> Optional[str]:
        """Find suitable agent with comprehensive audit trail."""
        start_time = time.time()
        
        # Get all potential agents
        candidates = []
        selection_criteria = {
            "required_capabilities": self._get_enhanced_capabilities(task),
            "performance_requirements": task.task_data.get("performance_requirement", "standard"),
            "load_balancing": True
        }
        
        for agent_id, agent in self.registered_agents.items():
            if not workflow_def or agent_id in workflow_def.agents:
                # Calculate comprehensive score
                capability_score = self._calculate_agent_score(agent, selection_criteria["required_capabilities"], task)
                performance_score = self._calculate_performance_score(agent, task)
                load_factor = agent.get_current_load()
                specialization_score = agent.get_specialization_score(task.task_type)
                
                # Combined score with load balancing
                combined_score = (
                    capability_score * 0.4 +
                    performance_score * 0.3 +
                    specialization_score * 0.2 +
                    (1.0 - load_factor) * 0.1  # Prefer less loaded agents
                )
                
                if combined_score > 0:
                    candidates.append({
                        "agent_id": agent_id,
                        "combined_score": combined_score,
                        "capability_score": capability_score,
                        "performance_score": performance_score,
                        "load_factor": load_factor,
                        "specialization_score": specialization_score
                    })
        
        # Sort by combined score
        candidates.sort(key=lambda x: x["combined_score"], reverse=True)
        
        # Select best candidate
        selected_agent = candidates[0]["agent_id"] if candidates else None
        
        # Create audit trail entry
        selection_time = time.time() - start_time
        audit_entry = {
            "task_id": task.task_id,
            "selected_agent": selected_agent,
            "selection_score": candidates[0]["combined_score"] if candidates else 0.0,
            "alternatives_considered": len(candidates),
            "selection_criteria": selection_criteria,
            "candidates": candidates[:5],  # Top 5 candidates
            "timestamp": time.time(),
            "selection_time_ms": round(selection_time * 1000, 2)
        }
        
        self.selection_audit_trail.append(audit_entry)
        
        # Keep only last 1000 entries
        if len(self.selection_audit_trail) > 1000:
            self.selection_audit_trail = self.selection_audit_trail[-1000:]
        
        return selected_agent
    
    def _find_suitable_agent_with_load_balancing(self, task: CoordinationTask, workflow_def: Optional[WorkflowDefinition]) -> Optional[str]:
        """Find suitable agent with load balancing optimization."""
        required_capabilities = self._get_enhanced_capabilities(task)
        
        # Get capable agents
        capable_agents = []
        for agent_id, agent in self.registered_agents.items():
            if not workflow_def or agent_id in workflow_def.agents:
                capability_score = self._calculate_agent_score(agent, required_capabilities, task)
                if capability_score >= 0.5:  # Minimum capability threshold
                    current_load = agent.get_current_load()
                    capable_agents.append({
                        "agent_id": agent_id,
                        "capability_score": capability_score,
                        "current_load": current_load,
                        "load_adjusted_score": capability_score * (1.0 - current_load)
                    })
        
        if not capable_agents:
            return None
        
        # Sort by load-adjusted score
        capable_agents.sort(key=lambda x: x["load_adjusted_score"], reverse=True)
        
        # Update load balancing metrics
        agent_loads = {agent["agent_id"]: agent["current_load"] for agent in capable_agents}
        self.load_balancing_metrics["agent_loads"] = agent_loads
        self.load_balancing_metrics["load_distribution"].append({
            "timestamp": time.time(),
            "loads": agent_loads.copy()
        })
        
        # Calculate balancing effectiveness
        loads = list(agent_loads.values())
        if loads:
            load_variance = sum((load - sum(loads)/len(loads))**2 for load in loads) / len(loads)
            self.load_balancing_metrics["balancing_effectiveness"] = max(0.0, 1.0 - load_variance)
        
        self.load_balancing_metrics["last_updated"] = time.time()
        
        # Keep only last 100 load distribution entries
        if len(self.load_balancing_metrics["load_distribution"]) > 100:
            self.load_balancing_metrics["load_distribution"] = self.load_balancing_metrics["load_distribution"][-100:]
        
        return capable_agents[0]["agent_id"]
    
    def _calculate_specialization_match(self, agent: 'BaseAgent', task: CoordinationTask) -> float:
        """Calculate how well agent specialization matches task requirements."""
        # Get specialization score from agent
        specialization_score = agent.get_specialization_score(task.task_type)
        
        # Factor in task complexity and requirements
        task_complexity = task.task_data.get("complexity", "medium")
        complexity_multiplier = {
            "low": 0.8,
            "medium": 1.0,
            "high": 1.2
        }.get(task_complexity, 1.0)
        
        # Adjust for data source specialization
        data_source = task.task_data.get("data_source", "")
        if hasattr(agent, 'specialization') and agent.specialization in data_source:
            specialization_score *= 1.1  # 10% bonus for data source match
        
        return min(specialization_score * complexity_multiplier, 1.0)
    
    def get_selection_audit_trail(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent agent selection audit trail."""
        return self.selection_audit_trail[-limit:]
    
    def get_load_balancing_metrics(self) -> Dict[str, Any]:
        """Get current load balancing metrics."""
        return self.load_balancing_metrics.copy()
    
    # Coordination pattern implementations
    
    async def _execute_sequential(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks sequentially in dependency order."""
        self.logger.info("Executing sequential coordination pattern")
        
        # Sort tasks by dependencies (topological sort)
        sorted_tasks = self._topological_sort_tasks(workflow_def.tasks)
        
        results = []
        for task in sorted_tasks:
            try:
                result = await self._execute_single_task(task, workflow_state)
                results.append(result)
                workflow_state["task_results"][task.task_id] = result
                
            except Exception as e:
                self.logger.error(f"Task {task.task_id} failed: {e}")
                if workflow_def.error_handling == "stop_on_error":
                    raise
                # Continue with next task if error handling allows
        
        return {
            "pattern": "sequential",
            "total_tasks": len(sorted_tasks),
            "completed_tasks": len(results),
            "results": results,
        }
    
    async def _execute_parallel(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute independent tasks in parallel."""
        self.logger.info("Executing parallel coordination pattern")
        
        # Group tasks by dependency level
        task_levels = self._group_tasks_by_dependency_level(workflow_def.tasks)
        all_results = []
        
        for level, tasks in enumerate(task_levels):
            self.logger.info(f"Executing level {level} with {len(tasks)} tasks")
            
            # Execute all tasks at this level in parallel
            level_tasks = [self._execute_single_task(task, workflow_state) for task in tasks]
            
            try:
                level_results = await asyncio.gather(*level_tasks, return_exceptions=True)
                
                # Process results
                for task, result in zip(tasks, level_results):
                    if isinstance(result, Exception):
                        self.logger.error(f"Task {task.task_id} failed: {result}")
                        if workflow_def.error_handling == "stop_on_error":
                            raise result
                    else:
                        workflow_state["task_results"][task.task_id] = result
                        all_results.append(result)
                        
            except Exception:
                if workflow_def.error_handling == "stop_on_error":
                    raise
        
        return {
            "pattern": "parallel",
            "total_levels": len(task_levels),
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(all_results),
            "results": all_results,
        }
    
    async def _execute_pipeline(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks as a data pipeline."""
        self.logger.info("Executing pipeline coordination pattern")
        
        # Sort tasks in pipeline order
        sorted_tasks = self._topological_sort_tasks(workflow_def.tasks)
        
        pipeline_data = None
        results = []
        
        for i, task in enumerate(sorted_tasks):
            try:
                # Pass output of previous task as input to current task
                if pipeline_data:
                    task.task_data["input_data"] = pipeline_data
                
                result = await self._execute_single_task(task, workflow_state)
                results.append(result)
                workflow_state["task_results"][task.task_id] = result
                
                # Extract output data for next task
                pipeline_data = result.get("output_data", result)
                
            except Exception as e:
                self.logger.error(f"Pipeline task {task.task_id} failed: {e}")
                if workflow_def.error_handling == "stop_on_error":
                    raise
        
        return {
            "pattern": "pipeline",
            "total_tasks": len(sorted_tasks),
            "completed_tasks": len(results),
            "final_output": pipeline_data,
            "results": results,
        }
    
    async def _execute_hierarchical(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks in a hierarchical pattern with delegation."""
        self.logger.info("Executing hierarchical coordination pattern")
        
        # Use orchestrator agent as the root of hierarchy
        if not self.orchestrator_agent:
            raise CoordinationException("Hierarchical pattern requires an orchestrator agent")
        
        # Delegate workflow execution to orchestrator
        orchestrator_task = AgentTask(
            task_type="execute_workflow",
            description=f"Execute hierarchical workflow: {workflow_def.name}",
            inputs={
                "workflow_definition": workflow_def,
                "coordination_mode": "hierarchical",
            },
        )
        
        result = await self.orchestrator_agent.execute_task(orchestrator_task)
        
        return {
            "pattern": "hierarchical",
            "orchestrator_result": result,
            "delegated_tasks": len(workflow_def.tasks),
        }
    
    async def _execute_consensus(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks requiring consensus among agents."""
        self.logger.info("Executing consensus coordination pattern")
        
        # For each task requiring consensus, collect votes from multiple agents
        consensus_results = []
        
        for task in workflow_def.tasks:
            if len(workflow_def.agents) < 2:
                # Single agent, no consensus needed
                result = await self._execute_single_task(task, workflow_state)
                consensus_results.append(result)
                continue
            
            # Execute task with multiple agents and collect results
            agent_results = []
            for agent_id in workflow_def.agents:
                if agent_id in self.registered_agents:
                    try:
                        task_copy = CoordinationTask(
                            task_id=f"{task.task_id}_{agent_id}",
                            agent_id=agent_id,
                            task_type=task.task_type,
                            task_data=task.task_data.copy(),
                        )
                        result = await self._execute_single_task(task_copy, workflow_state)
                        agent_results.append({"agent_id": agent_id, "result": result})
                    except Exception as e:
                        self.logger.error(f"Consensus task failed for agent {agent_id}: {e}")
            
            # Simple consensus: majority vote or best result
            consensus_result = await self._determine_consensus(agent_results)
            consensus_results.append(consensus_result)
            workflow_state["task_results"][task.task_id] = consensus_result
        
        return {
            "pattern": "consensus",
            "total_tasks": len(workflow_def.tasks),
            "consensus_results": consensus_results,
        }
    
    async def _execute_broadcast(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks by broadcasting to all agents."""
        self.logger.info("Executing broadcast coordination pattern")
        
        broadcast_results = []
        
        for task in workflow_def.tasks:
            # Send task to all registered agents
            broadcast_tasks = []
            for agent_id in self.registered_agents:
                task_copy = CoordinationTask(
                    task_id=f"{task.task_id}_{agent_id}",
                    agent_id=agent_id,
                    task_type=task.task_type,
                    task_data=task.task_data.copy(),
                )
                broadcast_tasks.append(self._execute_single_task(task_copy, workflow_state))
            
            # Wait for all responses
            results = await asyncio.gather(*broadcast_tasks, return_exceptions=True)
            
            # Collect successful results
            successful_results = [
                {"agent_id": agent_id, "result": result}
                for agent_id, result in zip(self.registered_agents.keys(), results)
                if not isinstance(result, Exception)
            ]
            
            broadcast_results.append({
                "task_id": task.task_id,
                "responses": successful_results,
                "total_responses": len(successful_results),
            })
        
        return {
            "pattern": "broadcast",
            "total_tasks": len(workflow_def.tasks),
            "broadcast_results": broadcast_results,
        }
    
    async def _execute_auction(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks using an auction mechanism for agent selection."""
        self.logger.info("Executing auction coordination pattern")
        
        auction_results = []
        
        for task in workflow_def.tasks:
            # Conduct auction for this task
            winner = await self._conduct_task_auction(task, workflow_def.agents)
            
            if winner:
                task.agent_id = winner
                result = await self._execute_single_task(task, workflow_state)
                auction_results.append({
                    "task_id": task.task_id,
                    "winning_agent": winner,
                    "result": result,
                })
                workflow_state["task_results"][task.task_id] = result
            else:
                auction_results.append({
                    "task_id": task.task_id,
                    "winning_agent": None,
                    "result": {"error": "No agent won the auction"},
                })
        
        return {
            "pattern": "auction",
            "total_tasks": len(workflow_def.tasks),
            "auction_results": auction_results,
        }
    
    async def _execute_negotiation(self, workflow_def: WorkflowDefinition, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute tasks using negotiation between agents."""
        self.logger.info("Executing negotiation coordination pattern")
        
        # Simple negotiation: agents negotiate resource allocation and task distribution
        negotiation_result = await self._conduct_negotiation(workflow_def.tasks, workflow_def.agents)
        
        # Execute tasks based on negotiation outcome
        results = []
        for task_assignment in negotiation_result["task_assignments"]:
            task_id = task_assignment["task_id"]
            agent_id = task_assignment["agent_id"]
            
            # Find the task
            task = next((t for t in workflow_def.tasks if t.task_id == task_id), None)
            if task:
                task.agent_id = agent_id
                result = await self._execute_single_task(task, workflow_state)
                results.append(result)
                workflow_state["task_results"][task.task_id] = result
        
        return {
            "pattern": "negotiation",
            "negotiation_result": negotiation_result,
            "executed_tasks": len(results),
            "results": results,
        }
    
    async def _execute_single_task(self, task: CoordinationTask, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single coordination task."""
        agent = self.registered_agents.get(task.agent_id)
        if not agent:
            raise CoordinationException(f"Agent not found: {task.agent_id}")
        
        # Create agent task
        agent_task = AgentTask(
            task_type=task.task_type,
            description=f"Coordination task: {task.task_id}",
            inputs=task.task_data,
            priority=task.priority,
        )
        
        # Execute task
        task.assigned_at = time.time()
        task.started_at = time.time()
        task.status = "running"
        
        try:
            result = await agent.execute_task(agent_task)
            task.completed_at = time.time()
            task.status = "completed"
            task.result = result
            
            workflow_state["execution_log"].append({
                "task_id": task.task_id,
                "agent_id": task.agent_id,
                "status": "completed",
                "execution_time": task.completed_at - task.started_at,
                "timestamp": task.completed_at,
            })
            
            return result
            
        except Exception as e:
            task.completed_at = time.time()
            task.status = "failed"
            task.error = str(e)
            
            workflow_state["execution_log"].append({
                "task_id": task.task_id,
                "agent_id": task.agent_id,
                "status": "failed",
                "error": str(e),
                "execution_time": task.completed_at - task.started_at,
                "timestamp": task.completed_at,
            })
            
            raise
    
    def _topological_sort_tasks(self, tasks: List[CoordinationTask]) -> List[CoordinationTask]:
        """Sort tasks in topological order based on dependencies."""
        task_map = {task.task_id: task for task in tasks}
        in_degree = {task.task_id: 0 for task in tasks}
        
        # Calculate in-degrees
        for task in tasks:
            for dep_id in task.dependencies:
                if dep_id in in_degree:
                    in_degree[task.task_id] += 1
        
        # Topological sort using Kahn's algorithm
        queue = [task_id for task_id, degree in in_degree.items() if degree == 0]
        sorted_tasks = []
        
        while queue:
            current_id = queue.pop(0)
            sorted_tasks.append(task_map[current_id])
            
            # Update in-degrees of dependent tasks
            for task in tasks:
                if current_id in task.dependencies:
                    in_degree[task.task_id] -= 1
                    if in_degree[task.task_id] == 0:
                        queue.append(task.task_id)
        
        return sorted_tasks
    
    def _group_tasks_by_dependency_level(self, tasks: List[CoordinationTask]) -> List[List[CoordinationTask]]:
        """Group tasks by their dependency level for parallel execution."""
        task_map = {task.task_id: task for task in tasks}
        levels = []
        remaining_tasks = set(task.task_id for task in tasks)
        completed_tasks = set()
        
        while remaining_tasks:
            current_level = []
            
            # Find tasks with no remaining dependencies
            for task_id in list(remaining_tasks):
                task = task_map[task_id]
                if all(dep_id in completed_tasks for dep_id in task.dependencies):
                    current_level.append(task)
                    remaining_tasks.remove(task_id)
                    completed_tasks.add(task_id)
            
            if not current_level:
                # Circular dependency or other issue
                break
            
            levels.append(current_level)
        
        return levels
    
    async def _determine_consensus(self, agent_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Determine consensus from multiple agent results."""
        if not agent_results:
            return {"consensus": "no_results"}
        
        if len(agent_results) == 1:
            return agent_results[0]["result"]
        
        # Simple consensus: return the most common result or first result if all different
        # In practice, this would be more sophisticated
        return {
            "consensus": "majority",
            "selected_result": agent_results[0]["result"],
            "all_results": agent_results,
            "consensus_method": "first_result",
        }
    
    async def _conduct_task_auction(self, task: CoordinationTask, eligible_agents: List[str]) -> Optional[str]:
        """Conduct an auction to select the best agent for a task."""
        # Simple auction: select agent with lowest current workload
        best_agent = None
        lowest_workload = float('inf')
        
        for agent_id in eligible_agents:
            if agent_id in self.registered_agents:
                agent = self.registered_agents[agent_id]
                workload = len(agent.active_tasks)
                
                if workload < lowest_workload:
                    lowest_workload = workload
                    best_agent = agent_id
        
        return best_agent
    
    async def _conduct_negotiation(self, tasks: List[CoordinationTask], agents: List[str]) -> Dict[str, Any]:
        """Conduct negotiation between agents for task allocation."""
        # Simple negotiation: round-robin assignment
        task_assignments = []
        
        for i, task in enumerate(tasks):
            assigned_agent = agents[i % len(agents)] if agents else None
            task_assignments.append({
                "task_id": task.task_id,
                "agent_id": assigned_agent,
            })
        
        return {
            "negotiation_method": "round_robin",
            "task_assignments": task_assignments,
            "participating_agents": agents,
        }
    
    def _get_workflow_status_info(self, workflow_state: Dict[str, Any]) -> Dict[str, Any]:
        """Get workflow status information."""
        workflow_def = workflow_state["workflow_def"]
        
        return {
            "workflow_id": workflow_state["workflow_id"],
            "name": workflow_def.name,
            "status": workflow_state["status"].value if isinstance(workflow_state["status"], WorkflowStatus) else workflow_state["status"],
            "coordination_pattern": workflow_def.coordination_pattern.value,
            "started_at": workflow_state.get("started_at"),
            "completed_at": workflow_state.get("completed_at"),
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len([t for t in workflow_def.tasks if t.status == "completed"]),
            "failed_tasks": len([t for t in workflow_def.tasks if t.status == "failed"]),
            "agents_involved": len(workflow_def.agents),
            "execution_time": (
                (workflow_state.get("completed_at", time.time()) - workflow_state.get("started_at", 0))
                if workflow_state.get("started_at") else None
            ),
        }
    
    def _update_coordination_metrics(self, workflow_state: Dict[str, Any], success: bool) -> None:
        """Update coordination performance metrics."""
        self.coordination_metrics["workflows_executed"] += 1
        
        if success:
            self.coordination_metrics["workflows_completed"] += 1
        else:
            self.coordination_metrics["workflows_failed"] += 1
        
        # Update execution time metrics
        if workflow_state.get("started_at") and workflow_state.get("completed_at"):
            execution_time = workflow_state["completed_at"] - workflow_state["started_at"]
            self.coordination_metrics["total_coordination_time"] += execution_time
            
            # Calculate average
            completed_workflows = self.coordination_metrics["workflows_completed"]
            if completed_workflows > 0:
                self.coordination_metrics["average_execution_time"] = (
                    self.coordination_metrics["total_coordination_time"] / completed_workflows
                )
    
    def get_coordination_status(self) -> Dict[str, Any]:
        """Get coordination system status."""
        return {
            "registered_agents": len(self.registered_agents),
            "agent_types": {
                role.value: len([a for a in self.registered_agents.values() if a.config.role == role])
                for role in {agent.config.role for agent in self.registered_agents.values()}
            },
            "active_workflows": len(self.active_workflows),
            "workflow_history_count": len(self.workflow_history),
            "supported_patterns": [pattern.value for pattern in CoordinationPattern],
            "coordination_metrics": self.coordination_metrics.copy(),
        }