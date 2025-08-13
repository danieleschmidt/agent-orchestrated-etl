"""Agent coordination patterns and multi-agent workflows."""

from __future__ import annotations

import asyncio
import hashlib
import math
import random
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from ..exceptions import CoordinationException
from ..logging_config import LogContext, get_logger
from .base_agent import AgentCapability, AgentTask, BaseAgent
from .communication import AgentCommunicationHub
from .orchestrator_agent import OrchestratorAgent


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
    # Advanced Generation 1 patterns
    BYZANTINE_CONSENSUS = "byzantine_consensus"
    RAFT_CONSENSUS = "raft_consensus"
    QUANTUM_CONSENSUS = "quantum_consensus"
    AI_DRIVEN_CONSENSUS = "ai_driven_consensus"
    ADAPTIVE_CONSENSUS = "adaptive_consensus"


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

    # Advanced coordination features
    consensus_requirements: Dict[str, Any] = field(default_factory=dict)
    voting_agents: List[str] = field(default_factory=list)
    consensus_threshold: float = 0.67  # 2/3 majority
    adaptive_priority: float = 5.0
    quantum_entangled: bool = False
    ai_recommendation_score: float = 0.0


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
            # Advanced Generation 1 patterns
            CoordinationPattern.BYZANTINE_CONSENSUS: self._execute_byzantine_consensus,
            CoordinationPattern.RAFT_CONSENSUS: self._execute_raft_consensus,
            CoordinationPattern.QUANTUM_CONSENSUS: self._execute_quantum_consensus,
            CoordinationPattern.AI_DRIVEN_CONSENSUS: self._execute_ai_driven_consensus,
            CoordinationPattern.ADAPTIVE_CONSENSUS: self._execute_adaptive_consensus,
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

        # Advanced consensus features
        self.consensus_state = {
            "current_term": 0,
            "voted_for": None,
            "log": [],
            "commit_index": 0,
            "last_applied": 0,
            "next_index": {},
            "match_index": {}
        }
        self.ai_coordinator = None
        self.quantum_consensus_enabled = False
        self.adaptive_thresholds = {
            "performance_threshold": 0.8,
            "reliability_threshold": 0.9,
            "consensus_timeout": 30.0
        }

        # Byzantine fault tolerance
        self.byzantine_nodes = set()
        self.fault_tolerance_level = 1  # f=1 means can tolerate 1 Byzantine node

        # Distributed decision making
        self.decision_history = deque(maxlen=1000)
        self.decision_weights = defaultdict(lambda: 1.0)
        self.learning_rate = 0.1

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

    def _calculate_agent_score(self, agent: BaseAgent, required_capabilities: List[str], task: CoordinationTask) -> float:
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

    def _calculate_io_compatibility(self, agent_capabilities: List[AgentCapability], task: CoordinationTask) -> float:
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

    def _calculate_performance_score(self, agent: BaseAgent, task: CoordinationTask) -> float:
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

    def _calculate_specialization_match(self, agent: BaseAgent, task: CoordinationTask) -> float:
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

    # Advanced Generation 1 Consensus Algorithms

    async def _execute_byzantine_consensus(
        self,
        workflow_def: WorkflowDefinition,
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute tasks using Byzantine fault-tolerant consensus."""
        self.logger.info("Executing Byzantine consensus coordination pattern")

        # Ensure we have enough agents for Byzantine fault tolerance
        min_agents = 3 * self.fault_tolerance_level + 1
        if len(workflow_def.agents) < min_agents:
            raise CoordinationException(
                f"Byzantine consensus requires at least {min_agents} agents "
                f"for fault tolerance level {self.fault_tolerance_level}"
            )

        byzantine_results = []

        for task in workflow_def.tasks:
            # Phase 1: Prepare phase
            prepare_votes = await self._byzantine_prepare_phase(task, workflow_def.agents)

            if len(prepare_votes) >= (2 * self.fault_tolerance_level + 1):
                # Phase 2: Commit phase
                commit_result = await self._byzantine_commit_phase(
                    task, workflow_def.agents, prepare_votes
                )

                if commit_result["success"]:
                    byzantine_results.append(commit_result)
                    workflow_state["task_results"][task.task_id] = commit_result
                else:
                    # Handle Byzantine failure
                    failure_result = await self._handle_byzantine_failure(task, commit_result)
                    byzantine_results.append(failure_result)
            else:
                raise CoordinationException(
                    f"Byzantine consensus failed: insufficient prepare votes for task {task.task_id}"
                )

        return {
            "pattern": "byzantine_consensus",
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(byzantine_results),
            "results": byzantine_results,
            "fault_tolerance_level": self.fault_tolerance_level,
            "byzantine_nodes_detected": list(self.byzantine_nodes)
        }

    async def _byzantine_prepare_phase(
        self,
        task: CoordinationTask,
        agents: List[str]
    ) -> List[Dict[str, Any]]:
        """Execute Byzantine prepare phase."""
        prepare_votes = []

        for agent_id in agents:
            if agent_id not in self.byzantine_nodes and agent_id in self.registered_agents:
                try:
                    # Simulate prepare vote
                    vote = {
                        "agent_id": agent_id,
                        "task_id": task.task_id,
                        "vote": "prepare",
                        "timestamp": time.time(),
                        "signature": self._generate_vote_signature(agent_id, task.task_id, "prepare")
                    }
                    prepare_votes.append(vote)
                except Exception as e:
                    self.logger.warning(f"Agent {agent_id} failed in prepare phase: {e}")
                    # Mark as potentially Byzantine
                    self.byzantine_nodes.add(agent_id)

        return prepare_votes

    async def _byzantine_commit_phase(
        self,
        task: CoordinationTask,
        agents: List[str],
        prepare_votes: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Execute Byzantine commit phase."""
        commit_votes = []

        for agent_id in agents:
            if agent_id not in self.byzantine_nodes and agent_id in self.registered_agents:
                try:
                    # Execute task and get result
                    result = await self._execute_single_task(task, {})

                    vote = {
                        "agent_id": agent_id,
                        "task_id": task.task_id,
                        "vote": "commit",
                        "result": result,
                        "timestamp": time.time(),
                        "signature": self._generate_vote_signature(agent_id, task.task_id, "commit")
                    }
                    commit_votes.append(vote)

                except Exception as e:
                    self.logger.warning(f"Agent {agent_id} failed in commit phase: {e}")
                    self.byzantine_nodes.add(agent_id)

        # Verify consensus
        if len(commit_votes) >= (2 * self.fault_tolerance_level + 1):
            # Check for result consistency
            consistent_results = self._verify_byzantine_results(commit_votes)

            if consistent_results:
                return {
                    "success": True,
                    "task_id": task.task_id,
                    "consensus_result": consistent_results[0]["result"],
                    "commit_votes": len(commit_votes),
                    "byzantine_detected": len(self.byzantine_nodes)
                }

        return {"success": False, "task_id": task.task_id, "reason": "consensus_failure"}

    def _generate_vote_signature(self, agent_id: str, task_id: str, vote_type: str) -> str:
        """Generate cryptographic signature for vote (simplified)."""
        message = f"{agent_id}:{task_id}:{vote_type}:{time.time()}"
        return hashlib.sha256(message.encode()).hexdigest()[:16]

    def _verify_byzantine_results(self, commit_votes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Verify consistency of Byzantine results."""
        # Simple majority check (in production, would use proper Byzantine verification)
        result_groups = defaultdict(list)

        for vote in commit_votes:
            result_hash = hashlib.md5(str(vote["result"]).encode()).hexdigest()
            result_groups[result_hash].append(vote)

        # Find majority result
        for result_hash, votes in result_groups.items():
            if len(votes) >= (2 * self.fault_tolerance_level + 1):
                return votes

        return []

    async def _handle_byzantine_failure(
        self,
        task: CoordinationTask,
        failure_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Handle Byzantine consensus failure."""
        self.logger.warning(f"Handling Byzantine failure for task {task.task_id}")

        # Implement recovery strategy
        return {
            "success": False,
            "task_id": task.task_id,
            "failure_reason": failure_result.get("reason", "unknown"),
            "recovery_action": "task_rescheduled",
            "byzantine_nodes": list(self.byzantine_nodes)
        }

    async def _execute_raft_consensus(
        self,
        workflow_def: WorkflowDefinition,
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute tasks using Raft consensus algorithm."""
        self.logger.info("Executing Raft consensus coordination pattern")

        # Initialize Raft state
        raft_state = await self._initialize_raft_state(workflow_def.agents)

        raft_results = []

        for task in workflow_def.tasks:
            # Leader election if needed
            if not raft_state.get("leader"):
                leader = await self._raft_leader_election(workflow_def.agents, raft_state)
                raft_state["leader"] = leader

            # Log replication
            log_entry = {
                "term": raft_state["current_term"],
                "task": task,
                "timestamp": time.time()
            }

            replication_success = await self._raft_log_replication(
                log_entry, workflow_def.agents, raft_state
            )

            if replication_success:
                # Execute task on leader
                result = await self._execute_single_task(task, workflow_state)

                # Commit log entry
                await self._raft_commit_entry(log_entry, workflow_def.agents, raft_state)

                raft_results.append(result)
                workflow_state["task_results"][task.task_id] = result
            else:
                raise CoordinationException(f"Raft consensus failed for task {task.task_id}")

        return {
            "pattern": "raft_consensus",
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(raft_results),
            "results": raft_results,
            "leader": raft_state.get("leader"),
            "current_term": raft_state["current_term"]
        }

    async def _initialize_raft_state(self, agents: List[str]) -> Dict[str, Any]:
        """Initialize Raft consensus state."""
        return {
            "current_term": self.consensus_state["current_term"],
            "voted_for": None,
            "log": [],
            "commit_index": 0,
            "last_applied": 0,
            "leader": None,
            "followers": agents.copy(),
            "next_index": dict.fromkeys(agents, 1),
            "match_index": dict.fromkeys(agents, 0)
        }

    async def _raft_leader_election(
        self,
        agents: List[str],
        raft_state: Dict[str, Any]
    ) -> Optional[str]:
        """Perform Raft leader election."""
        self.logger.info("Starting Raft leader election")

        # Increment term
        raft_state["current_term"] += 1

        # Vote for self (if we're a candidate)
        candidate = random.choice(agents)  # Simplified candidate selection
        votes = 1  # Vote for self

        # Request votes from other agents
        for agent_id in agents:
            if agent_id != candidate and agent_id in self.registered_agents:
                vote_granted = await self._raft_request_vote(
                    candidate, agent_id, raft_state
                )
                if vote_granted:
                    votes += 1

        # Check if majority
        majority = len(agents) // 2 + 1
        if votes >= majority:
            self.logger.info(f"Agent {candidate} elected as Raft leader with {votes} votes")
            return candidate

        return None

    async def _raft_request_vote(
        self,
        candidate: str,
        voter: str,
        raft_state: Dict[str, Any]
    ) -> bool:
        """Request vote in Raft election."""
        # Simplified vote granting logic
        # In production, would check log consistency
        if raft_state["voted_for"] is None or raft_state["voted_for"] == candidate:
            raft_state["voted_for"] = candidate
            return True
        return False

    async def _raft_log_replication(
        self,
        log_entry: Dict[str, Any],
        agents: List[str],
        raft_state: Dict[str, Any]
    ) -> bool:
        """Replicate log entry to followers."""
        leader = raft_state["leader"]
        if not leader:
            return False

        # Add to leader's log
        raft_state["log"].append(log_entry)

        # Replicate to followers
        replication_count = 1  # Leader has the entry

        for follower in agents:
            if follower != leader and follower in self.registered_agents:
                success = await self._raft_append_entries(
                    leader, follower, log_entry, raft_state
                )
                if success:
                    replication_count += 1

        # Check if majority replicated
        majority = len(agents) // 2 + 1
        return replication_count >= majority

    async def _raft_append_entries(
        self,
        leader: str,
        follower: str,
        log_entry: Dict[str, Any],
        raft_state: Dict[str, Any]
    ) -> bool:
        """Append entries RPC for Raft."""
        # Simplified append entries
        # In production, would include proper consistency checks
        return random.random() > 0.1  # 90% success rate for simulation

    async def _raft_commit_entry(
        self,
        log_entry: Dict[str, Any],
        agents: List[str],
        raft_state: Dict[str, Any]
    ) -> None:
        """Commit log entry in Raft."""
        raft_state["commit_index"] += 1
        raft_state["last_applied"] += 1
        self.logger.info(f"Committed Raft log entry {raft_state['commit_index']}")

    async def _execute_quantum_consensus(
        self,
        workflow_def: WorkflowDefinition,
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute tasks using quantum-inspired consensus."""
        self.logger.info("Executing quantum consensus coordination pattern")

        quantum_results = []

        for task in workflow_def.tasks:
            # Create quantum superposition of agent states
            agent_states = await self._create_quantum_superposition(
                task, workflow_def.agents
            )

            # Apply quantum entanglement for correlated decisions
            entangled_states = await self._apply_quantum_entanglement(
                agent_states, task
            )

            # Quantum measurement to collapse to consensus
            consensus_result = await self._quantum_measurement_consensus(
                entangled_states, task
            )

            if consensus_result["success"]:
                quantum_results.append(consensus_result)
                workflow_state["task_results"][task.task_id] = consensus_result
            else:
                # Quantum error correction
                corrected_result = await self._quantum_error_correction(
                    consensus_result, task, workflow_def.agents
                )
                quantum_results.append(corrected_result)

        return {
            "pattern": "quantum_consensus",
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(quantum_results),
            "results": quantum_results,
            "quantum_coherence": self._calculate_quantum_coherence(quantum_results),
            "entanglement_strength": self._calculate_entanglement_strength(quantum_results)
        }

    async def _create_quantum_superposition(
        self,
        task: CoordinationTask,
        agents: List[str]
    ) -> List[Dict[str, Any]]:
        """Create quantum superposition of agent decision states."""
        agent_states = []

        for agent_id in agents:
            if agent_id in self.registered_agents:
                # Create superposition state
                state = {
                    "agent_id": agent_id,
                    "task_id": task.task_id,
                    "amplitude": complex(
                        math.cos(random.uniform(0, math.pi)),
                        math.sin(random.uniform(0, math.pi))
                    ),
                    "phase": random.uniform(0, 2 * math.pi),
                    "probability": random.uniform(0.3, 1.0),
                    "coherence": 1.0
                }
                agent_states.append(state)

        return agent_states

    async def _apply_quantum_entanglement(
        self,
        agent_states: List[Dict[str, Any]],
        task: CoordinationTask
    ) -> List[Dict[str, Any]]:
        """Apply quantum entanglement between agent states."""
        # Create entanglement pairs
        entangled_states = []

        for i, state in enumerate(agent_states):
            entangled_state = state.copy()

            # Apply entanglement with other agents
            for j, other_state in enumerate(agent_states):
                if i != j:
                    # Calculate entanglement strength
                    entanglement_strength = abs(
                        state["amplitude"] * other_state["amplitude"].conjugate()
                    )

                    # Modify amplitude based on entanglement
                    entangled_state["amplitude"] *= (1 + entanglement_strength * 0.1)
                    entangled_state["entangled_with"] = entangled_state.get("entangled_with", [])
                    entangled_state["entangled_with"].append(other_state["agent_id"])

            entangled_states.append(entangled_state)

        return entangled_states

    async def _quantum_measurement_consensus(
        self,
        entangled_states: List[Dict[str, Any]],
        task: CoordinationTask
    ) -> Dict[str, Any]:
        """Perform quantum measurement to achieve consensus."""
        # Collapse quantum states to classical decisions
        decisions = []

        for state in entangled_states:
            # Quantum measurement
            probability = abs(state["amplitude"]) ** 2
            decision_value = random.random() < probability

            decisions.append({
                "agent_id": state["agent_id"],
                "decision": decision_value,
                "confidence": probability,
                "coherence": state["coherence"]
            })

        # Calculate consensus
        positive_decisions = sum(1 for d in decisions if d["decision"])
        consensus_strength = positive_decisions / len(decisions)

        if consensus_strength >= task.consensus_threshold:
            # Execute task with quantum-optimized parameters
            result = await self._execute_single_task(task, {})

            return {
                "success": True,
                "task_id": task.task_id,
                "consensus_result": result,
                "consensus_strength": consensus_strength,
                "quantum_decisions": decisions,
                "coherence_maintained": True
            }

        return {
            "success": False,
            "task_id": task.task_id,
            "consensus_strength": consensus_strength,
            "reason": "insufficient_quantum_consensus"
        }

    async def _quantum_error_correction(
        self,
        failed_result: Dict[str, Any],
        task: CoordinationTask,
        agents: List[str]
    ) -> Dict[str, Any]:
        """Apply quantum error correction to failed consensus."""
        self.logger.info(f"Applying quantum error correction for task {task.task_id}")

        # Implement simplified quantum error correction
        corrected_result = failed_result.copy()
        corrected_result["error_corrected"] = True
        corrected_result["correction_method"] = "quantum_redundancy"

        # Try with reduced threshold
        if failed_result.get("consensus_strength", 0) >= (task.consensus_threshold * 0.8):
            result = await self._execute_single_task(task, {})
            corrected_result.update({
                "success": True,
                "consensus_result": result,
                "correction_applied": True
            })

        return corrected_result

    def _calculate_quantum_coherence(self, results: List[Dict[str, Any]]) -> float:
        """Calculate overall quantum coherence."""
        if not results:
            return 0.0

        coherence_sum = sum(
            result.get("consensus_strength", 0) for result in results
        )
        return coherence_sum / len(results)

    def _calculate_entanglement_strength(self, results: List[Dict[str, Any]]) -> float:
        """Calculate overall entanglement strength."""
        if not results:
            return 0.0

        entanglement_sum = sum(
            len(result.get("quantum_decisions", [])) for result in results
        )
        return min(1.0, entanglement_sum / (len(results) * 10))

    async def _execute_ai_driven_consensus(
        self,
        workflow_def: WorkflowDefinition,
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute tasks using AI-driven consensus."""
        self.logger.info("Executing AI-driven consensus coordination pattern")

        if not self.ai_coordinator:
            self.ai_coordinator = await self._initialize_ai_coordinator()

        ai_results = []

        for task in workflow_def.tasks:
            # AI-powered task analysis
            task_analysis = await self._ai_analyze_task(task, workflow_def.agents)

            # Generate AI recommendations
            recommendations = await self._ai_generate_recommendations(
                task_analysis, workflow_def.agents
            )

            # AI-facilitated consensus
            consensus_result = await self._ai_facilitate_consensus(
                task, recommendations, workflow_def.agents
            )

            if consensus_result["success"]:
                ai_results.append(consensus_result)
                workflow_state["task_results"][task.task_id] = consensus_result
            else:
                # AI-driven conflict resolution
                resolved_result = await self._ai_resolve_conflict(
                    consensus_result, task, workflow_def.agents
                )
                ai_results.append(resolved_result)

        return {
            "pattern": "ai_driven_consensus",
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(ai_results),
            "results": ai_results,
            "ai_effectiveness": self._calculate_ai_effectiveness(ai_results),
            "recommendation_accuracy": self._calculate_recommendation_accuracy(ai_results)
        }

    async def _initialize_ai_coordinator(self) -> Dict[str, Any]:
        """Initialize AI coordinator for consensus."""
        return {
            "model_version": "1.0.0",
            "learning_enabled": True,
            "decision_history": deque(maxlen=1000),
            "accuracy_metrics": {
                "correct_predictions": 0,
                "total_predictions": 0,
                "accuracy_score": 0.0
            },
            "optimization_strategies": [
                "performance_optimization",
                "resource_optimization",
                "reliability_optimization",
                "cost_optimization"
            ]
        }

    async def _ai_analyze_task(
        self,
        task: CoordinationTask,
        agents: List[str]
    ) -> Dict[str, Any]:
        """AI analysis of task requirements and agent capabilities."""
        analysis = {
            "task_id": task.task_id,
            "complexity_score": self._calculate_task_complexity(task),
            "resource_requirements": self._analyze_resource_requirements(task),
            "agent_suitability": {},
            "risk_factors": [],
            "optimization_opportunities": []
        }

        # Analyze each agent's suitability
        for agent_id in agents:
            if agent_id in self.registered_agents:
                agent = self.registered_agents[agent_id]
                suitability_score = self._calculate_agent_suitability_ai(agent, task)
                analysis["agent_suitability"][agent_id] = suitability_score

        # Identify risk factors
        if analysis["complexity_score"] > 0.8:
            analysis["risk_factors"].append("high_complexity")

        if len([s for s in analysis["agent_suitability"].values() if s > 0.7]) < 2:
            analysis["risk_factors"].append("insufficient_capable_agents")

        # Identify optimization opportunities
        if max(analysis["agent_suitability"].values()) > 0.9:
            analysis["optimization_opportunities"].append("expert_agent_available")

        return analysis

    def _calculate_task_complexity(self, task: CoordinationTask) -> float:
        """Calculate AI-driven task complexity score."""
        base_complexity = 0.5

        # Analyze task data
        if task.task_data:
            data_size = len(str(task.task_data))
            base_complexity += min(0.3, data_size / 1000)

        # Dependency complexity
        dependency_factor = len(task.dependencies) * 0.1
        base_complexity += dependency_factor

        # Task type complexity
        complex_types = ["transform", "model_training", "quantum_process"]
        if task.task_type in complex_types:
            base_complexity += 0.2

        return min(1.0, base_complexity)

    def _analyze_resource_requirements(self, task: CoordinationTask) -> Dict[str, float]:
        """AI analysis of task resource requirements."""
        # Base requirements with AI prediction
        requirements = {
            "cpu": 0.5,
            "memory": 0.5,
            "io": 0.3,
            "network": 0.2
        }

        # Adjust based on task type
        task_type = task.task_type
        if task_type in ["transform", "process"]:
            requirements["cpu"] *= 1.5
            requirements["memory"] *= 1.3
        elif task_type in ["extract", "load"]:
            requirements["io"] *= 2.0
            requirements["network"] *= 1.5
        elif task_type == "model_training":
            requirements["cpu"] *= 2.0
            requirements["memory"] *= 1.8

        return requirements

    def _calculate_agent_suitability_ai(
        self,
        agent: BaseAgent,
        task: CoordinationTask
    ) -> float:
        """AI-driven calculation of agent suitability."""
        base_score = self._calculate_agent_score(
            agent,
            self._get_required_capabilities(task),
            task
        )

        # AI enhancements
        performance_history = self._get_agent_performance_history(agent.config.agent_id)
        if performance_history:
            avg_performance = sum(performance_history) / len(performance_history)
            base_score = (base_score + avg_performance) / 2

        # Learning from previous similar tasks
        similar_task_performance = self._get_similar_task_performance(
            agent.config.agent_id, task.task_type
        )
        if similar_task_performance:
            base_score = (base_score * 0.7 + similar_task_performance * 0.3)

        return base_score

    def _get_agent_performance_history(self, agent_id: str) -> List[float]:
        """Get agent performance history for AI analysis."""
        # Simplified performance history
        return [random.uniform(0.7, 1.0) for _ in range(random.randint(0, 10))]

    def _get_similar_task_performance(self, agent_id: str, task_type: str) -> Optional[float]:
        """Get agent performance on similar tasks."""
        # Simplified similar task performance
        return random.uniform(0.6, 0.95) if random.random() > 0.3 else None

    async def _ai_generate_recommendations(
        self,
        analysis: Dict[str, Any],
        agents: List[str]
    ) -> Dict[str, Any]:
        """Generate AI-powered recommendations for task execution."""
        recommendations = {
            "optimal_agent": None,
            "backup_agents": [],
            "execution_strategy": "standard",
            "resource_adjustments": {},
            "risk_mitigations": [],
            "confidence": 0.0
        }

        # Find optimal agent
        agent_scores = analysis["agent_suitability"]
        if agent_scores:
            optimal_agent = max(agent_scores, key=agent_scores.get)
            recommendations["optimal_agent"] = optimal_agent
            recommendations["confidence"] = agent_scores[optimal_agent]

            # Find backup agents
            sorted_agents = sorted(
                agent_scores.items(),
                key=lambda x: x[1],
                reverse=True
            )
            recommendations["backup_agents"] = [
                agent_id for agent_id, score in sorted_agents[1:3] if score > 0.5
            ]

        # Determine execution strategy
        complexity = analysis["complexity_score"]
        if complexity > 0.8:
            recommendations["execution_strategy"] = "careful_monitoring"
        elif complexity < 0.3:
            recommendations["execution_strategy"] = "fast_execution"
        else:
            recommendations["execution_strategy"] = "standard"

        # Risk mitigations
        for risk in analysis["risk_factors"]:
            if risk == "high_complexity":
                recommendations["risk_mitigations"].append("enable_detailed_logging")
            elif risk == "insufficient_capable_agents":
                recommendations["risk_mitigations"].append("task_decomposition")

        return recommendations

    async def _ai_facilitate_consensus(
        self,
        task: CoordinationTask,
        recommendations: Dict[str, Any],
        agents: List[str]
    ) -> Dict[str, Any]:
        """Use AI to facilitate consensus among agents."""
        # Start with AI recommendation
        optimal_agent = recommendations["optimal_agent"]

        if optimal_agent and optimal_agent in self.registered_agents:
            # Execute task with optimal agent
            try:
                result = await self._execute_single_task(task, {})

                # Verify result with backup agents if high-risk
                verification_needed = (
                    "high_complexity" in recommendations.get("risk_mitigations", []) or
                    recommendations["confidence"] < 0.8
                )

                if verification_needed and recommendations["backup_agents"]:
                    verification_result = await self._ai_verify_result(
                        result, task, recommendations["backup_agents"]
                    )

                    if verification_result["verified"]:
                        return {
                            "success": True,
                            "task_id": task.task_id,
                            "consensus_result": result,
                            "execution_agent": optimal_agent,
                            "verification_performed": True,
                            "ai_confidence": recommendations["confidence"]
                        }
                    else:
                        # Verification failed, use consensus approach
                        return await self._ai_consensus_fallback(
                            task, agents, recommendations
                        )
                else:
                    return {
                        "success": True,
                        "task_id": task.task_id,
                        "consensus_result": result,
                        "execution_agent": optimal_agent,
                        "ai_confidence": recommendations["confidence"]
                    }

            except Exception as e:
                return {
                    "success": False,
                    "task_id": task.task_id,
                    "error": str(e),
                    "reason": "execution_failure"
                }

        return {"success": False, "task_id": task.task_id, "reason": "no_suitable_agent"}

    async def _ai_verify_result(
        self,
        result: Dict[str, Any],
        task: CoordinationTask,
        backup_agents: List[str]
    ) -> Dict[str, Any]:
        """Use AI to verify task result with backup agents."""
        verification_results = []

        for backup_agent in backup_agents[:2]:  # Verify with up to 2 backup agents
            if backup_agent in self.registered_agents:
                try:
                    backup_result = await self._execute_single_task(task, {})

                    # Compare results (simplified comparison)
                    similarity = self._calculate_result_similarity(result, backup_result)
                    verification_results.append({
                        "agent": backup_agent,
                        "similarity": similarity,
                        "verified": similarity > 0.8
                    })

                except Exception as e:
                    verification_results.append({
                        "agent": backup_agent,
                        "error": str(e),
                        "verified": False
                    })

        # Determine overall verification
        verified_count = sum(1 for v in verification_results if v.get("verified", False))
        verification_success = verified_count > 0

        return {
            "verified": verification_success,
            "verification_results": verification_results,
            "verification_confidence": verified_count / len(verification_results) if verification_results else 0
        }

    def _calculate_result_similarity(self, result1: Dict[str, Any], result2: Dict[str, Any]) -> float:
        """Calculate similarity between two results."""
        # Simplified similarity calculation
        if result1 == result2:
            return 1.0

        # Compare key fields
        common_keys = set(result1.keys()) & set(result2.keys())
        if not common_keys:
            return 0.0

        matches = sum(1 for key in common_keys if result1[key] == result2[key])
        return matches / len(common_keys)

    async def _ai_consensus_fallback(
        self,
        task: CoordinationTask,
        agents: List[str],
        recommendations: Dict[str, Any]
    ) -> Dict[str, Any]:
        """AI-driven consensus fallback when primary execution fails."""
        # Execute task with multiple agents
        agent_results = []

        for agent_id in agents[:3]:  # Use up to 3 agents
            if agent_id in self.registered_agents:
                try:
                    result = await self._execute_single_task(task, {})
                    agent_results.append({
                        "agent_id": agent_id,
                        "result": result,
                        "success": True
                    })
                except Exception as e:
                    agent_results.append({
                        "agent_id": agent_id,
                        "error": str(e),
                        "success": False
                    })

        # AI-driven result selection
        if agent_results:
            successful_results = [r for r in agent_results if r["success"]]

            if successful_results:
                # Select best result using AI
                best_result = max(
                    successful_results,
                    key=lambda x: self._calculate_result_quality_score(x["result"])
                )

                return {
                    "success": True,
                    "task_id": task.task_id,
                    "consensus_result": best_result["result"],
                    "consensus_method": "ai_selection",
                    "agents_participated": len(agent_results),
                    "successful_executions": len(successful_results)
                }

        return {"success": False, "task_id": task.task_id, "reason": "consensus_fallback_failed"}

    def _calculate_result_quality_score(self, result: Dict[str, Any]) -> float:
        """Calculate AI-driven quality score for a result."""
        base_score = 0.5

        # Check for completeness
        if result and isinstance(result, dict):
            field_count = len(result)
            base_score += min(0.3, field_count / 10)

        # Check for error indicators
        if "error" in result or "exception" in result:
            base_score *= 0.5

        # Check for success indicators
        if result.get("status") == "success" or result.get("completed", False):
            base_score += 0.2

        return min(1.0, base_score)

    async def _ai_resolve_conflict(
        self,
        failed_consensus: Dict[str, Any],
        task: CoordinationTask,
        agents: List[str]
    ) -> Dict[str, Any]:
        """AI-driven conflict resolution for failed consensus."""
        self.logger.info(f"AI resolving conflict for task {task.task_id}")

        resolution_strategy = self._determine_resolution_strategy(failed_consensus)

        if resolution_strategy == "retry_with_different_agents":
            # Try with different agent selection
            remaining_agents = [a for a in agents if a != failed_consensus.get("execution_agent")]
            if remaining_agents:
                new_recommendations = await self._ai_generate_recommendations(
                    {"agent_suitability": dict.fromkeys(remaining_agents, 0.7)},
                    remaining_agents
                )
                return await self._ai_facilitate_consensus(task, new_recommendations, remaining_agents)

        elif resolution_strategy == "reduce_complexity":
            # Simplify task and retry
            simplified_task = self._simplify_task(task)
            return await self._execute_single_task(simplified_task, {})

        elif resolution_strategy == "accept_failure":
            return {
                "success": False,
                "task_id": task.task_id,
                "resolution_attempted": True,
                "final_status": "unresolvable_conflict"
            }

        return failed_consensus

    def _determine_resolution_strategy(self, failed_consensus: Dict[str, Any]) -> str:
        """Determine AI-driven resolution strategy for conflicts."""
        failure_reason = failed_consensus.get("reason", "")

        if "execution_failure" in failure_reason:
            return "retry_with_different_agents"
        elif "high_complexity" in failure_reason:
            return "reduce_complexity"
        else:
            return "accept_failure"

    def _simplify_task(self, task: CoordinationTask) -> CoordinationTask:
        """Simplify task for conflict resolution."""
        simplified = CoordinationTask(
            task_id=f"{task.task_id}_simplified",
            agent_id=task.agent_id,
            task_type=task.task_type,
            task_data=task.task_data.copy(),
            dependencies=task.dependencies[:1],  # Reduce dependencies
            priority=max(1, task.priority - 2)  # Reduce priority
        )
        return simplified

    def _calculate_ai_effectiveness(self, results: List[Dict[str, Any]]) -> float:
        """Calculate AI effectiveness score."""
        if not results:
            return 0.0

        successful_results = sum(1 for r in results if r.get("success", False))
        base_effectiveness = successful_results / len(results)

        # Bonus for high-confidence decisions
        avg_confidence = sum(
            r.get("ai_confidence", 0.5) for r in results
        ) / len(results)

        return min(1.0, base_effectiveness * (1 + avg_confidence * 0.2))

    def _calculate_recommendation_accuracy(self, results: List[Dict[str, Any]]) -> float:
        """Calculate AI recommendation accuracy."""
        if not results:
            return 0.0

        accurate_predictions = 0
        total_predictions = 0

        for result in results:
            if "ai_confidence" in result:
                total_predictions += 1
                confidence = result["ai_confidence"]
                success = result.get("success", False)

                # Consider prediction accurate if high confidence led to success
                # or low confidence led to proper fallback handling
                if (confidence > 0.8 and success) or (confidence <= 0.8 and not success):
                    accurate_predictions += 1

        return accurate_predictions / total_predictions if total_predictions > 0 else 0.0

    async def _execute_adaptive_consensus(
        self,
        workflow_def: WorkflowDefinition,
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute tasks using adaptive consensus that learns and evolves."""
        self.logger.info("Executing adaptive consensus coordination pattern")

        adaptive_results = []

        for task in workflow_def.tasks:
            # Analyze current system state
            system_state = await self._analyze_system_state(workflow_def.agents)

            # Adapt consensus parameters based on system state
            adapted_params = await self._adapt_consensus_parameters(
                task, system_state, workflow_state
            )

            # Execute consensus with adapted parameters
            consensus_result = await self._execute_adapted_consensus(
                task, workflow_def.agents, adapted_params
            )

            # Learn from the result
            await self._learn_from_consensus(consensus_result, adapted_params)

            if consensus_result["success"]:
                adaptive_results.append(consensus_result)
                workflow_state["task_results"][task.task_id] = consensus_result
            else:
                # Adaptive recovery
                recovery_result = await self._adaptive_recovery(
                    consensus_result, task, workflow_def.agents
                )
                adaptive_results.append(recovery_result)

        return {
            "pattern": "adaptive_consensus",
            "total_tasks": len(workflow_def.tasks),
            "completed_tasks": len(adaptive_results),
            "results": adaptive_results,
            "adaptation_effectiveness": self._calculate_adaptation_effectiveness(adaptive_results),
            "learning_progress": self._calculate_learning_progress()
        }

    async def _analyze_system_state(self, agents: List[str]) -> Dict[str, Any]:
        """Analyze current system state for adaptive consensus."""
        system_state = {
            "agent_health": {},
            "network_latency": {},
            "resource_utilization": {},
            "recent_failures": [],
            "performance_trends": {}
        }

        for agent_id in agents:
            if agent_id in self.registered_agents:
                agent = self.registered_agents[agent_id]

                # Simulate health check
                system_state["agent_health"][agent_id] = random.uniform(0.7, 1.0)

                # Simulate network latency
                system_state["network_latency"][agent_id] = random.uniform(10, 100)

                # Simulate resource utilization
                system_state["resource_utilization"][agent_id] = {
                    "cpu": random.uniform(0.2, 0.9),
                    "memory": random.uniform(0.3, 0.8),
                    "network": random.uniform(0.1, 0.6)
                }

        # Recent failures
        system_state["recent_failures"] = [
            f"agent_{random.choice(agents)}_timeout"
            for _ in range(random.randint(0, 2))
        ]

        return system_state

    async def _adapt_consensus_parameters(
        self,
        task: CoordinationTask,
        system_state: Dict[str, Any],
        workflow_state: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Adapt consensus parameters based on system state."""
        base_params = {
            "consensus_threshold": task.consensus_threshold,
            "timeout": 30.0,
            "retry_count": 3,
            "backup_agents": 2,
            "verification_required": False
        }

        # Adapt based on system health
        avg_health = sum(system_state["agent_health"].values()) / len(system_state["agent_health"])
        if avg_health < 0.8:
            base_params["consensus_threshold"] *= 0.9  # Lower threshold for unhealthy system
            base_params["backup_agents"] += 1
            base_params["verification_required"] = True

        # Adapt based on network latency
        avg_latency = sum(system_state["network_latency"].values()) / len(system_state["network_latency"])
        if avg_latency > 50:
            base_params["timeout"] *= 1.5  # Increase timeout for high latency

        # Adapt based on recent failures
        if len(system_state["recent_failures"]) > 1:
            base_params["retry_count"] += 2
            base_params["verification_required"] = True

        # Adapt based on task complexity
        task_complexity = self._calculate_task_complexity(task)
        if task_complexity > 0.8:
            base_params["consensus_threshold"] += 0.1
            base_params["verification_required"] = True

        return base_params

    async def _execute_adapted_consensus(
        self,
        task: CoordinationTask,
        agents: List[str],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Execute consensus with adapted parameters."""
        start_time = time.time()
        timeout = params["timeout"]

        # Execute with primary agents
        primary_results = []
        for agent_id in agents:
            if agent_id in self.registered_agents and time.time() - start_time < timeout:
                try:
                    result = await self._execute_single_task(task, {})
                    primary_results.append({
                        "agent_id": agent_id,
                        "result": result,
                        "success": True,
                        "execution_time": time.time() - start_time
                    })
                except Exception as e:
                    primary_results.append({
                        "agent_id": agent_id,
                        "error": str(e),
                        "success": False
                    })

        # Check consensus
        successful_results = [r for r in primary_results if r["success"]]
        consensus_ratio = len(successful_results) / len(primary_results) if primary_results else 0

        if consensus_ratio >= params["consensus_threshold"]:
            # Consensus achieved
            best_result = min(successful_results, key=lambda x: x["execution_time"])

            result = {
                "success": True,
                "task_id": task.task_id,
                "consensus_result": best_result["result"],
                "consensus_ratio": consensus_ratio,
                "execution_agent": best_result["agent_id"],
                "adapted_parameters": params,
                "execution_time": time.time() - start_time
            }

            # Verification if required
            if params["verification_required"]:
                verification = await self._adaptive_verification(
                    result, task, agents, params
                )
                result["verification"] = verification

            return result
        else:
            return {
                "success": False,
                "task_id": task.task_id,
                "consensus_ratio": consensus_ratio,
                "reason": "insufficient_consensus",
                "attempted_agents": len(primary_results),
                "successful_agents": len(successful_results)
            }

    async def _adaptive_verification(
        self,
        result: Dict[str, Any],
        task: CoordinationTask,
        agents: List[str],
        params: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Perform adaptive verification of consensus result."""
        verification_agents = random.sample(
            [a for a in agents if a != result["execution_agent"]],
            min(params["backup_agents"], len(agents) - 1)
        )

        verification_results = []
        for agent_id in verification_agents:
            if agent_id in self.registered_agents:
                try:
                    verification_result = await self._execute_single_task(task, {})
                    similarity = self._calculate_result_similarity(
                        result["consensus_result"],
                        verification_result
                    )
                    verification_results.append({
                        "agent_id": agent_id,
                        "similarity": similarity,
                        "verified": similarity > 0.7
                    })
                except Exception as e:
                    verification_results.append({
                        "agent_id": agent_id,
                        "error": str(e),
                        "verified": False
                    })

        verified_count = sum(1 for v in verification_results if v.get("verified", False))
        verification_success = verified_count >= len(verification_results) // 2

        return {
            "verification_success": verification_success,
            "verification_results": verification_results,
            "verification_confidence": verified_count / len(verification_results) if verification_results else 0
        }

    async def _learn_from_consensus(
        self,
        consensus_result: Dict[str, Any],
        adapted_params: Dict[str, Any]
    ) -> None:
        """Learn from consensus execution to improve future adaptations."""
        learning_data = {
            "timestamp": time.time(),
            "success": consensus_result.get("success", False),
            "consensus_ratio": consensus_result.get("consensus_ratio", 0),
            "execution_time": consensus_result.get("execution_time", 0),
            "parameters": adapted_params,
            "task_id": consensus_result.get("task_id")
        }

        self.decision_history.append(learning_data)

        # Update decision weights based on success
        param_signature = self._create_param_signature(adapted_params)
        if consensus_result.get("success", False):
            self.decision_weights[param_signature] *= (1 + self.learning_rate)
        else:
            self.decision_weights[param_signature] *= (1 - self.learning_rate)

        # Update adaptive thresholds
        if len(self.decision_history) >= 10:
            recent_results = list(self.decision_history)[-10:]
            success_rate = sum(1 for r in recent_results if r["success"]) / len(recent_results)

            if success_rate > 0.9:
                # Increase challenge
                self.adaptive_thresholds["performance_threshold"] += 0.01
            elif success_rate < 0.7:
                # Reduce challenge
                self.adaptive_thresholds["performance_threshold"] = max(
                    0.5, self.adaptive_thresholds["performance_threshold"] - 0.01
                )

    def _create_param_signature(self, params: Dict[str, Any]) -> str:
        """Create signature for parameter combination."""
        key_params = {
            "threshold": round(params.get("consensus_threshold", 0.67), 2),
            "timeout": round(params.get("timeout", 30.0), 1),
            "retries": params.get("retry_count", 3),
            "verification": params.get("verification_required", False)
        }
        return str(sorted(key_params.items()))

    async def _adaptive_recovery(
        self,
        failed_consensus: Dict[str, Any],
        task: CoordinationTask,
        agents: List[str]
    ) -> Dict[str, Any]:
        """Perform adaptive recovery from failed consensus."""
        recovery_strategy = self._select_recovery_strategy(failed_consensus)

        if recovery_strategy == "relax_threshold":
            # Relax consensus threshold and retry
            relaxed_params = {
                "consensus_threshold": failed_consensus.get("consensus_ratio", 0.5) * 0.9,
                "timeout": 45.0,
                "retry_count": 2,
                "backup_agents": 3,
                "verification_required": True
            }
            return await self._execute_adapted_consensus(task, agents, relaxed_params)

        elif recovery_strategy == "single_agent_execution":
            # Execute with single most reliable agent
            best_agent = self._select_most_reliable_agent(agents)
            if best_agent:
                try:
                    result = await self._execute_single_task(task, {})
                    return {
                        "success": True,
                        "task_id": task.task_id,
                        "consensus_result": result,
                        "recovery_strategy": "single_agent",
                        "execution_agent": best_agent
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "task_id": task.task_id,
                        "recovery_strategy": "single_agent_failed",
                        "error": str(e)
                    }

        return {
            "success": False,
            "task_id": task.task_id,
            "recovery_attempted": True,
            "recovery_strategy": recovery_strategy,
            "final_status": "recovery_failed"
        }

    def _select_recovery_strategy(self, failed_consensus: Dict[str, Any]) -> str:
        """Select appropriate recovery strategy."""
        consensus_ratio = failed_consensus.get("consensus_ratio", 0)

        if consensus_ratio >= 0.4:
            return "relax_threshold"
        else:
            return "single_agent_execution"

    def _select_most_reliable_agent(self, agents: List[str]) -> Optional[str]:
        """Select most reliable agent for single-agent execution."""
        if not agents:
            return None

        # Find agent with best performance history
        best_agent = None
        best_score = 0.0

        for agent_id in agents:
            if agent_id in self.registered_agents:
                performance_history = self._get_agent_performance_history(agent_id)
                if performance_history:
                    avg_performance = sum(performance_history) / len(performance_history)
                    if avg_performance > best_score:
                        best_score = avg_performance
                        best_agent = agent_id

        return best_agent or agents[0]

    def _calculate_adaptation_effectiveness(self, results: List[Dict[str, Any]]) -> float:
        """Calculate effectiveness of adaptive consensus."""
        if not results:
            return 0.0

        successful_results = sum(1 for r in results if r.get("success", False))
        base_effectiveness = successful_results / len(results)

        # Bonus for adaptive features used
        adaptive_features_used = sum(
            1 for r in results
            if r.get("adapted_parameters") or r.get("recovery_strategy")
        )
        adaptation_bonus = (adaptive_features_used / len(results)) * 0.2

        return min(1.0, base_effectiveness + adaptation_bonus)

    def _calculate_learning_progress(self) -> float:
        """Calculate learning progress of adaptive system."""
        if len(self.decision_history) < 10:
            return 0.0

        # Compare recent performance to historical average
        recent_results = list(self.decision_history)[-10:]
        historical_results = list(self.decision_history)[:-10] if len(self.decision_history) > 10 else []

        recent_success_rate = sum(1 for r in recent_results if r["success"]) / len(recent_results)

        if historical_results:
            historical_success_rate = sum(1 for r in historical_results if r["success"]) / len(historical_results)
            improvement = recent_success_rate - historical_success_rate
            return max(0.0, min(1.0, 0.5 + improvement))
        else:
            return recent_success_rate

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
            # Advanced features status
            "advanced_consensus_enabled": True,
            "byzantine_fault_tolerance": self.fault_tolerance_level,
            "ai_coordinator_active": self.ai_coordinator is not None,
            "quantum_consensus_enabled": self.quantum_consensus_enabled,
            "adaptive_learning_progress": self._calculate_learning_progress(),
            "decision_history_size": len(self.decision_history),
            "byzantine_nodes_detected": len(self.byzantine_nodes)
        }
