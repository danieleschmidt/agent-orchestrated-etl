"""Orchestrator agent for managing ETL workflows and coordinating other agents."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Set

from langchain_core.language_models.base import BaseLanguageModel

from .base_agent import BaseAgent, AgentConfig, AgentRole, AgentTask, AgentCapability
from .communication import AgentCommunicationHub
from .memory import AgentMemory, MemoryType, MemoryImportance
from .tools import AgentToolRegistry, get_tool_registry
from ..exceptions import AgentException, NetworkException, ExternalServiceException, RateLimitException
from ..logging_config import LogContext
from ..retry import RetryConfig, retry, RetryConfigs
from ..circuit_breaker import CircuitBreakerConfig, circuit_breaker, CircuitBreakerConfigs


class OrchestratorAgent(BaseAgent):
    """Orchestrator agent that manages ETL workflows and coordinates other agents."""
    
    def __init__(
        self,
        config: Optional[AgentConfig] = None,
        llm: Optional[BaseLanguageModel] = None,
        communication_hub: Optional[AgentCommunicationHub] = None,
        tool_registry: Optional[AgentToolRegistry] = None,
    ):
        # Set default config for orchestrator
        if config is None:
            config = AgentConfig(
                name="OrchestratotAgent",
                role=AgentRole.ORCHESTRATOR,
                max_concurrent_tasks=5,
                task_timeout_seconds=600.0,
            )
        elif config.role != AgentRole.ORCHESTRATOR:
            config.role = AgentRole.ORCHESTRATOR
        
        # Initialize attributes before calling super().__init__() 
        # because base class calls _initialize_agent() which needs these attributes
        self.tool_registry = tool_registry or get_tool_registry()
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_history: List[Dict[str, Any]] = []
        self.subordinate_agents: Set[str] = set()
        self.decision_context: Dict[str, Any] = {}  # Will be properly set in _initialize_agent()
        self.planning_strategies: List[str] = [
            "sequential_execution",
            "parallel_optimization",
            "resource_aware_scheduling",
            "error_recovery_planning",
        ]
        
        super().__init__(config, llm, communication_hub)
        
        # Note: decision_context is properly initialized in _initialize_agent() 
        # which is called by super().__init__()
        
        # Initialize memory after super().__init__() since it needs self.config.agent_id
        self.memory = AgentMemory(
            agent_id=self.config.agent_id,
            max_entries=50000,  # Orchestrators need more memory
            working_memory_size=200,
        )
    
    def _initialize_agent(self) -> None:
        """Initialize orchestrator-specific components."""
        self.logger.info("Initializing Orchestrator Agent")
        
        # Set up decision-making context
        self.decision_context = {
            "current_strategy": "sequential_execution",
            "risk_tolerance": "medium",
            "performance_priority": "balanced",
            "resource_constraints": {},
        }
    
    async def _process_task(self, task: AgentTask) -> Dict[str, Any]:
        """Process orchestrator-specific tasks."""
        task_type = task.task_type.lower()
        
        with LogContext(task_type=task_type):
            if task_type == "create_workflow":
                return await self._create_workflow(task)
            elif task_type == "execute_workflow":
                return await self._execute_workflow(task)
            elif task_type == "monitor_workflow":
                return await self._monitor_workflow(task)
            elif task_type == "coordinate_agents":
                return await self._coordinate_agents(task)
            elif task_type == "analyze_data_source":
                return await self._analyze_data_source(task)
            elif task_type == "generate_etl_plan":
                return await self._generate_etl_plan(task)
            elif task_type == "optimize_workflow":
                return await self._optimize_workflow(task)
            else:
                return await self._handle_generic_task(task)
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for the orchestrator agent."""
        return """You are an intelligent ETL Orchestrator Agent responsible for managing and coordinating ETL workflows.

Your primary responsibilities include:
1. Analyzing data sources and understanding requirements
2. Creating comprehensive ETL workflows and execution plans
3. Coordinating with specialized ETL agents and monitoring agents
4. Making intelligent decisions about workflow execution strategies
5. Handling errors and implementing recovery strategies
6. Optimizing performance and resource utilization

You have access to various tools for:
- Data source analysis
- DAG generation
- Pipeline execution
- Data quality validation
- Performance monitoring

When making decisions, consider:
- Data complexity and volume
- Available resources and constraints
- Error probability and recovery options
- Performance requirements
- Data quality standards

Always provide detailed explanations for your decisions and maintain comprehensive logs of all activities.

Respond in JSON format when structured data is requested, otherwise provide clear natural language explanations."""
    
    async def _create_workflow(self, task: AgentTask) -> Dict[str, Any]:
        """Create a new ETL workflow."""
        self.logger.info("Creating ETL workflow")
        
        try:
            # Extract workflow requirements
            requirements = task.inputs.get("requirements", {})
            data_source = requirements.get("data_source")
            target = requirements.get("target")
            workflow_id = task.inputs.get("workflow_id", f"workflow_{int(time.time())}")
            
            if not data_source:
                raise AgentException("Data source is required for workflow creation")
            
            # Step 1: Analyze data source
            analysis_result = await self._use_tool("analyze_data_source", {
                "source_path": data_source,
                "source_type": requirements.get("source_type", "auto"),
                "include_samples": True,
            })
            
            # Step 2: Apply workflow routing based on target
            routing_decision = await self._apply_workflow_routing(target, analysis_result, requirements, task.inputs.get("constraints", {}))
            
            # Step 3: Generate execution plan with routing configuration
            planning_context = {
                "data_analysis": analysis_result,
                "requirements": requirements,
                "constraints": task.inputs.get("constraints", {}),
                "routing_config": routing_decision["routing_config"],
            }
            
            execution_plan = await self._generate_execution_plan(planning_context)
            
            # Step 3: Create workflow record
            workflow = {
                "workflow_id": workflow_id,
                "status": "created",
                "created_at": time.time(),
                "requirements": requirements,
                "analysis_result": analysis_result,
                "execution_plan": execution_plan,
                "assigned_agents": [],
                "progress": {
                    "current_step": 0,
                    "total_steps": len(execution_plan.get("steps", [])),
                    "completion_percentage": 0.0,
                },
            }
            
            self.active_workflows[workflow_id] = workflow
            
            # Store in memory
            await self._store_workflow_memory(workflow, MemoryType.EPISODIC)
            
            self.logger.info(f"Workflow created: {workflow_id}")
            
            return {
                "workflow_id": workflow_id,
                "status": "created",
                "workflow_type": routing_decision["workflow_type"],
                "optimization_strategy": routing_decision["optimization_strategy"],
                "execution_plan": execution_plan,
                "estimated_duration": execution_plan.get("estimated_duration", "unknown"),
                "resource_requirements": execution_plan.get("resource_requirements", {}),
                "resource_allocation": routing_decision["resource_allocation"],
                "execution_priority": routing_decision["execution_priority"],
                "routing_decision": routing_decision["audit_trail"],
                "additional_steps": routing_decision.get("additional_steps", []),
                "storage_configuration": routing_decision.get("storage_configuration", {}),
                "execution_configuration": routing_decision.get("execution_configuration", {}),
                "constraints": task.inputs.get("constraints", {}),
            }
            
        except Exception as e:
            self.logger.error(f"Failed to create workflow: {e}", exc_info=e)
            raise AgentException(f"Workflow creation failed: {e}") from e
    
    async def _execute_workflow(self, task: AgentTask) -> Dict[str, Any]:
        """Execute an ETL workflow."""
        self.logger.info("Executing ETL workflow")
        
        try:
            workflow_id = task.inputs.get("workflow_id")
            if not workflow_id or workflow_id not in self.active_workflows:
                raise AgentException(f"Workflow {workflow_id} not found")
            
            workflow = self.active_workflows[workflow_id]
            execution_plan = workflow["execution_plan"]
            
            # Update workflow status
            workflow["status"] = "executing"
            workflow["execution_started_at"] = time.time()
            
            # Execute workflow steps
            execution_results = []
            total_steps = len(execution_plan.get("steps", []))
            
            for i, step in enumerate(execution_plan.get("steps", [])):
                self.logger.info(f"Executing step {i+1}/{total_steps}: {step.get('name', 'Unknown')}")
                
                try:
                    # Update progress
                    workflow["progress"]["current_step"] = i + 1
                    workflow["progress"]["completion_percentage"] = ((i + 1) / total_steps) * 100
                    
                    # Execute step
                    step_result = await self._execute_workflow_step(step, workflow)
                    execution_results.append({
                        "step": step,
                        "result": step_result,
                        "status": "completed",
                        "execution_time": step_result.get("execution_time", 0),
                    })
                    
                except Exception as step_error:
                    self.logger.error(f"Step {i+1} failed: {step_error}")
                    
                    # Handle step failure
                    failure_result = await self._handle_step_failure(step, step_error, workflow)
                    execution_results.append({
                        "step": step,
                        "result": failure_result,
                        "status": "failed",
                        "error": str(step_error),
                    })
                    
                    # Check if workflow should continue
                    if not step.get("continue_on_failure", False):
                        workflow["status"] = "failed"
                        break
            
            # Finalize workflow
            if workflow["status"] != "failed":
                workflow["status"] = "completed"
            
            workflow["execution_completed_at"] = time.time()
            workflow["execution_results"] = execution_results
            
            # Store execution results in memory
            await self._store_execution_memory(workflow, execution_results)
            
            # Move to history
            self.workflow_history.append(workflow.copy())
            if workflow["status"] == "completed":
                del self.active_workflows[workflow_id]
            
            return {
                "workflow_id": workflow_id,
                "status": workflow["status"],
                "execution_results": execution_results,
                "total_execution_time": workflow["execution_completed_at"] - workflow["execution_started_at"],
                "steps_completed": len([r for r in execution_results if r["status"] == "completed"]),
                "steps_failed": len([r for r in execution_results if r["status"] == "failed"]),
            }
            
        except Exception as e:
            self.logger.error(f"Failed to execute workflow: {e}", exc_info=e)
            raise AgentException(f"Workflow execution failed: {e}") from e
    
    async def _monitor_workflow(self, task: AgentTask) -> Dict[str, Any]:
        """Monitor workflow execution progress."""
        workflow_id = task.inputs.get("workflow_id")
        
        if workflow_id and workflow_id in self.active_workflows:
            workflow = self.active_workflows[workflow_id]
            return {
                "workflow_id": workflow_id,
                "status": workflow["status"],
                "progress": workflow["progress"],
                "current_step": workflow["progress"]["current_step"],
                "completion_percentage": workflow["progress"]["completion_percentage"],
                "execution_time": time.time() - workflow.get("execution_started_at", time.time()),
            }
        else:
            return {
                "workflow_id": workflow_id,
                "status": "not_found",
                "message": f"Workflow {workflow_id} not found in active workflows",
            }
    
    async def _coordinate_agents(self, task: AgentTask) -> Dict[str, Any]:
        """Coordinate with other agents."""
        coordination_type = task.inputs.get("coordination_type", "broadcast")
        message_content = task.inputs.get("message_content", {})
        target_agents = task.inputs.get("target_agents", [])
        
        coordination_results = []
        
        if coordination_type == "broadcast":
            # Send broadcast message
            success = await self.send_message("broadcast", message_content)
            coordination_results.append({
                "type": "broadcast",
                "success": success,
                "message": "Broadcast message sent" if success else "Failed to send broadcast",
            })
        
        elif coordination_type == "direct":
            # Send direct messages to specific agents
            for agent_id in target_agents:
                success = await self.send_message(agent_id, message_content)
                coordination_results.append({
                    "type": "direct",
                    "target_agent": agent_id,
                    "success": success,
                    "message": f"Message sent to {agent_id}" if success else f"Failed to send to {agent_id}",
                })
        
        elif coordination_type == "request_status":
            # Request status from subordinate agents
            for agent_id in self.subordinate_agents:
                status_request = {
                    "request_type": "status_update",
                    "timestamp": time.time(),
                }
                success = await self.send_message(agent_id, status_request)
                coordination_results.append({
                    "type": "status_request",
                    "target_agent": agent_id,
                    "success": success,
                })
        
        return {
            "coordination_type": coordination_type,
            "coordination_results": coordination_results,
            "total_messages": len(coordination_results),
            "successful_messages": len([r for r in coordination_results if r.get("success", False)]),
        }
    
    async def _analyze_data_source(self, task: AgentTask) -> Dict[str, Any]:
        """Analyze a data source using available tools."""
        source_path = task.inputs.get("source_path")
        source_type = task.inputs.get("source_type", "auto")
        
        if not source_path:
            raise AgentException("source_path is required for data source analysis")
        
        # Use data source analysis tool
        analysis_result = await self._use_tool("analyze_data_source", {
            "source_path": source_path,
            "source_type": source_type,
            "include_samples": task.inputs.get("include_samples", True),
            "max_sample_rows": task.inputs.get("max_sample_rows", 100),
        })
        
        # Store analysis in memory
        memory_content = {
            "analysis_type": "data_source",
            "source_path": source_path,
            "analysis_result": analysis_result,
            "analysis_timestamp": time.time(),
        }
        
        self.memory.store_memory(
            content=memory_content,
            memory_type=MemoryType.SEMANTIC,
            importance=MemoryImportance.HIGH,
            tags={"data_source", "analysis", source_type},
        )
        
        return analysis_result
    
    async def _generate_etl_plan(self, task: AgentTask) -> Dict[str, Any]:
        """Generate an ETL execution plan."""
        metadata = task.inputs.get("metadata")
        requirements = task.inputs.get("requirements", {})
        
        if not metadata:
            raise AgentException("metadata is required for ETL plan generation")
        
        # Use LLM to create intelligent execution plan
        planning_prompt = f"""Based on the following data source metadata and requirements, create a comprehensive ETL execution plan:

Metadata: {json.dumps(metadata, indent=2)}
Requirements: {json.dumps(requirements, indent=2)}

Create a plan that includes:
1. Execution strategy (sequential, parallel, hybrid)
2. Detailed steps with dependencies
3. Resource requirements
4. Error handling strategies
5. Performance optimizations
6. Data quality checks

Respond with a JSON structure containing the execution plan."""
        
        llm_response = await self.query_llm(planning_prompt)
        
        try:
            # Try to parse LLM response as JSON
            execution_plan = json.loads(llm_response)
        except json.JSONDecodeError:
            # If not valid JSON, create a structured plan
            execution_plan = await self._create_default_execution_plan(metadata, requirements)
        
        # Store plan in memory
        self.memory.store_memory(
            content={
                "plan_type": "etl_execution",
                "metadata": metadata,
                "requirements": requirements,
                "execution_plan": execution_plan,
            },
            memory_type=MemoryType.PROCEDURAL,
            importance=MemoryImportance.HIGH,
            tags={"etl_plan", "execution", "planning"},
        )
        
        return execution_plan
    
    async def _optimize_workflow(self, task: AgentTask) -> Dict[str, Any]:
        """Optimize an existing workflow."""
        workflow_id = task.inputs.get("workflow_id")
        optimization_type = task.inputs.get("optimization_type", "performance")
        
        if workflow_id not in self.active_workflows:
            raise AgentException(f"Workflow {workflow_id} not found")
        
        workflow = self.active_workflows[workflow_id]
        
        # Analyze current workflow performance
        performance_analysis = await self._analyze_workflow_performance(workflow)
        
        # Generate optimization recommendations
        optimization_prompt = f"""Analyze this ETL workflow and provide optimization recommendations:

Workflow: {json.dumps(workflow, indent=2)}
Performance Analysis: {json.dumps(performance_analysis, indent=2)}
Optimization Type: {optimization_type}

Provide specific recommendations for:
1. Performance improvements
2. Resource utilization optimization
3. Error reduction strategies
4. Parallelization opportunities
5. Cost optimization

Respond with actionable recommendations in JSON format."""
        
        llm_response = await self.query_llm(optimization_prompt)
        
        try:
            optimization_recommendations = json.loads(llm_response)
        except json.JSONDecodeError:
            optimization_recommendations = {
                "recommendations": [
                    "Consider parallelizing independent tasks",
                    "Implement caching for frequently accessed data",
                    "Add more comprehensive error handling",
                    "Optimize resource allocation based on task requirements",
                ],
                "estimated_improvement": "10-30% performance gain",
            }
        
        return {
            "workflow_id": workflow_id,
            "optimization_type": optimization_type,
            "performance_analysis": performance_analysis,
            "recommendations": optimization_recommendations,
            "analysis_timestamp": time.time(),
        }
    
    async def _handle_generic_task(self, task: AgentTask) -> Dict[str, Any]:
        """Handle generic tasks using LLM reasoning."""
        task_description = task.description
        task_inputs = task.inputs
        
        # Use LLM to process the task
        generic_prompt = f"""You are an ETL Orchestrator Agent. Process this task:

Task Description: {task_description}
Task Inputs: {json.dumps(task_inputs, indent=2)}

Available tools: {', '.join(self.tool_registry.list_tool_names())}
Available capabilities: {[cap.name for cap in self.capabilities]}

Analyze the task and provide:
1. Understanding of what needs to be done
2. Step-by-step approach
3. Required tools or capabilities
4. Expected outcomes
5. Potential challenges

If this is a task you can complete directly, provide the solution. Otherwise, explain what additional information or resources would be needed."""
        
        llm_response = await self.query_llm(generic_prompt)
        
        return {
            "task_type": task.task_type,
            "task_description": task_description,
            "analysis": llm_response,
            "processed_at": time.time(),
        }
    
    async def _generate_execution_plan(self, planning_context: Dict[str, Any]) -> Dict[str, Any]:
        """Generate a comprehensive execution plan."""
        # This is a simplified version - in practice, this would be much more sophisticated
        steps = [
            {
                "name": "data_extraction",
                "type": "extract",
                "tool": "analyze_data_source",
                "dependencies": [],
                "estimated_duration": 300,
                "continue_on_failure": False,
            },
            {
                "name": "data_transformation",
                "type": "transform",
                "tool": "execute_pipeline",
                "dependencies": ["data_extraction"],
                "estimated_duration": 600,
                "continue_on_failure": False,
            },
            {
                "name": "data_quality_validation",
                "type": "validate",
                "tool": "validate_data_quality",
                "dependencies": ["data_transformation"],
                "estimated_duration": 180,
                "continue_on_failure": True,
            },
            {
                "name": "data_loading",
                "type": "load",
                "tool": "execute_pipeline",
                "dependencies": ["data_quality_validation"],
                "estimated_duration": 300,
                "continue_on_failure": False,
            },
        ]
        
        return {
            "strategy": self.decision_context["current_strategy"],
            "steps": steps,
            "estimated_duration": sum(step["estimated_duration"] for step in steps),
            "resource_requirements": {
                "cpu": "medium",
                "memory": "2GB",
                "storage": "10GB",
            },
            "parallelization_opportunities": [
                "data_quality_validation can run in parallel with data_loading preparation"
            ],
            "error_handling": {
                "retry_strategy": "exponential_backoff",
                "max_retries": 3,
                "fallback_strategy": "partial_completion",
            },
        }
    
    async def _execute_workflow_step(self, step: Dict[str, Any], workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a single workflow step."""
        step_name = step.get("name", "unknown_step")
        tool_name = step.get("tool")
        step_type = step.get("type", "generic")
        
        self.logger.info(f"Executing step: {step_name} (type: {step_type})")
        
        start_time = time.time()
        
        try:
            if tool_name:
                # Execute using specified tool
                tool_inputs = step.get("inputs", {})
                result = await self._use_tool(tool_name, tool_inputs)
            else:
                # Execute step logic directly
                result = await self._execute_step_logic(step, workflow)
            
            execution_time = time.time() - start_time
            
            return {
                "step_name": step_name,
                "result": result,
                "execution_time": execution_time,
                "status": "completed",
            }
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Step {step_name} failed after {execution_time:.2f}s: {e}")
            raise
    
    async def _execute_step_logic(self, step: Dict[str, Any], workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Execute step logic when no specific tool is specified."""
        step_type = step.get("type", "generic")
        
        if step_type == "extract":
            return {"message": "Data extraction completed", "records_extracted": 1000}
        elif step_type == "transform":
            return {"message": "Data transformation completed", "records_transformed": 1000}
        elif step_type == "validate":
            return {"message": "Data validation completed", "validation_passed": True}
        elif step_type == "load":
            return {"message": "Data loading completed", "records_loaded": 1000}
        else:
            return {"message": f"Generic step {step.get('name', 'unknown')} completed"}
    
    async def _handle_step_failure(self, step: Dict[str, Any], error: Exception, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Handle step failure with sophisticated recovery strategies."""
        step_name = step.get("name", "unknown_step")
        step_type = step.get("type", "unknown")
        workflow_id = workflow.get("workflow_id", "unknown")
        
        # Initialize failure tracking if not exists
        if "failure_history" not in workflow:
            workflow["failure_history"] = []
            
        # Record failure
        failure_record = {
            "step_name": step_name,
            "step_type": step_type,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "timestamp": time.time(),
            "attempt_number": step.get("attempt_number", 1)
        }
        workflow["failure_history"].append(failure_record)
        
        # Determine recovery strategy based on error type, step configuration, and failure history
        recovery_strategy = await self._determine_recovery_strategy(step, error, workflow)
        
        # Log recovery decision
        self.logger.warning(
            f"Step {step_name} failed in workflow {workflow_id}: {error}. "
            f"Recovery strategy: {recovery_strategy['strategy']}"
        )
        
        # Store failure information in memory for learning
        await self.memory.store_entry(
            content=f"Step failure: {step_name} failed with {type(error).__name__}: {error}",
            entry_type=MemoryType.OBSERVATION,
            importance=MemoryImportance.HIGH,
            metadata={
                "workflow_id": workflow_id,
                "step_name": step_name,
                "error_type": type(error).__name__,
                "recovery_strategy": recovery_strategy["strategy"],
                "attempt_number": failure_record["attempt_number"]
            }
        )
        
        return recovery_strategy
    
    async def _determine_recovery_strategy(self, step: Dict[str, Any], error: Exception, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Determine the best recovery strategy based on error analysis and step configuration."""
        step_name = step.get("name", "unknown_step")
        step_type = step.get("type", "unknown")
        max_retries = step.get("max_retries", 3)
        current_attempt = step.get("attempt_number", 1)
        
        # Extract step configuration
        step_config = step.get("recovery_config", {})
        retry_on_failure = step_config.get("retry_on_failure", True)
        continue_on_failure = step_config.get("continue_on_failure", False)
        fallback_enabled = step_config.get("fallback_enabled", True)
        circuit_breaker_enabled = step_config.get("circuit_breaker_enabled", True)
        
        # Analyze error type and determine if it's retryable
        error_analysis = self._analyze_error(error, step, workflow)
        
        # Check circuit breaker status
        circuit_breaker_open = await self._check_circuit_breaker_status(step_name, step_type)
        
        # Strategy 1: Immediate retry for transient errors (with exponential backoff)
        if (error_analysis["is_retryable"] and 
            retry_on_failure and 
            current_attempt < max_retries and 
            not circuit_breaker_open):
            
            # Calculate exponential backoff with jitter
            base_delay = step_config.get("retry_base_delay", 1.0)
            backoff_factor = step_config.get("backoff_factor", 2.0)
            jitter = step_config.get("jitter_enabled", True)
            
            delay = base_delay * (backoff_factor ** (current_attempt - 1))
            if jitter:
                delay *= (0.8 + 0.4 * time.time() % 1)  # Add 20% jitter
            delay = min(delay, step_config.get("max_retry_delay", 60.0))
            
            return {
                "strategy": "retry_with_exponential_backoff",
                "message": f"Retrying {step_name} (attempt {current_attempt + 1}/{max_retries}) in {delay:.1f}s",
                "delay_seconds": delay,
                "retry_attempt": current_attempt + 1,
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": error_analysis["retry_reason"]
            }
        
        # Strategy 2: Fallback to alternative approach
        if (error_analysis["fallback_available"] and 
            fallback_enabled and 
            current_attempt < max_retries):
            
            fallback_approach = await self._get_fallback_approach(step, error, workflow)
            
            return {
                "strategy": "fallback_approach",
                "message": f"Using fallback approach for {step_name}: {fallback_approach['description']}",
                "fallback_config": fallback_approach,
                "original_error": str(error),
                "error_type": type(error).__name__,
                "reason": "Primary approach failed, switching to fallback"
            }
        
        # Strategy 3: Skip step with warning (if configured to continue on failure)
        if continue_on_failure and step_type not in ["critical", "load"]:
            return {
                "strategy": "skip_with_warning",
                "message": f"Skipping non-critical step {step_name} due to failure",
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": "Step configured to continue on failure",
                "impact_assessment": await self._assess_skip_impact(step, workflow)
            }
        
        # Strategy 4: Circuit breaker - temporarily disable step
        if (circuit_breaker_enabled and 
            error_analysis["triggers_circuit_breaker"] and 
            await self._should_open_circuit_breaker(step_name, step_type)):
            
            await self._open_circuit_breaker(step_name, step_type)
            
            return {
                "strategy": "circuit_breaker_open",
                "message": f"Circuit breaker opened for {step_name} due to repeated failures",
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": "Too many failures, temporarily disabling step",
                "recovery_time_estimate": step_config.get("circuit_breaker_recovery_time", 300)
            }
        
        # Strategy 5: Graceful degradation - reduce functionality
        if error_analysis["supports_degradation"]:
            degradation_config = await self._get_degradation_config(step, error, workflow)
            
            return {
                "strategy": "graceful_degradation",
                "message": f"Degrading functionality for {step_name}",
                "degradation_config": degradation_config,
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": "Reducing functionality to maintain partial operation"
            }
        
        # Strategy 6: Dead letter queue - store for later processing
        if error_analysis["can_defer"]:
            await self._add_to_dead_letter_queue(step, error, workflow)
            
            return {
                "strategy": "dead_letter_queue",
                "message": f"Added {step_name} to dead letter queue for later processing",
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": "Step can be processed later when conditions improve",
                "queue_id": f"dlq_{workflow['workflow_id']}_{step_name}_{int(time.time())}"
            }
        
        # Strategy 7: Rollback workflow to previous checkpoint
        if workflow.get("supports_rollback", False) and workflow.get("checkpoints"):
            rollback_point = await self._find_rollback_point(workflow)
            
            return {
                "strategy": "rollback_to_checkpoint",
                "message": f"Rolling back workflow to checkpoint: {rollback_point['name']}",
                "rollback_point": rollback_point,
                "error": str(error),
                "error_type": type(error).__name__,
                "reason": "Critical failure requires rollback to stable state"
            }
        
        # Strategy 8: Fail workflow (last resort)
        return {
            "strategy": "fail_workflow",
            "message": f"Critical failure in {step_name}, stopping workflow",
            "error": str(error),
            "error_type": type(error).__name__,
            "reason": "No viable recovery strategy available",
            "failure_analysis": error_analysis,
            "attempted_strategies": await self._get_attempted_strategies(workflow)
        }
    
    def _analyze_error(self, error: Exception, step: Dict[str, Any], workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze error to determine recovery characteristics."""
        error_type = type(error).__name__
        error_message = str(error).lower()
        step_type = step.get("type", "unknown")
        
        # Network and connectivity errors - highly retryable
        if isinstance(error, (NetworkException, ConnectionError)) or "network" in error_message:
            return {
                "is_retryable": True,
                "retry_reason": "Network connectivity issue",
                "fallback_available": True,
                "triggers_circuit_breaker": True,
                "supports_degradation": False,
                "can_defer": True
            }
        
        # Rate limiting - retryable with backoff
        if isinstance(error, RateLimitException) or "rate limit" in error_message or "throttl" in error_message:
            return {
                "is_retryable": True,
                "retry_reason": "Rate limiting, will backoff",
                "fallback_available": True,
                "triggers_circuit_breaker": False,
                "supports_degradation": True,
                "can_defer": True
            }
        
        # External service errors - moderately retryable
        if isinstance(error, ExternalServiceException) or "service unavailable" in error_message:
            return {
                "is_retryable": True,
                "retry_reason": "External service issue",
                "fallback_available": True,
                "triggers_circuit_breaker": True,
                "supports_degradation": True,
                "can_defer": True
            }
        
        # Timeout errors - retryable
        if "timeout" in error_message or isinstance(error, asyncio.TimeoutError):
            return {
                "is_retryable": True,
                "retry_reason": "Request timeout",
                "fallback_available": True,
                "triggers_circuit_breaker": True,
                "supports_degradation": False,
                "can_defer": True
            }
        
        # Authentication/authorization errors - not retryable
        if "auth" in error_message or "unauthorized" in error_message or "forbidden" in error_message:
            return {
                "is_retryable": False,
                "retry_reason": "Authentication/authorization failure",
                "fallback_available": False,
                "triggers_circuit_breaker": False,
                "supports_degradation": False,
                "can_defer": False
            }
        
        # Validation/configuration errors - not retryable
        if "validation" in error_message or "configuration" in error_message or isinstance(error, ValueError):
            return {
                "is_retryable": False,
                "retry_reason": "Configuration or validation error",
                "fallback_available": False,
                "triggers_circuit_breaker": False,
                "supports_degradation": False,
                "can_defer": False
            }
        
        # Data processing errors - may be retryable depending on context
        if step_type in ["transform", "validate"]:
            return {
                "is_retryable": True,
                "retry_reason": "Data processing error, may be transient",
                "fallback_available": True,
                "triggers_circuit_breaker": False,
                "supports_degradation": True,
                "can_defer": True
            }
        
        # Default analysis for unknown errors
        return {
            "is_retryable": True,
            "retry_reason": "Unknown error, attempting retry",
            "fallback_available": False,
            "triggers_circuit_breaker": False,
            "supports_degradation": False,
            "can_defer": False
        }
    
    async def _check_circuit_breaker_status(self, step_name: str, step_type: str) -> bool:
        """Check if circuit breaker is open for this step."""
        # This would integrate with actual circuit breaker implementation
        # For now, return False (circuit closed)
        return False
    
    async def _should_open_circuit_breaker(self, step_name: str, step_type: str) -> bool:
        """Determine if circuit breaker should be opened."""
        # This would check failure rate and thresholds
        # For now, return False
        return False
    
    async def _open_circuit_breaker(self, step_name: str, step_type: str) -> None:
        """Open circuit breaker for the specified step."""
        # This would implement actual circuit breaker opening
        self.logger.warning(f"Circuit breaker opened for {step_name} ({step_type})")
    
    async def _get_fallback_approach(self, step: Dict[str, Any], error: Exception, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Get fallback approach configuration for failed step."""
        step_type = step.get("type", "unknown")
        
        fallback_configs = {
            "extract": {
                "description": "Use cached data or alternative data source",
                "approach": "alternative_source",
                "reduced_functionality": True
            },
            "transform": {
                "description": "Apply simplified transformation rules",
                "approach": "simplified_transform",
                "reduced_functionality": True
            },
            "load": {
                "description": "Load to alternative destination or queue",
                "approach": "alternative_destination",
                "reduced_functionality": False
            },
            "validate": {
                "description": "Apply relaxed validation rules",
                "approach": "relaxed_validation",
                "reduced_functionality": True
            }
        }
        
        return fallback_configs.get(step_type, {
            "description": "Generic fallback approach",
            "approach": "generic_fallback",
            "reduced_functionality": True
        })
    
    async def _assess_skip_impact(self, step: Dict[str, Any], workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Assess the impact of skipping a step."""
        step_type = step.get("type", "unknown")
        step_name = step.get("name", "unknown")
        
        impact_levels = {
            "extract": "high",      # Skipping data extraction is serious
            "transform": "medium",  # May affect data quality
            "validate": "low",      # Validation can often be skipped temporarily
            "load": "high",         # Skipping load defeats the purpose
            "monitor": "low"        # Monitoring is important but not critical
        }
        
        return {
            "impact_level": impact_levels.get(step_type, "medium"),
            "downstream_effects": f"Skipping {step_name} may affect subsequent steps",
            "data_quality_impact": step_type in ["transform", "validate"],
            "business_continuity_impact": step_type in ["extract", "load"]
        }
    
    async def _get_degradation_config(self, step: Dict[str, Any], error: Exception, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Get graceful degradation configuration."""
        step_type = step.get("type", "unknown")
        
        degradation_configs = {
            "extract": {
                "reduced_data_volume": True,
                "sample_percentage": 10,
                "skip_optional_fields": True
            },
            "transform": {
                "simplified_rules": True,
                "skip_non_essential_transforms": True,
                "relaxed_data_types": True
            },
            "validate": {
                "essential_checks_only": True,
                "warning_instead_of_error": True,
                "skip_referential_integrity": True
            }
        }
        
        return degradation_configs.get(step_type, {
            "reduced_functionality": True,
            "best_effort_mode": True
        })
    
    async def _add_to_dead_letter_queue(self, step: Dict[str, Any], error: Exception, workflow: Dict[str, Any]) -> None:
        """Add failed step to dead letter queue for later processing."""
        # This would implement actual dead letter queue functionality
        self.logger.info(f"Added step {step.get('name')} to dead letter queue")
    
    async def _find_rollback_point(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Find the best rollback point in the workflow."""
        checkpoints = workflow.get("checkpoints", [])
        
        if checkpoints:
            # Return the most recent checkpoint
            return max(checkpoints, key=lambda c: c.get("timestamp", 0))
        
        # Default rollback point
        return {
            "name": "workflow_start",
            "timestamp": workflow.get("started_at", time.time()),
            "state": "initial"
        }
    
    async def _get_attempted_strategies(self, workflow: Dict[str, Any]) -> List[str]:
        """Get list of recovery strategies attempted for this workflow."""
        failure_history = workflow.get("failure_history", [])
        strategies = set()
        
        for failure in failure_history:
            if "recovery_strategy" in failure:
                strategies.add(failure["recovery_strategy"])
        
        return list(strategies)
    
    async def _use_tool(self, tool_name: str, tool_inputs: Dict[str, Any]) -> Any:
        """Use a tool from the registry."""
        tool = self.tool_registry.get_tool(tool_name)
        if not tool:
            raise AgentException(f"Tool {tool_name} not found in registry")
        
        try:
            result = await tool._arun(**tool_inputs)
            
            # Try to parse JSON result
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return {"raw_result": result}
                
        except Exception as e:
            self.logger.error(f"Tool {tool_name} execution failed: {e}")
            raise AgentException(f"Tool execution failed: {e}") from e
    
    async def _apply_workflow_routing(self, target: Optional[str], analysis_result: Dict[str, Any], requirements: Dict[str, Any], constraints: Dict[str, Any]) -> Dict[str, Any]:
        """Apply workflow routing based on target specification and data characteristics."""
        routing_timestamp = time.time()
        
        # Determine workflow type based on target and data analysis
        workflow_type, routing_strategy = self._determine_workflow_type(target, analysis_result, requirements)
        
        # Get routing configuration for the determined workflow type
        routing_config = self._get_routing_configuration(workflow_type, analysis_result, requirements, constraints)
        
        # Create audit trail for routing decision
        audit_trail = {
            "target_specified": target,
            "routing_strategy": routing_strategy,
            "timestamp": routing_timestamp,
            "decision_factors": self._get_routing_decision_factors(target, analysis_result, requirements),
            "fallback_reason": routing_config.get("fallback_reason"),
            "original_target": routing_config.get("original_target"),
            "reason": routing_config.get("routing_reason", "Target-based routing")
        }
        
        # Store routing decision in memory for learning
        await self.memory.store_entry(
            content=f"Workflow routing: {workflow_type} selected for target '{target}' with strategy '{routing_strategy}'",
            entry_type=MemoryType.OBSERVATION,
            importance=MemoryImportance.MEDIUM,
            metadata={
                "workflow_type": workflow_type,
                "target": target,
                "routing_strategy": routing_strategy,
                "data_volume": analysis_result.get("volume", "unknown"),
                "decision_timestamp": routing_timestamp
            }
        )
        
        return {
            "workflow_type": workflow_type,
            "optimization_strategy": routing_config["optimization_strategy"],
            "resource_allocation": routing_config["resource_allocation"],
            "execution_priority": routing_config["execution_priority"],
            "routing_config": routing_config,
            "audit_trail": audit_trail,
            "additional_steps": routing_config.get("additional_steps", []),
            "storage_configuration": routing_config.get("storage_configuration", {}),
            "execution_configuration": routing_config.get("execution_configuration", {})
        }
    
    def _determine_workflow_type(self, target: Optional[str], analysis_result: Dict[str, Any], requirements: Dict[str, Any]) -> tuple[str, str]:
        """Determine workflow type based on target and data characteristics."""
        # Handle auto-routing based on data characteristics
        if target == "auto":
            return self._auto_route_workflow(analysis_result, requirements)
        
        # Handle explicit target routing
        valid_targets = {
            "batch_processing": ("batch_processing", "target_based"),
            "streaming": ("streaming", "target_based"),
            "data_warehouse": ("data_warehouse", "target_based"),
            "machine_learning": ("machine_learning", "target_based"),
            "data_lake": ("data_lake", "target_based"),
            "analytics": ("data_warehouse", "target_based"),  # Alias
            "ml": ("machine_learning", "target_based"),        # Alias
            "realtime": ("streaming", "target_based"),         # Alias
        }
        
        if target and target in valid_targets:
            return valid_targets[target]
        
        # Fallback to auto-routing for invalid targets
        if target:
            self.logger.warning(f"Invalid target '{target}' specified, falling back to auto-routing")
            return self._auto_route_workflow(analysis_result, requirements, fallback_target=target)
        
        # Default to general purpose if no target specified
        return ("general_purpose", "default")
    
    def _auto_route_workflow(self, analysis_result: Dict[str, Any], requirements: Dict[str, Any], fallback_target: Optional[str] = None) -> tuple[str, str]:
        """Automatically route workflow based on data characteristics."""
        volume = analysis_result.get("volume", "unknown")
        data_source = requirements.get("data_source", "")
        
        # Route based on data volume
        if volume in ["very_large", "large"]:
            estimated_size = analysis_result.get("estimated_size", "")
            if "TB" in estimated_size or "PB" in estimated_size:
                return ("batch_processing", "volume_threshold_exceeded")
        
        # Route based on data source type
        if "kafka://" in data_source or "stream" in data_source.lower():
            return ("streaming", "streaming_source_detected")
        
        if "warehouse" in data_source.lower() or "redshift://" in data_source or "snowflake://" in data_source:
            return ("data_warehouse", "warehouse_source_detected")
        
        # Default fallback
        if fallback_target:
            return ("general_purpose", f"fallback_from_{fallback_target}")
        
        return ("general_purpose", "auto_routing_default")
    
    def _get_routing_configuration(self, workflow_type: str, analysis_result: Dict[str, Any], requirements: Dict[str, Any], constraints: Dict[str, Any]) -> Dict[str, Any]:
        """Get routing configuration for the specified workflow type."""
        base_configs = {
            "batch_processing": {
                "optimization_strategy": "throughput_optimized",
                "resource_allocation": {
                    "parallelism": "high",
                    "memory_mode": "batch",
                    "compute_mode": "distributed"
                },
                "execution_priority": requirements.get("priority", "standard"),
                "routing_reason": "Optimized for high-throughput batch processing"
            },
            "streaming": {
                "optimization_strategy": "latency_optimized",
                "resource_allocation": {
                    "memory_mode": "streaming",
                    "compute_mode": "continuous",
                    "parallelism": "medium"
                },
                "execution_priority": requirements.get("priority", "high"),
                "routing_reason": "Optimized for low-latency streaming processing"
            },
            "data_warehouse": {
                "optimization_strategy": "analytics_optimized",
                "resource_allocation": {
                    "compute_mode": "analytical",
                    "memory_mode": "columnar",
                    "parallelism": "medium"
                },
                "execution_priority": requirements.get("priority", "normal"),
                "additional_steps": ["data_quality_checks"],
                "routing_reason": "Optimized for analytical workloads"
            },
            "machine_learning": {
                "optimization_strategy": "ml_optimized",
                "resource_allocation": {
                    "gpu_enabled": True,
                    "compute_mode": "ml",
                    "memory_mode": "feature_store"
                },
                "execution_priority": requirements.get("priority", "normal"),
                "routing_reason": "Optimized for machine learning pipelines"
            },
            "data_lake": {
                "optimization_strategy": "storage_optimized",
                "resource_allocation": {
                    "partitioning": "enabled",
                    "compute_mode": "distributed",
                    "storage_mode": "parquet"
                },
                "execution_priority": requirements.get("priority", "normal"),
                "storage_configuration": {
                    "tier": requirements.get("storage_tier", "hot"),
                    "compression": "enabled"
                },
                "routing_reason": "Optimized for data lake storage and retrieval"
            },
            "general_purpose": {
                "optimization_strategy": "balanced",
                "resource_allocation": {
                    "parallelism": "medium",
                    "compute_mode": "standard",
                    "memory_mode": "standard"
                },
                "execution_priority": requirements.get("priority", "normal"),
                "routing_reason": "Balanced configuration for general ETL workloads"
            }
        }
        
        # Get base configuration
        config = base_configs.get(workflow_type, base_configs["general_purpose"]).copy()
        
        # Apply custom parameters if provided
        custom_params = requirements.get("custom_params", {})
        if custom_params:
            execution_config = {
                "max_parallel_tasks": custom_params.get("max_parallel_tasks"),
                "timeout_minutes": custom_params.get("timeout_minutes"),
                "retry_strategy": custom_params.get("retry_strategy")
            }
            # Remove None values
            config["execution_configuration"] = {k: v for k, v in execution_config.items() if v is not None}
        
        # Add fallback information for invalid targets
        if workflow_type == "general_purpose" and "fallback_from_" in config.get("routing_reason", ""):
            original_target = config["routing_reason"].replace("fallback_from_", "")
            config["fallback_reason"] = "invalid_target_specified"
            config["original_target"] = original_target
        
        return config
    
    def _get_routing_decision_factors(self, target: Optional[str], analysis_result: Dict[str, Any], requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Get factors that influenced the routing decision."""
        return {
            "target_specified": target is not None,
            "data_volume": analysis_result.get("volume", "unknown"),
            "estimated_size": analysis_result.get("estimated_size"),
            "data_source_type": requirements.get("data_source", "").split("://")[0] if "://" in requirements.get("data_source", "") else "file",
            "priority_specified": "priority" in requirements,
            "custom_params_provided": "custom_params" in requirements,
            "constraints_provided": len(requirements.get("constraints", {})) > 0
        }

    async def _store_workflow_memory(self, workflow: Dict[str, Any], memory_type: MemoryType) -> None:
        """Store workflow information in memory."""
        self.memory.store_memory(
            content={
                "workflow_id": workflow["workflow_id"],
                "workflow_data": workflow,
                "storage_timestamp": time.time(),
            },
            memory_type=memory_type,
            importance=MemoryImportance.HIGH,
            tags={"workflow", "creation", workflow["workflow_id"]},
        )
    
    async def _store_execution_memory(self, workflow: Dict[str, Any], execution_results: List[Dict[str, Any]]) -> None:
        """Store execution results in memory."""
        self.memory.store_memory(
            content={
                "workflow_id": workflow["workflow_id"],
                "execution_results": execution_results,
                "execution_summary": {
                    "total_steps": len(execution_results),
                    "successful_steps": len([r for r in execution_results if r["status"] == "completed"]),
                    "failed_steps": len([r for r in execution_results if r["status"] == "failed"]),
                    "total_execution_time": workflow.get("execution_completed_at", 0) - workflow.get("execution_started_at", 0),
                },
                "storage_timestamp": time.time(),
            },
            memory_type=MemoryType.EPISODIC,
            importance=MemoryImportance.HIGH,
            tags={"workflow", "execution", "results", workflow["workflow_id"]},
        )
    
    async def _analyze_workflow_performance(self, workflow: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze workflow performance metrics."""
        execution_results = workflow.get("execution_results", [])
        
        if not execution_results:
            return {"message": "No execution results available for analysis"}
        
        total_time = sum(r.get("result", {}).get("execution_time", 0) for r in execution_results)
        successful_steps = len([r for r in execution_results if r["status"] == "completed"])
        failed_steps = len([r for r in execution_results if r["status"] == "failed"])
        
        return {
            "total_execution_time": total_time,
            "average_step_time": total_time / len(execution_results) if execution_results else 0,
            "success_rate": successful_steps / len(execution_results) if execution_results else 0,
            "total_steps": len(execution_results),
            "successful_steps": successful_steps,
            "failed_steps": failed_steps,
            "performance_rating": "good" if failed_steps == 0 else "needs_improvement",
        }
    
    async def _create_default_execution_plan(self, metadata: Dict[str, Any], requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Create a default execution plan when LLM response is not valid JSON."""
        return {
            "strategy": "sequential_execution",
            "steps": [
                {
                    "name": "extract_data",
                    "type": "extract",
                    "tool": "analyze_data_source",
                    "inputs": {"source_path": metadata.get("source", ""), "source_type": "auto"},
                    "dependencies": [],
                    "estimated_duration": 300,
                },
                {
                    "name": "transform_data",
                    "type": "transform",
                    "dependencies": ["extract_data"],
                    "estimated_duration": 600,
                },
                {
                    "name": "load_data",
                    "type": "load",
                    "dependencies": ["transform_data"],
                    "estimated_duration": 300,
                },
            ],
            "estimated_duration": 1200,
            "resource_requirements": {"cpu": "medium", "memory": "2GB"},
        }
    
    async def _register_default_capabilities(self) -> None:
        """Register orchestrator-specific capabilities."""
        await super()._register_default_capabilities()
        
        orchestrator_capabilities = [
            AgentCapability(
                name="workflow_management",
                description="Create, execute, and monitor ETL workflows",
                input_types=["workflow_requirements", "data_source_info"],
                output_types=["workflow_plan", "execution_results"],
                confidence_level=0.95,
            ),
            AgentCapability(
                name="agent_coordination",
                description="Coordinate and communicate with other agents",
                input_types=["coordination_requests", "agent_messages"],
                output_types=["coordination_results", "agent_responses"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="strategic_planning",
                description="Create strategic execution plans for complex ETL processes",
                input_types=["requirements", "constraints", "metadata"],
                output_types=["execution_plan", "resource_allocation"],
                confidence_level=0.85,
            ),
            AgentCapability(
                name="performance_optimization",
                description="Analyze and optimize workflow performance",
                input_types=["performance_metrics", "execution_history"],
                output_types=["optimization_recommendations", "performance_analysis"],
                confidence_level=0.8,
            ),
        ]
        
        for capability in orchestrator_capabilities:
            self.add_capability(capability)
    
    def register_subordinate_agent(self, agent_id: str) -> None:
        """Register a subordinate agent."""
        self.subordinate_agents.add(agent_id)
        self.logger.info(f"Registered subordinate agent: {agent_id}")
    
    def unregister_subordinate_agent(self, agent_id: str) -> None:
        """Unregister a subordinate agent."""
        self.subordinate_agents.discard(agent_id)
        self.logger.info(f"Unregistered subordinate agent: {agent_id}")
    
    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get detailed orchestrator status."""
        base_status = self.get_status()
        
        orchestrator_status = {
            **base_status,
            "active_workflows": len(self.active_workflows),
            "workflow_history_count": len(self.workflow_history),
            "subordinate_agents": len(self.subordinate_agents),
            "memory_entries": self.memory.get_memory_summary()["total_memories"],
            "decision_context": self.decision_context.copy(),
            "available_tools": len(self.tool_registry.list_tool_names()),
        }
        
        return orchestrator_status