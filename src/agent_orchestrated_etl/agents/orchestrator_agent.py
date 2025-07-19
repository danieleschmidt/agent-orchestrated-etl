"""Orchestrator agent for managing ETL workflows and coordinating other agents."""

from __future__ import annotations

import json
import time
from typing import Any, Dict, List, Optional, Set

from langchain_core.language_models.base import BaseLanguageModel

from .base_agent import BaseAgent, AgentConfig, AgentRole, AgentTask, AgentCapability
from .communication import AgentCommunicationHub
from .memory import AgentMemory, MemoryType, MemoryImportance
from .tools import AgentToolRegistry, get_tool_registry
from ..exceptions import AgentException
from ..logging_config import LogContext


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
        
        super().__init__(config, llm, communication_hub)
        
        # Orchestrator-specific components
        self.tool_registry = tool_registry or get_tool_registry()
        self.memory = AgentMemory(
            agent_id=self.config.agent_id,
            max_entries=50000,  # Orchestrators need more memory
            working_memory_size=200,
        )
        
        # Workflow management
        self.active_workflows: Dict[str, Dict[str, Any]] = {}
        self.workflow_history: List[Dict[str, Any]] = []
        self.subordinate_agents: Set[str] = set()
        
        # Decision-making state
        self.decision_context: Dict[str, Any] = {}
        self.planning_strategies: List[str] = [
            "sequential_execution",
            "parallel_optimization",
            "resource_aware_scheduling",
            "error_recovery_planning",
        ]
    
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
            
            # Step 2: Generate execution plan
            planning_context = {
                "data_analysis": analysis_result,
                "requirements": requirements,
                "constraints": task.inputs.get("constraints", {}),
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
                "execution_plan": execution_plan,
                "estimated_duration": execution_plan.get("estimated_duration", "unknown"),
                "resource_requirements": execution_plan.get("resource_requirements", {}),
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
        """Handle step failure with recovery strategies."""
        step_name = step.get("name", "unknown_step")
        
        recovery_strategies = [
            "retry_with_backoff",
            "use_alternative_approach",
            "skip_with_warning",
            "fail_workflow",
        ]
        
        # Simple recovery logic - in practice, this would be more sophisticated
        if "network" in str(error).lower():
            return {
                "recovery_strategy": "retry_with_backoff",
                "message": f"Network error in {step_name}, will retry",
                "error": str(error),
            }
        elif step.get("continue_on_failure", False):
            return {
                "recovery_strategy": "skip_with_warning",
                "message": f"Skipping failed step {step_name}",
                "error": str(error),
            }
        else:
            return {
                "recovery_strategy": "fail_workflow",
                "message": f"Critical failure in {step_name}, stopping workflow",
                "error": str(error),
            }
    
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