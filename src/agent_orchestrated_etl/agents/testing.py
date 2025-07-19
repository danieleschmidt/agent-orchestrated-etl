"""Testing framework for agent-based ETL system."""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable
from unittest.mock import Mock, AsyncMock

from .base_agent import BaseAgent, AgentConfig, AgentRole, AgentTask
from .communication import AgentCommunicationHub, Message, MessageType
from .orchestrator_agent import OrchestratorAgent
from .etl_agent import ETLAgent
from .monitor_agent import MonitorAgent
from .coordination import AgentCoordinator, WorkflowDefinition, CoordinationTask, CoordinationPattern
from .tools import AgentToolRegistry
from ..exceptions import AgentException, TestingException
from ..logging_config import get_logger


@dataclass
class TestCase:
    """Represents a single test case for agent testing."""
    
    test_id: str
    name: str
    description: str
    test_type: str  # unit, integration, performance, stress
    
    # Test configuration
    agents_required: List[str] = field(default_factory=list)
    test_data: Dict[str, Any] = field(default_factory=dict)
    expected_results: Dict[str, Any] = field(default_factory=dict)
    timeout_seconds: float = 30.0
    
    # Test execution info
    status: str = "pending"  # pending, running, passed, failed, error
    started_at: Optional[float] = None
    completed_at: Optional[float] = None
    execution_time: Optional[float] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


@dataclass
class TestSuite:
    """Collection of test cases."""
    
    suite_id: str
    name: str
    description: str
    test_cases: List[TestCase] = field(default_factory=list)
    
    # Execution configuration
    parallel_execution: bool = False
    stop_on_failure: bool = False
    setup_functions: List[Callable] = field(default_factory=list)
    teardown_functions: List[Callable] = field(default_factory=list)


class MockAgent(BaseAgent):
    """Mock agent for testing purposes."""
    
    def __init__(self, config: AgentConfig, mock_responses: Optional[Dict[str, Any]] = None):
        super().__init__(config)
        self.mock_responses = mock_responses or {}
        self.call_history: List[Dict[str, Any]] = []
    
    def _initialize_agent(self) -> None:
        pass
    
    async def _process_task(self, task: AgentTask) -> Dict[str, Any]:
        # Record the call
        self.call_history.append({
            "task_type": task.task_type,
            "task_inputs": task.inputs,
            "timestamp": time.time(),
        })
        
        # Return mock response if available
        if task.task_type in self.mock_responses:
            return self.mock_responses[task.task_type]
        
        # Default mock response
        return {
            "status": "completed",
            "message": f"Mock response for {task.task_type}",
            "execution_time": 1.0,
        }
    
    def get_system_prompt(self) -> str:
        return "Mock agent for testing purposes"


class AgentTestFramework:
    """Testing framework for agent-based ETL system."""
    
    def __init__(self):
        self.test_suites: Dict[str, TestSuite] = {}
        self.test_results: List[Dict[str, Any]] = []
        self.mock_agents: Dict[str, MockAgent] = {}
        
        # Test environment
        self.communication_hub: Optional[AgentCommunicationHub] = None
        self.coordinator: Optional[AgentCoordinator] = None
        self.tool_registry: Optional[AgentToolRegistry] = None
        
        self.logger = get_logger("agent.testing")
        self.logger.info("Agent testing framework initialized")
    
    async def setup_test_environment(self) -> None:
        """Set up the test environment."""
        self.logger.info("Setting up test environment")
        
        # Create communication hub
        self.communication_hub = AgentCommunicationHub()
        await self.communication_hub.start()
        
        # Create tool registry
        from .tools import get_tool_registry
        self.tool_registry = get_tool_registry()
        
        # Create coordinator
        self.coordinator = AgentCoordinator(self.communication_hub)
        
        self.logger.info("Test environment setup complete")
    
    async def teardown_test_environment(self) -> None:
        """Tear down the test environment."""
        self.logger.info("Tearing down test environment")
        
        if self.communication_hub:
            await self.communication_hub.stop()
        
        self.mock_agents.clear()
        
        self.logger.info("Test environment teardown complete")
    
    def create_test_suite(self, suite_id: str, name: str, description: str) -> TestSuite:
        """Create a new test suite."""
        test_suite = TestSuite(
            suite_id=suite_id,
            name=name,
            description=description,
        )
        
        self.test_suites[suite_id] = test_suite
        self.logger.info(f"Created test suite: {name}")
        
        return test_suite
    
    def add_test_case(self, suite_id: str, test_case: TestCase) -> None:
        """Add a test case to a test suite."""
        if suite_id not in self.test_suites:
            raise TestingException(f"Test suite not found: {suite_id}")
        
        self.test_suites[suite_id].test_cases.append(test_case)
        self.logger.info(f"Added test case {test_case.name} to suite {suite_id}")
    
    def create_mock_agent(
        self,
        agent_id: str,
        role: AgentRole,
        mock_responses: Optional[Dict[str, Any]] = None,
    ) -> MockAgent:
        """Create a mock agent for testing."""
        config = AgentConfig(
            agent_id=agent_id,
            name=f"MockAgent_{role.value}",
            role=role,
        )
        
        mock_agent = MockAgent(config, mock_responses)
        self.mock_agents[agent_id] = mock_agent
        
        return mock_agent
    
    async def run_test_suite(self, suite_id: str) -> Dict[str, Any]:
        """Run all test cases in a test suite."""
        if suite_id not in self.test_suites:
            raise TestingException(f"Test suite not found: {suite_id}")
        
        test_suite = self.test_suites[suite_id]
        self.logger.info(f"Running test suite: {test_suite.name}")
        
        start_time = time.time()
        
        try:
            # Run setup functions
            for setup_func in test_suite.setup_functions:
                await setup_func()
            
            # Run test cases
            if test_suite.parallel_execution:
                results = await self._run_test_cases_parallel(test_suite)
            else:
                results = await self._run_test_cases_sequential(test_suite)
            
            # Run teardown functions
            for teardown_func in test_suite.teardown_functions:
                await teardown_func()
            
            execution_time = time.time() - start_time
            
            # Calculate summary
            passed_tests = len([r for r in results if r["status"] == "passed"])
            failed_tests = len([r for r in results if r["status"] == "failed"])
            error_tests = len([r for r in results if r["status"] == "error"])
            
            suite_result = {
                "suite_id": suite_id,
                "suite_name": test_suite.name,
                "execution_time": execution_time,
                "total_tests": len(results),
                "passed_tests": passed_tests,
                "failed_tests": failed_tests,
                "error_tests": error_tests,
                "success_rate": passed_tests / len(results) if results else 0,
                "test_results": results,
            }
            
            self.test_results.append(suite_result)
            
            self.logger.info(
                f"Test suite completed: {test_suite.name} "
                f"({passed_tests}/{len(results)} passed)"
            )
            
            return suite_result
            
        except Exception as e:
            self.logger.error(f"Test suite execution failed: {e}", exc_info=e)
            raise TestingException(f"Test suite execution failed: {e}") from e
    
    async def run_single_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a single test case."""
        self.logger.info(f"Running test case: {test_case.name}")
        
        test_case.started_at = time.time()
        test_case.status = "running"
        
        try:
            # Set up required agents
            await self._setup_test_agents(test_case)
            
            # Execute test based on type
            if test_case.test_type == "unit":
                result = await self._run_unit_test(test_case)
            elif test_case.test_type == "integration":
                result = await self._run_integration_test(test_case)
            elif test_case.test_type == "performance":
                result = await self._run_performance_test(test_case)
            elif test_case.test_type == "stress":
                result = await self._run_stress_test(test_case)
            else:
                result = await self._run_generic_test(test_case)
            
            # Validate results
            validation_result = await self._validate_test_results(test_case, result)
            
            test_case.completed_at = time.time()
            test_case.execution_time = test_case.completed_at - test_case.started_at
            test_case.result = result
            test_case.status = "passed" if validation_result["passed"] else "failed"
            
            return {
                "test_id": test_case.test_id,
                "test_name": test_case.name,
                "status": test_case.status,
                "execution_time": test_case.execution_time,
                "result": result,
                "validation": validation_result,
            }
            
        except Exception as e:
            test_case.completed_at = time.time()
            test_case.execution_time = test_case.completed_at - test_case.started_at
            test_case.status = "error"
            test_case.error = str(e)
            
            self.logger.error(f"Test case failed: {test_case.name} - {e}")
            
            return {
                "test_id": test_case.test_id,
                "test_name": test_case.name,
                "status": "error",
                "execution_time": test_case.execution_time,
                "error": str(e),
            }
    
    async def _run_test_cases_sequential(self, test_suite: TestSuite) -> List[Dict[str, Any]]:
        """Run test cases sequentially."""
        results = []
        
        for test_case in test_suite.test_cases:
            result = await self.run_single_test(test_case)
            results.append(result)
            
            if test_suite.stop_on_failure and result["status"] in ["failed", "error"]:
                self.logger.warning(f"Stopping test suite due to failure: {test_case.name}")
                break
        
        return results
    
    async def _run_test_cases_parallel(self, test_suite: TestSuite) -> List[Dict[str, Any]]:
        """Run test cases in parallel."""
        test_tasks = [self.run_single_test(test_case) for test_case in test_suite.test_cases]
        results = await asyncio.gather(*test_tasks, return_exceptions=True)
        
        # Convert exceptions to error results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    "test_id": test_suite.test_cases[i].test_id,
                    "test_name": test_suite.test_cases[i].name,
                    "status": "error",
                    "error": str(result),
                })
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def _setup_test_agents(self, test_case: TestCase) -> None:
        """Set up required agents for test case."""
        for agent_id in test_case.agents_required:
            if agent_id not in self.mock_agents:
                # Create default mock agent
                mock_agent = self.create_mock_agent(agent_id, AgentRole.ORCHESTRATOR)
                
                if self.communication_hub:
                    await self.communication_hub.register_agent(mock_agent)
                
                if self.coordinator:
                    await self.coordinator.register_agent(mock_agent)
    
    async def _run_unit_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a unit test for a single agent."""
        agent_id = test_case.test_data.get("agent_id")
        task_data = test_case.test_data.get("task_data", {})
        
        if not agent_id or agent_id not in self.mock_agents:
            raise TestingException(f"Agent not found for unit test: {agent_id}")
        
        agent = self.mock_agents[agent_id]
        
        # Create and execute task
        task = AgentTask(
            task_type=task_data.get("task_type", "test_task"),
            description=task_data.get("description", "Unit test task"),
            inputs=task_data.get("inputs", {}),
        )
        
        result = await agent.execute_task(task)
        
        return {
            "test_type": "unit",
            "agent_id": agent_id,
            "task_result": result,
            "call_history": agent.call_history,
        }
    
    async def _run_integration_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run an integration test involving multiple agents."""
        workflow_data = test_case.test_data.get("workflow", {})
        
        if not self.coordinator:
            raise TestingException("Coordinator not available for integration test")
        
        # Create workflow definition
        workflow_def = WorkflowDefinition(
            name=workflow_data.get("name", "Integration Test Workflow"),
            description=workflow_data.get("description", ""),
            coordination_pattern=CoordinationPattern(workflow_data.get("pattern", "sequential")),
            agents=test_case.agents_required,
        )
        
        # Add tasks
        for task_data in workflow_data.get("tasks", []):
            task = CoordinationTask(
                task_type=task_data.get("task_type", "test_task"),
                task_data=task_data.get("data", {}),
                dependencies=task_data.get("dependencies", []),
            )
            workflow_def.tasks.append(task)
        
        # Execute workflow
        result = await self.coordinator.execute_workflow(workflow_def)
        
        return {
            "test_type": "integration",
            "workflow_result": result,
            "coordination_pattern": workflow_def.coordination_pattern.value,
        }
    
    async def _run_performance_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a performance test."""
        performance_config = test_case.test_data.get("performance", {})
        iterations = performance_config.get("iterations", 10)
        
        # Run multiple iterations and measure performance
        execution_times = []
        results = []
        
        for i in range(iterations):
            start_time = time.time()
            
            # Run the actual test (could be unit or integration)
            if test_case.test_data.get("workflow"):
                iteration_result = await self._run_integration_test(test_case)
            else:
                iteration_result = await self._run_unit_test(test_case)
            
            execution_time = time.time() - start_time
            execution_times.append(execution_time)
            results.append(iteration_result)
        
        # Calculate performance metrics
        avg_time = sum(execution_times) / len(execution_times)
        min_time = min(execution_times)
        max_time = max(execution_times)
        
        return {
            "test_type": "performance",
            "iterations": iterations,
            "average_execution_time": avg_time,
            "min_execution_time": min_time,
            "max_execution_time": max_time,
            "execution_times": execution_times,
            "results": results,
        }
    
    async def _run_stress_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a stress test with high load."""
        stress_config = test_case.test_data.get("stress", {})
        concurrent_tasks = stress_config.get("concurrent_tasks", 10)
        duration_seconds = stress_config.get("duration_seconds", 30)
        
        # Start concurrent tasks
        start_time = time.time()
        active_tasks = []
        completed_tasks = []
        failed_tasks = []
        
        async def stress_task():
            try:
                if test_case.test_data.get("workflow"):
                    result = await self._run_integration_test(test_case)
                else:
                    result = await self._run_unit_test(test_case)
                completed_tasks.append(result)
            except Exception as e:
                failed_tasks.append(str(e))
        
        # Run stress test
        while time.time() - start_time < duration_seconds:
            # Maintain concurrent task count
            while len(active_tasks) < concurrent_tasks:
                task = asyncio.create_task(stress_task())
                active_tasks.append(task)
            
            # Clean up completed tasks
            active_tasks = [t for t in active_tasks if not t.done()]
            
            await asyncio.sleep(0.1)
        
        # Wait for remaining tasks to complete
        await asyncio.gather(*active_tasks, return_exceptions=True)
        
        return {
            "test_type": "stress",
            "duration_seconds": duration_seconds,
            "concurrent_tasks": concurrent_tasks,
            "completed_tasks": len(completed_tasks),
            "failed_tasks": len(failed_tasks),
            "success_rate": len(completed_tasks) / (len(completed_tasks) + len(failed_tasks)) if (completed_tasks or failed_tasks) else 0,
        }
    
    async def _run_generic_test(self, test_case: TestCase) -> Dict[str, Any]:
        """Run a generic test case."""
        return {
            "test_type": "generic",
            "message": "Generic test execution completed",
            "test_data": test_case.test_data,
        }
    
    async def _validate_test_results(self, test_case: TestCase, result: Dict[str, Any]) -> Dict[str, Any]:
        """Validate test results against expected outcomes."""
        expected = test_case.expected_results
        validation_errors = []
        
        # Check required fields
        for field in expected.get("required_fields", []):
            if field not in result:
                validation_errors.append(f"Missing required field: {field}")
        
        # Check specific values
        for field, expected_value in expected.get("expected_values", {}).items():
            if field in result and result[field] != expected_value:
                validation_errors.append(f"Field {field}: expected {expected_value}, got {result[field]}")
        
        # Check value ranges
        for field, range_config in expected.get("value_ranges", {}).items():
            if field in result:
                value = result[field]
                if "min" in range_config and value < range_config["min"]:
                    validation_errors.append(f"Field {field} below minimum: {value} < {range_config['min']}")
                if "max" in range_config and value > range_config["max"]:
                    validation_errors.append(f"Field {field} above maximum: {value} > {range_config['max']}")
        
        # Check status
        expected_status = expected.get("status")
        if expected_status and result.get("status") != expected_status:
            validation_errors.append(f"Status mismatch: expected {expected_status}, got {result.get('status')}")
        
        return {
            "passed": len(validation_errors) == 0,
            "validation_errors": validation_errors,
            "checks_performed": len(expected),
        }
    
    def get_test_summary(self) -> Dict[str, Any]:
        """Get summary of all test results."""
        if not self.test_results:
            return {"message": "No test results available"}
        
        total_suites = len(self.test_results)
        total_tests = sum(r["total_tests"] for r in self.test_results)
        total_passed = sum(r["passed_tests"] for r in self.test_results)
        total_failed = sum(r["failed_tests"] for r in self.test_results)
        total_errors = sum(r["error_tests"] for r in self.test_results)
        
        return {
            "total_test_suites": total_suites,
            "total_test_cases": total_tests,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "total_errors": total_errors,
            "overall_success_rate": total_passed / total_tests if total_tests > 0 else 0,
            "test_results": self.test_results,
        }
    
    def create_sample_tests(self) -> None:
        """Create sample test cases for demonstration."""
        # Create orchestrator test suite
        orchestrator_suite = self.create_test_suite(
            "orchestrator_tests",
            "Orchestrator Agent Tests",
            "Test suite for orchestrator agent functionality"
        )
        
        # Add unit test for workflow creation
        workflow_creation_test = TestCase(
            test_id="test_workflow_creation",
            name="Test Workflow Creation",
            description="Test orchestrator's ability to create workflows",
            test_type="unit",
            agents_required=["orchestrator_agent"],
            test_data={
                "agent_id": "orchestrator_agent",
                "task_data": {
                    "task_type": "create_workflow",
                    "inputs": {
                        "requirements": {
                            "data_source": "test_database",
                            "target": "test_target",
                        }
                    }
                }
            },
            expected_results={
                "required_fields": ["workflow_id", "status"],
                "expected_values": {"status": "created"},
            }
        )
        
        self.add_test_case("orchestrator_tests", workflow_creation_test)
        
        # Create ETL agent test suite
        etl_suite = self.create_test_suite(
            "etl_tests",
            "ETL Agent Tests",
            "Test suite for ETL agent functionality"
        )
        
        # Add integration test for multi-agent coordination
        coordination_test = TestCase(
            test_id="test_agent_coordination",
            name="Test Agent Coordination",
            description="Test coordination between multiple agents",
            test_type="integration",
            agents_required=["orchestrator_agent", "etl_agent", "monitor_agent"],
            test_data={
                "workflow": {
                    "name": "Test Coordination Workflow",
                    "pattern": "sequential",
                    "tasks": [
                        {
                            "task_type": "extract_data",
                            "data": {"source": "test_source"},
                        },
                        {
                            "task_type": "transform_data",
                            "data": {"rules": []},
                            "dependencies": ["extract_data"],
                        },
                        {
                            "task_type": "monitor_pipeline",
                            "data": {"pipeline_id": "test_pipeline"},
                            "dependencies": ["transform_data"],
                        },
                    ]
                }
            },
            expected_results={
                "required_fields": ["workflow_result"],
                "expected_values": {"workflow_result.status": "completed"},
            }
        )
        
        self.add_test_case("etl_tests", coordination_test)
        
        self.logger.info("Sample test cases created")


# Convenience functions for creating test scenarios

def create_basic_unit_test(
    test_id: str,
    agent_role: AgentRole,
    task_type: str,
    task_inputs: Dict[str, Any],
    expected_status: str = "completed",
) -> TestCase:
    """Create a basic unit test case."""
    return TestCase(
        test_id=test_id,
        name=f"Unit Test: {task_type}",
        description=f"Unit test for {task_type} on {agent_role.value} agent",
        test_type="unit",
        agents_required=[f"{agent_role.value}_agent"],
        test_data={
            "agent_id": f"{agent_role.value}_agent",
            "task_data": {
                "task_type": task_type,
                "inputs": task_inputs,
            }
        },
        expected_results={
            "expected_values": {"status": expected_status},
        }
    )


def create_performance_test(
    test_id: str,
    base_test: TestCase,
    iterations: int = 10,
    max_avg_time: float = 5.0,
) -> TestCase:
    """Create a performance test case from a base test."""
    return TestCase(
        test_id=test_id,
        name=f"Performance Test: {base_test.name}",
        description=f"Performance test for {base_test.description}",
        test_type="performance",
        agents_required=base_test.agents_required,
        test_data={
            **base_test.test_data,
            "performance": {"iterations": iterations},
        },
        expected_results={
            "value_ranges": {"average_execution_time": {"max": max_avg_time}},
        }
    )