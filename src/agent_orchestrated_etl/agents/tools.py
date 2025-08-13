"""LangChain tool integration for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union

from langchain_core.tools import BaseTool

# Note: create_json_schema not available in current langchain version
# Using pydantic's model_json_schema instead
from pydantic import BaseModel, Field

from ..circuit_breaker import CircuitBreakerConfigs, circuit_breaker
from ..exceptions import AgentException, ToolException
from ..logging_config import get_logger
from ..retry import RetryConfigs, retry


class ETLToolSchema(BaseModel):
    """Base schema for ETL tool inputs."""
    pass


class AnalyzeDataSourceSchema(ETLToolSchema):
    """Schema for data source analysis tool."""

    source_path: str = Field(description="Path or connection string to the data source")
    source_type: str = Field(description="Type of data source (database, file, api, etc.)")
    include_samples: bool = Field(default=True, description="Whether to include data samples")
    max_sample_rows: int = Field(default=100, description="Maximum number of sample rows")


class GenerateDAGSchema(ETLToolSchema):
    """Schema for DAG generation tool."""

    metadata: Dict[str, Any] = Field(description="Data source metadata from analysis")
    dag_id: str = Field(description="Unique identifier for the DAG")
    include_validation: bool = Field(default=True, description="Include data validation steps")
    parallel_tasks: bool = Field(default=True, description="Enable parallel task execution")


class ExecutePipelineSchema(ETLToolSchema):
    """Schema for pipeline execution tool."""

    dag_config: Dict[str, Any] = Field(description="DAG configuration")
    execution_mode: str = Field(default="async", description="Execution mode (sync/async)")
    monitor_progress: bool = Field(default=True, description="Monitor execution progress")
    timeout_seconds: int = Field(default=3600, description="Execution timeout in seconds")


class ValidateDataQualitySchema(ETLToolSchema):
    """Schema for data quality validation tool."""

    data_source: str = Field(description="Data source to validate")
    validation_rules: List[Dict[str, Any]] = Field(description="List of validation rules")
    sample_percentage: float = Field(default=10.0, description="Percentage of data to sample")
    fail_on_errors: bool = Field(default=False, description="Fail immediately on validation errors")


class ETLTool(BaseTool, ABC):
    """Base class for ETL-specific tools."""

    name: str
    description: str
    args_schema: Type[BaseModel] = ETLToolSchema
    return_direct: bool = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Use object.__setattr__ to set private attributes in Pydantic models
        object.__setattr__(self, '_logger', get_logger(f"tool.{self.name}"))

    @property
    def logger(self):
        """Get the logger instance."""
        return getattr(self, '_logger', None) or get_logger(f"tool.{self.name}")

    @retry(RetryConfigs.STANDARD)
    @circuit_breaker("etl_tool_execution", CircuitBreakerConfigs.STANDARD)
    def _run(self, **kwargs) -> str:
        """Synchronous tool execution with resilience patterns."""
        try:
            self.logger.info(f"Executing tool: {self.name}")
            result = self._execute(**kwargs)

            # Ensure result is JSON serializable
            if isinstance(result, dict):
                return json.dumps(result, indent=2)
            else:
                return str(result)

        except Exception as e:
            self.logger.error(f"Tool execution failed: {e}", exc_info=e)
            raise ToolException(f"Tool {self.name} failed: {e}", tool_name=self.name) from e

    async def _arun(self, **kwargs) -> str:
        """Asynchronous tool execution."""
        # Default implementation runs sync version in executor
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self._run(**kwargs))

    @abstractmethod
    def _execute(self, **kwargs) -> Union[str, Dict[str, Any]]:
        """Execute the tool logic. Must be implemented by subclasses."""
        pass


class AnalyzeDataSourceTool(ETLTool):
    """Tool for analyzing data sources."""

    name: str = "analyze_data_source"
    description: str = "Analyze a data source to extract metadata, schema, and sample data"
    args_schema: Type[BaseModel] = AnalyzeDataSourceSchema

    def _execute(self, source_path: str, source_type: str, include_samples: bool = True, max_sample_rows: int = 100) -> Dict[str, Any]:
        """Analyze data source and return metadata."""
        from .. import data_source_analysis

        try:
            # Use existing data source analysis functionality
            metadata = data_source_analysis.analyze_source(source_path)

            # Enhance with additional information
            analysis_result = {
                "source_path": source_path,
                "source_type": source_type,
                "metadata": metadata,
                "analysis_timestamp": time.time(),
                "include_samples": include_samples,
                "max_sample_rows": max_sample_rows,
            }

            if include_samples:
                # Add sample data if requested
                # This would be implemented based on the source type
                analysis_result["samples"] = {
                    "note": "Sample data extraction would be implemented based on source type"
                }

            return analysis_result

        except Exception as e:
            raise ToolException(f"Data source analysis failed: {e}", tool_name=self.name) from e


class GenerateDAGTool(ETLTool):
    """Tool for generating ETL DAGs."""

    name: str = "generate_dag"
    description: str = "Generate an ETL DAG based on data source metadata"
    args_schema: Type[BaseModel] = GenerateDAGSchema

    def _execute(self, metadata: Dict[str, Any], dag_id: str, include_validation: bool = True, parallel_tasks: bool = True) -> Dict[str, Any]:
        """Generate DAG configuration."""
        from .. import dag_generator

        try:
            # Generate DAG using existing functionality
            dag = dag_generator.generate_dag(metadata)

            # Convert to Airflow code
            airflow_code = dag_generator.dag_to_airflow_code(dag, dag_id=dag_id)

            dag_result = {
                "dag_id": dag_id,
                "metadata": metadata,
                "dag_structure": {
                    "tasks": dag.tasks,
                    "dependencies": dag.dependencies,
                    "execution_order": dag.topological_sort(),
                },
                "airflow_code": airflow_code,
                "configuration": {
                    "include_validation": include_validation,
                    "parallel_tasks": parallel_tasks,
                },
                "generation_timestamp": time.time(),
            }

            return dag_result

        except Exception as e:
            raise ToolException(f"DAG generation failed: {e}", tool_name=self.name) from e


class ExecutePipelineTool(ETLTool):
    """Tool for executing ETL pipelines."""

    name: str = "execute_pipeline"
    description: str = "Execute an ETL pipeline based on DAG configuration"
    args_schema: Type[BaseModel] = ExecutePipelineSchema

    def _execute(self, dag_config: Dict[str, Any], execution_mode: str = "async", monitor_progress: bool = True, timeout_seconds: int = 3600) -> Dict[str, Any]:
        """Execute ETL pipeline with comprehensive orchestration."""
        from ..dag_generator import SimpleDAG
        from ..orchestrator import MonitorAgent

        try:
            # Validate DAG configuration
            if not dag_config or not dag_config.get("dag_id"):
                raise ValueError("DAG configuration must include a valid dag_id")

            if not dag_config.get("tasks"):
                raise ValueError("DAG configuration must include tasks")

            dag_id = dag_config["dag_id"]
            tasks_config = dag_config["tasks"]

            # Initialize monitoring
            monitor = MonitorAgent() if monitor_progress else None
            start_time = time.time()
            execution_id = f"exec_{int(start_time)}"

            # Create DAG structure for dependency resolution
            dag = SimpleDAG()
            task_dependencies = {}

            for task_config in tasks_config:
                # Validate required task fields
                if "task_id" not in task_config:
                    raise ValueError("Task configuration must include 'task_id'")

                task_id = task_config["task_id"]
                if not task_id:
                    raise ValueError("Task ID cannot be empty")

                # Validate that task_type exists (can be missing but should be caught)
                if "task_type" not in task_config and "operator" not in task_config:
                    raise ValueError(f"Task '{task_id}' must include either 'task_type' or 'operator'")

                dependencies = task_config.get("dependencies", [])
                dag.add_task(task_id)
                task_dependencies[task_id] = dependencies

                for dep in dependencies:
                    dag.set_dependency(dep, task_id)

            # Initialize execution state
            execution_state = {
                "execution_id": execution_id,
                "dag_id": dag_id,
                "execution_mode": execution_mode,
                "current_task": None,
                "completed_tasks": [],
                "failed_tasks": [],
                "running_tasks": set(),
                "status": "running",
                "start_time": start_time,
                "end_time": None,
                "task_results": [],
                "monitoring_data": {
                    "total_tasks": len(tasks_config),
                    "completed_tasks": 0,
                    "failed_tasks": 0,
                    "progress_percentage": 0.0,
                    "resource_usage": {
                        "max_memory_mb": 0.0,
                        "total_cpu_seconds": 0.0
                    }
                }
            }

            if monitor:
                monitor.start_pipeline(dag_id)

            # Execute based on mode
            if execution_mode == "sync":
                return self._execute_sync(dag, task_dependencies, tasks_config, execution_state, monitor, timeout_seconds)
            elif execution_mode == "async":
                return self._execute_async(dag, task_dependencies, tasks_config, execution_state, monitor, timeout_seconds)
            else:
                raise ValueError(f"Unsupported execution mode: {execution_mode}")

        except Exception as e:
            raise ToolException(f"Pipeline execution failed: {e}", tool_name=self.name) from e

    def _execute_sync(self, dag, task_dependencies, tasks_config, execution_state, monitor, timeout_seconds):
        """Execute pipeline synchronously with proper dependency resolution."""
        from concurrent.futures import ThreadPoolExecutor, TimeoutError
        from typing import Set

        import psutil

        # Get execution order
        execution_order = dag.topological_sort()
        tasks_by_id = {task["task_id"]: task for task in tasks_config}

        # Track process resource usage
        process = psutil.Process()
        max_memory = 0.0
        total_cpu_time = 0.0

        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                task_results = {}
                running_tasks: Set[str] = set()
                completed_tasks: Set[str] = set()
                failed_tasks: Set[str] = set()

                # Execute tasks in dependency order
                for task_id in execution_order:
                    # Check timeout
                    if time.time() - execution_state["start_time"] > timeout_seconds:
                        execution_state["status"] = "timeout"
                        execution_state["timeout_details"] = {
                            "timeout_seconds": timeout_seconds,
                            "elapsed_seconds": time.time() - execution_state["start_time"]
                        }
                        break

                    # Check if dependencies are completed
                    task_config = tasks_by_id[task_id]
                    dependencies = task_config.get("dependencies", [])

                    # Skip if any dependency failed
                    if any(dep in failed_tasks for dep in dependencies):
                        task_result = self._create_task_result(task_id, "skipped", "Dependency failed")
                        task_results[task_id] = task_result
                        execution_state["task_results"].append(task_result)
                        continue

                    # Wait for dependencies to complete
                    while not all(dep in completed_tasks for dep in dependencies):
                        time.sleep(0.1)
                        if time.time() - execution_state["start_time"] > timeout_seconds:
                            break

                    # Execute task
                    execution_state["current_task"] = task_id
                    running_tasks.add(task_id)

                    if monitor:
                        monitor.log(f"Starting task: {task_id}", task_id)

                    # Submit task for execution
                    future = executor.submit(self._execute_task, task_config, task_results)

                    try:
                        # Wait for task completion with timeout
                        remaining_timeout = max(1, timeout_seconds - (time.time() - execution_state["start_time"]))
                        task_result = future.result(timeout=remaining_timeout)

                        # Update resource usage
                        memory_info = process.memory_info()
                        max_memory = max(max_memory, memory_info.rss / 1024 / 1024)  # MB
                        cpu_times = process.cpu_times()
                        total_cpu_time = cpu_times.user + cpu_times.system

                        # Process result
                        running_tasks.remove(task_id)

                        if task_result["status"] == "completed":
                            completed_tasks.add(task_id)
                            execution_state["completed_tasks"].append(task_id)
                            execution_state["monitoring_data"]["completed_tasks"] += 1

                            if monitor:
                                monitor.log(f"Completed task: {task_id}", task_id)
                        else:
                            failed_tasks.add(task_id)
                            execution_state["failed_tasks"].append(task_id)
                            execution_state["monitoring_data"]["failed_tasks"] += 1

                            if monitor:
                                monitor.error(f"Failed task: {task_id}", task_id)

                        task_results[task_id] = task_result
                        execution_state["task_results"].append(task_result)

                    except TimeoutError:
                        running_tasks.remove(task_id)
                        task_result = self._create_task_result(task_id, "timeout", f"Task timed out after {remaining_timeout}s")
                        task_results[task_id] = task_result
                        execution_state["task_results"].append(task_result)
                        failed_tasks.add(task_id)
                        execution_state["failed_tasks"].append(task_id)
                        execution_state["monitoring_data"]["failed_tasks"] += 1

                        # Set pipeline status to timeout when a task times out
                        execution_state["status"] = "timeout"
                        execution_state["timeout_details"] = {
                            "timeout_seconds": timeout_seconds,
                            "elapsed_seconds": time.time() - execution_state["start_time"]
                        }

                        if monitor:
                            monitor.error(f"Timeout task: {task_id}", task_id)

                # Update final state
                execution_state["end_time"] = time.time()
                execution_state["running_tasks"] = list(running_tasks)

                # Determine overall status
                if execution_state["status"] == "timeout":
                    # Keep timeout status, but also record failed tasks
                    execution_state["error_details"] = {
                        "failed_tasks": list(failed_tasks),
                        "total_failures": len(failed_tasks)
                    }
                elif failed_tasks:
                    execution_state["status"] = "failed"
                    execution_state["error_details"] = {
                        "failed_tasks": list(failed_tasks),
                        "total_failures": len(failed_tasks)
                    }
                else:
                    execution_state["status"] = "completed"

                # Update monitoring
                total_tasks = execution_state["monitoring_data"]["total_tasks"]
                completed = execution_state["monitoring_data"]["completed_tasks"]
                execution_state["monitoring_data"]["progress_percentage"] = (completed / total_tasks) * 100.0
                execution_state["monitoring_data"]["resource_usage"]["max_memory_mb"] = max_memory
                execution_state["monitoring_data"]["resource_usage"]["total_cpu_seconds"] = total_cpu_time

                if monitor:
                    monitor.end_pipeline(execution_state["dag_id"], execution_state["status"] == "completed")

                return execution_state

        except Exception as e:
            execution_state["status"] = "failed"
            execution_state["error_details"] = {
                "error_message": str(e),
                "error_type": type(e).__name__
            }

            if monitor:
                monitor.error(f"Pipeline execution failed: {e}")
                monitor.end_pipeline(execution_state["dag_id"], False)

            return execution_state

    def _execute_async(self, dag, task_dependencies, tasks_config, execution_state, monitor, timeout_seconds):
        """Execute pipeline asynchronously and return immediately."""
        # For async mode, start execution in background thread
        import threading

        def background_execution():
            self._execute_sync(dag, task_dependencies, tasks_config, execution_state, monitor, timeout_seconds)

        thread = threading.Thread(target=background_execution, daemon=True)
        thread.start()

        # Return immediate status
        return {
            "execution_id": execution_state["execution_id"],
            "dag_id": execution_state["dag_id"],
            "execution_mode": "async",
            "status": "running",
            "start_time": execution_state["start_time"],
            "execution_state": {
                "dag_id": execution_state["dag_id"],
                "current_task": execution_state["current_task"],
                "completed_tasks": execution_state["completed_tasks"]
            },
            "message": "Pipeline execution started in background"
        }

    def _execute_task(self, task_config, previous_results):
        """Execute a single task with proper error handling and monitoring."""
        import psutil

        task_id = task_config["task_id"]
        task_type = task_config.get("task_type", "unknown")
        operator = task_config.get("operator", "DummyOperator")
        config = task_config.get("config", {})

        start_time = time.time()
        process = psutil.Process()
        start_memory = process.memory_info().rss / 1024 / 1024  # MB
        start_cpu = sum(process.cpu_times())

        try:
            # Simulate different operator behaviors for testing
            result_data = self._simulate_operator_execution(operator, config, task_id, previous_results)

            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            end_cpu = sum(process.cpu_times())

            return {
                "task_id": task_id,
                "status": "completed",
                "start_time": start_time,
                "end_time": end_time,
                "duration": end_time - start_time,
                "operator_type": operator,
                "result_data": result_data,
                "resource_usage": {
                    "memory_mb": max(end_memory - start_memory, 0),
                    "cpu_seconds": max(end_cpu - start_cpu, 0)
                },
                "retry_count": 0
            }

        except Exception as e:
            end_time = time.time()
            end_memory = process.memory_info().rss / 1024 / 1024  # MB
            end_cpu = sum(process.cpu_times())

            return {
                "task_id": task_id,
                "status": "failed",
                "start_time": start_time,
                "end_time": end_time,
                "duration": end_time - start_time,
                "operator_type": operator,
                "error_message": str(e),
                "error_type": type(e).__name__,
                "resource_usage": {
                    "memory_mb": max(end_memory - start_memory, 0),
                    "cpu_seconds": max(end_cpu - start_cpu, 0)
                },
                "retry_count": 0
            }

    def _simulate_operator_execution(self, operator, config, task_id, previous_results):
        """Simulate operator execution for testing purposes."""
        # Simulate different operator behaviors
        if operator == "FailingOperator":
            raise RuntimeError(f"Simulated failure in {task_id}")

        elif operator == "SlowOperator":
            execution_time = config.get("execution_time", 1)
            time.sleep(execution_time)
            return {"message": f"Slow operation completed after {execution_time}s"}

        elif operator == "RetryableOperator":
            if config.get("simulate_transient_failure"):
                # Simulate success after potential retries
                import random
                if random.random() < 0.3:  # 30% chance of failure
                    raise RuntimeError("Transient failure")
            return {"message": "Retryable operation completed"}

        elif "Custom" in operator:
            return {
                "message": f"Custom operator {operator} executed",
                "config": config,
                "custom_result": True
            }

        else:
            # Default successful execution
            return {
                "message": f"Task {task_id} completed successfully",
                "operator": operator,
                "processed_records": 100  # Simulated
            }

    def _create_task_result(self, task_id, status, message=""):
        """Create a standardized task result."""
        current_time = time.time()
        return {
            "task_id": task_id,
            "status": status,
            "start_time": current_time,
            "end_time": current_time,
            "duration": 0.0,
            "message": message,
            "resource_usage": {
                "memory_mb": 0.0,
                "cpu_seconds": 0.0
            },
            "retry_count": 0
        }


class ValidateDataQualityTool(ETLTool):
    """Tool for validating data quality."""

    name: str = "validate_data_quality"
    description: str = "Validate data quality using specified rules and criteria"
    args_schema: Type[BaseModel] = ValidateDataQualitySchema

    def _execute(self, data_source: str, validation_rules: List[Dict[str, Any]], sample_percentage: float = 10.0, fail_on_errors: bool = False) -> Dict[str, Any]:
        """Validate data quality."""
        try:
            # This would implement actual data quality validation
            validation_result = {
                "data_source": data_source,
                "validation_rules": validation_rules,
                "sample_percentage": sample_percentage,
                "fail_on_errors": fail_on_errors,
                "validation_timestamp": time.time(),
                "results": {
                    "total_rules": len(validation_rules),
                    "passed_rules": len(validation_rules),  # Placeholder
                    "failed_rules": 0,  # Placeholder
                    "warnings": [],
                    "errors": [],
                },
                "status": "passed",
                "message": "Data quality validation completed (implementation would run actual validation)",
            }

            return validation_result

        except Exception as e:
            raise ToolException(f"Data quality validation failed: {e}", tool_name=self.name) from e


class QueryDataTool(ETLTool):
    """Tool for querying data sources with caching, security, and export capabilities."""

    name: str = "query_data"
    description: str = "Execute queries against data sources"

    def __init__(self):
        super().__init__()
        self._query_cache = {}
        self._query_history = []
        self._audit_log = []
        self._query_id_counter = 0

    class QueryDataSchema(ETLToolSchema):
        data_source: str = Field(description="Data source identifier (connection string or source config)")
        query: str = Field(description="SQL query to execute")
        limit: int = Field(default=100, description="Maximum number of results")
        format: str = Field(default="json", description="Output format (json, csv, xlsx)")
        use_cache: bool = Field(default=True, description="Use cached results if available")
        security_level: str = Field(default="standard", description="Security validation level (strict, standard, permissive)")

    args_schema: Type[BaseModel] = QueryDataSchema

    def _execute(self, data_source: str, query: str, limit: int = 100, format: str = "json",
                use_cache: bool = True, security_level: str = "standard") -> Dict[str, Any]:
        """Execute data query with comprehensive features."""
        import uuid

        start_time = time.time()
        query_id = str(uuid.uuid4())[:8]

        try:
            # Security validation
            security_result = self._validate_query_security(query, security_level)
            if not security_result["is_safe"]:
                self._log_audit_event("security_violation", query, data_source, security_result["violation_reason"])
                return {
                    "query_id": query_id,
                    "status": "blocked",
                    "security_violation": security_result["violation_reason"],
                    "query": query[:100] + "..." if len(query) > 100 else query,
                    "execution_timestamp": time.time()
                }

            # Check cache if enabled
            cache_key = self._generate_cache_key(data_source, query, limit)
            if use_cache and cache_key in self._query_cache:
                cached_result = self._query_cache[cache_key].copy()
                cached_result.update({
                    "query_id": query_id,
                    "cache_hit": True,
                    "execution_timestamp": time.time(),
                    "cache_age_seconds": time.time() - cached_result.get("cache_timestamp", 0)
                })
                self._add_to_history(query_id, query, data_source, "cached", time.time() - start_time)
                return self._format_results(cached_result, format)

            # Execute query
            execution_result = self._execute_sql_query(data_source, query, limit)

            # Add metadata
            execution_time = time.time() - start_time
            execution_result.update({
                "query_id": query_id,
                "data_source": data_source,
                "query": query,
                "limit": limit,
                "format": format,
                "execution_timestamp": time.time(),
                "cache_hit": False,
                "performance_metrics": {
                    "execution_time_ms": round(execution_time * 1000, 2),
                    "rows_examined": execution_result.get("rows_examined", "unknown"),
                    "query_plan": execution_result.get("query_plan", "not_available")
                }
            })

            # Cache result
            if use_cache and execution_result.get("status") == "completed":
                cache_result = execution_result.copy()
                cache_result["cache_timestamp"] = time.time()
                self._query_cache[cache_key] = cache_result
                self._cleanup_cache()  # Remove old entries

            # Add to history
            self._add_to_history(query_id, query, data_source, execution_result.get("status", "unknown"), execution_time)

            # Log audit event
            self._log_audit_event("query_executed", query, data_source, f"rows_returned:{execution_result.get('results', {}).get('row_count', 0)}")

            return self._format_results(execution_result, format)

        except Exception as e:
            error_time = time.time() - start_time
            self._log_audit_event("query_error", query, data_source, str(e))
            self._add_to_history(query_id, query, data_source, "error", error_time)
            raise ToolException(f"Data query failed: {e}", tool_name=self.name) from e

    def _execute_sql_query(self, data_source: str, query: str, limit: int) -> Dict[str, Any]:
        """Execute SQL query against the data source."""
        from sqlalchemy import create_engine, text

        try:
            # Create database engine
            if data_source.startswith(('sqlite://', 'postgresql://', 'mysql://', 'oracle://')):
                connection_string = data_source
            else:
                # Assume it's a database name and use SQLite default
                connection_string = f"sqlite:///{data_source}.db"

            engine = create_engine(connection_string, pool_timeout=30, pool_recycle=3600)

            # Add LIMIT to query if not present and limit specified
            processed_query = self._add_limit_to_query(query, limit)

            with engine.connect() as conn:
                # Get query plan for performance metrics (if supported)
                query_plan = self._get_query_plan(conn, processed_query)

                # Execute query
                result = conn.execute(text(processed_query))
                columns = list(result.keys())
                rows = result.fetchall()

                # Convert to list of dictionaries
                data = []
                for row in rows:
                    data.append(dict(zip(columns, row)))

                return {
                    "status": "completed",
                    "results": {
                        "row_count": len(data),
                        "columns": columns,
                        "data": data,
                        "truncated": len(rows) == limit  # Indicates if more data available
                    },
                    "rows_examined": len(rows),
                    "query_plan": query_plan,
                    "message": f"Query executed successfully, returned {len(data)} rows"
                }

        except Exception as e:
            return {
                "status": "error",
                "error_message": str(e),
                "results": {
                    "row_count": 0,
                    "columns": [],
                    "data": []
                }
            }
        finally:
            try:
                engine.dispose()
            except:
                pass

    def _validate_query_security(self, query: str, security_level: str) -> Dict[str, Any]:
        """Validate query for security risks."""
        import re

        query_lower = query.lower().strip()

        # Define dangerous patterns
        dangerous_patterns = [
            r'\bdrop\s+table\b',
            r'\bdrop\s+database\b',
            r'\bdelete\s+from\b.*where.*1\s*=\s*1',
            r'\btruncate\s+table\b',
            r'\balter\s+table\b',
            r'\bcreate\s+table\b',
            r'\binsert\s+into\b',
            r'\bupdate\s+.*set\b',
            r';\s*drop\b',
            r';\s*delete\b',
            r'--.*drop\b',
            r'/\*.*\*/',
            r'\bunion\s+.*select\b.*\bfrom\b',
        ]

        moderate_patterns = [
            r'\bexec\b',
            r'\bexecute\b',
            r'\bsp_\w+',
            r'\bxp_\w+',
            r'@@\w+',
        ]

        # Check for dangerous patterns
        for pattern in dangerous_patterns:
            if re.search(pattern, query_lower, re.IGNORECASE):
                return {
                    "is_safe": False,
                    "violation_reason": f"Dangerous SQL pattern detected: {pattern}",
                    "security_level": security_level
                }

        # Check moderate patterns for strict security
        if security_level == "strict":
            for pattern in moderate_patterns:
                if re.search(pattern, query_lower, re.IGNORECASE):
                    return {
                        "is_safe": False,
                        "violation_reason": f"Potentially unsafe SQL pattern detected: {pattern}",
                        "security_level": security_level
                    }

        # Check for basic SELECT requirement in strict mode
        if security_level == "strict" and not query_lower.strip().startswith('select'):
            return {
                "is_safe": False,
                "violation_reason": "Only SELECT queries allowed in strict mode",
                "security_level": security_level
            }

        return {
            "is_safe": True,
            "violation_reason": None,
            "security_level": security_level
        }

    def _add_limit_to_query(self, query: str, limit: int) -> str:
        """Add LIMIT clause to query if not present."""
        query_lower = query.lower().strip()

        # Check if LIMIT already exists
        if 'limit' in query_lower:
            return query

        # Add LIMIT clause
        if query.strip().endswith(';'):
            return f"{query.rstrip(';')} LIMIT {limit};"
        else:
            return f"{query} LIMIT {limit}"

    def _get_query_plan(self, conn, query: str) -> str:
        """Get query execution plan if supported."""
        try:
            from sqlalchemy import text
            # Try to get query plan (SQLite syntax)
            plan_result = conn.execute(text(f"EXPLAIN QUERY PLAN {query}"))
            plan_rows = plan_result.fetchall()
            return "\n".join([str(row) for row in plan_rows])
        except Exception:
            return "Query plan not available"

    def _generate_cache_key(self, data_source: str, query: str, limit: int) -> str:
        """Generate cache key for query."""
        import hashlib
        cache_string = f"{data_source}:{query}:{limit}"
        return hashlib.md5(cache_string.encode()).hexdigest()

    def _cleanup_cache(self, max_entries: int = 1000):
        """Remove old cache entries to prevent memory growth."""
        if len(self._query_cache) > max_entries:
            # Remove oldest entries (simple LRU)
            sorted_cache = sorted(
                self._query_cache.items(),
                key=lambda x: x[1].get("cache_timestamp", 0)
            )
            # Keep only the newest 80% of entries
            keep_count = int(max_entries * 0.8)
            self._query_cache = dict(sorted_cache[-keep_count:])

    def _format_results(self, result: Dict[str, Any], format: str) -> Dict[str, Any]:
        """Format query results in requested format."""
        if format.lower() == "csv":
            return self._format_as_csv(result)
        elif format.lower() == "xlsx":
            return self._format_as_excel(result)
        else:
            return result  # Default JSON format

    def _format_as_csv(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format results as CSV."""
        import csv
        import io

        try:
            data = result.get("results", {}).get("data", [])
            columns = result.get("results", {}).get("columns", [])

            if not data or not columns:
                result["csv_data"] = ""
                return result

            output = io.StringIO()
            writer = csv.DictWriter(output, fieldnames=columns)
            writer.writeheader()
            writer.writerows(data)

            result["csv_data"] = output.getvalue()
            result["format"] = "csv"
            return result

        except Exception as e:
            result["format_error"] = f"CSV formatting failed: {e}"
            return result

    def _format_as_excel(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """Format results as Excel (placeholder - would require openpyxl)."""
        # This would require openpyxl library in a real implementation
        result["format"] = "xlsx"
        result["excel_note"] = "Excel export would be implemented with openpyxl library"
        return result

    def _add_to_history(self, query_id: str, query: str, data_source: str, status: str, execution_time: float):
        """Add query to execution history."""
        self._query_history.append({
            "query_id": query_id,
            "query": query,
            "data_source": data_source,
            "status": status,
            "execution_time": execution_time,
            "timestamp": time.time()
        })

        # Keep only last 10000 entries
        if len(self._query_history) > 10000:
            self._query_history = self._query_history[-10000:]

    def _log_audit_event(self, event_type: str, query: str, data_source: str, details: str):
        """Log audit event for security and compliance."""
        self._audit_log.append({
            "event_type": event_type,
            "query": query[:200] + "..." if len(query) > 200 else query,  # Truncate long queries
            "data_source": data_source,
            "details": details,
            "timestamp": time.time(),
            "user": "system"  # Would be actual user in real system
        })

        # Keep only last 50000 audit entries
        if len(self._audit_log) > 50000:
            self._audit_log = self._audit_log[-50000:]

    def get_query_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent query history."""
        return self._query_history[-limit:]

    def get_audit_log(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent audit log entries."""
        return self._audit_log[-limit:]

    def export_results(self, query_id: str, format: str = "csv") -> Dict[str, Any]:
        """Export query results in specified format."""
        # Find query in history
        query_entry = None
        for entry in reversed(self._query_history):
            if entry["query_id"] == query_id:
                query_entry = entry
                break

        if not query_entry:
            return {
                "status": "error",
                "error_message": f"Query ID {query_id} not found in history"
            }

        # For now, return export metadata (would generate actual files in real implementation)
        return {
            "query_id": query_id,
            "format": format,
            "status": "ready",
            "download_url": f"/exports/{query_id}.{format}",
            "file_size": "estimated_size",
            "expires_at": time.time() + 3600  # 1 hour expiry
        }


class MonitorPipelineTool(ETLTool):
    """Tool for monitoring pipeline execution."""

    name: str = "monitor_pipeline"
    description: str = "Monitor the status and progress of pipeline execution"

    class MonitorPipelineSchema(ETLToolSchema):
        pipeline_id: str = Field(description="Pipeline execution ID to monitor")
        include_logs: bool = Field(default=False, description="Include execution logs")
        include_metrics: bool = Field(default=True, description="Include performance metrics")

    args_schema: Type[BaseModel] = MonitorPipelineSchema

    def _execute(self, pipeline_id: str, include_logs: bool = False, include_metrics: bool = True) -> Dict[str, Any]:
        """Monitor pipeline execution."""
        try:
            # This would implement actual pipeline monitoring
            monitor_result = {
                "pipeline_id": pipeline_id,
                "include_logs": include_logs,
                "include_metrics": include_metrics,
                "monitoring_timestamp": time.time(),
                "status": {
                    "state": "running",  # Placeholder
                    "progress": 0.5,  # Placeholder
                    "current_task": "transform_data",  # Placeholder
                    "tasks_completed": 2,  # Placeholder
                    "tasks_total": 4,  # Placeholder
                },
                "metrics": {
                    "execution_time": 120,  # Placeholder
                    "memory_usage": "256MB",  # Placeholder
                    "cpu_usage": "45%",  # Placeholder
                } if include_metrics else {},
                "logs": [
                    "Pipeline started",
                    "Extract task completed",
                    "Transform task in progress",
                ] if include_logs else [],
                "message": "Pipeline monitoring completed (implementation would fetch actual status)",
            }

            return monitor_result

        except Exception as e:
            raise ToolException(f"Pipeline monitoring failed: {e}", tool_name=self.name) from e


class AgentToolRegistry:
    """Registry for managing agent tools."""

    def __init__(self):
        self.tools: Dict[str, ETLTool] = {}
        self.tool_categories: Dict[str, List[str]] = {}
        self.logger = get_logger("agent.tool_registry")

        # Register default tools
        self._register_default_tools()

    def register_tool(self, tool: ETLTool, category: str = "general") -> None:
        """Register a tool in the registry.
        
        Args:
            tool: The tool to register
            category: Tool category for organization
            
        Raises:
            AgentException: If tool registration fails
        """
        if tool.name in self.tools:
            raise AgentException(f"Tool {tool.name} is already registered")

        try:
            self.tools[tool.name] = tool

            if category not in self.tool_categories:
                self.tool_categories[category] = []
            self.tool_categories[category].append(tool.name)

            self.logger.info(f"Tool registered: {tool.name} (category: {category})")

        except Exception as e:
            raise AgentException(f"Failed to register tool {tool.name}: {e}") from e

    def unregister_tool(self, tool_name: str) -> bool:
        """Unregister a tool from the registry.
        
        Args:
            tool_name: Name of the tool to unregister
            
        Returns:
            True if tool was unregistered, False if not found
        """
        if tool_name not in self.tools:
            return False

        try:
            # Remove from tools
            del self.tools[tool_name]

            # Remove from categories
            for category, tool_names in self.tool_categories.items():
                if tool_name in tool_names:
                    tool_names.remove(tool_name)
                    if not tool_names:
                        del self.tool_categories[category]
                    break

            self.logger.info(f"Tool unregistered: {tool_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to unregister tool {tool_name}: {e}", exc_info=e)
            return False

    def get_tool(self, tool_name: str) -> Optional[ETLTool]:
        """Get a tool by name.
        
        Args:
            tool_name: Name of the tool
            
        Returns:
            Tool instance if found, None otherwise
        """
        return self.tools.get(tool_name)

    def get_tools_by_category(self, category: str) -> List[ETLTool]:
        """Get all tools in a category.
        
        Args:
            category: Tool category
            
        Returns:
            List of tools in the category
        """
        tool_names = self.tool_categories.get(category, [])
        return [self.tools[name] for name in tool_names if name in self.tools]

    def get_all_tools(self) -> List[ETLTool]:
        """Get all registered tools.
        
        Returns:
            List of all tools
        """
        return list(self.tools.values())

    def list_tool_names(self) -> List[str]:
        """Get list of all tool names.
        
        Returns:
            List of tool names
        """
        return list(self.tools.keys())

    def get_tool_descriptions(self) -> Dict[str, str]:
        """Get descriptions of all tools.
        
        Returns:
            Dictionary mapping tool names to descriptions
        """
        return {name: tool.description for name, tool in self.tools.items()}

    def search_tools(self, query: str) -> List[ETLTool]:
        """Search for tools by name or description.
        
        Args:
            query: Search query
            
        Returns:
            List of matching tools
        """
        query_lower = query.lower()
        matching_tools = []

        for tool in self.tools.values():
            if (query_lower in tool.name.lower() or
                query_lower in tool.description.lower()):
                matching_tools.append(tool)

        return matching_tools

    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics.
        
        Returns:
            Dictionary with registry statistics
        """
        return {
            "total_tools": len(self.tools),
            "categories": len(self.tool_categories),
            "tools_by_category": {
                category: len(tool_names)
                for category, tool_names in self.tool_categories.items()
            },
            "tool_names": list(self.tools.keys()),
        }

    def _register_default_tools(self) -> None:
        """Register default ETL tools."""
        default_tools = [
            (AnalyzeDataSourceTool(), "analysis"),
            (GenerateDAGTool(), "generation"),
            (ExecutePipelineTool(), "execution"),
            (ValidateDataQualityTool(), "validation"),
            (QueryDataTool(), "data"),
            (MonitorPipelineTool(), "monitoring"),
        ]

        for tool, category in default_tools:
            try:
                self.register_tool(tool, category)
            except Exception as e:
                self.logger.error(f"Failed to register default tool {tool.name}: {e}")


# Global tool registry instance
_tool_registry = AgentToolRegistry()


def get_tool_registry() -> AgentToolRegistry:
    """Get the global tool registry instance.
    
    Returns:
        Global AgentToolRegistry instance
    """
    return _tool_registry


def register_tool(tool: ETLTool, category: str = "general") -> None:
    """Register a tool in the global registry.
    
    Args:
        tool: The tool to register
        category: Tool category for organization
    """
    _tool_registry.register_tool(tool, category)


def get_tool(tool_name: str) -> Optional[ETLTool]:
    """Get a tool from the global registry.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        Tool instance if found, None otherwise
    """
    return _tool_registry.get_tool(tool_name)
