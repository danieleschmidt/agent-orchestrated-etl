"""Enhanced orchestrator that builds and executes ETL pipelines with resilience features."""

from __future__ import annotations

import logging
import time
from typing import Callable, Dict, Any, List, Optional
from pathlib import Path

from . import data_source_analysis, dag_generator
from .core import primary_data_extraction, transform_data
from .exceptions import (
    PipelineExecutionException,
    PipelineTimeoutException,
    DataProcessingException,
    AgentETLException,
)
from .retry import retry, RetryConfigs
from .circuit_breaker import circuit_breaker, CircuitBreakerConfigs
from .graceful_degradation import with_graceful_degradation, DegradationConfigs
from .logging_config import get_logger, TimedOperation, LogContext


# Placeholder load function for demonstration purposes.
def load_data(data: Any) -> bool:
    """Pretend to load data and return success."""
    return True


class MonitorAgent:
    """Enhanced monitor agent with structured logging and metrics collection."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.events: List[str] = []
        self._path = Path(path) if path else None
        self.logger = get_logger("agent_etl.monitor")
        self.metrics: Dict[str, Any] = {
            "tasks_started": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_execution_time": 0.0,
            "start_time": None,
        }

    def _write(self, message: str) -> None:
        """Write message to file if path is configured."""
        if self._path:
            try:
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(f"{time.time()}: {message}\n")
            except Exception as e:
                self.logger.error(f"Failed to write to monitor file: {e}")

    def start_pipeline(self, pipeline_id: str) -> None:
        """Record pipeline start."""
        self.metrics["start_time"] = time.time()
        message = f"Pipeline {pipeline_id} started"
        self.events.append(message)
        self._write(message)
        self.logger.info(
            "Pipeline execution started",
            extra={
                "event_type": "pipeline_start",
                "pipeline_id": pipeline_id,
            }
        )

    def end_pipeline(self, pipeline_id: str, success: bool = True) -> None:
        """Record pipeline completion."""
        if self.metrics["start_time"]:
            duration = time.time() - self.metrics["start_time"]
            self.metrics["total_execution_time"] = duration
        
        status = "completed" if success else "failed"
        message = f"Pipeline {pipeline_id} {status}"
        self.events.append(message)
        self._write(message)
        
        self.logger.info(
            f"Pipeline execution {status}",
            extra={
                "event_type": "pipeline_end",
                "pipeline_id": pipeline_id,
                "success": success,
                "duration_seconds": self.metrics["total_execution_time"],
                "metrics": self.get_metrics(),
            }
        )

    def log(self, message: str, task_id: Optional[str] = None) -> None:
        """Log a general message."""
        self.events.append(message)
        self._write(message)
        
        if task_id and "starting" in message.lower():
            self.metrics["tasks_started"] += 1
        elif task_id and "completed" in message.lower():
            self.metrics["tasks_completed"] += 1
        
        self.logger.info(
            message,
            extra={
                "event_type": "task_log",
                "task_id": task_id,
            }
        )

    def error(self, message: str, task_id: Optional[str] = None, exception: Optional[Exception] = None) -> None:
        """Log an error message."""
        entry = f"ERROR: {message}"
        self.events.append(entry)
        self._write(entry)
        
        if task_id:
            self.metrics["tasks_failed"] += 1
        
        extra = {
            "event_type": "task_error",
            "task_id": task_id,
        }
        
        if exception:
            extra["exception_type"] = type(exception).__name__
            extra["exception_message"] = str(exception)
        
        self.logger.error(message, extra=extra, exc_info=exception)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return self.metrics.copy()


class Pipeline:
    """Enhanced executable pipeline with resilience features."""

    def __init__(
        self,
        dag: dag_generator.SimpleDAG,
        operations: Dict[str, Callable[..., Any]],
        *,
        dag_id: str = "generated",
        timeout: Optional[float] = None,
        enable_retries: bool = True,
        enable_circuit_breaker: bool = True,
        enable_graceful_degradation: bool = True,
    ) -> None:
        self.dag = dag
        self.operations = operations
        self.dag_id = dag_id
        self.timeout = timeout
        self.enable_retries = enable_retries
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_graceful_degradation = enable_graceful_degradation
        self.logger = get_logger(f"agent_etl.pipeline.{dag_id}")

    def execute(self, monitor: Optional[MonitorAgent] = None) -> Dict[str, Any]:
        """Run tasks in topological order with enhanced error handling."""
        results: Dict[str, Any] = {}
        start_time = time.time()
        
        if monitor:
            monitor.start_pipeline(self.dag_id)
        
        try:
            with LogContext(pipeline_id=self.dag_id):
                with TimedOperation(f"pipeline_execution_{self.dag_id}", self.logger):
                    for task_id in self.dag.topological_sort():
                        # Check timeout
                        if self.timeout and (time.time() - start_time) > self.timeout:
                            raise PipelineTimeoutException(
                                f"Pipeline {self.dag_id} timed out",
                                timeout_seconds=self.timeout,
                                pipeline_id=self.dag_id,
                            )
                        
                        results[task_id] = self._execute_task(task_id, results, monitor)
                        
            if monitor:
                monitor.end_pipeline(self.dag_id, success=True)
                
            return results
            
        except Exception as exc:
            if monitor:
                monitor.end_pipeline(self.dag_id, success=False)
            
            # Wrap non-ETL exceptions
            if not isinstance(exc, AgentETLException):
                exc = PipelineExecutionException(
                    f"Pipeline {self.dag_id} execution failed: {exc}",
                    pipeline_id=self.dag_id,
                    cause=exc,
                )
            
            raise exc

    def _execute_task(
        self,
        task_id: str,
        results: Dict[str, Any],
        monitor: Optional[MonitorAgent],
    ) -> Any:
        """Execute a single task with resilience features."""
        if monitor:
            monitor.log(f"starting {task_id}", task_id)
        
        func = self.operations.get(task_id)
        if func is None:
            self.logger.warning(f"No operation defined for task {task_id}")
            return None
        
        # Apply resilience decorators based on configuration
        resilient_func = self._make_resilient(func, task_id)
        
        try:
            with LogContext(task_id=task_id):
                with TimedOperation(f"task_execution_{task_id}", self.logger):
                    # Determine task arguments based on dependencies
                    if task_id.startswith("transform"):
                        src_task = task_id.replace("transform", "extract")
                        src_data = results.get(src_task)
                        result = resilient_func(src_data)
                    elif task_id.startswith("load"):
                        src_task = task_id.replace("load", "transform")
                        src_data = results.get(src_task)
                        result = resilient_func(src_data)
                    else:
                        result = resilient_func()
                    
                    if monitor:
                        monitor.log(f"completed {task_id}", task_id)
                    
                    return result
                    
        except Exception as exc:
            if monitor:
                monitor.error(f"{task_id} failed: {exc}", task_id, exc)
            
            # Wrap in task-specific exception if needed
            if not isinstance(exc, AgentETLException):
                exc = PipelineExecutionException(
                    f"Task {task_id} failed: {exc}",
                    task_id=task_id,
                    pipeline_id=self.dag_id,
                    cause=exc,
                )
            
            raise exc

    def _make_resilient(self, func: Callable, task_id: str) -> Callable:
        """Apply resilience patterns to a function."""
        resilient_func = func
        
        # Apply graceful degradation
        if self.enable_graceful_degradation:
            resilient_func = with_graceful_degradation(
                f"pipeline_{self.dag_id}_task_{task_id}",
                DegradationConfigs.DATA_PROCESSING,
            )(resilient_func)
        
        # Apply circuit breaker
        if self.enable_circuit_breaker:
            resilient_func = circuit_breaker(
                f"pipeline_{self.dag_id}_task_{task_id}",
                CircuitBreakerConfigs.STANDARD,
            )(resilient_func)
        
        # Apply retry logic
        if self.enable_retries:
            resilient_func = retry(RetryConfigs.STANDARD)(resilient_func)
        
        return resilient_func

    def get_status(self) -> Dict[str, Any]:
        """Get pipeline status information."""
        return {
            "dag_id": self.dag_id,
            "total_tasks": len(self.dag.tasks),
            "task_order": self.dag.topological_sort(),
            "timeout": self.timeout,
            "resilience_features": {
                "retries_enabled": self.enable_retries,
                "circuit_breaker_enabled": self.enable_circuit_breaker,
                "graceful_degradation_enabled": self.enable_graceful_degradation,
            },
        }


class DataOrchestrator:
    """Enhanced high-level interface to build and run ETL pipelines with resilience."""

    def __init__(self):
        """Initialize the data orchestrator."""
        self.logger = get_logger("agent_etl.orchestrator")

    @retry(RetryConfigs.STANDARD)
    @circuit_breaker("data_source_analysis", CircuitBreakerConfigs.EXTERNAL_API)
    def create_pipeline(
        self,
        source: str,
        *,
        dag_id: str = "generated",
        operations: Optional[Dict[str, Callable[..., Any]]] = None,
        timeout: Optional[float] = None,
        enable_retries: bool = True,
        enable_circuit_breaker: bool = True,
        enable_graceful_degradation: bool = True,
    ) -> Pipeline:
        """Create a resilient ETL pipeline from a data source.
        
        Args:
            source: Data source identifier
            dag_id: Unique identifier for the DAG
            operations: Custom operations to override defaults
            timeout: Pipeline execution timeout in seconds
            enable_retries: Whether to enable retry mechanisms
            enable_circuit_breaker: Whether to enable circuit breaker pattern
            enable_graceful_degradation: Whether to enable graceful degradation
            
        Returns:
            Configured Pipeline instance
            
        Raises:
            DataSourceException: If data source analysis fails
            ValidationError: If input validation fails
        """
        with LogContext(dag_id=dag_id, source=source):
            self.logger.info(
                f"Creating pipeline for source: {source}",
                extra={
                    "event_type": "pipeline_creation_start",
                    "source": source,
                    "dag_id": dag_id,
                }
            )
            
            try:
                # Analyze data source with resilience
                metadata = data_source_analysis.analyze_source(source)
                
                # Generate DAG
                dag = dag_generator.generate_dag(metadata)
                
                # Set up default operations with resilience
                ops: Dict[str, Callable[..., Any]] = {
                    "extract": self._make_resilient_extract(),
                    "transform": self._make_resilient_transform(),
                    "load": self._make_resilient_load(),
                }
                
                # Add per-table operations
                for table in metadata["tables"]:
                    ops[f"extract_{table}"] = self._make_resilient_extract()
                    ops[f"transform_{table}"] = self._make_resilient_transform()
                    ops[f"load_{table}"] = self._make_resilient_load()
                
                # Override with custom operations
                if operations:
                    ops.update(operations)
                
                # Create pipeline with resilience features
                pipeline = Pipeline(
                    dag=dag,
                    operations=ops,
                    dag_id=dag_id,
                    timeout=timeout,
                    enable_retries=enable_retries,
                    enable_circuit_breaker=enable_circuit_breaker,
                    enable_graceful_degradation=enable_graceful_degradation,
                )
                
                self.logger.info(
                    f"Pipeline created successfully: {dag_id}",
                    extra={
                        "event_type": "pipeline_creation_success",
                        "dag_id": dag_id,
                        "source": source,
                        "total_tasks": len(dag.tasks),
                    }
                )
                
                return pipeline
                
            except Exception as exc:
                self.logger.error(
                    f"Failed to create pipeline: {exc}",
                    extra={
                        "event_type": "pipeline_creation_error",
                        "dag_id": dag_id,
                        "source": source,
                    },
                    exc_info=exc,
                )
                raise

    def _make_resilient_extract(self) -> Callable:
        """Create a resilient extract function."""
        @with_graceful_degradation("data_extraction", DegradationConfigs.DATA_PROCESSING)
        @circuit_breaker("data_extraction", CircuitBreakerConfigs.DATABASE)
        @retry(RetryConfigs.DATABASE)
        def resilient_extract(*args, **kwargs):
            return primary_data_extraction(*args, **kwargs)
        
        return resilient_extract

    def _make_resilient_transform(self) -> Callable:
        """Create a resilient transform function."""
        @with_graceful_degradation("data_transformation", DegradationConfigs.DATA_PROCESSING)
        @retry(RetryConfigs.STANDARD)
        def resilient_transform(*args, **kwargs):
            return transform_data(*args, **kwargs)
        
        return resilient_transform

    def _make_resilient_load(self) -> Callable:
        """Create a resilient load function."""
        @with_graceful_degradation("data_loading", DegradationConfigs.DATABASE)
        @circuit_breaker("data_loading", CircuitBreakerConfigs.DATABASE)
        @retry(RetryConfigs.DATABASE)
        def resilient_load(*args, **kwargs):
            return load_data(*args, **kwargs)
        
        return resilient_load

    def dag_to_airflow(self, pipeline: Pipeline, dag_id: Optional[str] = None) -> str:
        """Convert pipeline to Airflow DAG code.
        
        Args:
            pipeline: Pipeline to convert
            dag_id: Optional DAG ID override
            
        Returns:
            Airflow DAG Python code
        """
        try:
            return dag_generator.dag_to_airflow_code(
                pipeline.dag, dag_id=dag_id or pipeline.dag_id
            )
        except Exception as exc:
            self.logger.error(f"Failed to generate Airflow code: {exc}", exc_info=exc)
            raise DataProcessingException(
                f"Failed to generate Airflow code: {exc}",
                transformation_step="dag_to_airflow",
                cause=exc,
            )

    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get orchestrator status and metrics.
        
        Returns:
            Status information dictionary
        """
        from .circuit_breaker import circuit_breaker_registry
        from .graceful_degradation import degradation_manager
        
        return {
            "circuit_breakers": circuit_breaker_registry.get_all_stats(),
            "degradation_services": degradation_manager.get_all_service_status(),
            "logger_name": self.logger.name,
        }
