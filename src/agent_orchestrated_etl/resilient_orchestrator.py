"""Resilient Orchestrator with Advanced Error Handling and Recovery."""

import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .exceptions import DataProcessingException
from .intelligent_monitoring import IntelligentMonitor, MonitoredExecution
from .intelligent_pipeline_detector import (
    IntelligentPipelineDetector,
    PipelineRecommendation,
)
from .logging_config import get_logger


class ExecutionState(str, Enum):
    """Pipeline execution state."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRY = "retry"
    PAUSED = "paused"


class RecoveryStrategy(str, Enum):
    """Recovery strategy options."""
    RETRY = "retry"
    SKIP = "skip"
    FAIL = "fail"
    PARTIAL_RECOVERY = "partial_recovery"
    DEGRADED_MODE = "degraded_mode"
    MANUAL_INTERVENTION = "manual_intervention"


@dataclass
class ExecutionResult:
    """Result of pipeline execution."""
    pipeline_id: str
    state: ExecutionState
    start_time: float
    end_time: float
    records_processed: int = 0
    errors_count: int = 0
    warnings_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)
    error_details: List[str] = field(default_factory=list)

    @property
    def execution_time_seconds(self) -> float:
        """Calculate execution time in seconds."""
        return self.end_time - self.start_time

    @property
    def success_rate(self) -> float:
        """Calculate success rate."""
        total_operations = self.records_processed + self.errors_count
        if total_operations == 0:
            return 1.0
        return (self.records_processed / total_operations) * 100


@dataclass
class RetryConfig:
    """Retry configuration."""
    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    jitter: bool = True
    retriable_exceptions: List[type] = field(default_factory=lambda: [
        ConnectionError,
        TimeoutError,
        DataProcessingException
    ])


@dataclass
class CircuitBreakerConfig:
    """Circuit breaker configuration."""
    failure_threshold: int = 5
    recovery_timeout_seconds: float = 60.0
    half_open_max_calls: int = 3


class CircuitBreakerState(str, Enum):
    """Circuit breaker state."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Preventing calls
    HALF_OPEN = "half_open"  # Testing if service recovered


class CircuitBreaker:
    """Circuit breaker implementation for resilient service calls."""

    def __init__(self, name: str, config: CircuitBreakerConfig):
        self.name = name
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self.logger = get_logger(f"circuit_breaker_{name}")
        self._lock = threading.Lock()

    def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
        
        Returns:
            Function result
        
        Raises:
            Exception: If circuit is open or function fails
        """
        with self._lock:
            if self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time > self.config.recovery_timeout_seconds:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    self.logger.info(f"Circuit breaker {self.name} moving to HALF_OPEN state")
                else:
                    raise Exception(f"Circuit breaker {self.name} is OPEN")

            elif self.state == CircuitBreakerState.HALF_OPEN:
                if self.half_open_calls >= self.config.half_open_max_calls:
                    raise Exception(f"Circuit breaker {self.name} HALF_OPEN call limit exceeded")

        try:
            result = func(*args, **kwargs)

            with self._lock:
                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.half_open_calls += 1
                    if self.half_open_calls >= self.config.half_open_max_calls:
                        self.state = CircuitBreakerState.CLOSED
                        self.failure_count = 0
                        self.logger.info(f"Circuit breaker {self.name} recovered to CLOSED state")
                elif self.state == CircuitBreakerState.CLOSED:
                    self.failure_count = 0  # Reset on success

            return result

        except Exception as e:
            with self._lock:
                self.failure_count += 1
                self.last_failure_time = time.time()

                if self.state == CircuitBreakerState.HALF_OPEN:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.warning(f"Circuit breaker {self.name} failed during HALF_OPEN, moving to OPEN")
                elif self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.warning(f"Circuit breaker {self.name} opened due to {self.failure_count} failures")

            raise e


class ResilientOrchestrator:
    """Resilient orchestrator with advanced error handling and recovery capabilities."""

    def __init__(self, monitor: Optional[IntelligentMonitor] = None):
        """
        Initialize the resilient orchestrator.
        
        Args:
            monitor: Optional monitoring system
        """
        self.logger = get_logger("resilient_orchestrator")
        self.monitor = monitor or IntelligentMonitor()
        self.pipeline_detector = IntelligentPipelineDetector()

        # Execution tracking
        self.active_pipelines: Dict[str, ExecutionResult] = {}
        self.execution_history: List[ExecutionResult] = []
        self.max_history_size = 1000

        # Resilience configuration
        self.default_retry_config = RetryConfig()
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.recovery_strategies: Dict[str, RecoveryStrategy] = {}

        # Execution resources
        self.executor = ThreadPoolExecutor(
            max_workers=10,
            thread_name_prefix="resilient_pipeline"
        )

        # State management
        self._lock = threading.RLock()
        self._shutdown_event = threading.Event()

        # Start monitoring
        if not self.monitor._monitoring_active:
            self.monitor.start_monitoring()

        self.logger.info("Resilient orchestrator initialized")

    def shutdown(self) -> None:
        """Shutdown the orchestrator gracefully."""
        self.logger.info("Shutting down resilient orchestrator")

        self._shutdown_event.set()

        # Cancel active pipelines
        with self._lock:
            for pipeline_id in list(self.active_pipelines.keys()):
                self.cancel_pipeline(pipeline_id)

        # Shutdown executor
        self.executor.shutdown(wait=True)

        # Stop monitoring
        self.monitor.stop_monitoring()

        self.logger.info("Resilient orchestrator shutdown complete")

    def execute_pipeline_resilient(self, source_identifier: str,
                                 pipeline_config: Optional[Dict[str, Any]] = None,
                                 retry_config: Optional[RetryConfig] = None,
                                 recovery_strategy: RecoveryStrategy = RecoveryStrategy.RETRY) -> str:
        """
        Execute pipeline with resilience features.
        
        Args:
            source_identifier: Data source identifier
            pipeline_config: Optional pipeline configuration
            retry_config: Optional retry configuration
            recovery_strategy: Recovery strategy to use
        
        Returns:
            Pipeline execution ID
        """
        pipeline_id = f"pipeline_{int(time.time() * 1000)}"

        self.logger.info(f"Starting resilient pipeline execution: {pipeline_id}")

        # Initialize execution result
        execution_result = ExecutionResult(
            pipeline_id=pipeline_id,
            state=ExecutionState.PENDING,
            start_time=time.time(),
            end_time=0.0,
            metadata={
                "source_identifier": source_identifier,
                "recovery_strategy": recovery_strategy.value,
                "retry_config": retry_config.__dict__ if retry_config else None
            }
        )

        with self._lock:
            self.active_pipelines[pipeline_id] = execution_result
            self.recovery_strategies[pipeline_id] = recovery_strategy

        # Submit execution to thread pool
        future = self.executor.submit(
            self._execute_pipeline_with_retry,
            pipeline_id,
            source_identifier,
            pipeline_config,
            retry_config or self.default_retry_config
        )

        # Monitor execution asynchronously
        self.executor.submit(self._monitor_pipeline_execution, pipeline_id, future)

        return pipeline_id

    def get_pipeline_status(self, pipeline_id: str) -> Optional[ExecutionResult]:
        """
        Get pipeline execution status.
        
        Args:
            pipeline_id: Pipeline identifier
        
        Returns:
            Execution result or None if not found
        """
        with self._lock:
            if pipeline_id in self.active_pipelines:
                return self.active_pipelines[pipeline_id]

        # Check history
        for result in self.execution_history:
            if result.pipeline_id == pipeline_id:
                return result

        return None

    def cancel_pipeline(self, pipeline_id: str) -> bool:
        """
        Cancel pipeline execution.
        
        Args:
            pipeline_id: Pipeline identifier
        
        Returns:
            True if cancellation was initiated
        """
        with self._lock:
            if pipeline_id not in self.active_pipelines:
                return False

            execution_result = self.active_pipelines[pipeline_id]
            if execution_result.state in [ExecutionState.SUCCESS, ExecutionState.FAILED, ExecutionState.CANCELLED]:
                return False

            execution_result.state = ExecutionState.CANCELLED
            execution_result.end_time = time.time()

        self.logger.info(f"Pipeline {pipeline_id} cancellation requested")
        return True

    def get_active_pipelines(self) -> List[ExecutionResult]:
        """Get list of currently active pipelines."""
        with self._lock:
            return list(self.active_pipelines.values())

    def get_execution_statistics(self, time_window_hours: int = 24) -> Dict[str, Any]:
        """
        Get execution statistics for the specified time window.
        
        Args:
            time_window_hours: Time window in hours
        
        Returns:
            Execution statistics
        """
        cutoff_time = time.time() - (time_window_hours * 3600)

        # Get relevant executions
        relevant_executions = [
            result for result in self.execution_history
            if result.start_time >= cutoff_time
        ]

        if not relevant_executions:
            return {
                "total_executions": 0,
                "success_rate": 0.0,
                "average_execution_time": 0.0,
                "total_records_processed": 0,
                "total_errors": 0
            }

        # Calculate statistics
        total_executions = len(relevant_executions)
        successful_executions = len([r for r in relevant_executions if r.state == ExecutionState.SUCCESS])
        success_rate = (successful_executions / total_executions) * 100

        total_execution_time = sum(r.execution_time_seconds for r in relevant_executions)
        average_execution_time = total_execution_time / total_executions

        total_records = sum(r.records_processed for r in relevant_executions)
        total_errors = sum(r.errors_count for r in relevant_executions)

        # State distribution
        state_counts = {}
        for result in relevant_executions:
            state_counts[result.state.value] = state_counts.get(result.state.value, 0) + 1

        return {
            "time_window_hours": time_window_hours,
            "total_executions": total_executions,
            "success_rate": success_rate,
            "average_execution_time": average_execution_time,
            "total_records_processed": total_records,
            "total_errors": total_errors,
            "state_distribution": state_counts,
            "throughput_records_per_hour": total_records / time_window_hours if time_window_hours > 0 else 0
        }

    def _execute_pipeline_with_retry(self, pipeline_id: str, source_identifier: str,
                                   pipeline_config: Optional[Dict[str, Any]],
                                   retry_config: RetryConfig) -> ExecutionResult:
        """Execute pipeline with retry logic."""
        execution_result = self.active_pipelines[pipeline_id]
        execution_result.state = ExecutionState.RUNNING

        last_exception = None
        delay = retry_config.initial_delay_seconds

        for attempt in range(retry_config.max_retries + 1):
            if self._shutdown_event.is_set():
                execution_result.state = ExecutionState.CANCELLED
                break

            try:
                with MonitoredExecution(self.monitor, pipeline_id, "pipeline_execution") as monitor_ctx:

                    # Step 1: Detect optimal pipeline configuration
                    self.logger.info(f"Detecting pipeline configuration (attempt {attempt + 1})")
                    recommendation = self._execute_with_circuit_breaker(
                        "pipeline_detection",
                        self.pipeline_detector.generate_pipeline_recommendation,
                        source_identifier
                    )

                    # Step 2: Execute data extraction
                    self.logger.info(f"Executing data extraction (attempt {attempt + 1})")
                    extracted_data = self._execute_extraction_step(
                        source_identifier, recommendation, monitor_ctx
                    )

                    # Step 3: Execute data transformation
                    self.logger.info(f"Executing data transformation (attempt {attempt + 1})")
                    transformed_data = self._execute_transformation_step(
                        extracted_data, recommendation, monitor_ctx
                    )

                    # Step 4: Execute data loading
                    self.logger.info(f"Executing data loading (attempt {attempt + 1})")
                    self._execute_loading_step(
                        transformed_data, recommendation, monitor_ctx
                    )

                    # Success
                    execution_result.state = ExecutionState.SUCCESS
                    execution_result.records_processed = monitor_ctx.records_processed
                    execution_result.errors_count = monitor_ctx.errors_count
                    execution_result.end_time = time.time()

                    self.logger.info(f"Pipeline {pipeline_id} completed successfully")
                    return execution_result

            except Exception as e:
                last_exception = e
                execution_result.error_details.append(f"Attempt {attempt + 1}: {str(e)}")

                self.logger.warning(f"Pipeline {pipeline_id} attempt {attempt + 1} failed: {str(e)}")

                # Check if exception is retriable
                if not self._is_retriable_exception(e, retry_config):
                    self.logger.error(f"Non-retriable exception in pipeline {pipeline_id}: {str(e)}")
                    break

                # Check if we should retry
                if attempt < retry_config.max_retries:
                    # Apply recovery strategy
                    recovery_applied = self._apply_recovery_strategy(
                        pipeline_id, e, attempt + 1
                    )

                    if recovery_applied:
                        # Wait before retry with exponential backoff
                        self._wait_with_backoff(delay, retry_config)
                        delay = min(delay * retry_config.backoff_multiplier, retry_config.max_delay_seconds)
                    else:
                        self.logger.error(f"Recovery strategy failed for pipeline {pipeline_id}")
                        break
                else:
                    self.logger.error(f"Max retries exceeded for pipeline {pipeline_id}")

        # All retries exhausted or non-retriable error
        execution_result.state = ExecutionState.FAILED
        execution_result.end_time = time.time()
        execution_result.metadata["final_error"] = str(last_exception) if last_exception else "Unknown error"

        return execution_result

    def _execute_with_circuit_breaker(self, operation_name: str, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        if operation_name not in self.circuit_breakers:
            self.circuit_breakers[operation_name] = CircuitBreaker(
                operation_name,
                CircuitBreakerConfig()
            )

        circuit_breaker = self.circuit_breakers[operation_name]
        return circuit_breaker.call(func, *args, **kwargs)

    def _execute_extraction_step(self, source_identifier: str,
                               recommendation: PipelineRecommendation,
                               monitor_ctx: MonitoredExecution) -> List[Dict[str, Any]]:
        """Execute data extraction step."""
        from .core import primary_data_extraction

        # Configure extraction based on recommendation
        source_config = {
            "type": recommendation.transformation_strategy.value,
            "source_identifier": source_identifier
        }

        extracted_data = self._execute_with_circuit_breaker(
            "data_extraction",
            primary_data_extraction,
            source_config=source_config
        )

        if not isinstance(extracted_data, list):
            extracted_data = [extracted_data] if extracted_data else []

        monitor_ctx.add_processed_records(len(extracted_data))

        # Record extraction metrics
        self.monitor.record_metric(
            "extraction_records_count",
            len(extracted_data),
            {"pipeline_id": monitor_ctx.pipeline_id}
        )

        return extracted_data

    def _execute_transformation_step(self, data: List[Dict[str, Any]],
                                   recommendation: PipelineRecommendation,
                                   monitor_ctx: MonitoredExecution) -> List[Dict[str, Any]]:
        """Execute data transformation step."""
        from .core import transform_data

        # Configure transformations based on recommendation
        transformation_rules = self._generate_transformation_rules(recommendation)

        try:
            transformed_data = self._execute_with_circuit_breaker(
                "data_transformation",
                transform_data,
                data,
                transformation_rules=transformation_rules
            )

            if not isinstance(transformed_data, list):
                transformed_data = [transformed_data] if transformed_data else []

            # Record transformation metrics
            self.monitor.record_metric(
                "transformation_records_count",
                len(transformed_data),
                {"pipeline_id": monitor_ctx.pipeline_id}
            )

            return transformed_data

        except Exception as e:
            monitor_ctx.add_error(e)

            # Apply degraded mode transformation if possible
            if self.recovery_strategies.get(monitor_ctx.pipeline_id) == RecoveryStrategy.DEGRADED_MODE:
                self.logger.warning(f"Applying degraded mode transformation for pipeline {monitor_ctx.pipeline_id}")
                return self._apply_degraded_transformation(data)
            else:
                raise e

    def _execute_loading_step(self, data: List[Dict[str, Any]],
                            recommendation: PipelineRecommendation,
                            monitor_ctx: MonitoredExecution) -> None:
        """Execute data loading step."""
        # Configure loading based on recommendation
        for destination in recommendation.output_destinations:
            try:
                self._load_to_destination(data, destination.value, monitor_ctx)
            except Exception as e:
                monitor_ctx.add_error(e)

                # Check if we can skip this destination
                if self.recovery_strategies.get(monitor_ctx.pipeline_id) == RecoveryStrategy.PARTIAL_RECOVERY:
                    self.logger.warning(f"Skipping failed destination {destination.value} in partial recovery mode")
                    continue
                else:
                    raise e

    def _load_to_destination(self, data: List[Dict[str, Any]], destination: str,
                           monitor_ctx: MonitoredExecution) -> None:
        """Load data to specific destination."""
        # Simulate loading based on destination type
        if destination == "database":
            # Database loading logic
            self._simulate_database_load(data)
        elif destination == "s3" or destination == "data_lake":
            # S3/Data Lake loading logic
            self._simulate_s3_load(data)
        elif destination == "api":
            # API loading logic
            self._simulate_api_load(data)
        else:
            # Generic loading
            self.logger.info(f"Loading {len(data)} records to {destination}")

        # Record loading metrics
        self.monitor.record_metric(
            "loading_records_count",
            len(data),
            {"pipeline_id": monitor_ctx.pipeline_id, "destination": destination}
        )

    def _simulate_database_load(self, data: List[Dict[str, Any]]) -> None:
        """Simulate database loading."""
        # In real implementation, this would connect to database
        self.logger.info(f"Simulating database load of {len(data)} records")
        time.sleep(0.1)  # Simulate processing time

    def _simulate_s3_load(self, data: List[Dict[str, Any]]) -> None:
        """Simulate S3 loading."""
        # In real implementation, this would upload to S3
        self.logger.info(f"Simulating S3 load of {len(data)} records")
        time.sleep(0.05)  # Simulate processing time

    def _simulate_api_load(self, data: List[Dict[str, Any]]) -> None:
        """Simulate API loading."""
        # In real implementation, this would POST to API
        self.logger.info(f"Simulating API load of {len(data)} records")
        time.sleep(0.2)  # Simulate processing time

    def _generate_transformation_rules(self, recommendation: PipelineRecommendation) -> List[Dict[str, Any]]:
        """Generate transformation rules based on recommendation."""
        rules = []

        # Add standard enrichment rules
        rules.append({"type": "add_field", "field_name": "pipeline_timestamp", "field_value": time.time()})
        rules.append({"type": "add_field", "field_name": "transformation_strategy", "field_value": recommendation.transformation_strategy.value})

        # Strategy-specific rules
        if recommendation.transformation_strategy.value == "cleansing_focused":
            rules.extend([
                {"type": "null_handling", "strategy": "remove_nulls"},
                {"type": "data_validation", "strict": True}
            ])
        elif recommendation.transformation_strategy.value == "aggregation_focused":
            rules.extend([
                {"type": "groupby", "fields": ["category"], "aggregation": "sum"},
                {"type": "calculate_field", "field_name": "aggregated_value", "calculation": {"type": "sum", "fields": ["value"]}}
            ])

        return rules

    def _apply_degraded_transformation(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Apply basic transformation in degraded mode."""
        self.logger.info("Applying degraded mode transformation")

        # Apply only basic transformations that are unlikely to fail
        degraded_data = []
        for record in data:
            if isinstance(record, dict):
                # Add basic metadata
                degraded_record = record.copy()
                degraded_record["processed_timestamp"] = time.time()
                degraded_record["processing_mode"] = "degraded"
                degraded_data.append(degraded_record)

        return degraded_data

    def _apply_recovery_strategy(self, pipeline_id: str, error: Exception, attempt: int) -> bool:
        """
        Apply recovery strategy for failed pipeline.
        
        Args:
            pipeline_id: Pipeline identifier
            error: Exception that caused failure
            attempt: Current attempt number
        
        Returns:
            True if recovery was applied successfully
        """
        strategy = self.recovery_strategies.get(pipeline_id, RecoveryStrategy.RETRY)

        self.logger.info(f"Applying recovery strategy {strategy.value} for pipeline {pipeline_id}")

        try:
            if strategy == RecoveryStrategy.RETRY:
                # Simple retry - no special action needed
                return True

            elif strategy == RecoveryStrategy.DEGRADED_MODE:
                # Switch to degraded mode processing
                self.logger.info(f"Enabling degraded mode for pipeline {pipeline_id}")
                return True

            elif strategy == RecoveryStrategy.PARTIAL_RECOVERY:
                # Allow partial failures in loading steps
                self.logger.info(f"Enabling partial recovery mode for pipeline {pipeline_id}")
                return True

            elif strategy == RecoveryStrategy.SKIP:
                # Skip the current operation
                self.logger.info(f"Skipping failed operation for pipeline {pipeline_id}")
                return False  # Don't retry

            elif strategy == RecoveryStrategy.MANUAL_INTERVENTION:
                # Generate alert for manual intervention
                if hasattr(self.monitor, '_generate_alert'):
                    self.monitor._generate_alert(
                        AlertType.PIPELINE_FAILURE,
                        MonitoringLevel.ERROR,
                        f"Pipeline {pipeline_id} requires manual intervention after {attempt} attempts",
                        {"pipeline_id": pipeline_id, "error": str(error), "attempt": attempt}
                    )
                return False  # Don't retry automatically

            else:
                # Unknown strategy
                self.logger.warning(f"Unknown recovery strategy: {strategy.value}")
                return True  # Default to retry

        except Exception as recovery_error:
            self.logger.error(f"Error applying recovery strategy {strategy.value}: {recovery_error}")
            return False

    def _is_retriable_exception(self, exception: Exception, retry_config: RetryConfig) -> bool:
        """Check if exception is retriable."""
        exception_type = type(exception)

        # Check against configured retriable exceptions
        for retriable_type in retry_config.retriable_exceptions:
            if issubclass(exception_type, retriable_type):
                return True

        # Check specific error messages that might be temporary
        error_message = str(exception).lower()
        temporary_errors = [
            "connection reset",
            "timeout",
            "temporary failure",
            "service unavailable",
            "rate limit",
            "throttle"
        ]

        for temp_error in temporary_errors:
            if temp_error in error_message:
                return True

        return False

    def _wait_with_backoff(self, delay: float, retry_config: RetryConfig) -> None:
        """Wait with exponential backoff and optional jitter."""
        actual_delay = delay

        if retry_config.jitter:
            import random
            # Add random jitter up to 25% of delay
            jitter = random.uniform(0, delay * 0.25)
            actual_delay += jitter

        self.logger.info(f"Waiting {actual_delay:.2f} seconds before retry")
        time.sleep(actual_delay)

    def _monitor_pipeline_execution(self, pipeline_id: str, future: Future) -> None:
        """Monitor pipeline execution and handle completion."""
        try:
            # Wait for completion
            execution_result = future.result()

            # Move to history
            with self._lock:
                if pipeline_id in self.active_pipelines:
                    del self.active_pipelines[pipeline_id]

                self.execution_history.append(execution_result)

                # Trim history if too large
                if len(self.execution_history) > self.max_history_size:
                    self.execution_history = self.execution_history[-self.max_history_size:]

            # Record final metrics
            self.monitor.record_metric(
                "pipeline_final_state",
                1.0 if execution_result.state == ExecutionState.SUCCESS else 0.0,
                {"pipeline_id": pipeline_id, "state": execution_result.state.value}
            )

            self.logger.info(f"Pipeline {pipeline_id} monitoring complete: {execution_result.state.value}")

        except Exception as e:
            self.logger.error(f"Error monitoring pipeline {pipeline_id}: {e}")

            # Ensure pipeline is removed from active list
            with self._lock:
                if pipeline_id in self.active_pipelines:
                    execution_result = self.active_pipelines.pop(pipeline_id)
                    execution_result.state = ExecutionState.FAILED
                    execution_result.end_time = time.time()
                    execution_result.error_details.append(f"Monitoring error: {str(e)}")
                    self.execution_history.append(execution_result)
