"""Error recovery and self-healing mechanisms for ETL pipelines."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .exceptions import PipelineExecutionException
from .logging_config import get_logger


class RecoveryStrategy(Enum):
    """Available recovery strategies."""
    RETRY = "retry"
    SKIP_AND_CONTINUE = "skip_and_continue"
    FALLBACK_DATA = "fallback_data"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    CIRCUIT_BREAKER = "circuit_breaker"


@dataclass
class RecoveryConfig:
    """Configuration for error recovery."""
    max_retries: int = 3
    retry_delay: float = 1.0
    exponential_backoff: bool = True
    fallback_enabled: bool = True
    circuit_breaker_threshold: int = 5
    circuit_breaker_timeout: float = 60.0


@dataclass
class ErrorContext:
    """Context information about an error."""
    error: Exception
    operation: str
    attempt_number: int
    timestamp: float
    metadata: Dict[str, Any]


class ErrorRecoveryManager:
    """Manages error recovery strategies for ETL operations."""

    def __init__(self, config: Optional[RecoveryConfig] = None):
        self.config = config or RecoveryConfig()
        self.logger = get_logger("error_recovery")
        self.error_history: List[ErrorContext] = []
        self.circuit_breaker_states: Dict[str, Dict[str, Any]] = {}

    async def execute_with_recovery(
        self,
        operation: Callable,
        operation_name: str,
        recovery_strategies: List[RecoveryStrategy],
        *args,
        **kwargs
    ) -> Any:
        """Execute operation with specified recovery strategies."""

        # Check circuit breaker first
        if self._is_circuit_open(operation_name):
            self.logger.warning(f"Circuit breaker OPEN for {operation_name}")
            raise PipelineExecutionException(f"Circuit breaker open for {operation_name}")

        for attempt in range(self.config.max_retries + 1):
            try:
                result = await self._execute_operation(operation, *args, **kwargs)

                # Reset circuit breaker on success
                self._reset_circuit_breaker(operation_name)

                if attempt > 0:
                    self.logger.info(f"Operation {operation_name} succeeded after {attempt} retries")

                return result

            except Exception as e:
                error_context = ErrorContext(
                    error=e,
                    operation=operation_name,
                    attempt_number=attempt + 1,
                    timestamp=time.time(),
                    metadata={"args": str(args), "kwargs": str(kwargs)}
                )

                self.error_history.append(error_context)
                self.logger.warning(f"Operation {operation_name} failed (attempt {attempt + 1}): {str(e)}")

                # Update circuit breaker
                self._record_failure(operation_name)

                # Try recovery strategies
                recovery_result = await self._try_recovery_strategies(
                    error_context, recovery_strategies, operation, *args, **kwargs
                )

                if recovery_result is not None:
                    return recovery_result

                # If not last attempt, wait before retry
                if attempt < self.config.max_retries:
                    delay = self._calculate_retry_delay(attempt)
                    self.logger.info(f"Retrying in {delay:.1f} seconds...")
                    await asyncio.sleep(delay)

        # All retries exhausted
        self.logger.error(f"Operation {operation_name} failed after all retries")
        raise PipelineExecutionException(f"Operation failed after {self.config.max_retries + 1} attempts")

    async def _execute_operation(self, operation: Callable, *args, **kwargs) -> Any:
        """Execute the operation (async or sync)."""
        if asyncio.iscoroutinefunction(operation):
            return await operation(*args, **kwargs)
        else:
            return operation(*args, **kwargs)

    async def _try_recovery_strategies(
        self,
        error_context: ErrorContext,
        strategies: List[RecoveryStrategy],
        operation: Callable,
        *args,
        **kwargs
    ) -> Optional[Any]:
        """Try recovery strategies in order."""

        for strategy in strategies:
            self.logger.info(f"Trying recovery strategy: {strategy.value}")

            try:
                if strategy == RecoveryStrategy.SKIP_AND_CONTINUE:
                    return self._skip_and_continue_recovery(error_context)

                elif strategy == RecoveryStrategy.FALLBACK_DATA:
                    return self._fallback_data_recovery(error_context)

                elif strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
                    return self._graceful_degradation_recovery(error_context, operation, *args, **kwargs)

            except Exception as recovery_error:
                self.logger.warning(f"Recovery strategy {strategy.value} failed: {str(recovery_error)}")
                continue

        return None

    def _skip_and_continue_recovery(self, error_context: ErrorContext) -> Dict[str, Any]:
        """Skip the failed operation and return a placeholder result."""
        self.logger.info(f"Skipping failed operation: {error_context.operation}")

        return {
            "status": "skipped",
            "reason": f"Operation failed: {str(error_context.error)}",
            "recovered_at": time.time(),
            "recovery_strategy": "skip_and_continue",
            "data": []
        }

    def _fallback_data_recovery(self, error_context: ErrorContext) -> Dict[str, Any]:
        """Provide fallback data when operation fails."""
        self.logger.info(f"Using fallback data for: {error_context.operation}")

        # Generate basic fallback data based on operation type
        if "extract" in error_context.operation.lower():
            fallback_data = [
                {"id": 1, "fallback": True, "message": "Fallback extraction data"},
                {"id": 2, "fallback": True, "message": "Primary source unavailable"}
            ]
        elif "transform" in error_context.operation.lower():
            fallback_data = {"transformed": False, "original_data": "preserved"}
        else:
            fallback_data = {"fallback": True, "operation": error_context.operation}

        return {
            "status": "fallback",
            "data": fallback_data,
            "recovery_strategy": "fallback_data",
            "recovered_at": time.time()
        }

    async def _graceful_degradation_recovery(
        self,
        error_context: ErrorContext,
        operation: Callable,
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """Attempt a simplified version of the operation."""
        self.logger.info(f"Attempting graceful degradation for: {error_context.operation}")

        try:
            # Try with simplified parameters
            if "extract" in error_context.operation.lower():
                # Try extracting smaller subset
                simplified_kwargs = kwargs.copy()
                simplified_kwargs.update({"limit": 10, "timeout": 5})
                result = await self._execute_operation(operation, *args, **simplified_kwargs)

                return {
                    "status": "degraded",
                    "data": result,
                    "recovery_strategy": "graceful_degradation",
                    "note": "Limited data due to error recovery"
                }

        except Exception as degraded_error:
            self.logger.warning(f"Graceful degradation failed: {str(degraded_error)}")
            raise

        return {
            "status": "degraded_failed",
            "recovery_strategy": "graceful_degradation",
            "error": str(error_context.error)
        }

    def _calculate_retry_delay(self, attempt: int) -> float:
        """Calculate delay before retry with exponential backoff."""
        if self.config.exponential_backoff:
            return self.config.retry_delay * (2 ** attempt)
        else:
            return self.config.retry_delay

    def _is_circuit_open(self, operation_name: str) -> bool:
        """Check if circuit breaker is open for operation."""
        if operation_name not in self.circuit_breaker_states:
            return False

        state = self.circuit_breaker_states[operation_name]

        if state["status"] == "open":
            # Check if timeout has passed
            if time.time() - state["opened_at"] > self.config.circuit_breaker_timeout:
                # Move to half-open state
                state["status"] = "half_open"
                self.logger.info(f"Circuit breaker for {operation_name} moved to HALF_OPEN")
                return False
            return True

        return False

    def _record_failure(self, operation_name: str):
        """Record failure for circuit breaker logic."""
        if operation_name not in self.circuit_breaker_states:
            self.circuit_breaker_states[operation_name] = {
                "status": "closed",
                "failure_count": 0,
                "last_failure": time.time()
            }

        state = self.circuit_breaker_states[operation_name]
        state["failure_count"] += 1
        state["last_failure"] = time.time()

        # Check if should open circuit
        if state["failure_count"] >= self.config.circuit_breaker_threshold:
            state["status"] = "open"
            state["opened_at"] = time.time()
            self.logger.warning(f"Circuit breaker OPENED for {operation_name}")

    def _reset_circuit_breaker(self, operation_name: str):
        """Reset circuit breaker on successful operation."""
        if operation_name in self.circuit_breaker_states:
            self.circuit_breaker_states[operation_name] = {
                "status": "closed",
                "failure_count": 0,
                "last_success": time.time()
            }

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error statistics for monitoring."""
        if not self.error_history:
            return {"total_errors": 0, "operations": {}}

        operation_stats = {}

        for error in self.error_history:
            op = error.operation
            if op not in operation_stats:
                operation_stats[op] = {
                    "error_count": 0,
                    "last_error": None,
                    "common_errors": {}
                }

            operation_stats[op]["error_count"] += 1
            operation_stats[op]["last_error"] = error.timestamp

            error_type = type(error.error).__name__
            if error_type not in operation_stats[op]["common_errors"]:
                operation_stats[op]["common_errors"][error_type] = 0
            operation_stats[op]["common_errors"][error_type] += 1

        return {
            "total_errors": len(self.error_history),
            "operations": operation_stats,
            "circuit_breakers": self.circuit_breaker_states
        }


class SelfHealingPipeline:
    """Self-healing pipeline that automatically recovers from errors."""

    def __init__(self, recovery_config: Optional[RecoveryConfig] = None):
        self.recovery_manager = ErrorRecoveryManager(recovery_config)
        self.logger = get_logger("self_healing_pipeline")

    async def execute_step(
        self,
        step_name: str,
        operation: Callable,
        recovery_strategies: List[RecoveryStrategy],
        *args,
        **kwargs
    ) -> Dict[str, Any]:
        """Execute a pipeline step with self-healing capabilities."""

        self.logger.info(f"Executing self-healing step: {step_name}")

        result = await self.recovery_manager.execute_with_recovery(
            operation=operation,
            operation_name=step_name,
            recovery_strategies=recovery_strategies,
            *args,
            **kwargs
        )

        return {
            "step_name": step_name,
            "result": result,
            "execution_time": time.time(),
            "status": "success"
        }
