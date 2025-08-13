"""
Resilience Framework for Autonomous SDLC

This module implements comprehensive resilience patterns including circuit breakers,
retry mechanisms, bulkheads, timeouts, and self-healing capabilities for the
autonomous SDLC system.
"""

from __future__ import annotations

import asyncio
import random
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .exceptions import (
    ErrorCategory,
    ErrorSeverity,
    PipelineExecutionException,
)
from .logging_config import get_logger


class ResiliencePattern(Enum):
    """Types of resilience patterns available."""
    CIRCUIT_BREAKER = "circuit_breaker"
    RETRY = "retry"
    TIMEOUT = "timeout"
    BULKHEAD = "bulkhead"
    RATE_LIMITER = "rate_limiter"
    FALLBACK = "fallback"
    HEALTH_CHECK = "health_check"


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Blocking requests
    HALF_OPEN = "half_open"  # Testing recovery


@dataclass
class ResilienceMetrics:
    """Metrics for resilience patterns."""
    pattern_type: ResiliencePattern
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0

    # Timing metrics
    total_execution_time: float = 0.0
    avg_execution_time: float = 0.0
    min_execution_time: float = float('inf')
    max_execution_time: float = 0.0

    # Pattern-specific metrics
    circuit_trips: int = 0
    retry_attempts: int = 0
    timeouts: int = 0
    fallback_executions: int = 0

    # Recent performance
    recent_response_times: deque = field(default_factory=lambda: deque(maxlen=100))
    recent_success_rate: float = 1.0

    def update_execution(self, execution_time: float, success: bool) -> None:
        """Update metrics with execution result."""
        self.total_requests += 1
        self.total_execution_time += execution_time

        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

        # Update timing metrics
        self.avg_execution_time = self.total_execution_time / self.total_requests
        self.min_execution_time = min(self.min_execution_time, execution_time)
        self.max_execution_time = max(self.max_execution_time, execution_time)

        # Update recent metrics
        self.recent_response_times.append(execution_time)
        if len(self.recent_response_times) >= 10:
            recent_successes = sum(1 for _ in range(-10, 0) if self.recent_response_times[_] > 0)
            self.recent_success_rate = recent_successes / 10

    def get_success_rate(self) -> float:
        """Get overall success rate."""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests


class CircuitBreaker:
    """Intelligent circuit breaker with adaptive thresholds."""

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        success_threshold: int = 3,
        name: str = "circuit_breaker"
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        self.name = name

        # State management
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0

        # Adaptive thresholds
        self.adaptive_threshold = failure_threshold
        self.performance_history = deque(maxlen=100)

        # Metrics
        self.metrics = ResilienceMetrics(ResiliencePattern.CIRCUIT_BREAKER)
        self.logger = get_logger(f"resilience.circuit_breaker.{name}")

    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""

        if self.state == CircuitState.OPEN:
            if time.time() - self.last_failure_time < self.recovery_timeout:
                self.metrics.circuit_trips += 1
                raise PipelineExecutionException(
                    f"Circuit breaker {self.name} is OPEN",
                    category=ErrorCategory.INFRASTRUCTURE,
                    severity=ErrorSeverity.HIGH
                )
            else:
                # Move to half-open state for testing
                self.state = CircuitState.HALF_OPEN
                self.logger.info(f"Circuit breaker {self.name} moving to HALF_OPEN")

        start_time = time.time()

        try:
            result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

            execution_time = time.time() - start_time
            self.metrics.update_execution(execution_time, True)
            self.performance_history.append(execution_time)

            # Handle success
            self._handle_success()

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            self.metrics.update_execution(execution_time, False)

            # Handle failure
            self._handle_failure()

            raise e

    def _handle_success(self) -> None:
        """Handle successful execution."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
                self.success_count = 0
                self.logger.info(f"Circuit breaker {self.name} recovered to CLOSED")
        else:
            self.failure_count = max(0, self.failure_count - 1)

    def _handle_failure(self) -> None:
        """Handle failed execution."""
        self.failure_count += 1
        self.last_failure_time = time.time()

        # Adaptive threshold adjustment
        self._adjust_adaptive_threshold()

        if self.failure_count >= self.adaptive_threshold:
            self.state = CircuitState.OPEN
            self.success_count = 0
            self.logger.warning(f"Circuit breaker {self.name} tripped to OPEN")

    def _adjust_adaptive_threshold(self) -> None:
        """Adjust failure threshold based on recent performance."""
        if len(self.performance_history) < 10:
            return

        recent_avg = statistics.mean(list(self.performance_history)[-10:])
        historical_avg = statistics.mean(self.performance_history)

        # If recent performance is much worse, lower threshold
        if recent_avg > historical_avg * 1.5:
            self.adaptive_threshold = max(2, self.failure_threshold - 1)
        else:
            self.adaptive_threshold = self.failure_threshold


class RetryPolicy:
    """Intelligent retry policy with exponential backoff and jitter."""

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_backoff: bool = True,
        jitter: bool = True,
        retryable_exceptions: Optional[List[type]] = None
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_backoff = exponential_backoff
        self.jitter = jitter
        self.retryable_exceptions = retryable_exceptions or [Exception]

        self.metrics = ResilienceMetrics(ResiliencePattern.RETRY)
        self.logger = get_logger("resilience.retry")

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with retry policy."""

        last_exception = None

        for attempt in range(self.max_attempts):
            start_time = time.time()

            try:
                result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

                execution_time = time.time() - start_time
                self.metrics.update_execution(execution_time, True)

                return result

            except Exception as e:
                execution_time = time.time() - start_time
                self.metrics.update_execution(execution_time, False)
                self.metrics.retry_attempts += 1

                last_exception = e

                # Check if exception is retryable
                if not self._is_retryable(e):
                    self.logger.info(f"Exception {type(e).__name__} is not retryable")
                    raise e

                # Don't sleep on the last attempt
                if attempt < self.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    self.logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f}s"
                    )
                    await asyncio.sleep(delay)

        # All attempts failed
        self.logger.error(f"All {self.max_attempts} attempts failed")
        raise last_exception

    def _is_retryable(self, exception: Exception) -> bool:
        """Check if exception is retryable."""
        for retryable_type in self.retryable_exceptions:
            if isinstance(exception, retryable_type):
                # Special handling for certain error types
                if isinstance(exception, PipelineExecutionException):
                    # Don't retry validation errors or user errors
                    if exception.category in [ErrorCategory.VALIDATION, ErrorCategory.USER_ERROR]:
                        return False
                    # Don't retry critical security errors
                    if exception.severity == ErrorSeverity.CRITICAL:
                        return False
                return True
        return False

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt."""
        if self.exponential_backoff:
            delay = self.base_delay * (2 ** attempt)
        else:
            delay = self.base_delay

        # Apply maximum delay limit
        delay = min(delay, self.max_delay)

        # Add jitter to prevent thundering herd
        if self.jitter:
            jitter_amount = delay * 0.1 * random.random()
            delay += jitter_amount

        return delay


class TimeoutManager:
    """Timeout manager with adaptive timeout adjustment."""

    def __init__(
        self,
        default_timeout: float = 30.0,
        adaptive: bool = True,
        name: str = "timeout_manager"
    ):
        self.default_timeout = default_timeout
        self.adaptive = adaptive
        self.name = name

        # Adaptive timeout calculation
        self.execution_history = deque(maxlen=50)
        self.adaptive_timeout = default_timeout

        self.metrics = ResilienceMetrics(ResiliencePattern.TIMEOUT)
        self.logger = get_logger(f"resilience.timeout.{name}")

    async def execute(self, func: Callable, timeout: Optional[float] = None, *args, **kwargs) -> Any:
        """Execute function with timeout protection."""

        effective_timeout = timeout or self.adaptive_timeout
        start_time = time.time()

        try:
            if asyncio.iscoroutinefunction(func):
                result = await asyncio.wait_for(
                    func(*args, **kwargs),
                    timeout=effective_timeout
                )
            else:
                # For sync functions, use a different approach
                result = func(*args, **kwargs)

            execution_time = time.time() - start_time
            self.metrics.update_execution(execution_time, True)

            # Update adaptive timeout
            if self.adaptive:
                self._update_adaptive_timeout(execution_time)

            return result

        except asyncio.TimeoutError:
            execution_time = time.time() - start_time
            self.metrics.update_execution(execution_time, False)
            self.metrics.timeouts += 1

            self.logger.warning(f"Operation timed out after {effective_timeout}s")

            raise PipelineExecutionException(
                f"Operation timed out after {effective_timeout:.2f} seconds",
                category=ErrorCategory.INFRASTRUCTURE,
                severity=ErrorSeverity.MEDIUM
            )
        except Exception as e:
            execution_time = time.time() - start_time
            self.metrics.update_execution(execution_time, False)
            raise e

    def _update_adaptive_timeout(self, execution_time: float) -> None:
        """Update adaptive timeout based on execution history."""
        self.execution_history.append(execution_time)

        if len(self.execution_history) >= 10:
            # Calculate new adaptive timeout as 95th percentile + buffer
            sorted_times = sorted(self.execution_history)
            percentile_95 = sorted_times[int(len(sorted_times) * 0.95)]

            # Add 50% buffer to handle variations
            self.adaptive_timeout = percentile_95 * 1.5

            # Ensure it's within reasonable bounds
            self.adaptive_timeout = max(self.default_timeout * 0.5, self.adaptive_timeout)
            self.adaptive_timeout = min(self.default_timeout * 5, self.adaptive_timeout)


class BulkheadIsolation:
    """Bulkhead pattern for resource isolation."""

    def __init__(
        self,
        max_concurrent: int = 10,
        queue_size: int = 100,
        name: str = "bulkhead"
    ):
        self.max_concurrent = max_concurrent
        self.queue_size = queue_size
        self.name = name

        # Resource tracking
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_requests = 0
        self.queued_requests = 0

        self.metrics = ResilienceMetrics(ResiliencePattern.BULKHEAD)
        self.logger = get_logger(f"resilience.bulkhead.{name}")

    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function within bulkhead constraints."""

        # Check queue capacity
        if self.queued_requests >= self.queue_size:
            raise PipelineExecutionException(
                f"Bulkhead {self.name} queue is full",
                category=ErrorCategory.INFRASTRUCTURE,
                severity=ErrorSeverity.HIGH
            )

        self.queued_requests += 1
        start_time = time.time()

        try:
            # Acquire semaphore (may wait)
            async with self.semaphore:
                self.active_requests += 1
                self.queued_requests -= 1

                execution_start = time.time()

                try:
                    result = await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

                    execution_time = time.time() - execution_start
                    total_time = time.time() - start_time

                    self.metrics.update_execution(total_time, True)

                    return result

                except Exception as e:
                    execution_time = time.time() - execution_start
                    total_time = time.time() - start_time

                    self.metrics.update_execution(total_time, False)
                    raise e
                finally:
                    self.active_requests -= 1

        except Exception as e:
            self.queued_requests -= 1
            raise e


class ResilienceOrchestrator:
    """Orchestrates multiple resilience patterns."""

    def __init__(self):
        self.logger = get_logger("resilience.orchestrator")

        # Pattern instances
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_policies: Dict[str, RetryPolicy] = {}
        self.timeout_managers: Dict[str, TimeoutManager] = {}
        self.bulkheads: Dict[str, BulkheadIsolation] = {}

        # Global metrics
        self.global_metrics = defaultdict(lambda: ResilienceMetrics(ResiliencePattern.CIRCUIT_BREAKER))

    def create_circuit_breaker(
        self,
        name: str,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0
    ) -> CircuitBreaker:
        """Create and register a circuit breaker."""
        circuit_breaker = CircuitBreaker(failure_threshold, recovery_timeout, name=name)
        self.circuit_breakers[name] = circuit_breaker
        return circuit_breaker

    def create_retry_policy(
        self,
        name: str,
        max_attempts: int = 3,
        base_delay: float = 1.0
    ) -> RetryPolicy:
        """Create and register a retry policy."""
        retry_policy = RetryPolicy(max_attempts, base_delay)
        self.retry_policies[name] = retry_policy
        return retry_policy

    def create_timeout_manager(
        self,
        name: str,
        default_timeout: float = 30.0
    ) -> TimeoutManager:
        """Create and register a timeout manager."""
        timeout_manager = TimeoutManager(default_timeout, name=name)
        self.timeout_managers[name] = timeout_manager
        return timeout_manager

    def create_bulkhead(
        self,
        name: str,
        max_concurrent: int = 10
    ) -> BulkheadIsolation:
        """Create and register a bulkhead."""
        bulkhead = BulkheadIsolation(max_concurrent, name=name)
        self.bulkheads[name] = bulkhead
        return bulkhead

    async def execute_with_patterns(
        self,
        func: Callable,
        patterns: List[str],
        *args,
        **kwargs
    ) -> Any:
        """Execute function with multiple resilience patterns."""

        async def wrapped_execution():
            return await func(*args, **kwargs) if asyncio.iscoroutinefunction(func) else func(*args, **kwargs)

        execution_func = wrapped_execution

        # Apply patterns in order: bulkhead -> timeout -> circuit breaker -> retry
        for pattern_name in reversed(patterns):
            if pattern_name in self.bulkheads:
                bulkhead = self.bulkheads[pattern_name]
                execution_func = lambda f=execution_func: bulkhead.execute(f)

            if pattern_name in self.timeout_managers:
                timeout_manager = self.timeout_managers[pattern_name]
                execution_func = lambda f=execution_func: timeout_manager.execute(f)

            if pattern_name in self.circuit_breakers:
                circuit_breaker = self.circuit_breakers[pattern_name]
                execution_func = lambda f=execution_func: circuit_breaker.call(f)

            if pattern_name in self.retry_policies:
                retry_policy = self.retry_policies[pattern_name]
                execution_func = lambda f=execution_func: retry_policy.execute(f)

        return await execution_func()

    def get_resilience_status(self) -> Dict[str, Any]:
        """Get status of all resilience patterns."""
        status = {
            "circuit_breakers": {},
            "retry_policies": {},
            "timeout_managers": {},
            "bulkheads": {},
            "overall_health": "healthy"
        }

        # Circuit breaker status
        for name, cb in self.circuit_breakers.items():
            status["circuit_breakers"][name] = {
                "state": cb.state.value,
                "failure_count": cb.failure_count,
                "success_rate": cb.metrics.get_success_rate(),
                "total_requests": cb.metrics.total_requests
            }

            if cb.state == CircuitState.OPEN:
                status["overall_health"] = "degraded"

        # Retry policy status
        for name, rp in self.retry_policies.items():
            status["retry_policies"][name] = {
                "total_retries": rp.metrics.retry_attempts,
                "success_rate": rp.metrics.get_success_rate(),
                "avg_execution_time": rp.metrics.avg_execution_time
            }

        # Timeout manager status
        for name, tm in self.timeout_managers.items():
            status["timeout_managers"][name] = {
                "adaptive_timeout": tm.adaptive_timeout,
                "timeouts": tm.metrics.timeouts,
                "success_rate": tm.metrics.get_success_rate()
            }

        # Bulkhead status
        for name, bh in self.bulkheads.items():
            status["bulkheads"][name] = {
                "active_requests": bh.active_requests,
                "queued_requests": bh.queued_requests,
                "max_concurrent": bh.max_concurrent,
                "utilization": bh.active_requests / bh.max_concurrent
            }

            if bh.active_requests / bh.max_concurrent > 0.8:
                status["overall_health"] = "under_pressure"

        return status


# Global resilience orchestrator
_resilience_orchestrator: Optional[ResilienceOrchestrator] = None


def get_resilience_orchestrator() -> ResilienceOrchestrator:
    """Get the global resilience orchestrator."""
    global _resilience_orchestrator
    if _resilience_orchestrator is None:
        _resilience_orchestrator = ResilienceOrchestrator()
    return _resilience_orchestrator


# Convenience functions
async def execute_with_resilience(
    func: Callable,
    circuit_breaker: Optional[str] = None,
    retry_policy: Optional[str] = None,
    timeout: Optional[str] = None,
    bulkhead: Optional[str] = None,
    *args,
    **kwargs
) -> Any:
    """Execute function with resilience patterns."""

    orchestrator = get_resilience_orchestrator()
    patterns = []

    if bulkhead:
        patterns.append(bulkhead)
    if timeout:
        patterns.append(timeout)
    if circuit_breaker:
        patterns.append(circuit_breaker)
    if retry_policy:
        patterns.append(retry_policy)

    return await orchestrator.execute_with_patterns(func, patterns, *args, **kwargs)
