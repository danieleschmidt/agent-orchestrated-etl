"""Enhanced error handling and recovery system with comprehensive logging."""

from __future__ import annotations

import asyncio
import json
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .exceptions import DataProcessingException
from .logging_config import get_logger


class ErrorSeverity(Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class RecoveryStrategy(Enum):
    """Recovery strategies for different error types."""
    RETRY = "retry"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    FAIL_FAST = "fail_fast"
    IGNORE = "ignore"


@dataclass
class ErrorContext:
    """Context information for error handling."""
    error_id: str
    timestamp: float
    operation_name: str
    input_data: Any
    error_type: str
    error_message: str
    stack_trace: str
    severity: ErrorSeverity
    retry_count: int
    max_retries: int
    recovery_strategy: RecoveryStrategy
    metadata: Dict[str, Any]


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker pattern."""
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3
    success_threshold: int = 2


class CircuitBreakerState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    """Circuit breaker implementation for error handling."""

    def __init__(self, config: CircuitBreakerConfig):
        self.config = config
        self.state = CircuitBreakerState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = 0.0
        self.half_open_calls = 0
        self._lock = threading.RLock()
        self.logger = get_logger("agent_etl.circuit_breaker")

    @asynccontextmanager
    async def call(self, operation_name: str):
        """Context manager for circuit breaker calls."""
        if not self._can_execute():
            raise DataProcessingException(
                f"Circuit breaker is OPEN for operation: {operation_name}"
            )

        try:
            if self.state == CircuitBreakerState.HALF_OPEN:
                with self._lock:
                    self.half_open_calls += 1

            yield

            # Success - handle state transition
            await self._on_success()

        except Exception as e:
            await self._on_failure(e)
            raise

    def _can_execute(self) -> bool:
        """Check if operation can be executed."""
        with self._lock:
            if self.state == CircuitBreakerState.CLOSED:
                return True
            elif self.state == CircuitBreakerState.OPEN:
                if time.time() - self.last_failure_time >= self.config.recovery_timeout:
                    self.state = CircuitBreakerState.HALF_OPEN
                    self.half_open_calls = 0
                    self.logger.info("Circuit breaker transitioning to HALF_OPEN")
                    return True
                return False
            elif self.state == CircuitBreakerState.HALF_OPEN:
                return self.half_open_calls < self.config.half_open_max_calls

        return False

    async def _on_success(self):
        """Handle successful operation."""
        with self._lock:
            if self.state == CircuitBreakerState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self.state = CircuitBreakerState.CLOSED
                    self.failure_count = 0
                    self.success_count = 0
                    self.logger.info("Circuit breaker CLOSED - service recovered")
            elif self.state == CircuitBreakerState.CLOSED:
                self.failure_count = 0  # Reset failure count on success

    async def _on_failure(self, error: Exception):
        """Handle failed operation."""
        with self._lock:
            self.failure_count += 1
            self.last_failure_time = time.time()

            if self.state == CircuitBreakerState.CLOSED:
                if self.failure_count >= self.config.failure_threshold:
                    self.state = CircuitBreakerState.OPEN
                    self.logger.error(f"Circuit breaker OPENED after {self.failure_count} failures")
            elif self.state == CircuitBreakerState.HALF_OPEN:
                self.state = CircuitBreakerState.OPEN
                self.success_count = 0
                self.logger.error("Circuit breaker returned to OPEN state")


class EnhancedErrorHandler:
    """Enhanced error handling system with multiple recovery strategies."""

    def __init__(self, max_concurrent_retries: int = 10):
        self.logger = get_logger("agent_etl.error_handler")
        self.error_history: List[ErrorContext] = []
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.retry_executor = ThreadPoolExecutor(max_workers=max_concurrent_retries)
        self.error_patterns: Dict[str, Dict[str, Any]] = {}
        self._stats = {
            "total_errors": 0,
            "recovered_errors": 0,
            "critical_errors": 0,
            "retry_success_rate": 0.0
        }

        # Default error patterns and recovery strategies
        self._initialize_default_patterns()

    def _initialize_default_patterns(self):
        """Initialize default error patterns and recovery strategies."""
        self.error_patterns.update({
            "connection_error": {
                "keywords": ["connection", "timeout", "network", "socket"],
                "severity": ErrorSeverity.MEDIUM,
                "strategy": RecoveryStrategy.RETRY,
                "max_retries": 3,
                "backoff_multiplier": 2.0
            },
            "rate_limit_error": {
                "keywords": ["rate limit", "quota", "throttle", "429"],
                "severity": ErrorSeverity.LOW,
                "strategy": RecoveryStrategy.RETRY,
                "max_retries": 5,
                "backoff_multiplier": 3.0
            },
            "data_validation_error": {
                "keywords": ["validation", "invalid", "malformed", "schema"],
                "severity": ErrorSeverity.HIGH,
                "strategy": RecoveryStrategy.FALLBACK,
                "max_retries": 1,
                "backoff_multiplier": 1.0
            },
            "authentication_error": {
                "keywords": ["auth", "unauthorized", "forbidden", "401", "403"],
                "severity": ErrorSeverity.CRITICAL,
                "strategy": RecoveryStrategy.FAIL_FAST,
                "max_retries": 0,
                "backoff_multiplier": 1.0
            },
            "resource_exhaustion": {
                "keywords": ["memory", "disk", "resource", "capacity"],
                "severity": ErrorSeverity.HIGH,
                "strategy": RecoveryStrategy.GRACEFUL_DEGRADATION,
                "max_retries": 2,
                "backoff_multiplier": 1.5
            }
        })

    @asynccontextmanager
    async def handle_errors(self,
                           operation_name: str,
                           input_data: Any = None,
                           fallback_func: Optional[Callable] = None,
                           custom_strategy: Optional[RecoveryStrategy] = None,
                           max_retries: Optional[int] = None):
        """Context manager for comprehensive error handling."""

        error_context = None
        retry_count = 0
        max_retry_attempts = max_retries or 3

        while retry_count <= max_retry_attempts:
            try:
                # Check circuit breaker if configured
                circuit_breaker = self.circuit_breakers.get(operation_name)
                if circuit_breaker:
                    async with circuit_breaker.call(operation_name):
                        yield
                else:
                    yield

                # Success - break retry loop
                if error_context:
                    self._stats["recovered_errors"] += 1
                    self.logger.info(f"Operation {operation_name} recovered after {retry_count} retries")
                break

            except Exception as e:
                error_context = self._create_error_context(
                    operation_name, input_data, e, retry_count, max_retry_attempts
                )

                self.error_history.append(error_context)
                self._stats["total_errors"] += 1

                if error_context.severity == ErrorSeverity.CRITICAL:
                    self._stats["critical_errors"] += 1

                # Determine recovery strategy
                strategy = custom_strategy or error_context.recovery_strategy

                if strategy == RecoveryStrategy.FAIL_FAST or retry_count >= max_retry_attempts:
                    self.logger.error(f"Operation {operation_name} failed with {strategy.value} strategy: {str(e)}")
                    raise

                # Handle different recovery strategies
                await self._execute_recovery_strategy(error_context, fallback_func)

                retry_count += 1

                if retry_count <= max_retry_attempts:
                    backoff_time = self._calculate_backoff(retry_count, error_context)
                    self.logger.warning(
                        f"Retrying operation {operation_name} in {backoff_time:.2f}s "
                        f"(attempt {retry_count}/{max_retry_attempts})"
                    )
                    await asyncio.sleep(backoff_time)

    def _create_error_context(self,
                             operation_name: str,
                             input_data: Any,
                             error: Exception,
                             retry_count: int,
                             max_retries: int) -> ErrorContext:
        """Create error context with classification and strategy."""

        error_message = str(error)
        error_type = type(error).__name__
        stack_trace = traceback.format_exc()

        # Classify error and determine strategy
        pattern = self._classify_error(error_message, error_type)
        severity = pattern.get("severity", ErrorSeverity.MEDIUM)
        strategy = pattern.get("strategy", RecoveryStrategy.RETRY)

        return ErrorContext(
            error_id=f"err_{int(time.time() * 1000000)}",
            timestamp=time.time(),
            operation_name=operation_name,
            input_data=input_data,
            error_type=error_type,
            error_message=error_message,
            stack_trace=stack_trace,
            severity=severity,
            retry_count=retry_count,
            max_retries=max_retries,
            recovery_strategy=strategy,
            metadata=pattern
        )

    def _classify_error(self, error_message: str, error_type: str) -> Dict[str, Any]:
        """Classify error based on patterns and return metadata."""

        error_text = f"{error_message} {error_type}".lower()

        for pattern_name, pattern_config in self.error_patterns.items():
            keywords = pattern_config.get("keywords", [])
            if any(keyword in error_text for keyword in keywords):
                self.logger.debug(f"Error classified as: {pattern_name}")
                return pattern_config

        # Default pattern for unclassified errors
        return {
            "severity": ErrorSeverity.MEDIUM,
            "strategy": RecoveryStrategy.RETRY,
            "max_retries": 2,
            "backoff_multiplier": 2.0
        }

    async def _execute_recovery_strategy(self,
                                       error_context: ErrorContext,
                                       fallback_func: Optional[Callable] = None):
        """Execute the appropriate recovery strategy."""

        strategy = error_context.recovery_strategy

        if strategy == RecoveryStrategy.RETRY:
            # Retry is handled by the main loop
            pass

        elif strategy == RecoveryStrategy.FALLBACK and fallback_func:
            try:
                self.logger.info(f"Executing fallback for operation: {error_context.operation_name}")
                if asyncio.iscoroutinefunction(fallback_func):
                    await fallback_func(error_context.input_data)
                else:
                    fallback_func(error_context.input_data)
            except Exception as fallback_error:
                self.logger.error(f"Fallback failed: {str(fallback_error)}")

        elif strategy == RecoveryStrategy.CIRCUIT_BREAKER:
            # Ensure circuit breaker is configured
            if error_context.operation_name not in self.circuit_breakers:
                self.circuit_breakers[error_context.operation_name] = CircuitBreaker(
                    CircuitBreakerConfig()
                )

        elif strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
            self.logger.warning(
                f"Graceful degradation for operation: {error_context.operation_name}"
            )
            # Could implement reduced functionality here

    def _calculate_backoff(self, retry_count: int, error_context: ErrorContext) -> float:
        """Calculate exponential backoff with jitter."""

        multiplier = error_context.metadata.get("backoff_multiplier", 2.0)
        base_delay = 1.0

        # Exponential backoff
        delay = base_delay * (multiplier ** retry_count)

        # Add jitter (±20%)
        import random
        jitter = delay * 0.2 * (2 * random.random() - 1)

        return max(0.1, delay + jitter)

    def register_circuit_breaker(self,
                                operation_name: str,
                                config: Optional[CircuitBreakerConfig] = None):
        """Register a circuit breaker for an operation."""

        circuit_config = config or CircuitBreakerConfig()
        self.circuit_breakers[operation_name] = CircuitBreaker(circuit_config)
        self.logger.info(f"Registered circuit breaker for operation: {operation_name}")

    def add_error_pattern(self,
                         pattern_name: str,
                         keywords: List[str],
                         severity: ErrorSeverity,
                         strategy: RecoveryStrategy,
                         max_retries: int = 3,
                         backoff_multiplier: float = 2.0):
        """Add custom error pattern."""

        self.error_patterns[pattern_name] = {
            "keywords": keywords,
            "severity": severity,
            "strategy": strategy,
            "max_retries": max_retries,
            "backoff_multiplier": backoff_multiplier
        }

        self.logger.info(f"Added error pattern: {pattern_name}")

    def get_error_analytics(self) -> Dict[str, Any]:
        """Get comprehensive error analytics."""

        if not self.error_history:
            return {"message": "No errors recorded"}

        # Calculate statistics
        total_errors = len(self.error_history)
        severity_counts = {}
        strategy_counts = {}
        operation_counts = {}

        for error in self.error_history:
            # Count by severity
            severity = error.severity.value
            severity_counts[severity] = severity_counts.get(severity, 0) + 1

            # Count by strategy
            strategy = error.recovery_strategy.value
            strategy_counts[strategy] = strategy_counts.get(strategy, 0) + 1

            # Count by operation
            operation = error.operation_name
            operation_counts[operation] = operation_counts.get(operation, 0) + 1

        # Calculate retry success rate
        total_retries = sum(1 for error in self.error_history if error.retry_count > 0)
        successful_retries = self._stats["recovered_errors"]
        retry_success_rate = (successful_retries / total_retries) if total_retries > 0 else 0.0

        return {
            "total_errors": total_errors,
            "recovered_errors": self._stats["recovered_errors"],
            "critical_errors": self._stats["critical_errors"],
            "retry_success_rate": retry_success_rate,
            "error_by_severity": severity_counts,
            "error_by_strategy": strategy_counts,
            "error_by_operation": operation_counts,
            "circuit_breakers": {
                name: {"state": cb.state.value, "failure_count": cb.failure_count}
                for name, cb in self.circuit_breakers.items()
            },
            "recent_errors": [
                {
                    "timestamp": error.timestamp,
                    "operation": error.operation_name,
                    "error_type": error.error_type,
                    "severity": error.severity.value,
                    "retry_count": error.retry_count
                }
                for error in self.error_history[-10:]  # Last 10 errors
            ]
        }

    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on error handling system."""

        recent_errors = [
            error for error in self.error_history
            if time.time() - error.timestamp < 300  # Last 5 minutes
        ]

        critical_recent = [
            error for error in recent_errors
            if error.severity == ErrorSeverity.CRITICAL
        ]

        health_status = "healthy"
        if len(critical_recent) > 0:
            health_status = "critical"
        elif len(recent_errors) > 10:
            health_status = "degraded"

        return {
            "status": health_status,
            "recent_error_count": len(recent_errors),
            "critical_recent_errors": len(critical_recent),
            "circuit_breaker_open_count": sum(
                1 for cb in self.circuit_breakers.values()
                if cb.state == CircuitBreakerState.OPEN
            ),
            "overall_stats": self._stats,
            "timestamp": time.time()
        }


# Decorators for easy error handling
def robust_operation(operation_name: str,
                    max_retries: int = 3,
                    fallback_func: Optional[Callable] = None,
                    error_handler: Optional[EnhancedErrorHandler] = None):
    """Decorator for robust operation execution."""

    def decorator(func: Callable):
        async def async_wrapper(*args, **kwargs):
            handler = error_handler or _get_global_error_handler()

            async with handler.handle_errors(
                operation_name=operation_name,
                input_data={"args": args, "kwargs": kwargs},
                fallback_func=fallback_func,
                max_retries=max_retries
            ):
                return await func(*args, **kwargs)

        def sync_wrapper(*args, **kwargs):
            handler = error_handler or _get_global_error_handler()

            # Convert to async for error handling
            async def async_func():
                return func(*args, **kwargs)

            return asyncio.run(async_wrapper(*args, **kwargs))

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator


# Global error handler instance
_global_error_handler: Optional[EnhancedErrorHandler] = None

def _get_global_error_handler() -> EnhancedErrorHandler:
    """Get or create global error handler instance."""
    global _global_error_handler
    if _global_error_handler is None:
        _global_error_handler = EnhancedErrorHandler()
    return _global_error_handler


# Usage examples
async def demo_error_handling():
    """Demonstrate enhanced error handling capabilities."""
    logger = get_logger("agent_etl.error_demo")

    # Create error handler
    error_handler = EnhancedErrorHandler()

    # Register circuit breaker for database operations
    error_handler.register_circuit_breaker(
        "database_query",
        CircuitBreakerConfig(failure_threshold=3, recovery_timeout=30.0)
    )

    # Add custom error pattern
    error_handler.add_error_pattern(
        "api_timeout",
        keywords=["timeout", "request timeout", "deadline exceeded"],
        severity=ErrorSeverity.MEDIUM,
        strategy=RecoveryStrategy.RETRY,
        max_retries=5,
        backoff_multiplier=1.5
    )

    # Demonstrate error handling with fallback
    async def fallback_function(input_data):
        logger.info(f"Executing fallback for data: {input_data}")
        return {"status": "fallback_executed", "data": "default_value"}

    try:
        async with error_handler.handle_errors(
            operation_name="risky_operation",
            input_data={"test": "data"},
            fallback_func=fallback_function,
            max_retries=3
        ):
            # Simulate an operation that might fail
            import random
            if random.random() < 0.7:  # 70% chance of failure
                raise ConnectionError("Simulated network error")

            logger.info("Operation succeeded!")
            return {"status": "success"}

    except Exception as e:
        logger.error(f"Operation ultimately failed: {str(e)}")

    # Get analytics
    analytics = error_handler.get_error_analytics()
    logger.info(f"Error analytics: {json.dumps(analytics, indent=2)}")

    # Health check
    health = await error_handler.health_check()
    logger.info(f"Health status: {health}")


# Example using decorator
@robust_operation("decorated_operation", max_retries=2)
async def example_decorated_function(data: str) -> str:
    """Example function with decorator-based error handling."""
    # Simulate potential failure
    import random
    if random.random() < 0.5:
        raise ValueError(f"Random validation error for data: {data}")

    return f"Processed: {data}"


if __name__ == "__main__":
    asyncio.run(demo_error_handling())
