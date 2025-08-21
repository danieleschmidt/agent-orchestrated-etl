"""Enhanced error recovery system with intelligent fault detection and auto-healing."""

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .exceptions import DataProcessingException, PipelineExecutionException
from .logging_config import get_logger


class FailureType(Enum):
    """Types of failures that can occur in the pipeline."""
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    RESOURCE = "resource"
    NETWORK = "network"
    DATA_QUALITY = "data_quality"
    AUTHENTICATION = "authentication"
    TIMEOUT = "timeout"


class RecoveryStrategy(Enum):
    """Recovery strategies for different failure types."""
    RETRY = "retry"
    FALLBACK = "fallback"
    CIRCUIT_BREAKER = "circuit_breaker"
    GRACEFUL_DEGRADATION = "graceful_degradation"
    ESCALATE = "escalate"
    SKIP = "skip"


@dataclass
class FailureContext:
    """Context information about a failure."""
    failure_type: FailureType
    error: Exception
    component: str
    timestamp: float = field(default_factory=time.time)
    attempt_count: int = 0
    context_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RecoveryAction:
    """Action to take for recovery."""
    strategy: RecoveryStrategy
    parameters: Dict[str, Any] = field(default_factory=dict)
    timeout: Optional[float] = None
    fallback_action: Optional['RecoveryAction'] = None


class IntelligentFailureDetector:
    """AI-powered failure detection and classification."""

    def __init__(self):
        self.logger = get_logger("agent_etl.error_recovery.detector")
        self.failure_patterns: Dict[str, FailureType] = {
            "connection reset": FailureType.NETWORK,
            "timeout": FailureType.TIMEOUT,
            "memory": FailureType.RESOURCE,
            "disk space": FailureType.RESOURCE,
            "authentication": FailureType.AUTHENTICATION,
            "authorization": FailureType.AUTHENTICATION,
            "rate limit": FailureType.TRANSIENT,
            "temporary": FailureType.TRANSIENT,
            "data quality": FailureType.DATA_QUALITY,
            "validation": FailureType.DATA_QUALITY,
            "permission": FailureType.PERMANENT,
            "not found": FailureType.PERMANENT,
        }

    def detect_failure_type(self, error: Exception, context: Dict[str, Any]) -> FailureType:
        """Detect the type of failure based on error and context."""
        error_message = str(error).lower()
        error_type = type(error).__name__.lower()

        # Pattern-based detection
        for pattern, failure_type in self.failure_patterns.items():
            if pattern in error_message or pattern in error_type:
                self.logger.info(f"Detected {failure_type.value} failure: {pattern}")
                return failure_type

        # Context-based detection
        if "network" in context:
            return FailureType.NETWORK
        elif "resource" in context:
            return FailureType.RESOURCE
        elif "data" in context:
            return FailureType.DATA_QUALITY

        # Default to transient for unknown errors
        self.logger.warning(f"Unknown failure type, defaulting to transient: {error}")
        return FailureType.TRANSIENT


class AdaptiveRecoveryEngine:
    """Adaptive recovery engine that learns from failures."""

    def __init__(self):
        self.logger = get_logger("agent_etl.error_recovery.engine")
        self.detector = IntelligentFailureDetector()
        self.recovery_history: List[FailureContext] = []
        self.success_rates: Dict[str, float] = {}

        # Recovery strategy mappings
        self.strategy_mapping = {
            FailureType.TRANSIENT: RecoveryStrategy.RETRY,
            FailureType.NETWORK: RecoveryStrategy.RETRY,
            FailureType.TIMEOUT: RecoveryStrategy.RETRY,
            FailureType.RESOURCE: RecoveryStrategy.GRACEFUL_DEGRADATION,
            FailureType.DATA_QUALITY: RecoveryStrategy.FALLBACK,
            FailureType.AUTHENTICATION: RecoveryStrategy.ESCALATE,
            FailureType.PERMANENT: RecoveryStrategy.SKIP,
        }

    def analyze_failure(self, error: Exception, component: str, context: Dict[str, Any]) -> FailureContext:
        """Analyze a failure and create failure context."""
        failure_type = self.detector.detect_failure_type(error, context)

        failure_context = FailureContext(
            failure_type=failure_type,
            error=error,
            component=component,
            context_data=context
        )

        # Count previous attempts for this component
        failure_context.attempt_count = sum(
            1 for fc in self.recovery_history
            if fc.component == component and fc.failure_type == failure_type
        )

        self.recovery_history.append(failure_context)
        return failure_context

    def get_recovery_action(self, failure_context: FailureContext) -> RecoveryAction:
        """Get the appropriate recovery action for a failure."""
        base_strategy = self.strategy_mapping[failure_context.failure_type]

        # Adapt strategy based on history
        if failure_context.attempt_count > 3:
            if base_strategy == RecoveryStrategy.RETRY:
                base_strategy = RecoveryStrategy.CIRCUIT_BREAKER
            elif base_strategy == RecoveryStrategy.FALLBACK:
                base_strategy = RecoveryStrategy.GRACEFUL_DEGRADATION

        # Create recovery action with parameters
        action = RecoveryAction(strategy=base_strategy)

        if base_strategy == RecoveryStrategy.RETRY:
            action.parameters = {
                "max_attempts": 3,
                "backoff_multiplier": 2.0,
                "initial_delay": 1.0,
                "jitter": True
            }
            action.timeout = 30.0

        elif base_strategy == RecoveryStrategy.CIRCUIT_BREAKER:
            action.parameters = {
                "failure_threshold": 5,
                "recovery_timeout": 60.0,
                "half_open_max_calls": 3
            }

        elif base_strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
            action.parameters = {
                "reduced_functionality": True,
                "performance_limit": 0.5
            }

        elif base_strategy == RecoveryStrategy.FALLBACK:
            action.parameters = {
                "fallback_source": "backup",
                "quality_threshold": 0.7
            }

        self.logger.info(f"Recovery action for {failure_context.component}: {action.strategy.value}")
        return action

    async def execute_recovery(self, action: RecoveryAction, operation: Callable, *args, **kwargs) -> Any:
        """Execute a recovery action."""
        try:
            if action.strategy == RecoveryStrategy.RETRY:
                return await self._execute_retry(action, operation, *args, **kwargs)
            elif action.strategy == RecoveryStrategy.FALLBACK:
                return await self._execute_fallback(action, operation, *args, **kwargs)
            elif action.strategy == RecoveryStrategy.CIRCUIT_BREAKER:
                return await self._execute_circuit_breaker(action, operation, *args, **kwargs)
            elif action.strategy == RecoveryStrategy.GRACEFUL_DEGRADATION:
                return await self._execute_graceful_degradation(action, operation, *args, **kwargs)
            elif action.strategy == RecoveryStrategy.SKIP:
                self.logger.info("Skipping operation due to permanent failure")
                return None
            elif action.strategy == RecoveryStrategy.ESCALATE:
                raise PipelineExecutionException("Escalating failure - manual intervention required")
            else:
                raise ValueError(f"Unknown recovery strategy: {action.strategy}")

        except Exception as e:
            if action.fallback_action:
                self.logger.warning(f"Primary recovery failed, trying fallback: {e}")
                return await self.execute_recovery(action.fallback_action, operation, *args, **kwargs)
            raise

    async def _execute_retry(self, action: RecoveryAction, operation: Callable, *args, **kwargs) -> Any:
        """Execute retry recovery strategy."""
        params = action.parameters
        max_attempts = params.get("max_attempts", 3)
        backoff_multiplier = params.get("backoff_multiplier", 2.0)
        initial_delay = params.get("initial_delay", 1.0)
        jitter = params.get("jitter", True)

        for attempt in range(max_attempts):
            try:
                if asyncio.iscoroutinefunction(operation):
                    result = await operation(*args, **kwargs)
                else:
                    result = operation(*args, **kwargs)

                self.logger.info(f"Retry successful on attempt {attempt + 1}")
                return result

            except Exception as e:
                if attempt == max_attempts - 1:
                    self.logger.error(f"All retry attempts failed: {e}")
                    raise

                # Calculate delay with backoff and jitter
                delay = initial_delay * (backoff_multiplier ** attempt)
                if jitter:
                    import random
                    delay *= (0.5 + random.random() * 0.5)

                self.logger.warning(f"Retry attempt {attempt + 1} failed, waiting {delay:.2f}s: {e}")
                await asyncio.sleep(delay)

    async def _execute_fallback(self, action: RecoveryAction, operation: Callable, *args, **kwargs) -> Any:
        """Execute fallback recovery strategy."""
        params = action.parameters
        fallback_source = params.get("fallback_source", "backup")

        # Modify kwargs to use fallback source
        fallback_kwargs = kwargs.copy()
        fallback_kwargs["source"] = fallback_source
        fallback_kwargs["fallback_mode"] = True

        self.logger.info(f"Executing fallback with source: {fallback_source}")

        if asyncio.iscoroutinefunction(operation):
            return await operation(*args, **fallback_kwargs)
        else:
            return operation(*args, **fallback_kwargs)

    async def _execute_circuit_breaker(self, action: RecoveryAction, operation: Callable, *args, **kwargs) -> Any:
        """Execute circuit breaker recovery strategy."""
        params = action.parameters
        failure_threshold = params.get("failure_threshold", 5)
        recovery_timeout = params.get("recovery_timeout", 60.0)

        # Simple circuit breaker implementation
        component_key = kwargs.get("component", "default")
        failure_count = sum(
            1 for fc in self.recovery_history[-failure_threshold:]
            if fc.component == component_key
        )

        if failure_count >= failure_threshold:
            self.logger.warning(f"Circuit breaker open for {component_key}")
            raise PipelineExecutionException(f"Circuit breaker open for {component_key}")

        try:
            if asyncio.iscoroutinefunction(operation):
                return await operation(*args, **kwargs)
            else:
                return operation(*args, **kwargs)
        except Exception as e:
            self.logger.error(f"Circuit breaker detected failure: {e}")
            raise

    async def _execute_graceful_degradation(self, action: RecoveryAction, operation: Callable, *args, **kwargs) -> Any:
        """Execute graceful degradation recovery strategy."""
        params = action.parameters
        performance_limit = params.get("performance_limit", 0.5)

        # Modify operation parameters for reduced functionality
        degraded_kwargs = kwargs.copy()
        degraded_kwargs["performance_limit"] = performance_limit
        degraded_kwargs["degraded_mode"] = True

        if "batch_size" in degraded_kwargs:
            degraded_kwargs["batch_size"] = int(degraded_kwargs["batch_size"] * performance_limit)

        self.logger.info(f"Executing with graceful degradation (limit: {performance_limit})")

        if asyncio.iscoroutinefunction(operation):
            return await operation(*args, **degraded_kwargs)
        else:
            return operation(*args, **degraded_kwargs)


class SelfHealingPipeline:
    """Self-healing pipeline that automatically recovers from failures."""

    def __init__(self):
        self.logger = get_logger("agent_etl.error_recovery.pipeline")
        self.recovery_engine = AdaptiveRecoveryEngine()
        self.health_checks: Dict[str, Callable] = {}
        self.monitoring_active = True

    def register_health_check(self, component: str, check_function: Callable) -> None:
        """Register a health check for a component."""
        self.health_checks[component] = check_function
        self.logger.info(f"Registered health check for {component}")

    async def execute_with_recovery(
        self,
        operation: Callable,
        component: str,
        context: Optional[Dict[str, Any]] = None,
        *args,
        **kwargs
    ) -> Any:
        """Execute an operation with automatic recovery."""
        context = context or {}
        context["component"] = component

        try:
            # Pre-execution health check
            if component in self.health_checks:
                await self._run_health_check(component)

            # Execute operation
            if asyncio.iscoroutinefunction(operation):
                result = await operation(*args, **kwargs)
            else:
                result = operation(*args, **kwargs)

            self.logger.info(f"Operation {component} completed successfully")
            return result

        except Exception as error:
            self.logger.error(f"Operation {component} failed: {error}")

            # Analyze failure and get recovery action
            failure_context = self.recovery_engine.analyze_failure(error, component, context)
            recovery_action = self.recovery_engine.get_recovery_action(failure_context)

            # Execute recovery
            return await self.recovery_engine.execute_recovery(
                recovery_action, operation, *args, **kwargs
            )

    async def _run_health_check(self, component: str) -> None:
        """Run health check for a component."""
        health_check = self.health_checks[component]

        try:
            if asyncio.iscoroutinefunction(health_check):
                healthy = await health_check()
            else:
                healthy = health_check()

            if not healthy:
                raise DataProcessingException(f"Health check failed for {component}")

        except Exception as e:
            self.logger.error(f"Health check failed for {component}: {e}")
            raise

    async def monitor_pipeline_health(self) -> None:
        """Continuously monitor pipeline health."""
        while self.monitoring_active:
            try:
                for component in self.health_checks:
                    await self._run_health_check(component)

                await asyncio.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Health monitoring error: {e}")
                await asyncio.sleep(60)  # Wait longer on error

    def get_recovery_statistics(self) -> Dict[str, Any]:
        """Get statistics about recovery operations."""
        history = self.recovery_engine.recovery_history

        if not history:
            return {"total_failures": 0, "recovery_rate": 0.0}

        failure_types = {}
        component_failures = {}

        for failure in history:
            failure_types[failure.failure_type.value] = failure_types.get(failure.failure_type.value, 0) + 1
            component_failures[failure.component] = component_failures.get(failure.component, 0) + 1

        return {
            "total_failures": len(history),
            "failure_types": failure_types,
            "component_failures": component_failures,
            "recent_failures": len([f for f in history if time.time() - f.timestamp < 3600]),  # Last hour
        }


# Export main classes
__all__ = [
    "SelfHealingPipeline",
    "AdaptiveRecoveryEngine",
    "IntelligentFailureDetector",
    "FailureType",
    "RecoveryStrategy",
    "FailureContext",
    "RecoveryAction"
]
