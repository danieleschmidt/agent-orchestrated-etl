"""Graceful degradation strategies for Agent-Orchestrated ETL."""

from __future__ import annotations

import functools
import logging
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

from .exceptions import (
    AgentETLException,
    ExternalServiceException,
    NetworkException,
    ErrorSeverity,
)

F = TypeVar('F', bound=Callable[..., Any])
logger = logging.getLogger(__name__)


class DegradationLevel(Enum):
    """Levels of service degradation."""
    
    FULL = "full"              # Full functionality
    REDUCED = "reduced"        # Reduced functionality with fallbacks
    MINIMAL = "minimal"        # Minimal functionality, essential features only
    EMERGENCY = "emergency"    # Emergency mode, basic operations only
    OFFLINE = "offline"        # Service unavailable


class FallbackStrategy(Enum):
    """Strategies for handling failures."""
    
    RETURN_DEFAULT = "return_default"        # Return a default value
    RETURN_CACHED = "return_cached"          # Return cached result
    RETURN_PARTIAL = "return_partial"        # Return partial result
    SKIP_OPERATION = "skip_operation"        # Skip the operation entirely
    USE_ALTERNATIVE = "use_alternative"      # Use alternative implementation
    FAIL_GRACEFULLY = "fail_gracefully"     # Fail with user-friendly message


@dataclass
class DegradationConfig:
    """Configuration for graceful degradation."""
    
    enabled: bool = True
    default_level: DegradationLevel = DegradationLevel.FULL
    fallback_strategy: FallbackStrategy = FallbackStrategy.RETURN_DEFAULT
    max_degradation_time: float = 300.0  # Maximum time to stay in degraded state
    auto_recovery: bool = True
    recovery_check_interval: float = 60.0  # How often to check for recovery
    failure_threshold: int = 3  # Number of failures before degrading
    success_threshold: int = 2  # Number of successes before upgrading
    
    # Fallback values
    default_value: Any = None
    cache_ttl: float = 300.0  # Cache time-to-live in seconds
    partial_result_threshold: float = 0.5  # Minimum completion rate for partial results


class DegradationContext:
    """Context for tracking degradation state."""
    
    def __init__(self, service_name: str, config: DegradationConfig):
        self.service_name = service_name
        self.config = config
        self.current_level = config.default_level
        self.failure_count = 0
        self.success_count = 0
        self.degradation_start_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.last_failure_time: Optional[float] = None
        self.cache: Dict[str, Any] = {}
        self.cache_timestamps: Dict[str, float] = {}
    
    def record_success(self):
        """Record a successful operation."""
        self.success_count += 1
        self.failure_count = 0  # Reset failure count on success
        self.last_success_time = time.time()
        
        # Check if we can upgrade service level
        if (self.current_level != DegradationLevel.FULL and 
            self.success_count >= self.config.success_threshold):
            self._upgrade_service_level()
    
    def record_failure(self, exception: Exception):
        """Record a failed operation."""
        self.failure_count += 1
        self.success_count = 0  # Reset success count on failure
        self.last_failure_time = time.time()
        
        # Check if we need to degrade service level
        if self.failure_count >= self.config.failure_threshold:
            self._degrade_service_level(exception)
    
    def _degrade_service_level(self, exception: Exception):
        """Degrade the service level based on failure patterns."""
        current_index = list(DegradationLevel).index(self.current_level)
        
        # Determine degradation severity based on exception type
        if isinstance(exception, (NetworkException, ExternalServiceException)):
            # Network issues - degrade gradually
            new_index = min(current_index + 1, len(DegradationLevel) - 2)
        elif isinstance(exception, AgentETLException) and exception.severity == ErrorSeverity.CRITICAL:
            # Critical errors - degrade more aggressively
            new_index = min(current_index + 2, len(DegradationLevel) - 1)
        else:
            # Other errors - standard degradation
            new_index = min(current_index + 1, len(DegradationLevel) - 1)
        
        new_level = list(DegradationLevel)[new_index]
        
        if new_level != self.current_level:
            old_level = self.current_level
            self.current_level = new_level
            self.degradation_start_time = time.time()
            self.failure_count = 0  # Reset counter after degradation
            
            logger.warning(
                f"Service '{self.service_name}' degraded from {old_level.value} "
                f"to {new_level.value} due to {type(exception).__name__}: {exception}"
            )
    
    def _upgrade_service_level(self):
        """Upgrade the service level after successful operations."""
        current_index = list(DegradationLevel).index(self.current_level)
        
        if current_index > 0:
            new_level = list(DegradationLevel)[current_index - 1]
            old_level = self.current_level
            self.current_level = new_level
            self.success_count = 0  # Reset counter after upgrade
            
            if new_level == DegradationLevel.FULL:
                self.degradation_start_time = None
            
            logger.info(
                f"Service '{self.service_name}' upgraded from {old_level.value} "
                f"to {new_level.value}"
            )
    
    def should_auto_recover(self) -> bool:
        """Check if we should attempt auto-recovery."""
        if not self.config.auto_recovery:
            return False
        
        if self.degradation_start_time is None:
            return False
        
        # Check if we've been degraded too long
        degradation_time = time.time() - self.degradation_start_time
        if degradation_time > self.config.max_degradation_time:
            return True
        
        # Check if it's time for a recovery attempt
        if self.last_success_time:
            time_since_success = time.time() - self.last_success_time
            return time_since_success >= self.config.recovery_check_interval
        
        return False
    
    def cache_result(self, key: str, value: Any):
        """Cache a result for fallback use."""
        self.cache[key] = value
        self.cache_timestamps[key] = time.time()
    
    def get_cached_result(self, key: str) -> Optional[Any]:
        """Get a cached result if it's still valid."""
        if key not in self.cache:
            return None
        
        cache_time = self.cache_timestamps[key]
        if time.time() - cache_time > self.config.cache_ttl:
            # Cache expired
            del self.cache[key]
            del self.cache_timestamps[key]
            return None
        
        return self.cache[key]
    
    def get_status(self) -> Dict[str, Any]:
        """Get current degradation status."""
        return {
            "service_name": self.service_name,
            "current_level": self.current_level.value,
            "failure_count": self.failure_count,
            "success_count": self.success_count,
            "degradation_start_time": self.degradation_start_time,
            "last_success_time": self.last_success_time,
            "last_failure_time": self.last_failure_time,
            "cache_entries": len(self.cache),
            "auto_recovery_due": self.should_auto_recover(),
        }


class FallbackHandler(ABC):
    """Abstract base class for fallback handlers."""
    
    @abstractmethod
    def handle_failure(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        """Handle a failure with appropriate fallback strategy."""
        pass
    
    @abstractmethod
    def can_handle(self, context: DegradationContext, exception: Exception) -> bool:
        """Check if this handler can handle the given failure."""
        pass


class DefaultValueHandler(FallbackHandler):
    """Handler that returns a default value on failure."""
    
    def can_handle(self, context: DegradationContext, exception: Exception) -> bool:
        return context.config.fallback_strategy == FallbackStrategy.RETURN_DEFAULT
    
    def handle_failure(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        logger.info(
            f"Returning default value for {context.service_name} due to {type(exception).__name__}"
        )
        return context.config.default_value


class CachedResultHandler(FallbackHandler):
    """Handler that returns cached results on failure."""
    
    def can_handle(self, context: DegradationContext, exception: Exception) -> bool:
        return context.config.fallback_strategy == FallbackStrategy.RETURN_CACHED
    
    def handle_failure(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        # Generate cache key from function arguments
        cache_key = f"{context.service_name}:{hash(str(args) + str(kwargs))}"
        cached_result = context.get_cached_result(cache_key)
        
        if cached_result is not None:
            logger.info(
                f"Returning cached result for {context.service_name} due to {type(exception).__name__}"
            )
            return cached_result
        
        # Fall back to default value if no cache available
        logger.warning(
            f"No cached result available for {context.service_name}, returning default value"
        )
        return context.config.default_value


class PartialResultHandler(FallbackHandler):
    """Handler that returns partial results when possible."""
    
    def can_handle(self, context: DegradationContext, exception: Exception) -> bool:
        return context.config.fallback_strategy == FallbackStrategy.RETURN_PARTIAL
    
    def handle_failure(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        # Check if the exception contains partial results
        if hasattr(exception, 'partial_results'):
            partial_results = exception.partial_results
            completion_rate = getattr(exception, 'completion_rate', 0.0)
            
            if completion_rate >= context.config.partial_result_threshold:
                logger.info(
                    f"Returning partial results for {context.service_name} "
                    f"({completion_rate:.1%} complete)"
                )
                return partial_results
        
        # Fall back to cached or default value
        cache_key = f"{context.service_name}:{hash(str(args) + str(kwargs))}"
        cached_result = context.get_cached_result(cache_key)
        
        if cached_result is not None:
            return cached_result
        
        return context.config.default_value


class SkipOperationHandler(FallbackHandler):
    """Handler that skips operations gracefully."""
    
    def can_handle(self, context: DegradationContext, exception: Exception) -> bool:
        return context.config.fallback_strategy == FallbackStrategy.SKIP_OPERATION
    
    def handle_failure(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        logger.info(
            f"Skipping operation for {context.service_name} due to {type(exception).__name__}"
        )
        return None


class GracefulDegradationManager:
    """Manager for graceful degradation across services."""
    
    def __init__(self):
        self._contexts: Dict[str, DegradationContext] = {}
        self._handlers: List[FallbackHandler] = [
            DefaultValueHandler(),
            CachedResultHandler(),
            PartialResultHandler(),
            SkipOperationHandler(),
        ]
    
    def get_or_create_context(
        self,
        service_name: str,
        config: Optional[DegradationConfig] = None
    ) -> DegradationContext:
        """Get or create a degradation context for a service."""
        if service_name not in self._contexts:
            self._contexts[service_name] = DegradationContext(
                service_name,
                config or DegradationConfig()
            )
        return self._contexts[service_name]
    
    def execute_with_degradation(
        self,
        service_name: str,
        func: Callable,
        *args,
        config: Optional[DegradationConfig] = None,
        **kwargs
    ) -> Any:
        """Execute a function with graceful degradation."""
        context = self.get_or_create_context(service_name, config)
        
        # Check if we should skip execution based on degradation level
        if context.current_level == DegradationLevel.OFFLINE:
            logger.warning(f"Service {service_name} is offline, using fallback")
            return self._handle_fallback(context, Exception("Service offline"), *args, **kwargs)
        
        try:
            # Execute the function
            result = func(*args, **kwargs)
            
            # Cache successful results
            cache_key = f"{service_name}:{hash(str(args) + str(kwargs))}"
            context.cache_result(cache_key, result)
            
            # Record success
            context.record_success()
            
            return result
            
        except Exception as e:
            # Record failure
            context.record_failure(e)
            
            # Handle fallback
            return self._handle_fallback(context, e, *args, **kwargs)
    
    def _handle_fallback(
        self,
        context: DegradationContext,
        exception: Exception,
        *args,
        **kwargs
    ) -> Any:
        """Handle fallback using appropriate strategy."""
        for handler in self._handlers:
            if handler.can_handle(context, exception):
                return handler.handle_failure(context, exception, *args, **kwargs)
        
        # No suitable handler found, re-raise exception
        logger.error(f"No fallback handler available for {context.service_name}")
        raise exception
    
    def get_service_status(self, service_name: str) -> Optional[Dict[str, Any]]:
        """Get status for a specific service."""
        context = self._contexts.get(service_name)
        return context.get_status() if context else None
    
    def get_all_service_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status for all services."""
        return {
            name: context.get_status()
            for name, context in self._contexts.items()
        }
    
    def reset_service(self, service_name: str):
        """Reset a service to full functionality."""
        if service_name in self._contexts:
            context = self._contexts[service_name]
            context.current_level = DegradationLevel.FULL
            context.failure_count = 0
            context.success_count = 0
            context.degradation_start_time = None
            logger.info(f"Service {service_name} reset to full functionality")
    
    def force_degradation(self, service_name: str, level: DegradationLevel):
        """Force a service to a specific degradation level."""
        context = self.get_or_create_context(service_name)
        old_level = context.current_level
        context.current_level = level
        context.degradation_start_time = time.time()
        logger.warning(f"Service {service_name} forced from {old_level.value} to {level.value}")


# Global degradation manager
degradation_manager = GracefulDegradationManager()


def with_graceful_degradation(
    service_name: str,
    config: Optional[DegradationConfig] = None,
    fallback_strategy: FallbackStrategy = FallbackStrategy.RETURN_DEFAULT,
    default_value: Any = None,
) -> Callable[[F], F]:
    """Decorator to add graceful degradation to functions.
    
    Args:
        service_name: Name of the service for tracking
        config: Degradation configuration
        fallback_strategy: Strategy for handling failures
        default_value: Default value to return on failure
        
    Returns:
        Decorated function with graceful degradation
    """
    if config is None:
        config = DegradationConfig(
            fallback_strategy=fallback_strategy,
            default_value=default_value,
        )
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return degradation_manager.execute_with_degradation(
                service_name, func, *args, config=config, **kwargs
            )
        
        return wrapper
    
    return decorator


# Predefined degradation configurations
class DegradationConfigs:
    """Predefined degradation configurations for common scenarios."""
    
    # Conservative degradation for critical services
    CONSERVATIVE = DegradationConfig(
        failure_threshold=5,
        success_threshold=3,
        max_degradation_time=600.0,
        fallback_strategy=FallbackStrategy.RETURN_CACHED,
        cache_ttl=600.0,
    )
    
    # Aggressive degradation for non-critical services
    AGGRESSIVE = DegradationConfig(
        failure_threshold=2,
        success_threshold=1,
        max_degradation_time=300.0,
        fallback_strategy=FallbackStrategy.RETURN_DEFAULT,
    )
    
    # External service degradation
    EXTERNAL_SERVICE = DegradationConfig(
        failure_threshold=3,
        success_threshold=2,
        max_degradation_time=900.0,
        fallback_strategy=FallbackStrategy.RETURN_CACHED,
        cache_ttl=900.0,
        recovery_check_interval=120.0,
    )
    
    # Database degradation
    DATABASE = DegradationConfig(
        failure_threshold=5,
        success_threshold=3,
        max_degradation_time=300.0,
        fallback_strategy=FallbackStrategy.RETURN_CACHED,
        cache_ttl=300.0,
    )
    
    # Data processing degradation
    DATA_PROCESSING = DegradationConfig(
        failure_threshold=3,
        success_threshold=2,
        max_degradation_time=600.0,
        fallback_strategy=FallbackStrategy.RETURN_PARTIAL,
        partial_result_threshold=0.7,
    )