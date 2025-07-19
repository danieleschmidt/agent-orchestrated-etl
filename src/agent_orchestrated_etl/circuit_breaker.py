"""Circuit breaker patterns for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import functools
import logging
import threading
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, TypeVar

from .exceptions import (
    CircuitBreakerException,
)

F = TypeVar('F', bound=Callable[..., Any])
logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    
    CLOSED = "closed"      # Normal operation, requests are allowed
    OPEN = "open"          # Circuit is open, requests are rejected
    HALF_OPEN = "half_open"  # Testing if service has recovered


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker behavior."""
    
    failure_threshold: int = 5  # Number of failures before opening circuit
    recovery_timeout: float = 60.0  # Time to wait before trying half-open state
    success_threshold: int = 2  # Number of successes in half-open to close circuit
    timeout: float = 30.0  # Request timeout in seconds
    expected_exception: type = Exception  # Exception type that triggers circuit breaker
    
    def __post_init__(self):
        """Validate configuration values."""
        if self.failure_threshold <= 0:
            raise ValueError("failure_threshold must be greater than 0")
        if self.recovery_timeout <= 0:
            raise ValueError("recovery_timeout must be greater than 0")
        if self.success_threshold <= 0:
            raise ValueError("success_threshold must be greater than 0")
        if self.timeout <= 0:
            raise ValueError("timeout must be greater than 0")


class CircuitBreakerStats:
    """Statistics for circuit breaker monitoring."""
    
    def __init__(self):
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rejected_requests = 0
        self.state_changes = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.state_change_times: Dict[CircuitState, float] = {}
    
    def record_success(self):
        """Record a successful request."""
        self.total_requests += 1
        self.successful_requests += 1
        self.last_success_time = time.time()
    
    def record_failure(self):
        """Record a failed request."""
        self.total_requests += 1
        self.failed_requests += 1
        self.last_failure_time = time.time()
    
    def record_rejection(self):
        """Record a rejected request."""
        self.total_requests += 1
        self.rejected_requests += 1
    
    def record_state_change(self, new_state: CircuitState):
        """Record a state change."""
        self.state_changes += 1
        self.state_change_times[new_state] = time.time()
    
    def get_failure_rate(self) -> float:
        """Get the current failure rate."""
        if self.total_requests == 0:
            return 0.0
        return self.failed_requests / self.total_requests
    
    def get_success_rate(self) -> float:
        """Get the current success rate."""
        if self.total_requests == 0:
            return 0.0
        return self.successful_requests / self.total_requests
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary for monitoring."""
        return {
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "rejected_requests": self.rejected_requests,
            "state_changes": self.state_changes,
            "failure_rate": self.get_failure_rate(),
            "success_rate": self.get_success_rate(),
            "last_failure_time": self.last_failure_time,
            "last_success_time": self.last_success_time,
            "state_change_times": {
                state.value: timestamp
                for state, timestamp in self.state_change_times.items()
            }
        }


class CircuitBreaker:
    """Circuit breaker implementation for protecting external services."""
    
    def __init__(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ):
        """Initialize circuit breaker.
        
        Args:
            name: Name of the circuit breaker for identification
            config: Circuit breaker configuration
        """
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time: Optional[float] = None
        self.last_success_time: Optional[float] = None
        self.stats = CircuitBreakerStats()
        self._lock = threading.RLock()
        
        logger.info(f"Circuit breaker '{name}' initialized with config: {self.config}")
    
    def _change_state(self, new_state: CircuitState, reason: str = ""):
        """Change circuit breaker state."""
        if self.state != new_state:
            old_state = self.state
            self.state = new_state
            self.stats.record_state_change(new_state)
            
            logger.info(
                f"Circuit breaker '{self.name}' state changed: "
                f"{old_state.value} -> {new_state.value}"
                + (f" ({reason})" if reason else "")
            )
    
    def _should_attempt_reset(self) -> bool:
        """Check if we should attempt to reset the circuit breaker."""
        if self.state != CircuitState.OPEN:
            return False
        
        if self.last_failure_time is None:
            return True
        
        time_since_failure = time.time() - self.last_failure_time
        return time_since_failure >= self.config.recovery_timeout
    
    def _record_success(self):
        """Record a successful operation."""
        with self._lock:
            self.last_success_time = time.time()
            self.stats.record_success()
            
            if self.state == CircuitState.HALF_OPEN:
                self.success_count += 1
                if self.success_count >= self.config.success_threshold:
                    self._change_state(CircuitState.CLOSED, "success threshold reached")
                    self.failure_count = 0
                    self.success_count = 0
            elif self.state == CircuitState.CLOSED:
                # Reset failure count on success in closed state
                self.failure_count = 0
    
    def _record_failure(self, exception: Exception):
        """Record a failed operation."""
        with self._lock:
            self.last_failure_time = time.time()
            self.stats.record_failure()
            
            if self.state == CircuitState.CLOSED:
                self.failure_count += 1
                if self.failure_count >= self.config.failure_threshold:
                    self._change_state(CircuitState.OPEN, "failure threshold reached")
            elif self.state == CircuitState.HALF_OPEN:
                self._change_state(CircuitState.OPEN, "failure in half-open state")
                self.success_count = 0
    
    def _can_execute(self) -> bool:
        """Check if we can execute a request."""
        with self._lock:
            if self.state == CircuitState.CLOSED:
                return True
            elif self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self._change_state(CircuitState.HALF_OPEN, "attempting reset")
                    return True
                return False
            elif self.state == CircuitState.HALF_OPEN:
                return True
            else:
                return False
    
    def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute a function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerException: If circuit is open
        """
        if not self._can_execute():
            self.stats.record_rejection()
            raise CircuitBreakerException(
                f"Circuit breaker '{self.name}' is open",
                service_name=self.name,
                retry_after=self.config.recovery_timeout,
            )
        
        try:
            # Execute with timeout
            start_time = time.time()
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            
            # Check if execution took too long
            if execution_time > self.config.timeout:
                raise TimeoutError(f"Operation timed out after {execution_time:.2f} seconds")
            
            self._record_success()
            return result
            
        except Exception as e:
            # Only count certain exceptions as circuit breaker failures
            if isinstance(e, (self.config.expected_exception, TimeoutError)):
                self._record_failure(e)
            
            # Re-raise the original exception
            raise
    
    async def call_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute an async function with circuit breaker protection.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerException: If circuit is open
        """
        if not self._can_execute():
            self.stats.record_rejection()
            raise CircuitBreakerException(
                f"Circuit breaker '{self.name}' is open",
                service_name=self.name,
                retry_after=self.config.recovery_timeout,
            )
        
        try:
            # Execute with timeout
            result = await asyncio.wait_for(
                func(*args, **kwargs),
                timeout=self.config.timeout
            )
            
            self._record_success()
            return result
            
        except Exception as e:
            # Only count certain exceptions as circuit breaker failures
            if isinstance(e, (self.config.expected_exception, asyncio.TimeoutError)):
                self._record_failure(e)
            
            # Re-raise the original exception
            raise
    
    def reset(self):
        """Manually reset the circuit breaker to closed state."""
        with self._lock:
            self._change_state(CircuitState.CLOSED, "manual reset")
            self.failure_count = 0
            self.success_count = 0
    
    def force_open(self):
        """Manually force the circuit breaker to open state."""
        with self._lock:
            self._change_state(CircuitState.OPEN, "manual force open")
    
    def get_state(self) -> CircuitState:
        """Get current circuit breaker state."""
        return self.state
    
    def get_stats(self) -> Dict[str, Any]:
        """Get circuit breaker statistics."""
        with self._lock:
            return {
                "name": self.name,
                "state": self.state.value,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "config": {
                    "failure_threshold": self.config.failure_threshold,
                    "recovery_timeout": self.config.recovery_timeout,
                    "success_threshold": self.config.success_threshold,
                    "timeout": self.config.timeout,
                },
                **self.stats.to_dict()
            }


class CircuitBreakerRegistry:
    """Registry for managing multiple circuit breakers."""
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._lock = threading.RLock()
    
    def get_or_create(
        self,
        name: str,
        config: Optional[CircuitBreakerConfig] = None,
    ) -> CircuitBreaker:
        """Get or create a circuit breaker.
        
        Args:
            name: Name of the circuit breaker
            config: Configuration for new circuit breakers
            
        Returns:
            Circuit breaker instance
        """
        with self._lock:
            if name not in self._breakers:
                self._breakers[name] = CircuitBreaker(name, config)
            return self._breakers[name]
    
    def get(self, name: str) -> Optional[CircuitBreaker]:
        """Get a circuit breaker by name.
        
        Args:
            name: Name of the circuit breaker
            
        Returns:
            Circuit breaker instance or None if not found
        """
        return self._breakers.get(name)
    
    def remove(self, name: str) -> bool:
        """Remove a circuit breaker.
        
        Args:
            name: Name of the circuit breaker
            
        Returns:
            True if removed, False if not found
        """
        with self._lock:
            if name in self._breakers:
                del self._breakers[name]
                return True
            return False
    
    def list_breakers(self) -> List[str]:
        """List all circuit breaker names."""
        return list(self._breakers.keys())
    
    def get_all_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all circuit breakers."""
        with self._lock:
            return {
                name: breaker.get_stats()
                for name, breaker in self._breakers.items()
            }
    
    def reset_all(self):
        """Reset all circuit breakers."""
        with self._lock:
            for breaker in self._breakers.values():
                breaker.reset()


# Global circuit breaker registry
circuit_breaker_registry = CircuitBreakerRegistry()


def circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    success_threshold: int = 2,
    timeout: float = 30.0,
) -> Callable[[F], F]:
    """Decorator to add circuit breaker protection to functions.
    
    Args:
        name: Name of the circuit breaker
        config: Circuit breaker configuration (if provided, other args are ignored)
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before trying half-open state
        success_threshold: Number of successes in half-open to close circuit
        timeout: Request timeout in seconds
        
    Returns:
        Decorated function with circuit breaker protection
    """
    if config is None:
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            success_threshold=success_threshold,
            timeout=timeout,
        )
    
    def decorator(func: F) -> F:
        breaker = circuit_breaker_registry.get_or_create(name, config)
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return breaker.call(func, *args, **kwargs)
        
        return wrapper
    
    return decorator


def async_circuit_breaker(
    name: str,
    config: Optional[CircuitBreakerConfig] = None,
    failure_threshold: int = 5,
    recovery_timeout: float = 60.0,
    success_threshold: int = 2,
    timeout: float = 30.0,
) -> Callable[[F], F]:
    """Decorator to add circuit breaker protection to async functions.
    
    Args:
        name: Name of the circuit breaker
        config: Circuit breaker configuration (if provided, other args are ignored)
        failure_threshold: Number of failures before opening circuit
        recovery_timeout: Time to wait before trying half-open state
        success_threshold: Number of successes in half-open to close circuit
        timeout: Request timeout in seconds
        
    Returns:
        Decorated async function with circuit breaker protection
    """
    if config is None:
        config = CircuitBreakerConfig(
            failure_threshold=failure_threshold,
            recovery_timeout=recovery_timeout,
            success_threshold=success_threshold,
            timeout=timeout,
        )
    
    def decorator(func: F) -> F:
        breaker = circuit_breaker_registry.get_or_create(name, config)
        
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            return await breaker.call_async(func, *args, **kwargs)
        
        return wrapper
    
    return decorator


# Predefined circuit breaker configurations
class CircuitBreakerConfigs:
    """Predefined circuit breaker configurations for common scenarios."""
    
    # Fast-failing circuit breaker for quick operations
    FAST = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=30.0,
        success_threshold=2,
        timeout=5.0,
    )
    
    # Standard circuit breaker for normal operations
    STANDARD = CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=60.0,
        success_threshold=2,
        timeout=30.0,
    )
    
    # Tolerant circuit breaker for unreliable services
    TOLERANT = CircuitBreakerConfig(
        failure_threshold=10,
        recovery_timeout=120.0,
        success_threshold=3,
        timeout=60.0,
    )
    
    # Database circuit breaker
    DATABASE = CircuitBreakerConfig(
        failure_threshold=5,
        recovery_timeout=30.0,
        success_threshold=2,
        timeout=10.0,
        expected_exception=Exception,
    )
    
    # External API circuit breaker
    EXTERNAL_API = CircuitBreakerConfig(
        failure_threshold=3,
        recovery_timeout=60.0,
        success_threshold=2,
        timeout=30.0,
        expected_exception=Exception,
    )