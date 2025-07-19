"""Retry mechanisms with exponential backoff for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import functools
import logging
import random
import time
from typing import Any, Callable, List, Optional, Type, TypeVar, Union

from .exceptions import (
    AgentETLException,
    NetworkException,
    ExternalServiceException,
    RateLimitException,
    CircuitBreakerException,
    is_retryable_exception,
    get_retry_delay,
)

F = TypeVar('F', bound=Callable[..., Any])
logger = logging.getLogger(__name__)


class RetryConfig:
    """Configuration for retry behavior."""
    
    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        exponential_base: float = 2.0,
        jitter: bool = True,
        jitter_range: float = 0.1,
        retryable_exceptions: Optional[List[Type[Exception]]] = None,
        backoff_strategy: str = "exponential",
    ):
        """Initialize retry configuration.
        
        Args:
            max_attempts: Maximum number of retry attempts
            base_delay: Base delay in seconds for the first retry
            max_delay: Maximum delay between retries
            exponential_base: Base for exponential backoff calculation
            jitter: Whether to add random jitter to delays
            jitter_range: Range for jitter (0.0 to 1.0)
            retryable_exceptions: List of exception types to retry
            backoff_strategy: Strategy for calculating delays ("exponential", "linear", "fixed")
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.exponential_base = exponential_base
        self.jitter = jitter
        self.jitter_range = jitter_range
        self.retryable_exceptions = retryable_exceptions or [
            NetworkException,
            ExternalServiceException,
            RateLimitException,
            ConnectionError,
            TimeoutError,
        ]
        self.backoff_strategy = backoff_strategy
    
    def is_retryable(self, exception: Exception) -> bool:
        """Check if an exception is retryable based on configuration."""
        # Check our custom exception logic first
        if is_retryable_exception(exception):
            return True
        
        # Check against configured exception types
        return any(isinstance(exception, exc_type) for exc_type in self.retryable_exceptions)
    
    def calculate_delay(self, attempt: int, exception: Optional[Exception] = None) -> float:
        """Calculate delay for a retry attempt.
        
        Args:
            attempt: The attempt number (1-based)
            exception: The exception that triggered the retry
            
        Returns:
            Delay in seconds
        """
        # If exception specifies a retry delay, use it
        if exception:
            custom_delay = get_retry_delay(exception)
            if custom_delay is not None:
                return min(custom_delay, self.max_delay)
        
        # Calculate delay based on strategy
        if self.backoff_strategy == "exponential":
            delay = self.base_delay * (self.exponential_base ** (attempt - 1))
        elif self.backoff_strategy == "linear":
            delay = self.base_delay * attempt
        elif self.backoff_strategy == "fixed":
            delay = self.base_delay
        else:
            raise ValueError(f"Unknown backoff strategy: {self.backoff_strategy}")
        
        # Apply maximum delay limit
        delay = min(delay, self.max_delay)
        
        # Add jitter if enabled
        if self.jitter:
            jitter_amount = delay * self.jitter_range * (2 * random.random() - 1)
            delay = max(0, delay + jitter_amount)
        
        return delay


class RetryContext:
    """Context information for retry operations."""
    
    def __init__(self, config: RetryConfig, function_name: str = "unknown"):
        self.config = config
        self.function_name = function_name
        self.attempt = 0
        self.total_delay = 0.0
        self.start_time = time.time()
        self.exceptions: List[Exception] = []
    
    def record_attempt(self, exception: Exception, delay: float) -> None:
        """Record a failed attempt."""
        self.attempt += 1
        self.total_delay += delay
        self.exceptions.append(exception)
        
        logger.warning(
            f"Retry attempt {self.attempt}/{self.config.max_attempts} for {self.function_name} "
            f"failed with {type(exception).__name__}: {exception}. "
            f"Retrying in {delay:.2f} seconds."
        )
    
    def should_retry(self, exception: Exception) -> bool:
        """Check if we should retry based on current state."""
        if self.attempt >= self.config.max_attempts:
            return False
        
        return self.config.is_retryable(exception)
    
    def get_final_exception(self) -> Exception:
        """Get the final exception to raise after all retries failed."""
        if not self.exceptions:
            return RuntimeError("No exceptions recorded in retry context")
        
        last_exception = self.exceptions[-1]
        
        # If it's our custom exception, add retry context
        if isinstance(last_exception, AgentETLException):
            last_exception.context.update({
                "retry_attempts": self.attempt,
                "total_retry_delay": self.total_delay,
                "retry_duration": time.time() - self.start_time,
            })
        
        return last_exception


def retry(
    config: Optional[RetryConfig] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> Callable[[F], F]:
    """Decorator to add retry logic to functions.
    
    Args:
        config: RetryConfig object (if provided, other args are ignored)
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds for the first retry
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        
    Returns:
        Decorated function with retry logic
    """
    if config is None:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
        )
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            context = RetryContext(config, func.__name__)
            
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if not context.should_retry(e):
                        # Not retryable or max attempts reached
                        if context.attempt > 0:
                            logger.error(
                                f"Function {func.__name__} failed after {context.attempt} "
                                f"retry attempts in {time.time() - context.start_time:.2f} seconds"
                            )
                        raise context.get_final_exception() if context.exceptions else e
                    
                    # Calculate delay and record attempt
                    delay = config.calculate_delay(context.attempt + 1, e)
                    context.record_attempt(e, delay)
                    
                    # Wait before retrying
                    time.sleep(delay)
        
        return wrapper
    
    return decorator


def async_retry(
    config: Optional[RetryConfig] = None,
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
    jitter: bool = True,
) -> Callable[[F], F]:
    """Async decorator to add retry logic to async functions.
    
    Args:
        config: RetryConfig object (if provided, other args are ignored)
        max_attempts: Maximum number of retry attempts
        base_delay: Base delay in seconds for the first retry
        max_delay: Maximum delay between retries
        exponential_base: Base for exponential backoff calculation
        jitter: Whether to add random jitter to delays
        
    Returns:
        Decorated async function with retry logic
    """
    if config is None:
        config = RetryConfig(
            max_attempts=max_attempts,
            base_delay=base_delay,
            max_delay=max_delay,
            exponential_base=exponential_base,
            jitter=jitter,
        )
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            context = RetryContext(config, func.__name__)
            
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:
                    if not context.should_retry(e):
                        # Not retryable or max attempts reached
                        if context.attempt > 0:
                            logger.error(
                                f"Async function {func.__name__} failed after {context.attempt} "
                                f"retry attempts in {time.time() - context.start_time:.2f} seconds"
                            )
                        raise context.get_final_exception() if context.exceptions else e
                    
                    # Calculate delay and record attempt
                    delay = config.calculate_delay(context.attempt + 1, e)
                    context.record_attempt(e, delay)
                    
                    # Wait before retrying
                    await asyncio.sleep(delay)
        
        return wrapper
    
    return decorator


class RetryManager:
    """Manager for executing functions with retry logic."""
    
    def __init__(self, config: Optional[RetryConfig] = None):
        """Initialize retry manager.
        
        Args:
            config: Default retry configuration
        """
        self.default_config = config or RetryConfig()
    
    def execute(
        self,
        func: Callable,
        *args,
        config: Optional[RetryConfig] = None,
        **kwargs
    ) -> Any:
        """Execute a function with retry logic.
        
        Args:
            func: Function to execute
            *args: Positional arguments for the function
            config: Retry configuration (uses default if None)
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
        """
        retry_config = config or self.default_config
        decorated_func = retry(retry_config)(func)
        return decorated_func(*args, **kwargs)
    
    async def execute_async(
        self,
        func: Callable,
        *args,
        config: Optional[RetryConfig] = None,
        **kwargs
    ) -> Any:
        """Execute an async function with retry logic.
        
        Args:
            func: Async function to execute
            *args: Positional arguments for the function
            config: Retry configuration (uses default if None)
            **kwargs: Keyword arguments for the function
            
        Returns:
            Function result
        """
        retry_config = config or self.default_config
        decorated_func = async_retry(retry_config)(func)
        return await decorated_func(*args, **kwargs)


# Predefined retry configurations for common scenarios
class RetryConfigs:
    """Predefined retry configurations for common scenarios."""
    
    # Quick retries for fast operations
    FAST = RetryConfig(
        max_attempts=3,
        base_delay=0.1,
        max_delay=1.0,
        exponential_base=2.0,
    )
    
    # Standard retries for normal operations
    STANDARD = RetryConfig(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        exponential_base=2.0,
    )
    
    # Aggressive retries for critical operations
    AGGRESSIVE = RetryConfig(
        max_attempts=5,
        base_delay=0.5,
        max_delay=60.0,
        exponential_base=1.5,
    )
    
    # Patient retries for external services
    EXTERNAL_SERVICE = RetryConfig(
        max_attempts=5,
        base_delay=2.0,
        max_delay=120.0,
        exponential_base=2.0,
        retryable_exceptions=[
            NetworkException,
            ExternalServiceException,
            RateLimitException,
            ConnectionError,
            TimeoutError,
        ]
    )
    
    # Database operation retries
    DATABASE = RetryConfig(
        max_attempts=3,
        base_delay=1.0,
        max_delay=30.0,
        exponential_base=2.0,
        retryable_exceptions=[
            NetworkException,
            ConnectionError,
            TimeoutError,
        ]
    )
    
    # File system operation retries
    FILESYSTEM = RetryConfig(
        max_attempts=3,
        base_delay=0.1,
        max_delay=5.0,
        exponential_base=2.0,
        retryable_exceptions=[
            OSError,
            IOError,
        ]
    )


# Convenience functions for common retry patterns
def retry_on_network_error(max_attempts: int = 3, base_delay: float = 1.0):
    """Decorator for retrying on network errors."""
    return retry(RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        retryable_exceptions=[NetworkException, ConnectionError, TimeoutError]
    ))


def retry_on_external_service_error(max_attempts: int = 5, base_delay: float = 2.0):
    """Decorator for retrying on external service errors."""
    return retry(RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=120.0,
        retryable_exceptions=[
            ExternalServiceException,
            RateLimitException,
            NetworkException,
            ConnectionError,
            TimeoutError,
        ]
    ))


def retry_with_backoff(
    max_attempts: int = 3,
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    exponential_base: float = 2.0,
):
    """Decorator for retrying with exponential backoff."""
    return retry(RetryConfig(
        max_attempts=max_attempts,
        base_delay=base_delay,
        max_delay=max_delay,
        exponential_base=exponential_base,
    ))


# Global retry manager instance
retry_manager = RetryManager()