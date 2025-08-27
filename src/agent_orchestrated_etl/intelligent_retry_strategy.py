"""
Intelligent Retry Strategy System
Advanced retry mechanisms with adaptive backoff, failure pattern recognition, and contextual retry decisions.
"""
import asyncio
import logging
import random
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, Callable, List, Union
from dataclasses import dataclass, asdict
import json
import math
from pathlib import Path

logger = logging.getLogger(__name__)


class RetryStrategy(Enum):
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    LINEAR_BACKOFF = "linear_backoff"
    FIBONACCI_BACKOFF = "fibonacci_backoff"
    ADAPTIVE_BACKOFF = "adaptive_backoff"
    CIRCUIT_BREAKER_AWARE = "circuit_breaker_aware"


class RetryDecision(Enum):
    RETRY = "retry"
    STOP = "stop"
    ESCALATE = "escalate"
    CIRCUIT_BREAK = "circuit_break"


@dataclass
class RetryAttempt:
    attempt_number: int
    timestamp: datetime
    delay_before: float
    execution_time: float
    exception_type: str
    exception_message: str
    context: Dict[str, Any]
    decision: RetryDecision


@dataclass
class RetryConfiguration:
    max_attempts: int = 5
    base_delay: float = 1.0
    max_delay: float = 300.0
    backoff_multiplier: float = 2.0
    jitter_factor: float = 0.1
    exponential_base: float = 2.0
    timeout_per_attempt: Optional[float] = None
    retryable_exceptions: List[str] = None
    non_retryable_exceptions: List[str] = None


class IntelligentRetryStrategy:
    """Intelligent retry system with adaptive learning and pattern recognition."""
    
    def __init__(self, service_name: str, config: Optional[RetryConfiguration] = None):
        self.service_name = service_name
        self.config = config or RetryConfiguration()
        
        # Learning and adaptation components
        self.retry_history: List[RetryAttempt] = []
        self.failure_patterns: Dict[str, List[Dict[str, Any]]] = {}
        self.success_patterns: Dict[str, List[Dict[str, Any]]] = {}
        self.adaptive_parameters: Dict[str, float] = {}
        
        # Pattern recognition
        self.pattern_window_size = 50
        self.learning_threshold = 10
        
        # Default retryable/non-retryable exceptions
        if self.config.retryable_exceptions is None:
            self.config.retryable_exceptions = [
                "ConnectionError", "TimeoutError", "HTTPError",
                "TemporaryFailure", "ServiceUnavailable"
            ]
        
        if self.config.non_retryable_exceptions is None:
            self.config.non_retryable_exceptions = [
                "AuthenticationError", "PermissionError", "ValidationError",
                "NotFoundError", "BadRequestError"
            ]
        
        # Load historical data for learning
        self._load_historical_data()
        
        logger.info(f"Initialized intelligent retry strategy for {self.service_name}")
    
    def _load_historical_data(self) -> None:
        """Load historical retry data for pattern recognition."""
        try:
            history_path = Path(f"reports/retry_strategy_{self.service_name}_history.json")
            if history_path.exists():
                with open(history_path, 'r') as f:
                    data = json.load(f)
                    
                    # Restore failure and success patterns
                    self.failure_patterns = data.get('failure_patterns', {})
                    self.success_patterns = data.get('success_patterns', {})
                    self.adaptive_parameters = data.get('adaptive_parameters', {})
                    
                    logger.info(f"Loaded {len(self.failure_patterns)} failure patterns and "
                               f"{len(self.success_patterns)} success patterns")
        except Exception as e:
            logger.warning(f"Failed to load historical retry data: {e}")
    
    async def execute_with_retry(self, func: Callable, *args, 
                               context: Optional[Dict[str, Any]] = None, 
                               **kwargs) -> Any:
        """Execute function with intelligent retry logic."""
        context = context or {}
        attempt_number = 0
        last_exception = None
        
        while attempt_number < self.config.max_attempts:
            attempt_number += 1
            
            # Calculate delay before this attempt (except first)
            if attempt_number > 1:
                delay = await self._calculate_adaptive_delay(
                    attempt_number, last_exception, context
                )
                
                logger.info(f"Retry attempt {attempt_number} for {self.service_name} "
                           f"after {delay:.2f}s delay")
                await asyncio.sleep(delay)
            else:
                delay = 0.0
            
            # Execute the function with timeout
            start_time = time.time()
            try:
                if self.config.timeout_per_attempt:
                    if asyncio.iscoroutinefunction(func):
                        result = await asyncio.wait_for(
                            func(*args, **kwargs), 
                            timeout=self.config.timeout_per_attempt
                        )
                    else:
                        # For non-async functions, we can't easily apply timeout
                        result = func(*args, **kwargs)
                else:
                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                
                # Success - record and learn
                execution_time = time.time() - start_time
                await self._record_success(attempt_number, delay, execution_time, context)
                
                return result
            
            except Exception as e:
                execution_time = time.time() - start_time
                last_exception = e
                
                # Determine retry decision
                decision = await self._make_retry_decision(
                    e, attempt_number, context, execution_time
                )
                
                # Record retry attempt
                retry_attempt = RetryAttempt(
                    attempt_number=attempt_number,
                    timestamp=datetime.now(),
                    delay_before=delay,
                    execution_time=execution_time,
                    exception_type=type(e).__name__,
                    exception_message=str(e),
                    context=context.copy(),
                    decision=decision
                )
                
                self.retry_history.append(retry_attempt)
                await self._record_failure_pattern(retry_attempt)
                
                # Act on retry decision
                if decision == RetryDecision.STOP:
                    logger.error(f"Stopping retries for {self.service_name} after "
                               f"{attempt_number} attempts: {decision}")
                    break
                elif decision == RetryDecision.CIRCUIT_BREAK:
                    logger.error(f"Circuit breaking for {self.service_name}: {e}")
                    # Could integrate with circuit breaker here
                    break
                elif decision == RetryDecision.ESCALATE:
                    logger.warning(f"Escalating retry for {self.service_name}: {e}")
                    # Could trigger escalation logic here
                    context['escalated'] = True
        
        # All retries exhausted
        logger.error(f"All retry attempts exhausted for {self.service_name}")
        
        # Save learning data periodically
        if len(self.retry_history) % 20 == 0:
            await self._save_historical_data()
        
        # Re-raise the last exception
        raise last_exception
    
    async def _calculate_adaptive_delay(self, attempt_number: int, 
                                      last_exception: Exception,
                                      context: Dict[str, Any]) -> float:
        """Calculate adaptive delay based on learned patterns."""
        exception_type = type(last_exception).__name__
        
        # Check if we have learned patterns for this exception type
        if exception_type in self.failure_patterns:
            patterns = self.failure_patterns[exception_type]
            if len(patterns) >= self.learning_threshold:
                # Use learned adaptive delay
                return await self._calculate_learned_delay(
                    exception_type, attempt_number, context
                )
        
        # Fall back to configured strategy
        strategy = self._determine_retry_strategy(exception_type, context)
        return self._calculate_strategy_delay(strategy, attempt_number)
    
    async def _calculate_learned_delay(self, exception_type: str, 
                                     attempt_number: int,
                                     context: Dict[str, Any]) -> float:
        """Calculate delay based on learned patterns."""
        patterns = self.failure_patterns[exception_type]
        recent_patterns = patterns[-self.learning_threshold:]
        
        # Analyze success rates at different delays
        delay_success_map = {}
        for pattern in recent_patterns:
            delay_bucket = int(pattern.get('delay_before', 0) // 5) * 5  # 5s buckets
            if delay_bucket not in delay_success_map:
                delay_success_map[delay_bucket] = {'attempts': 0, 'successes': 0}
            
            delay_success_map[delay_bucket]['attempts'] += 1
            if pattern.get('eventually_succeeded', False):
                delay_success_map[delay_bucket]['successes'] += 1
        
        # Find optimal delay bucket
        best_delay = self.config.base_delay
        best_success_rate = 0.0
        
        for delay_bucket, stats in delay_success_map.items():
            if stats['attempts'] >= 3:  # Minimum sample size
                success_rate = stats['successes'] / stats['attempts']
                if success_rate > best_success_rate:
                    best_success_rate = success_rate
                    best_delay = max(delay_bucket, self.config.base_delay)
        
        # Adjust for attempt number
        adaptive_delay = best_delay * (attempt_number ** 0.5)
        
        # Apply jitter
        jitter = random.uniform(-self.config.jitter_factor, self.config.jitter_factor)
        adaptive_delay *= (1 + jitter)
        
        return min(adaptive_delay, self.config.max_delay)
    
    def _determine_retry_strategy(self, exception_type: str, 
                                context: Dict[str, Any]) -> RetryStrategy:
        """Determine appropriate retry strategy based on exception and context."""
        # Circuit breaker aware for connection issues
        if exception_type in ["ConnectionError", "TimeoutError"]:
            return RetryStrategy.CIRCUIT_BREAKER_AWARE
        
        # Adaptive for learned patterns
        if exception_type in self.failure_patterns:
            return RetryStrategy.ADAPTIVE_BACKOFF
        
        # Exponential for general failures
        if exception_type in self.config.retryable_exceptions:
            return RetryStrategy.EXPONENTIAL_BACKOFF
        
        # Default strategy
        return RetryStrategy.EXPONENTIAL_BACKOFF
    
    def _calculate_strategy_delay(self, strategy: RetryStrategy, 
                                attempt_number: int) -> float:
        """Calculate delay based on retry strategy."""
        if strategy == RetryStrategy.LINEAR_BACKOFF:
            delay = self.config.base_delay * attempt_number
        
        elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.config.base_delay * (
                self.config.backoff_multiplier ** (attempt_number - 1)
            )
        
        elif strategy == RetryStrategy.FIBONACCI_BACKOFF:
            delay = self.config.base_delay * self._fibonacci(attempt_number)
        
        elif strategy == RetryStrategy.ADAPTIVE_BACKOFF:
            # Use adaptive parameters if available
            multiplier = self.adaptive_parameters.get(
                'backoff_multiplier', self.config.backoff_multiplier
            )
            delay = self.config.base_delay * (multiplier ** (attempt_number - 1))
        
        elif strategy == RetryStrategy.CIRCUIT_BREAKER_AWARE:
            # Longer delays for circuit breaker scenarios
            delay = self.config.base_delay * (3 ** (attempt_number - 1))
        
        else:
            # Default to exponential
            delay = self.config.base_delay * (
                self.config.backoff_multiplier ** (attempt_number - 1)
            )
        
        # Apply jitter
        jitter = random.uniform(-self.config.jitter_factor, self.config.jitter_factor)
        delay *= (1 + jitter)
        
        # Bound the delay
        return min(max(delay, self.config.base_delay), self.config.max_delay)
    
    def _fibonacci(self, n: int) -> int:
        """Calculate nth Fibonacci number for Fibonacci backoff."""
        if n <= 1:
            return 1
        
        a, b = 1, 1
        for _ in range(2, n + 1):
            a, b = b, a + b
        
        return b
    
    async def _make_retry_decision(self, exception: Exception, attempt_number: int,
                                 context: Dict[str, Any], 
                                 execution_time: float) -> RetryDecision:
        """Make intelligent retry decision based on exception and patterns."""
        exception_type = type(exception).__name__
        
        # Never retry non-retryable exceptions
        if exception_type in self.config.non_retryable_exceptions:
            return RetryDecision.STOP
        
        # Stop if max attempts reached
        if attempt_number >= self.config.max_attempts:
            return RetryDecision.STOP
        
        # Check for learned patterns
        if exception_type in self.failure_patterns:
            patterns = self.failure_patterns[exception_type]
            if len(patterns) >= self.learning_threshold:
                return await self._make_pattern_based_decision(
                    exception_type, attempt_number, context, patterns
                )
        
        # Default retry for retryable exceptions
        if exception_type in self.config.retryable_exceptions:
            return RetryDecision.RETRY
        
        # Circuit breaker logic for connection issues
        if exception_type in ["ConnectionError", "TimeoutError"] and attempt_number >= 3:
            return RetryDecision.CIRCUIT_BREAK
        
        # Default to retry
        return RetryDecision.RETRY
    
    async def _make_pattern_based_decision(self, exception_type: str, 
                                         attempt_number: int,
                                         context: Dict[str, Any],
                                         patterns: List[Dict[str, Any]]) -> RetryDecision:
        """Make retry decision based on learned patterns."""
        recent_patterns = patterns[-20:]  # Last 20 occurrences
        
        # Calculate success rate for this attempt number
        same_attempt_patterns = [
            p for p in recent_patterns 
            if p.get('attempt_number') == attempt_number
        ]
        
        if len(same_attempt_patterns) >= 3:
            successes = sum(
                1 for p in same_attempt_patterns 
                if p.get('eventually_succeeded', False)
            )
            success_rate = successes / len(same_attempt_patterns)
            
            # If success rate is very low, consider stopping
            if success_rate < 0.1:
                return RetryDecision.STOP
            
            # If success rate is moderate, continue with caution
            if success_rate < 0.3:
                return RetryDecision.ESCALATE
        
        # Look for rapid failure patterns (circuit breaker scenario)
        recent_failures = [
            p for p in recent_patterns[-5:] 
            if p.get('execution_time', 0) < 1.0  # Quick failures
        ]
        
        if len(recent_failures) >= 3:
            return RetryDecision.CIRCUIT_BREAK
        
        # Default to retry
        return RetryDecision.RETRY
    
    async def _record_success(self, attempt_number: int, delay: float, 
                            execution_time: float, context: Dict[str, Any]) -> None:
        """Record successful execution for learning."""
        success_pattern = {
            'timestamp': datetime.now().isoformat(),
            'attempt_number': attempt_number,
            'delay_before': delay,
            'execution_time': execution_time,
            'context': context.copy()
        }
        
        # Update failure patterns with eventual success
        if len(self.retry_history) > 0:
            # Mark recent failed attempts as eventually successful
            for attempt in reversed(self.retry_history[-10:]):
                if attempt.exception_type not in self.failure_patterns:
                    self.failure_patterns[attempt.exception_type] = []
                
                # Find and update the corresponding failure pattern
                for pattern in reversed(self.failure_patterns[attempt.exception_type][-10:]):
                    if (pattern.get('timestamp') == attempt.timestamp.isoformat() and
                        'eventually_succeeded' not in pattern):
                        pattern['eventually_succeeded'] = True
                        pattern['success_after_attempts'] = attempt_number
                        break
        
        # Store success pattern
        context_key = f"{context.get('operation', 'default')}"
        if context_key not in self.success_patterns:
            self.success_patterns[context_key] = []
        
        self.success_patterns[context_key].append(success_pattern)
        
        # Limit pattern history
        if len(self.success_patterns[context_key]) > self.pattern_window_size:
            self.success_patterns[context_key].pop(0)
        
        # Update adaptive parameters
        await self._update_adaptive_parameters()
    
    async def _record_failure_pattern(self, retry_attempt: RetryAttempt) -> None:
        """Record failure pattern for learning."""
        exception_type = retry_attempt.exception_type
        
        if exception_type not in self.failure_patterns:
            self.failure_patterns[exception_type] = []
        
        pattern = {
            'timestamp': retry_attempt.timestamp.isoformat(),
            'attempt_number': retry_attempt.attempt_number,
            'delay_before': retry_attempt.delay_before,
            'execution_time': retry_attempt.execution_time,
            'context': retry_attempt.context.copy(),
            'decision': retry_attempt.decision.value
        }
        
        self.failure_patterns[exception_type].append(pattern)
        
        # Limit pattern history
        if len(self.failure_patterns[exception_type]) > self.pattern_window_size:
            self.failure_patterns[exception_type].pop(0)
    
    async def _update_adaptive_parameters(self) -> None:
        """Update adaptive parameters based on learned patterns."""
        if len(self.retry_history) < 20:
            return  # Need sufficient data
        
        recent_attempts = self.retry_history[-20:]
        
        # Analyze success rates by backoff multiplier
        multiplier_success = {}
        for attempt in recent_attempts:
            # Estimate multiplier used (simplified)
            estimated_multiplier = max(1.5, attempt.delay_before / self.config.base_delay)
            multiplier_key = round(estimated_multiplier, 1)
            
            if multiplier_key not in multiplier_success:
                multiplier_success[multiplier_key] = {'attempts': 0, 'successes': 0}
            
            multiplier_success[multiplier_key]['attempts'] += 1
            
            # Check if this attempt eventually led to success
            if attempt.decision == RetryDecision.RETRY:
                # Look for eventual success in patterns
                for pattern_list in self.failure_patterns.values():
                    for pattern in pattern_list:
                        if (pattern.get('timestamp') == attempt.timestamp.isoformat() and
                            pattern.get('eventually_succeeded', False)):
                            multiplier_success[multiplier_key]['successes'] += 1
                            break
        
        # Find optimal multiplier
        best_multiplier = self.config.backoff_multiplier
        best_success_rate = 0.0
        
        for multiplier, stats in multiplier_success.items():
            if stats['attempts'] >= 3:
                success_rate = stats['successes'] / stats['attempts']
                if success_rate > best_success_rate:
                    best_success_rate = success_rate
                    best_multiplier = multiplier
        
        # Update adaptive parameters
        self.adaptive_parameters['backoff_multiplier'] = best_multiplier
        
        logger.info(f"Updated adaptive parameters for {self.service_name}: "
                   f"backoff_multiplier={best_multiplier:.1f}")
    
    async def _save_historical_data(self) -> None:
        """Save historical retry data for persistence."""
        try:
            # Convert retry history to serializable format
            serializable_history = []
            for attempt in self.retry_history[-100:]:  # Keep last 100
                attempt_dict = asdict(attempt)
                attempt_dict['timestamp'] = attempt.timestamp.isoformat()
                attempt_dict['decision'] = attempt.decision.value
                serializable_history.append(attempt_dict)
            
            data = {
                'retry_history': serializable_history,
                'failure_patterns': self.failure_patterns,
                'success_patterns': self.success_patterns,
                'adaptive_parameters': self.adaptive_parameters,
                'config': asdict(self.config),
                'last_updated': datetime.now().isoformat()
            }
            
            history_path = Path(f"reports/retry_strategy_{self.service_name}_history.json")
            history_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(history_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to save retry historical data: {e}")
    
    def get_retry_stats(self) -> Dict[str, Any]:
        """Get retry statistics and insights."""
        if not self.retry_history:
            return {"status": "no_data", "message": "No retry attempts recorded"}
        
        total_attempts = len(self.retry_history)
        
        # Calculate success rates by exception type
        exception_stats = {}
        for attempt in self.retry_history:
            exc_type = attempt.exception_type
            if exc_type not in exception_stats:
                exception_stats[exc_type] = {
                    'attempts': 0, 'eventual_successes': 0, 'avg_attempts': 0
                }
            
            exception_stats[exc_type]['attempts'] += 1
            
            # Check for eventual success in patterns
            if exc_type in self.failure_patterns:
                for pattern in self.failure_patterns[exc_type]:
                    if (pattern.get('timestamp') == attempt.timestamp.isoformat() and
                        pattern.get('eventually_succeeded', False)):
                        exception_stats[exc_type]['eventual_successes'] += 1
                        break
        
        # Calculate average attempts to success
        for exc_type, stats in exception_stats.items():
            if stats['eventual_successes'] > 0:
                success_patterns = [
                    p for p in self.failure_patterns.get(exc_type, [])
                    if p.get('eventually_succeeded', False)
                ]
                if success_patterns:
                    avg_attempts = sum(
                        p.get('success_after_attempts', 1) for p in success_patterns
                    ) / len(success_patterns)
                    stats['avg_attempts'] = avg_attempts
        
        return {
            "service_name": self.service_name,
            "total_retry_attempts": total_attempts,
            "exception_statistics": exception_stats,
            "adaptive_parameters": self.adaptive_parameters.copy(),
            "learned_patterns": {
                "failure_types": len(self.failure_patterns),
                "success_contexts": len(self.success_patterns)
            },
            "recent_activity": {
                "last_retry": self.retry_history[-1].timestamp.isoformat(),
                "recent_exceptions": list(set(
                    attempt.exception_type for attempt in self.retry_history[-10:]
                ))
            }
        }


class RetryManager:
    """Manager for multiple intelligent retry strategies."""
    
    def __init__(self):
        self.retry_strategies: Dict[str, IntelligentRetryStrategy] = {}
        self.global_config = self._load_global_config()
    
    def _load_global_config(self) -> Dict[str, Any]:
        """Load global retry configuration."""
        try:
            config_path = Path("config/retry_strategies.json")
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load global retry config: {e}")
        
        return {}
    
    def get_retry_strategy(self, service_name: str, 
                         config: Optional[RetryConfiguration] = None) -> IntelligentRetryStrategy:
        """Get or create retry strategy for a service."""
        if service_name not in self.retry_strategies:
            # Merge global config with service-specific config
            merged_config = RetryConfiguration()
            
            if 'default' in self.global_config:
                for field, value in self.global_config['default'].items():
                    if hasattr(merged_config, field):
                        setattr(merged_config, field, value)
            
            service_config = self.global_config.get('services', {}).get(service_name, {})
            for field, value in service_config.items():
                if hasattr(merged_config, field):
                    setattr(merged_config, field, value)
            
            if config:
                for field, value in asdict(config).items():
                    if value is not None:
                        setattr(merged_config, field, value)
            
            self.retry_strategies[service_name] = IntelligentRetryStrategy(
                service_name, merged_config
            )
        
        return self.retry_strategies[service_name]
    
    async def execute_with_intelligent_retry(self, service_name: str, func: Callable, 
                                           *args, 
                                           context: Optional[Dict[str, Any]] = None,
                                           config: Optional[RetryConfiguration] = None,
                                           **kwargs) -> Any:
        """Execute function with intelligent retry strategy."""
        retry_strategy = self.get_retry_strategy(service_name, config)
        return await retry_strategy.execute_with_retry(func, *args, context=context, **kwargs)
    
    def get_all_retry_stats(self) -> Dict[str, Any]:
        """Get retry statistics for all services."""
        return {
            name: strategy.get_retry_stats() 
            for name, strategy in self.retry_strategies.items()
        }


# Global retry manager instance
retry_manager = RetryManager()


# Convenience functions
async def execute_with_intelligent_retry(service_name: str, func: Callable, 
                                       *args, **kwargs) -> Any:
    """Execute function with intelligent retry strategy."""
    return await retry_manager.execute_with_intelligent_retry(
        service_name, func, *args, **kwargs
    )


def get_retry_stats(service_name: str) -> Optional[Dict[str, Any]]:
    """Get retry statistics for specific service."""
    if service_name in retry_manager.retry_strategies:
        return retry_manager.retry_strategies[service_name].get_retry_stats()
    return None


def get_all_retry_stats() -> Dict[str, Any]:
    """Get retry statistics for all services."""
    return retry_manager.get_all_retry_stats()