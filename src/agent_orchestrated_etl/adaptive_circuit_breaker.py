"""
Adaptive Circuit Breaker System
Self-learning circuit breaker that adapts failure thresholds based on system behavior patterns.
"""
import asyncio
import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, Any, Optional, Callable, List
from dataclasses import dataclass, asdict
import json
from pathlib import Path
import statistics

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"
    LEARNING = "learning"


@dataclass
class CircuitMetrics:
    total_requests: int = 0
    failed_requests: int = 0
    success_requests: int = 0
    avg_response_time: float = 0.0
    failure_rate: float = 0.0
    last_failure_time: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


@dataclass
class AdaptiveThresholds:
    failure_threshold: float = 0.5
    min_requests_threshold: int = 10
    timeout_duration: float = 60.0
    half_open_max_calls: int = 5
    success_threshold_half_open: int = 3


class AdaptiveCircuitBreaker:
    """Self-learning circuit breaker that adapts to system behavior patterns."""
    
    def __init__(self, service_name: str, config: Optional[Dict[str, Any]] = None):
        self.service_name = service_name
        self.state = CircuitState.CLOSED
        self.metrics = CircuitMetrics()
        self.thresholds = AdaptiveThresholds()
        self.state_change_time = datetime.now()
        self.half_open_calls = 0
        
        # Adaptive learning components
        self.failure_patterns: List[Dict[str, Any]] = []
        self.performance_history: List[float] = []
        self.learning_window_size = 100
        self.adaptation_frequency = 50  # Adapt every N requests
        
        # Load configuration and historical data
        self._load_config(config)
        self._load_historical_data()
        
        logger.info(f"Initialized adaptive circuit breaker for {service_name}")
    
    def _load_config(self, config: Optional[Dict[str, Any]]) -> None:
        """Load configuration with defaults."""
        if config:
            for field, value in config.items():
                if hasattr(self.thresholds, field):
                    setattr(self.thresholds, field, value)
    
    def _load_historical_data(self) -> None:
        """Load historical performance data for adaptive learning."""
        try:
            history_path = Path(f"reports/circuit_breaker_{self.service_name}_history.json")
            if history_path.exists():
                with open(history_path, 'r') as f:
                    data = json.load(f)
                    self.failure_patterns = data.get('failure_patterns', [])
                    self.performance_history = data.get('performance_history', [])
                    
                    # Restore adaptive thresholds if available
                    if 'adaptive_thresholds' in data:
                        for field, value in data['adaptive_thresholds'].items():
                            if hasattr(self.thresholds, field):
                                setattr(self.thresholds, field, value)
        except Exception as e:
            logger.warning(f"Failed to load historical data: {e}")
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function with circuit breaker protection."""
        # Check if circuit is open
        if self.state == CircuitState.OPEN:
            if self._should_attempt_reset():
                self._transition_to_half_open()
            else:
                raise CircuitBreakerOpenError(f"Circuit breaker is open for {self.service_name}")
        
        # Half-open state logic
        if self.state == CircuitState.HALF_OPEN:
            if self.half_open_calls >= self.thresholds.half_open_max_calls:
                raise CircuitBreakerOpenError(f"Half-open limit reached for {self.service_name}")
            self.half_open_calls += 1
        
        # Execute the function
        start_time = time.time()
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Record success
            execution_time = time.time() - start_time
            await self._record_success(execution_time)
            
            return result
        
        except Exception as e:
            # Record failure
            execution_time = time.time() - start_time
            await self._record_failure(e, execution_time)
            raise
    
    async def _record_success(self, execution_time: float) -> None:
        """Record successful execution and update metrics."""
        self.metrics.total_requests += 1
        self.metrics.success_requests += 1
        self.metrics.consecutive_successes += 1
        self.metrics.consecutive_failures = 0
        
        # Update average response time
        self._update_average_response_time(execution_time)
        self.performance_history.append(execution_time)
        
        # Limit history size
        if len(self.performance_history) > self.learning_window_size:
            self.performance_history.pop(0)
        
        # Update failure rate
        self._update_failure_rate()
        
        # Handle half-open state transitions
        if self.state == CircuitState.HALF_OPEN:
            if self.metrics.consecutive_successes >= self.thresholds.success_threshold_half_open:
                self._transition_to_closed()
        
        # Adaptive learning
        if self.metrics.total_requests % self.adaptation_frequency == 0:
            await self._adapt_thresholds()
        
        # Persist data periodically
        if self.metrics.total_requests % 100 == 0:
            await self._save_historical_data()
    
    async def _record_failure(self, exception: Exception, execution_time: float) -> None:
        """Record failed execution and update metrics."""
        self.metrics.total_requests += 1
        self.metrics.failed_requests += 1
        self.metrics.consecutive_failures += 1
        self.metrics.consecutive_successes = 0
        self.metrics.last_failure_time = datetime.now()
        
        # Record failure pattern for learning
        failure_pattern = {
            "timestamp": datetime.now().isoformat(),
            "exception_type": type(exception).__name__,
            "execution_time": execution_time,
            "consecutive_failures": self.metrics.consecutive_failures,
            "total_requests": self.metrics.total_requests
        }
        self.failure_patterns.append(failure_pattern)
        
        # Limit pattern history
        if len(self.failure_patterns) > self.learning_window_size:
            self.failure_patterns.pop(0)
        
        # Update failure rate
        self._update_failure_rate()
        
        # Check if circuit should open
        if self._should_open_circuit():
            self._transition_to_open()
        elif self.state == CircuitState.HALF_OPEN:
            # Reset to open if failure in half-open state
            self._transition_to_open()
        
        # Adaptive learning on failures
        await self._learn_from_failure(failure_pattern)
    
    def _update_average_response_time(self, execution_time: float) -> None:
        """Update average response time with exponential moving average."""
        alpha = 0.1  # Smoothing factor
        if self.metrics.avg_response_time == 0:
            self.metrics.avg_response_time = execution_time
        else:
            self.metrics.avg_response_time = (
                alpha * execution_time + 
                (1 - alpha) * self.metrics.avg_response_time
            )
    
    def _update_failure_rate(self) -> None:
        """Update current failure rate."""
        if self.metrics.total_requests > 0:
            self.metrics.failure_rate = (
                self.metrics.failed_requests / self.metrics.total_requests
            )
    
    def _should_open_circuit(self) -> bool:
        """Determine if circuit should be opened based on adaptive thresholds."""
        # Must have minimum number of requests
        if self.metrics.total_requests < self.thresholds.min_requests_threshold:
            return False
        
        # Check failure rate threshold
        if self.metrics.failure_rate >= self.thresholds.failure_threshold:
            return True
        
        # Check consecutive failures (adaptive)
        consecutive_threshold = self._calculate_adaptive_consecutive_threshold()
        if self.metrics.consecutive_failures >= consecutive_threshold:
            return True
        
        return False
    
    def _calculate_adaptive_consecutive_threshold(self) -> int:
        """Calculate adaptive consecutive failure threshold."""
        base_threshold = 5
        
        # Adjust based on historical patterns
        if len(self.failure_patterns) >= 10:
            recent_patterns = self.failure_patterns[-10:]
            avg_consecutive = statistics.mean(
                p.get('consecutive_failures', 0) for p in recent_patterns
            )
            
            # If we typically see higher consecutive failures, increase threshold
            if avg_consecutive > base_threshold:
                base_threshold = min(int(avg_consecutive * 1.2), 10)
        
        return base_threshold
    
    def _should_attempt_reset(self) -> bool:
        """Determine if circuit should attempt to reset from open state."""
        if self.state != CircuitState.OPEN:
            return False
        
        time_since_open = datetime.now() - self.state_change_time
        return time_since_open.total_seconds() >= self.thresholds.timeout_duration
    
    def _transition_to_open(self) -> None:
        """Transition circuit to open state."""
        if self.state != CircuitState.OPEN:
            logger.warning(f"Circuit breaker opened for {self.service_name}")
            self.state = CircuitState.OPEN
            self.state_change_time = datetime.now()
    
    def _transition_to_half_open(self) -> None:
        """Transition circuit to half-open state."""
        logger.info(f"Circuit breaker transitioning to half-open for {self.service_name}")
        self.state = CircuitState.HALF_OPEN
        self.state_change_time = datetime.now()
        self.half_open_calls = 0
    
    def _transition_to_closed(self) -> None:
        """Transition circuit to closed state."""
        logger.info(f"Circuit breaker closed for {self.service_name}")
        self.state = CircuitState.CLOSED
        self.state_change_time = datetime.now()
        self.half_open_calls = 0
    
    async def _adapt_thresholds(self) -> None:
        """Adapt thresholds based on learned patterns."""
        if len(self.performance_history) < 20:
            return  # Need sufficient data for adaptation
        
        # Analyze performance trends
        recent_performance = self.performance_history[-20:]
        performance_stability = statistics.stdev(recent_performance)
        
        # Adapt failure threshold based on system stability
        if performance_stability < 0.1:  # Very stable system
            # Can be more aggressive (lower threshold)
            self.thresholds.failure_threshold = max(0.3, self.thresholds.failure_threshold - 0.05)
        elif performance_stability > 1.0:  # Unstable system
            # Be more conservative (higher threshold)
            self.thresholds.failure_threshold = min(0.8, self.thresholds.failure_threshold + 0.05)
        
        # Adapt timeout based on recovery patterns
        if len(self.failure_patterns) >= 10:
            recovery_times = self._analyze_recovery_patterns()
            if recovery_times:
                avg_recovery = statistics.mean(recovery_times)
                # Set timeout to 2x average recovery time, bounded
                new_timeout = max(30.0, min(300.0, avg_recovery * 2))
                self.thresholds.timeout_duration = new_timeout
        
        logger.info(f"Adapted thresholds for {self.service_name}: "
                   f"failure={self.thresholds.failure_threshold:.2f}, "
                   f"timeout={self.thresholds.timeout_duration:.1f}s")
    
    async def _learn_from_failure(self, failure_pattern: Dict[str, Any]) -> None:
        """Learn from failure patterns to improve circuit behavior."""
        # Analyze failure clustering
        if len(self.failure_patterns) >= 5:
            recent_failures = self.failure_patterns[-5:]
            time_diffs = []
            
            for i in range(1, len(recent_failures)):
                prev_time = datetime.fromisoformat(recent_failures[i-1]['timestamp'])
                curr_time = datetime.fromisoformat(recent_failures[i]['timestamp'])
                time_diffs.append((curr_time - prev_time).total_seconds())
            
            # If failures are clustered (happening quickly), be more aggressive
            if time_diffs and statistics.mean(time_diffs) < 5.0:
                self.thresholds.min_requests_threshold = max(5, 
                    self.thresholds.min_requests_threshold - 1)
    
    def _analyze_recovery_patterns(self) -> List[float]:
        """Analyze how long the system typically takes to recover."""
        recovery_times = []
        
        # Look for patterns of open->closed transitions
        for i, pattern in enumerate(self.failure_patterns):
            if i < len(self.failure_patterns) - 1:
                current_time = datetime.fromisoformat(pattern['timestamp'])
                # Look for next success (proxy for recovery)
                # This is simplified - in practice would track state transitions
                recovery_times.append(60.0)  # Placeholder logic
        
        return recovery_times
    
    async def _save_historical_data(self) -> None:
        """Save historical data for persistence."""
        try:
            data = {
                'failure_patterns': self.failure_patterns,
                'performance_history': self.performance_history,
                'adaptive_thresholds': asdict(self.thresholds),
                'metrics': asdict(self.metrics),
                'last_updated': datetime.now().isoformat()
            }
            
            history_path = Path(f"reports/circuit_breaker_{self.service_name}_history.json")
            history_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(history_path, 'w') as f:
                json.dump(data, f, indent=2)
                
        except Exception as e:
            logger.warning(f"Failed to save historical data: {e}")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current circuit breaker status."""
        return {
            "service_name": self.service_name,
            "state": self.state.value,
            "metrics": asdict(self.metrics),
            "thresholds": asdict(self.thresholds),
            "state_change_time": self.state_change_time.isoformat(),
            "half_open_calls": self.half_open_calls if self.state == CircuitState.HALF_OPEN else None,
            "learning_stats": {
                "failure_patterns_count": len(self.failure_patterns),
                "performance_history_count": len(self.performance_history),
                "adaptations_applied": self.metrics.total_requests // self.adaptation_frequency
            }
        }


class CircuitBreakerOpenError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreakerManager:
    """Manager for multiple adaptive circuit breakers."""
    
    def __init__(self):
        self.circuit_breakers: Dict[str, AdaptiveCircuitBreaker] = {}
        self.global_config = self._load_global_config()
    
    def _load_global_config(self) -> Dict[str, Any]:
        """Load global circuit breaker configuration."""
        try:
            config_path = Path("config/circuit_breakers.json")
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load global circuit breaker config: {e}")
        
        return {}
    
    def get_circuit_breaker(self, service_name: str, 
                          config: Optional[Dict[str, Any]] = None) -> AdaptiveCircuitBreaker:
        """Get or create circuit breaker for a service."""
        if service_name not in self.circuit_breakers:
            # Merge global config with service-specific config
            merged_config = self.global_config.get('default', {}).copy()
            service_config = self.global_config.get('services', {}).get(service_name, {})
            merged_config.update(service_config)
            if config:
                merged_config.update(config)
            
            self.circuit_breakers[service_name] = AdaptiveCircuitBreaker(
                service_name, merged_config
            )
        
        return self.circuit_breakers[service_name]
    
    async def call_with_circuit_breaker(self, service_name: str, func: Callable, 
                                      *args, config: Optional[Dict[str, Any]] = None, 
                                      **kwargs) -> Any:
        """Call function with circuit breaker protection."""
        circuit_breaker = self.get_circuit_breaker(service_name, config)
        return await circuit_breaker.call(func, *args, **kwargs)
    
    def get_all_status(self) -> Dict[str, Any]:
        """Get status of all circuit breakers."""
        return {
            name: cb.get_status() 
            for name, cb in self.circuit_breakers.items()
        }
    
    async def health_check(self) -> Dict[str, Any]:
        """Perform health check on all circuit breakers."""
        health_status = {
            "timestamp": datetime.now().isoformat(),
            "total_circuits": len(self.circuit_breakers),
            "circuits": {}
        }
        
        open_circuits = 0
        half_open_circuits = 0
        
        for name, cb in self.circuit_breakers.items():
            status = cb.get_status()
            health_status["circuits"][name] = {
                "state": status["state"],
                "failure_rate": status["metrics"]["failure_rate"],
                "total_requests": status["metrics"]["total_requests"]
            }
            
            if status["state"] == "open":
                open_circuits += 1
            elif status["state"] == "half_open":
                half_open_circuits += 1
        
        health_status["summary"] = {
            "healthy_circuits": len(self.circuit_breakers) - open_circuits - half_open_circuits,
            "open_circuits": open_circuits,
            "half_open_circuits": half_open_circuits
        }
        
        return health_status


# Global circuit breaker manager instance
circuit_manager = CircuitBreakerManager()


# Convenience functions
async def call_with_circuit_breaker(service_name: str, func: Callable, 
                                  *args, **kwargs) -> Any:
    """Call function with adaptive circuit breaker protection."""
    return await circuit_manager.call_with_circuit_breaker(
        service_name, func, *args, **kwargs
    )


def get_circuit_breaker_status(service_name: str) -> Optional[Dict[str, Any]]:
    """Get status of specific circuit breaker."""
    if service_name in circuit_manager.circuit_breakers:
        return circuit_manager.circuit_breakers[service_name].get_status()
    return None


async def get_circuit_breaker_health() -> Dict[str, Any]:
    """Get health status of all circuit breakers."""
    return await circuit_manager.health_check()