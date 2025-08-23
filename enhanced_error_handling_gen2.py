#!/usr/bin/env python3
"""
Generation 2: Enhanced Error Handling and Robustness
Implements comprehensive error recovery, validation, and resilience patterns
"""

import functools
import logging
import time
from typing import Any, Dict, List, Optional, Callable, Union
from contextlib import contextmanager

# Enhanced Exception Hierarchy
class ETLBaseException(Exception):
    """Base exception for all ETL operations"""
    def __init__(self, message: str, error_code: str = None, context: Dict[str, Any] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code or "ETL_UNKNOWN"
        self.context = context or {}
        self.timestamp = time.time()

class DataExtractionError(ETLBaseException):
    """Raised when data extraction fails"""
    def __init__(self, message: str, source: str = None, **kwargs):
        super().__init__(message, "ETL_EXTRACT_FAIL", {"source": source, **kwargs})

class DataTransformationError(ETLBaseException):
    """Raised when data transformation fails"""
    def __init__(self, message: str, transformation_step: str = None, **kwargs):
        super().__init__(message, "ETL_TRANSFORM_FAIL", {"step": transformation_step, **kwargs})

class DataValidationError(ETLBaseException):
    """Raised when data validation fails"""
    def __init__(self, message: str, validation_rule: str = None, **kwargs):
        super().__init__(message, "ETL_VALIDATE_FAIL", {"rule": validation_rule, **kwargs})

class PipelineExecutionError(ETLBaseException):
    """Raised when pipeline execution fails"""
    def __init__(self, message: str, pipeline_id: str = None, **kwargs):
        super().__init__(message, "ETL_PIPELINE_FAIL", {"pipeline_id": pipeline_id, **kwargs})

# Enhanced Retry Mechanism
class ExponentialBackoff:
    """Exponential backoff retry strategy with jitter"""
    
    def __init__(self, max_retries: int = 3, base_delay: float = 1.0, max_delay: float = 60.0, jitter: bool = True):
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
    
    def calculate_delay(self, attempt: int) -> float:
        """Calculate delay for the given attempt"""
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        if self.jitter:
            import random
            delay *= (0.5 + random.random() * 0.5)  # Add 0-50% jitter
        return delay

def retry_with_backoff(
    max_retries: int = 3,
    backoff_strategy: ExponentialBackoff = None,
    exceptions: tuple = (Exception,),
    on_retry: Callable = None
):
    """Enhanced retry decorator with exponential backoff and callbacks"""
    
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            strategy = backoff_strategy or ExponentialBackoff(max_retries)
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = func(*args, **kwargs)
                    if attempt > 0:
                        logging.info(f"Function {func.__name__} succeeded on attempt {attempt + 1}")
                    return result
                
                except exceptions as e:
                    last_exception = e
                    if attempt == max_retries:
                        logging.error(f"Function {func.__name__} failed after {max_retries + 1} attempts: {e}")
                        break
                    
                    delay = strategy.calculate_delay(attempt)
                    logging.warning(f"Function {func.__name__} failed on attempt {attempt + 1}, retrying in {delay:.2f}s: {e}")
                    
                    if on_retry:
                        on_retry(attempt, e, delay)
                    
                    time.sleep(delay)
            
            raise last_exception
        return wrapper
    return decorator

# Circuit Breaker Pattern
class CircuitBreakerState:
    CLOSED = "CLOSED"      # Normal operation
    OPEN = "OPEN"          # Failing fast
    HALF_OPEN = "HALF_OPEN"  # Testing if service recovered

class CircuitBreaker:
    """Circuit breaker pattern implementation for ETL operations"""
    
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0, success_threshold: int = 2):
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.success_threshold = success_threshold
        
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.state = CircuitBreakerState.CLOSED
    
    def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection"""
        if self.state == CircuitBreakerState.OPEN:
            if time.time() - self.last_failure_time < self.recovery_timeout:
                raise PipelineExecutionError(f"Circuit breaker OPEN for {func.__name__}")
            else:
                self.state = CircuitBreakerState.HALF_OPEN
                self.success_count = 0
        
        try:
            result = func(*args, **kwargs)
            self._on_success()
            return result
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful execution"""
        if self.state == CircuitBreakerState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.success_threshold:
                self.state = CircuitBreakerState.CLOSED
                self.failure_count = 0
        elif self.state == CircuitBreakerState.CLOSED:
            self.failure_count = 0
    
    def _on_failure(self):
        """Handle failed execution"""
        self.failure_count += 1
        self.last_failure_time = time.time()
        
        if self.failure_count >= self.failure_threshold:
            self.state = CircuitBreakerState.OPEN

# Enhanced Data Validation
class DataValidator:
    """Comprehensive data validation with detailed reporting"""
    
    def __init__(self):
        self.validation_rules = {}
        self.logger = logging.getLogger(f"{__name__}.DataValidator")
    
    def add_rule(self, name: str, rule: Callable, error_message: str = None):
        """Add a validation rule"""
        self.validation_rules[name] = {
            'rule': rule,
            'error_message': error_message or f"Validation rule '{name}' failed"
        }
    
    def validate_data(self, data: Union[List[Dict], Dict], rules: List[str] = None) -> Dict[str, Any]:
        """Validate data against specified rules"""
        rules_to_check = rules or list(self.validation_rules.keys())
        results = {
            'valid': True,
            'errors': [],
            'warnings': [],
            'details': {}
        }
        
        for rule_name in rules_to_check:
            if rule_name not in self.validation_rules:
                results['warnings'].append(f"Unknown validation rule: {rule_name}")
                continue
            
            try:
                rule_config = self.validation_rules[rule_name]
                rule_result = rule_config['rule'](data)
                
                if not rule_result:
                    error_msg = rule_config['error_message']
                    results['errors'].append(error_msg)
                    results['valid'] = False
                
                results['details'][rule_name] = rule_result
                
            except Exception as e:
                error_msg = f"Validation rule '{rule_name}' failed with exception: {e}"
                results['errors'].append(error_msg)
                results['valid'] = False
                self.logger.error(error_msg)
        
        return results

# Graceful Degradation Handler
class GracefulDegradationManager:
    """Manages graceful degradation of ETL operations"""
    
    def __init__(self):
        self.fallback_strategies = {}
        self.service_health = {}
        self.logger = logging.getLogger(f"{__name__}.GracefulDegradation")
    
    def register_fallback(self, service_name: str, fallback_func: Callable):
        """Register a fallback strategy for a service"""
        self.fallback_strategies[service_name] = fallback_func
        self.service_health[service_name] = True
    
    def mark_service_unhealthy(self, service_name: str):
        """Mark a service as unhealthy"""
        self.service_health[service_name] = False
        self.logger.warning(f"Service '{service_name}' marked as unhealthy")
    
    def mark_service_healthy(self, service_name: str):
        """Mark a service as healthy"""
        self.service_health[service_name] = True
        self.logger.info(f"Service '{service_name}' marked as healthy")
    
    @contextmanager
    def graceful_execution(self, service_name: str, *args, **kwargs):
        """Context manager for graceful execution with fallback"""
        try:
            if self.service_health.get(service_name, True):
                yield "primary"
            else:
                yield "fallback"
        except Exception as e:
            self.mark_service_unhealthy(service_name)
            if service_name in self.fallback_strategies:
                self.logger.info(f"Using fallback strategy for {service_name}")
                yield "fallback"
            else:
                self.logger.error(f"No fallback available for {service_name}, re-raising exception")
                raise

# Enhanced Pipeline Health Monitor
class PipelineHealthMonitor:
    """Comprehensive pipeline health monitoring"""
    
    def __init__(self):
        self.metrics = {}
        self.alerts = []
        self.thresholds = {
            'error_rate': 0.05,  # 5% error rate threshold
            'response_time': 5.0,  # 5 second response time threshold
            'memory_usage': 0.8,   # 80% memory usage threshold
        }
        self.logger = logging.getLogger(f"{__name__}.HealthMonitor")
    
    def record_metric(self, metric_name: str, value: float, context: Dict[str, Any] = None):
        """Record a metric measurement"""
        timestamp = time.time()
        if metric_name not in self.metrics:
            self.metrics[metric_name] = []
        
        self.metrics[metric_name].append({
            'value': value,
            'timestamp': timestamp,
            'context': context or {}
        })
        
        # Check thresholds
        self._check_thresholds(metric_name, value, context)
    
    def _check_thresholds(self, metric_name: str, value: float, context: Dict[str, Any]):
        """Check if metric exceeds thresholds"""
        if metric_name in self.thresholds and value > self.thresholds[metric_name]:
            alert = {
                'metric': metric_name,
                'value': value,
                'threshold': self.thresholds[metric_name],
                'timestamp': time.time(),
                'context': context
            }
            self.alerts.append(alert)
            self.logger.warning(f"Threshold exceeded for {metric_name}: {value} > {self.thresholds[metric_name]}")
    
    def get_health_report(self) -> Dict[str, Any]:
        """Generate comprehensive health report"""
        return {
            'metrics_summary': {name: len(values) for name, values in self.metrics.items()},
            'recent_alerts': self.alerts[-10:],  # Last 10 alerts
            'thresholds': self.thresholds,
            'timestamp': time.time()
        }

# Test Generation 2 Robustness Features
def test_generation2_robustness():
    """Test Generation 2 robustness features"""
    print("🛡️ Testing Generation 2: MAKE IT ROBUST (Reliable)")
    print("=" * 60)
    
    # Test Enhanced Error Handling
    print("🔧 Testing enhanced error handling...")
    try:
        raise DataExtractionError("Test extraction error", source="test-source", retry_count=3)
    except ETLBaseException as e:
        print(f"   ✅ Custom exception caught: {e.error_code}")
        print(f"   📝 Context: {e.context}")
    
    # Test Retry Mechanism
    print("\n🔄 Testing retry mechanism...")
    attempt_count = 0
    
    @retry_with_backoff(max_retries=2, exceptions=(ValueError,))
    def flaky_function():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise ValueError(f"Attempt {attempt_count} failed")
        return f"Success on attempt {attempt_count}"
    
    try:
        result = flaky_function()
        print(f"   ✅ Retry successful: {result}")
    except Exception as e:
        print(f"   ❌ Retry failed: {e}")
    
    # Test Circuit Breaker
    print("\n⚡ Testing circuit breaker...")
    cb = CircuitBreaker(failure_threshold=2, recovery_timeout=1.0)
    
    def failing_service():
        raise Exception("Service unavailable")
    
    # Trigger failures to open circuit
    for i in range(3):
        try:
            cb.call(failing_service)
        except:
            pass
    
    print(f"   ✅ Circuit breaker state: {cb.state}")
    
    # Test Data Validator
    print("\n📊 Testing enhanced data validation...")
    validator = DataValidator()
    validator.add_rule('not_empty', lambda data: len(data) > 0, "Data cannot be empty")
    validator.add_rule('has_id', lambda data: all('id' in record for record in data if isinstance(record, dict)), "All records must have ID")
    
    test_data = [{'id': 1, 'name': 'test'}]
    validation_result = validator.validate_data(test_data)
    print(f"   ✅ Validation result: {'Valid' if validation_result['valid'] else 'Invalid'}")
    
    # Test Health Monitor
    print("\n🏥 Testing health monitoring...")
    health_monitor = PipelineHealthMonitor()
    health_monitor.record_metric('response_time', 2.5)
    health_monitor.record_metric('error_rate', 0.02)
    health_monitor.record_metric('response_time', 6.0)  # Should trigger alert
    
    health_report = health_monitor.get_health_report()
    print(f"   ✅ Health alerts triggered: {len(health_report['recent_alerts'])}")
    
    print("\n" + "=" * 60)
    print("🎉 Generation 2 Robustness Testing Complete!")
    return True

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_generation2_robustness()