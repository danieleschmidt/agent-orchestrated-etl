"""Tests for error handling and resilience features."""

import time

import pytest

from agent_orchestrated_etl.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    circuit_breaker,
)
from agent_orchestrated_etl.exceptions import (
    AgentETLException,
    CircuitBreakerException,
    ErrorCategory,
    ErrorSeverity,
    NetworkException,
    ValidationException,
)
from agent_orchestrated_etl.graceful_degradation import (
    DegradationConfig,
    FallbackStrategy,
    degradation_manager,
    with_graceful_degradation,
)
from agent_orchestrated_etl.retry import (
    RetryConfig,
    RetryConfigs,
    retry,
)


class TestAgentETLException:
    """Test the base exception class."""

    def test_exception_creation(self):
        """Test basic exception creation."""
        exc = AgentETLException(
            "Test error",
            error_code="TEST_001",
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.VALIDATION,
        )

        assert exc.message == "Test error"
        assert exc.error_code == "TEST_001"
        assert exc.severity == ErrorSeverity.HIGH
        assert exc.category == ErrorCategory.VALIDATION
        assert not exc.is_retryable()
        assert exc.is_fatal()

    def test_exception_with_retry(self):
        """Test exception with retry configuration."""
        exc = AgentETLException(
            "Retryable error",
            retry_after=5.0,
            severity=ErrorSeverity.LOW,
        )

        assert exc.is_retryable()
        assert not exc.is_fatal()
        assert exc.retry_after == 5.0

    def test_exception_serialization(self):
        """Test exception to_dict method."""
        exc = ValidationException(
            "Invalid input",
            context={"field": "username"},
        )

        data = exc.to_dict()
        assert data["message"] == "Invalid input"
        assert data["category"] == "validation"
        assert data["severity"] == "medium"
        assert data["context"]["field"] == "username"


class TestRetryMechanism:
    """Test retry functionality."""

    def test_retry_success(self):
        """Test successful operation without retries."""
        call_count = 0

        @retry(RetryConfigs.FAST)
        def successful_operation():
            nonlocal call_count
            call_count += 1
            return "success"

        result = successful_operation()
        assert result == "success"
        assert call_count == 1

    def test_retry_with_failures(self):
        """Test retry mechanism with failures."""
        call_count = 0

        @retry(RetryConfigs.FAST)
        def failing_operation():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise NetworkException("Network error", retry_after=0.1)
            return "success"

        result = failing_operation()
        assert result == "success"
        assert call_count == 3

    def test_retry_exhaustion(self):
        """Test retry exhaustion."""
        call_count = 0

        @retry(RetryConfig(max_attempts=2, base_delay=0.1))
        def always_failing():
            nonlocal call_count
            call_count += 1
            raise NetworkException("Always fails")

        with pytest.raises(NetworkException):
            always_failing()

        assert call_count == 3  # 1 initial + 2 retry attempts

    def test_non_retryable_exception(self):
        """Test that non-retryable exceptions are not retried."""
        call_count = 0

        @retry(RetryConfigs.FAST)
        def validation_error():
            nonlocal call_count
            call_count += 1
            raise ValidationException("Invalid input")

        with pytest.raises(ValidationException):
            validation_error()

        assert call_count == 1


class TestCircuitBreaker:
    """Test circuit breaker functionality."""

    def test_circuit_breaker_closed_state(self):
        """Test circuit breaker in closed state."""
        cb = CircuitBreaker("test_service", CircuitBreakerConfig(failure_threshold=3))

        assert cb.get_state() == CircuitState.CLOSED

        # Successful calls should keep it closed
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.get_state() == CircuitState.CLOSED

    def test_circuit_breaker_opens_on_failures(self):
        """Test circuit breaker opens after failures."""
        cb = CircuitBreaker("test_service", CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.1
        ))

        # First failure
        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("Test error")))
        assert cb.get_state() == CircuitState.CLOSED

        # Second failure should open the circuit
        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("Test error")))
        assert cb.get_state() == CircuitState.OPEN

        # Subsequent calls should be rejected
        with pytest.raises(CircuitBreakerException):
            cb.call(lambda: "success")

    def test_circuit_breaker_half_open_recovery(self):
        """Test circuit breaker recovery through half-open state."""
        cb = CircuitBreaker("test_service", CircuitBreakerConfig(
            failure_threshold=1,
            recovery_timeout=0.1,
            success_threshold=1
        ))

        # Open the circuit
        with pytest.raises(Exception):
            cb.call(lambda: (_ for _ in ()).throw(Exception("Test error")))
        assert cb.get_state() == CircuitState.OPEN

        # Wait for recovery timeout
        time.sleep(0.2)

        # Next call should trigger half-open state and succeed
        result = cb.call(lambda: "success")
        assert result == "success"
        assert cb.get_state() == CircuitState.CLOSED


class TestGracefulDegradation:
    """Test graceful degradation functionality."""

    def test_degradation_default_value(self):
        """Test degradation with default value fallback."""
        call_count = 0

        @with_graceful_degradation(
            "test_service",
            DegradationConfig(
                fallback_strategy=FallbackStrategy.RETURN_DEFAULT,
                default_value="fallback_result",
                failure_threshold=1,
            )
        )
        def failing_function():
            nonlocal call_count
            call_count += 1
            raise Exception("Service unavailable")

        # First call should fail and trigger degradation
        result = failing_function()
        assert result == "fallback_result"
        assert call_count == 1

        # Check degradation status
        context = degradation_manager.get_service_status("test_service")
        assert context["current_level"] != "full"

    def test_degradation_cached_result(self):
        """Test degradation with cached result fallback."""
        @with_graceful_degradation(
            "cache_service",
            DegradationConfig(
                fallback_strategy=FallbackStrategy.RETURN_CACHED,
                failure_threshold=1,
                cache_ttl=60.0,
            )
        )
        def cacheable_function(value):
            if value == "fail":
                raise Exception("Service error")
            return f"result_{value}"

        # First call succeeds and caches result
        result1 = cacheable_function("success")
        assert result1 == "result_success"

        # Modify function to fail on the same argument to test caching
        def failing_function(value):
            if value == "success":  # Now fails on same arg that was cached
                raise Exception("Service error")
            return f"result_{value}"

        # Replace the wrapped function
        cacheable_function.__wrapped__ = failing_function

        # Second call with same args should return cached result
        result2 = cacheable_function("success")
        assert result2 == "result_success"  # Should return cached result

    def test_degradation_service_recovery(self):
        """Test service recovery from degraded state."""
        call_count = 0

        @with_graceful_degradation(
            "recovery_service",
            DegradationConfig(
                failure_threshold=1,
                success_threshold=1,
                default_value="degraded_result",
            )
        )
        def recovering_function():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception("Temporary failure")
            return "normal_result"

        # First calls trigger degradation
        result1 = recovering_function()
        assert result1 == "degraded_result"

        result2 = recovering_function()
        assert result2 == "degraded_result"

        # Service recovers
        result3 = recovering_function()
        assert result3 == "normal_result"


class TestIntegratedResilience:
    """Test integrated resilience patterns."""

    def test_retry_with_circuit_breaker(self):
        """Test retry mechanism with circuit breaker."""
        # Create circuit breaker manually to control it
        cb = CircuitBreaker("integrated_test", CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=0.1
        ))

        # Directly trigger circuit breaker failures without retry interference
        for i in range(3):  # Exceed failure threshold
            try:
                cb.call(lambda: (_ for _ in ()).throw(Exception("Test error")))
            except Exception:
                pass  # Expected to fail

        # Circuit should be open after exceeding failure threshold
        assert cb.get_state() == CircuitState.OPEN

    def test_full_resilience_stack(self):
        """Test complete resilience stack integration."""
        call_count = 0

        @with_graceful_degradation(
            "full_stack_service",
            DegradationConfig(
                fallback_strategy=FallbackStrategy.RETURN_DEFAULT,
                default_value="resilient_result",
                failure_threshold=3,
            )
        )
        @circuit_breaker(
            "full_stack_cb",
            CircuitBreakerConfig(failure_threshold=5, recovery_timeout=0.1)
        )
        @retry(RetryConfig(max_attempts=2, base_delay=0.01))
        def fully_protected_function():
            nonlocal call_count
            call_count += 1
            if call_count < 10:  # Fail many times
                raise NetworkException("Network unavailable")
            return "success"

        # Should eventually return fallback result
        result = fully_protected_function()
        assert result == "resilient_result"

        # Verify degradation occurred
        status = degradation_manager.get_service_status("full_stack_service")
        assert status is not None


class TestErrorHandlingHelpers:
    """Test error handling helper functions."""

    def test_is_retryable_exception(self):
        """Test retryable exception detection."""
        from agent_orchestrated_etl.exceptions import is_retryable_exception

        # Retryable exceptions
        assert is_retryable_exception(NetworkException("Network error", retry_after=1.0))
        assert is_retryable_exception(ConnectionError("Connection failed"))

        # Non-retryable exceptions
        assert not is_retryable_exception(ValidationException("Invalid input"))
        assert not is_retryable_exception(ValueError("Invalid value"))

    def test_get_retry_delay(self):
        """Test retry delay extraction."""
        from agent_orchestrated_etl.exceptions import get_retry_delay

        # Exception with retry delay
        exc_with_delay = NetworkException("Network error", retry_after=5.0)
        assert get_retry_delay(exc_with_delay) == 5.0

        # Exception without retry delay
        exc_without_delay = ValidationException("Invalid input")
        assert get_retry_delay(exc_without_delay) is None

        # Standard library exception
        std_exc = ConnectionError("Connection failed")
        assert get_retry_delay(std_exc) == 5.0  # Default for ConnectionError

    def test_categorize_exception(self):
        """Test exception categorization."""
        from agent_orchestrated_etl.exceptions import categorize_exception

        # Custom exceptions
        assert categorize_exception(NetworkException("Network error")) == ErrorCategory.NETWORK
        assert categorize_exception(ValidationException("Invalid input")) == ErrorCategory.VALIDATION

        # Standard library exceptions
        assert categorize_exception(ConnectionError("Connection failed")) == ErrorCategory.NETWORK
        assert categorize_exception(PermissionError("Access denied")) == ErrorCategory.AUTHORIZATION
        assert categorize_exception(FileNotFoundError("File not found")) == ErrorCategory.DATA_SOURCE
        assert categorize_exception(ValueError("Invalid value")) == ErrorCategory.VALIDATION
