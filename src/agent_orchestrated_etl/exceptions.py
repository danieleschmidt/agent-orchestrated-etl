"""Comprehensive exception hierarchy for Agent-Orchestrated ETL."""

from __future__ import annotations

import time
from enum import Enum
from typing import Any, Dict, List, Optional


class ErrorSeverity(Enum):
    """Error severity levels for categorizing exceptions."""

    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Categories of errors for better error handling."""

    VALIDATION = "validation"
    CONFIGURATION = "configuration"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    NETWORK = "network"
    DATA_SOURCE = "data_source"
    DATA_PROCESSING = "data_processing"
    PIPELINE = "pipeline"
    SYSTEM = "system"
    EXTERNAL = "external"
    AGENT = "agent"


class AgentETLException(Exception):
    """Base exception class for all Agent-Orchestrated ETL errors.
    
    Provides structured error information including severity, category,
    error codes, and context for better error handling and monitoring.
    """

    def __init__(
        self,
        message: str,
        *,
        error_code: Optional[str] = None,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        category: ErrorCategory = ErrorCategory.SYSTEM,
        context: Optional[Dict[str, Any]] = None,
        cause: Optional[Exception] = None,
        retry_after: Optional[float] = None,
        user_message: Optional[str] = None,
    ):
        """Initialize the exception with structured error information.
        
        Args:
            message: Technical error message for developers
            error_code: Unique error code for programmatic handling
            severity: Severity level of the error
            category: Category of the error for routing
            context: Additional context information
            cause: Original exception that caused this error
            retry_after: Suggested retry delay in seconds
            user_message: User-friendly error message
        """
        super().__init__(message)
        self.message = message
        self.error_code = error_code or self._generate_error_code()
        self.severity = severity
        self.category = category
        self.context = context or {}
        self.cause = cause
        self.retry_after = retry_after
        self.user_message = user_message or self._generate_user_message()
        self.timestamp = time.time()

    def _generate_error_code(self) -> str:
        """Generate a default error code based on the exception class."""
        class_name = self.__class__.__name__
        return f"ETL_{class_name.upper().replace('EXCEPTION', '').replace('ERROR', '')}"

    def _generate_user_message(self) -> str:
        """Generate a user-friendly error message."""
        return "An error occurred during ETL processing. Please check the logs for details."

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for serialization."""
        return {
            "error_code": self.error_code,
            "message": self.message,
            "user_message": self.user_message,
            "severity": self.severity.value,
            "category": self.category.value,
            "context": self.context,
            "timestamp": self.timestamp,
            "retry_after": self.retry_after,
            "cause": str(self.cause) if self.cause else None,
        }

    def is_retryable(self) -> bool:
        """Check if this error is retryable."""
        return self.retry_after is not None

    def is_fatal(self) -> bool:
        """Check if this error is fatal and should stop processing."""
        return self.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]


# Configuration and Validation Exceptions
class ConfigurationException(AgentETLException):
    """Raised when configuration is invalid or missing."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Configuration error. Please check your configuration settings."


class ValidationException(AgentETLException):
    """Raised when input validation fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.VALIDATION,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Invalid input provided. Please check your parameters."


# Compliance and Security Exceptions
class ComplianceException(AgentETLException):
    """Raised when compliance requirements are not met."""

    def __init__(self, message: str, standard: str = "unknown", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.COMPLIANCE,
            context={"compliance_standard": standard},
            **kwargs
        )

    def _generate_user_message(self) -> str:
        standard = self.context.get("compliance_standard", "compliance standard")
        return f"Compliance violation detected for {standard}. Please review your data handling practices."


class SecurityException(AgentETLException):
    """Raised when security policies are violated."""

    def __init__(self, message: str, security_level: str = "medium", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SECURITY,
            context={"security_level": security_level},
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Security policy violation detected. Please review your security configuration."


# Legacy aliases for backward compatibility
ValidationError = ValidationException
ConfigurationError = ConfigurationException


# Authentication and Authorization Exceptions
class AuthenticationException(AgentETLException):
    """Raised when authentication fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Authentication failed. Please check your credentials."


class AuthorizationException(AgentETLException):
    """Raised when authorization fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHORIZATION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Access denied. You don't have permission to perform this operation."


# Network and External Service Exceptions
class NetworkException(AgentETLException):
    """Raised when network operations fail."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.NETWORK,
            severity=ErrorSeverity.MEDIUM,
            retry_after=kwargs.pop("retry_after", 5.0),
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Network connection error. The operation will be retried automatically."


class ExternalServiceException(AgentETLException):
    """Raised when external service operations fail."""

    def __init__(self, message: str, service_name: str = "unknown", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXTERNAL,
            severity=ErrorSeverity.MEDIUM,
            context={"service_name": service_name},
            retry_after=kwargs.pop("retry_after", 10.0),
            **kwargs
        )

    def _generate_user_message(self) -> str:
        service = self.context.get("service_name", "external service")
        return f"Error communicating with {service}. The operation will be retried."


class RateLimitException(ExternalServiceException):
    """Raised when rate limits are exceeded."""

    def __init__(self, message: str, retry_after: float = 60.0, **kwargs):
        super().__init__(
            message,
            retry_after=retry_after,
            severity=ErrorSeverity.LOW,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return f"Rate limit exceeded. Please wait {self.retry_after} seconds before retrying."


# Data Source Exceptions
class DataSourceException(AgentETLException):
    """Base exception for data source related errors."""

    def __init__(self, message: str, source_type: str = "unknown", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATA_SOURCE,
            context={"source_type": source_type},
            **kwargs
        )


class DataSourceConnectionException(DataSourceException):
    """Raised when data source connection fails."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            retry_after=kwargs.pop("retry_after", 30.0),
            **kwargs
        )

    def _generate_user_message(self) -> str:
        source = self.context.get("source_type", "data source")
        return f"Unable to connect to {source}. Connection will be retried."


class DataSourceNotFoundException(DataSourceException):
    """Raised when data source is not found."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        source = self.context.get("source_type", "data source")
        return f"The specified {source} was not found."


class DataSourcePermissionException(DataSourceException):
    """Raised when data source access is denied."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            category=ErrorCategory.AUTHORIZATION,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        source = self.context.get("source_type", "data source")
        return f"Access denied to {source}. Please check your permissions."


# Data Processing Exceptions
class DataProcessingException(AgentETLException):
    """Base exception for data processing errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATA_PROCESSING,
            **kwargs
        )


class DataValidationException(DataProcessingException):
    """Raised when data validation fails."""

    def __init__(self, message: str, invalid_records: Optional[List[Dict[str, Any]]] = None, **kwargs):
        super().__init__(
            message,
            context={"invalid_records_count": len(invalid_records or [])},
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )
        self.invalid_records = invalid_records or []

    def _generate_user_message(self) -> str:
        count = self.context.get("invalid_records_count", 0)
        return f"Data validation failed for {count} records. Please check the data format."


class DataTransformationException(DataProcessingException):
    """Raised when data transformation fails."""

    def __init__(self, message: str, transformation_step: str = "unknown", **kwargs):
        super().__init__(
            message,
            context={"transformation_step": transformation_step},
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        step = self.context.get("transformation_step", "transformation")
        return f"Data transformation failed at step: {step}."


class DataCorruptionException(DataProcessingException):
    """Raised when data corruption is detected."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.CRITICAL,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Data corruption detected. Processing has been halted for safety."


class DatabaseException(DataSourceException):
    """Raised when database operations fail."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            source_type="database",
            severity=ErrorSeverity.MEDIUM,
            retry_after=kwargs.pop("retry_after", 30.0),
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "Database operation failed. The operation will be retried automatically."


# Pipeline Exceptions
class PipelineException(AgentETLException):
    """Base exception for pipeline related errors."""

    def __init__(self, message: str, pipeline_id: str = "unknown", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.PIPELINE,
            context={"pipeline_id": pipeline_id},
            **kwargs
        )


class PipelineExecutionException(PipelineException):
    """Raised when pipeline execution fails."""

    def __init__(self, message: str, task_id: str = "unknown", **kwargs):
        super().__init__(
            message,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )
        self.context["task_id"] = task_id

    def _generate_user_message(self) -> str:
        task = self.context.get("task_id", "task")
        return f"Pipeline execution failed at task: {task}."


class PipelineTimeoutException(PipelineException):
    """Raised when pipeline execution times out."""

    def __init__(self, message: str, timeout_seconds: float = 0, **kwargs):
        super().__init__(
            message,
            context={"timeout_seconds": timeout_seconds},
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        timeout = self.context.get("timeout_seconds", "unknown")
        return f"Pipeline execution timed out after {timeout} seconds."


class PipelineDependencyException(PipelineException):
    """Raised when pipeline dependencies are not met."""

    def __init__(self, message: str, missing_dependencies: Optional[List[str]] = None, **kwargs):
        super().__init__(
            message,
            context={"missing_dependencies": missing_dependencies or []},
            severity=ErrorSeverity.HIGH,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        deps = self.context.get("missing_dependencies", [])
        if deps:
            return f"Pipeline dependencies not met: {', '.join(deps)}"
        return "Pipeline dependencies not met."


# System Exceptions
class SystemException(AgentETLException):
    """Raised for system-level errors."""

    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.SYSTEM,
            severity=ErrorSeverity.CRITICAL,
            **kwargs
        )

    def _generate_user_message(self) -> str:
        return "A system error occurred. Please contact support if the issue persists."


class ResourceExhaustedException(SystemException):
    """Raised when system resources are exhausted."""

    def __init__(self, message: str, resource_type: str = "unknown", **kwargs):
        super().__init__(
            message,
            context={"resource_type": resource_type},
            **kwargs
        )

    def _generate_user_message(self) -> str:
        resource = self.context.get("resource_type", "system resource")
        return f"Insufficient {resource} available. Please try again later."


class CircuitBreakerException(AgentETLException):
    """Raised when circuit breaker is open."""

    def __init__(self, message: str, service_name: str = "unknown", **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.EXTERNAL,
            severity=ErrorSeverity.MEDIUM,
            context={"service_name": service_name},
            retry_after=kwargs.pop("retry_after", 60.0),
            **kwargs
        )

    def _generate_user_message(self) -> str:
        service = self.context.get("service_name", "service")
        return f"Service {service} is temporarily unavailable. Please try again later."


# Helper functions for exception handling
def is_retryable_exception(exception: Exception) -> bool:
    """Check if an exception is retryable."""
    if isinstance(exception, AgentETLException):
        return exception.is_retryable()

    # Check for specific standard library exceptions that are retryable
    retryable_types = (
        ConnectionError,
        TimeoutError,
        OSError,  # Includes network errors
    )

    return isinstance(exception, retryable_types)


def get_retry_delay(exception: Exception) -> Optional[float]:
    """Get the retry delay for an exception."""
    if isinstance(exception, AgentETLException):
        return exception.retry_after

    # Default retry delays for standard exceptions
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return 5.0
    elif isinstance(exception, OSError):
        return 2.0

    return None


def categorize_exception(exception: Exception) -> ErrorCategory:
    """Categorize an exception for routing and handling."""
    if isinstance(exception, AgentETLException):
        return exception.category

    # Categorize standard library exceptions
    if isinstance(exception, (ConnectionError, TimeoutError)):
        return ErrorCategory.NETWORK
    elif isinstance(exception, PermissionError):
        return ErrorCategory.AUTHORIZATION
    elif isinstance(exception, FileNotFoundError):
        return ErrorCategory.DATA_SOURCE
    elif isinstance(exception, ValueError):
        return ErrorCategory.VALIDATION
    else:
        return ErrorCategory.SYSTEM


# Agent-related Exceptions
class AgentException(AgentETLException):
    """Base exception for agent-related errors."""

    def __init__(
        self,
        message: str,
        *,
        agent_id: Optional[str] = None,
        agent_role: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.AGENT, **kwargs)
        self.agent_id = agent_id
        self.agent_role = agent_role

    def _generate_user_message(self) -> str:
        return "Agent operation failed. Please check agent status and configuration."


class CommunicationException(AgentException):
    """Exception raised for agent communication errors."""

    def __init__(
        self,
        message: str,
        *,
        sender_id: Optional[str] = None,
        recipient_id: Optional[str] = None,
        message_type: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.sender_id = sender_id
        self.recipient_id = recipient_id
        self.message_type = message_type

    def _generate_user_message(self) -> str:
        return "Agent communication failed. Please check network connectivity and agent status."


class CoordinationException(AgentException):
    """Exception raised for agent coordination errors."""

    def __init__(
        self,
        message: str,
        *,
        workflow_id: Optional[str] = None,
        coordination_pattern: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, **kwargs)
        self.workflow_id = workflow_id
        self.coordination_pattern = coordination_pattern

    def _generate_user_message(self) -> str:
        return "Agent coordination failed. Please check workflow configuration and agent availability."


class ToolException(AgentETLException):
    """Exception raised for tool execution errors."""

    def __init__(
        self,
        message: str,
        *,
        tool_name: Optional[str] = None,
        tool_inputs: Optional[Dict[str, Any]] = None,
        **kwargs,
    ):
        # Set tool-specific attributes first
        self.tool_name = tool_name
        self.tool_inputs = tool_inputs
        # Now call parent constructor
        super().__init__(message, category=ErrorCategory.SYSTEM, **kwargs)

    def _generate_user_message(self) -> str:
        tool = self.tool_name or "tool"
        return f"Tool execution failed: {tool}. Please check tool configuration and inputs."


class MonitoringException(AgentETLException):
    """Exception raised for monitoring system errors."""

    def __init__(
        self,
        message: str,
        *,
        monitoring_target: Optional[str] = None,
        monitoring_type: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.SYSTEM, **kwargs)
        self.monitoring_target = monitoring_target
        self.monitoring_type = monitoring_type

    def _generate_user_message(self) -> str:
        target = self.monitoring_target or "system"
        return f"Monitoring failed for {target}. Please check monitoring configuration."


class TestingException(AgentETLException):
    """Exception raised for testing framework errors."""

    def __init__(
        self,
        message: str,
        *,
        test_id: Optional[str] = None,
        test_type: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.SYSTEM, **kwargs)
        self.test_id = test_id
        self.test_type = test_type

    def _generate_user_message(self) -> str:
        test = self.test_id or "test"
        return f"Test execution failed: {test}. Please check test configuration and environment."


# Production Deployment Exceptions
class DeploymentException(AgentETLException):
    """Exception raised for production deployment errors."""

    def __init__(
        self,
        message: str,
        *,
        deployment_id: Optional[str] = None,
        environment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.SYSTEM, severity=ErrorSeverity.HIGH, **kwargs)
        self.deployment_id = deployment_id
        self.environment = environment

    def _generate_user_message(self) -> str:
        env = self.environment or "environment"
        return f"Deployment failed to {env}. Please check deployment configuration and logs."


class InfrastructureException(AgentETLException):
    """Exception raised for infrastructure-related errors."""

    def __init__(
        self,
        message: str,
        *,
        resource_type: Optional[str] = None,
        resource_id: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.SYSTEM, severity=ErrorSeverity.CRITICAL, **kwargs)
        self.resource_type = resource_type
        self.resource_id = resource_id

    def _generate_user_message(self) -> str:
        resource = self.resource_type or "infrastructure resource"
        return f"Infrastructure error with {resource}. Please check system status and configuration."


# Security Exceptions
class SecurityException(AgentETLException):
    """Exception raised for security-related errors."""

    def __init__(
        self,
        message: str,
        *,
        security_violation_type: Optional[str] = None,
        resource_accessed: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(message, category=ErrorCategory.AUTHORIZATION, severity=ErrorSeverity.CRITICAL, **kwargs)
        self.security_violation_type = security_violation_type
        self.resource_accessed = resource_accessed

    def _generate_user_message(self) -> str:
        violation = self.security_violation_type or "security violation"
        return f"Security error: {violation}. Access denied for security reasons."


# Legacy aliases for backward compatibility
ConfigurationError = ConfigurationException
DatabaseException = DataSourceException
BaseETLException = AgentETLException
AgentCommunicationException = CommunicationException
ResourceExhaustionException = ResourceExhaustedException
ComplianceException = SecurityException
