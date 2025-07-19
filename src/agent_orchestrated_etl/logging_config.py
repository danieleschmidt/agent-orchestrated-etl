"""Structured logging configuration for Agent-Orchestrated ETL."""

from __future__ import annotations

import json
import logging
import logging.handlers
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Union

from .config import get_config


class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging."""
    
    def __init__(
        self,
        include_extra: bool = True,
        include_stack_trace: bool = True,
        timestamp_format: str = "iso",
    ):
        """Initialize structured formatter.
        
        Args:
            include_extra: Whether to include extra fields from log records
            include_stack_trace: Whether to include stack traces for exceptions
            timestamp_format: Format for timestamps ("iso", "epoch", "readable")
        """
        super().__init__()
        self.include_extra = include_extra
        self.include_stack_trace = include_stack_trace
        self.timestamp_format = timestamp_format
        
        # Standard fields that are always included
        self.standard_fields = {
            'timestamp', 'level', 'logger', 'message', 'module', 'function',
            'line_number', 'thread_id', 'process_id', 'exc_info', 'stack_info'
        }
    
    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as structured JSON."""
        # Base log entry
        log_entry = {
            'timestamp': self._format_timestamp(record.created),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'module': record.module,
            'function': record.funcName,
            'line_number': record.lineno,
            'thread_id': record.thread,
            'process_id': record.process,
        }
        
        # Add exception information if present
        if record.exc_info and self.include_stack_trace:
            log_entry['exception'] = {
                'type': record.exc_info[0].__name__ if record.exc_info[0] else None,
                'message': str(record.exc_info[1]) if record.exc_info[1] else None,
                'traceback': self.formatException(record.exc_info) if record.exc_info else None,
            }
        
        # Add stack info if present
        if record.stack_info:
            log_entry['stack_trace'] = record.stack_info
        
        # Add extra fields if enabled
        if self.include_extra:
            extra_fields = {}
            for key, value in record.__dict__.items():
                if key not in self.standard_fields and not key.startswith('_'):
                    # Serialize complex objects
                    try:
                        json.dumps(value)  # Test if it's JSON serializable
                        extra_fields[key] = value
                    except (TypeError, ValueError):
                        extra_fields[key] = str(value)
            
            if extra_fields:
                log_entry['extra'] = extra_fields
        
        # Convert to JSON
        try:
            return json.dumps(log_entry, default=str, ensure_ascii=False)
        except Exception:
            # Fallback to simple string format if JSON serialization fails
            return f"{log_entry['timestamp']} {log_entry['level']} {log_entry['logger']}: {log_entry['message']}"
    
    def _format_timestamp(self, timestamp: float) -> str:
        """Format timestamp according to configuration."""
        if self.timestamp_format == "epoch":
            return str(timestamp)
        elif self.timestamp_format == "readable":
            return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
        else:  # iso format (default)
            return datetime.fromtimestamp(timestamp).isoformat()


class ContextFilter(logging.Filter):
    """Filter to add contextual information to log records."""
    
    def __init__(self, service_name: str = "agent-etl", version: str = "0.0.1"):
        """Initialize context filter.
        
        Args:
            service_name: Name of the service
            version: Version of the service
        """
        super().__init__()
        self.service_name = service_name
        self.version = version
        self.hostname = os.uname().nodename if hasattr(os, 'uname') else 'unknown'
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Add contextual information to log record."""
        # Add service context
        record.service_name = self.service_name
        record.service_version = self.version
        record.hostname = self.hostname
        
        # Add execution context
        record.execution_id = getattr(record, 'execution_id', None)
        record.pipeline_id = getattr(record, 'pipeline_id', None)
        record.task_id = getattr(record, 'task_id', None)
        record.user_id = getattr(record, 'user_id', None)
        
        # Add performance context
        record.duration_ms = getattr(record, 'duration_ms', None)
        record.memory_usage_mb = getattr(record, 'memory_usage_mb', None)
        
        return True


class SecurityFilter(logging.Filter):
    """Filter to prevent logging of sensitive information."""
    
    def __init__(self):
        """Initialize security filter."""
        super().__init__()
        self.sensitive_patterns = [
            'password', 'passwd', 'pwd', 'secret', 'token', 'key', 'api_key',
            'access_token', 'refresh_token', 'auth', 'authorization', 'credential',
            'private_key', 'certificate', 'cert', 'ssl_key', 'session_id'
        ]
    
    def filter(self, record: logging.LogRecord) -> bool:
        """Filter out sensitive information from log records."""
        # Check message for sensitive patterns
        message = record.getMessage().lower()
        for pattern in self.sensitive_patterns:
            if pattern in message:
                # Replace the entire message with a sanitized version
                record.msg = "[REDACTED - SENSITIVE DATA DETECTED]"
                record.args = ()
                break
        
        # Check extra fields for sensitive information
        for key in list(record.__dict__.keys()):
            if any(pattern in key.lower() for pattern in self.sensitive_patterns):
                setattr(record, key, "[REDACTED]")
        
        return True


class PerformanceLogger:
    """Logger for performance metrics and timing."""
    
    def __init__(self, logger_name: str = "agent_etl.performance"):
        """Initialize performance logger.
        
        Args:
            logger_name: Name of the logger to use
        """
        self.logger = logging.getLogger(logger_name)
        self.start_times: Dict[str, float] = {}
    
    def start_timer(self, operation: str, **context) -> None:
        """Start timing an operation.
        
        Args:
            operation: Name of the operation being timed
            **context: Additional context information
        """
        self.start_times[operation] = time.perf_counter()
        self.logger.info(
            f"Started operation: {operation}",
            extra={
                "event_type": "operation_start",
                "operation": operation,
                **context
            }
        )
    
    def end_timer(self, operation: str, **context) -> float:
        """End timing an operation and log the duration.
        
        Args:
            operation: Name of the operation being timed
            **context: Additional context information
            
        Returns:
            Duration in seconds
        """
        if operation not in self.start_times:
            self.logger.warning(f"No start time found for operation: {operation}")
            return 0.0
        
        duration = time.perf_counter() - self.start_times[operation]
        duration_ms = duration * 1000
        
        self.logger.info(
            f"Completed operation: {operation} in {duration_ms:.2f}ms",
            extra={
                "event_type": "operation_complete",
                "operation": operation,
                "duration_ms": duration_ms,
                "duration_seconds": duration,
                **context
            }
        )
        
        del self.start_times[operation]
        return duration
    
    def log_metric(self, metric_name: str, value: Union[int, float], unit: str = "", **context):
        """Log a custom metric.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            unit: Unit of measurement
            **context: Additional context information
        """
        self.logger.info(
            f"Metric {metric_name}: {value}{unit}",
            extra={
                "event_type": "metric",
                "metric_name": metric_name,
                "metric_value": value,
                "metric_unit": unit,
                **context
            }
        )


class AuditLogger:
    """Logger for security and audit events."""
    
    def __init__(self, logger_name: str = "agent_etl.audit"):
        """Initialize audit logger.
        
        Args:
            logger_name: Name of the logger to use
        """
        self.logger = logging.getLogger(logger_name)
    
    def log_authentication(self, user_id: str, success: bool, **context):
        """Log authentication events.
        
        Args:
            user_id: User identifier
            success: Whether authentication was successful
            **context: Additional context information
        """
        self.logger.info(
            f"Authentication {'succeeded' if success else 'failed'} for user: {user_id}",
            extra={
                "event_type": "authentication",
                "user_id": user_id,
                "success": success,
                **context
            }
        )
    
    def log_authorization(self, user_id: str, resource: str, action: str, success: bool, **context):
        """Log authorization events.
        
        Args:
            user_id: User identifier
            resource: Resource being accessed
            action: Action being performed
            success: Whether authorization was successful
            **context: Additional context information
        """
        self.logger.info(
            f"Authorization {'granted' if success else 'denied'} for user {user_id} "
            f"to {action} {resource}",
            extra={
                "event_type": "authorization",
                "user_id": user_id,
                "resource": resource,
                "action": action,
                "success": success,
                **context
            }
        )
    
    def log_data_access(self, user_id: str, data_source: str, operation: str, **context):
        """Log data access events.
        
        Args:
            user_id: User identifier
            data_source: Data source being accessed
            operation: Type of operation (read, write, delete, etc.)
            **context: Additional context information
        """
        self.logger.info(
            f"Data access: user {user_id} performed {operation} on {data_source}",
            extra={
                "event_type": "data_access",
                "user_id": user_id,
                "data_source": data_source,
                "operation": operation,
                **context
            }
        )
    
    def log_security_event(self, event_type: str, severity: str, description: str, **context):
        """Log security events.
        
        Args:
            event_type: Type of security event
            severity: Severity level
            description: Event description
            **context: Additional context information
        """
        level = logging.WARNING if severity.lower() in ['high', 'critical'] else logging.INFO
        
        self.logger.log(
            level,
            f"Security event ({severity}): {description}",
            extra={
                "event_type": "security_event",
                "security_event_type": event_type,
                "severity": severity,
                "description": description,
                **context
            }
        )


def setup_logging(
    log_level: Optional[str] = None,
    log_format: Optional[str] = None,
    log_output: Optional[str] = None,
    log_file: Optional[str] = None,
) -> None:
    """Set up structured logging for the application.
    
    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Logging format ("json" or "text")
        log_output: Output destination ("console", "file", or "both")
        log_file: Path to log file (if file output is enabled)
    """
    # Get configuration
    config = get_config()
    
    # Use provided values or fall back to configuration
    level = log_level or config.logging.level
    format_type = log_format or config.logging.format
    output = log_output or config.logging.output
    file_path = log_file or config.logging.file_path
    
    # Convert level string to logging constant
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    
    # Get root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(numeric_level)
    
    # Clear existing handlers
    root_logger.handlers.clear()
    
    # Create formatter
    if format_type.lower() == "json":
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    # Create filters
    context_filter = ContextFilter()
    security_filter = SecurityFilter()
    
    # Set up console handler if needed
    if output in ["console", "both"]:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(numeric_level)
        console_handler.setFormatter(formatter)
        console_handler.addFilter(context_filter)
        console_handler.addFilter(security_filter)
        root_logger.addHandler(console_handler)
    
    # Set up file handler if needed
    if output in ["file", "both"] and file_path:
        # Ensure log directory exists
        log_dir = Path(file_path).parent
        log_dir.mkdir(parents=True, exist_ok=True)
        
        # Create rotating file handler
        file_handler = logging.handlers.RotatingFileHandler(
            file_path,
            maxBytes=config.logging.max_file_size_mb * 1024 * 1024,
            backupCount=config.logging.backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        file_handler.addFilter(context_filter)
        file_handler.addFilter(security_filter)
        root_logger.addHandler(file_handler)
    
    # Set up specific loggers with appropriate levels
    
    # Application loggers
    logging.getLogger("agent_orchestrated_etl").setLevel(numeric_level)
    
    # Performance logger
    perf_logger = logging.getLogger("agent_etl.performance")
    perf_logger.setLevel(logging.INFO)
    
    # Audit logger - always at INFO level for compliance
    audit_logger = logging.getLogger("agent_etl.audit")
    audit_logger.setLevel(logging.INFO)
    
    # Security logger - always at WARNING level
    security_logger = logging.getLogger("agent_etl.security")
    security_logger.setLevel(logging.WARNING)
    
    # External library loggers (reduce noise)
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    logging.getLogger("requests").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    
    # Log startup message
    logger = logging.getLogger("agent_orchestrated_etl.startup")
    logger.info(
        "Logging configured",
        extra={
            "log_level": level,
            "log_format": format_type,
            "log_output": output,
            "log_file": file_path,
        }
    )


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the specified name.
    
    Args:
        name: Logger name
        
    Returns:
        Configured logger instance
    """
    return logging.getLogger(name)


def get_performance_logger() -> PerformanceLogger:
    """Get the performance logger instance.
    
    Returns:
        Performance logger instance
    """
    return PerformanceLogger()


def get_audit_logger() -> AuditLogger:
    """Get the audit logger instance.
    
    Returns:
        Audit logger instance
    """
    return AuditLogger()


# Context managers for structured logging
class LogContext:
    """Context manager for adding structured context to logs."""
    
    def __init__(self, **context):
        """Initialize log context.
        
        Args:
            **context: Context fields to add to all log records
        """
        self.context = context
        self.old_factory = logging.getLogRecordFactory()
    
    def __enter__(self):
        """Enter context and set up log record factory."""
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            for key, value in self.context.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore original log record factory."""
        logging.setLogRecordFactory(self.old_factory)


class TimedOperation:
    """Context manager for timing operations."""
    
    def __init__(self, operation_name: str, logger: Optional[logging.Logger] = None, **context):
        """Initialize timed operation.
        
        Args:
            operation_name: Name of the operation
            logger: Logger to use (defaults to performance logger)
            **context: Additional context information
        """
        self.operation_name = operation_name
        self.logger = logger or get_performance_logger()
        self.context = context
        self.start_time = None
    
    def __enter__(self):
        """Start timing the operation."""
        self.start_time = time.perf_counter()
        if hasattr(self.logger, 'start_timer'):
            self.logger.start_timer(self.operation_name, **self.context)
        else:
            self.logger.info(f"Started operation: {self.operation_name}", extra=self.context)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """End timing and log the duration."""
        if self.start_time:
            duration = time.perf_counter() - self.start_time
            duration_ms = duration * 1000
            
            if hasattr(self.logger, 'end_timer'):
                self.logger.end_timer(self.operation_name, **self.context)
            else:
                self.logger.info(
                    f"Completed operation: {self.operation_name} in {duration_ms:.2f}ms",
                    extra={**self.context, "duration_ms": duration_ms}
                )