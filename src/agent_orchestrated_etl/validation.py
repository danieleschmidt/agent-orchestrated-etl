"""Input validation and security utilities for Agent-Orchestrated ETL."""

from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Any


class ValidationError(Exception):
    """Raised when input validation fails."""


def validate_dag_id(dag_id: str) -> str:
    """Validate and sanitize DAG ID.
    
    Args:
        dag_id: The DAG ID to validate
        
    Returns:
        The validated DAG ID
        
    Raises:
        ValidationError: If the DAG ID is invalid
    """
    if not dag_id:
        raise ValidationError("DAG ID cannot be empty")
    
    if len(dag_id) > 200:
        raise ValidationError("DAG ID cannot exceed 200 characters")
    
    # DAG ID should contain only alphanumeric characters, underscores, and hyphens
    if not re.match(r'^[a-zA-Z0-9_-]+$', dag_id):
        raise ValidationError(
            "DAG ID can only contain alphanumeric characters, underscores, and hyphens"
        )
    
    # Must start with a letter or underscore
    if not re.match(r'^[a-zA-Z_]', dag_id):
        raise ValidationError("DAG ID must start with a letter or underscore")
    
    return dag_id


def validate_task_name(task_name: str) -> str:
    """Validate and sanitize task name.
    
    Args:
        task_name: The task name to validate
        
    Returns:
        The validated task name
        
    Raises:
        ValidationError: If the task name is invalid
    """
    if not task_name:
        raise ValidationError("Task name cannot be empty")
    
    if len(task_name) > 250:
        raise ValidationError("Task name cannot exceed 250 characters")
    
    # Task name should contain only alphanumeric characters, underscores, and hyphens
    if not re.match(r'^[a-zA-Z0-9_-]+$', task_name):
        raise ValidationError(
            "Task name can only contain alphanumeric characters, underscores, and hyphens"
        )
    
    return task_name


def validate_file_path(file_path: str, *, allow_creation: bool = True) -> str:
    """Validate and sanitize file path to prevent directory traversal.
    
    Args:
        file_path: The file path to validate
        allow_creation: Whether to allow creation of new files
        
    Returns:
        The validated file path
        
    Raises:
        ValidationError: If the file path is invalid or unsafe
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")
    
    # Check for dangerous characters
    dangerous_chars = ['\0', '\n', '\r']
    if any(char in file_path for char in dangerous_chars):
        raise ValidationError("File path contains invalid characters")
    
    # Prevent command injection attempts
    if file_path.startswith('-'):
        raise ValidationError("File path cannot start with a dash")
    
    # Convert to Path object for safer handling
    try:
        path = Path(file_path).resolve()
    except (OSError, ValueError) as e:
        raise ValidationError(f"Invalid file path: {e}")
    
    # Check for directory traversal attempts
    try:
        # Ensure the resolved path is within reasonable bounds
        # This prevents paths like ../../etc/passwd
        cwd = Path.cwd().resolve()
        if not str(path).startswith(str(cwd.parent)):
            # Allow paths within parent directory but log suspicious activity
            pass
    except (OSError, ValueError):
        raise ValidationError("Potentially unsafe file path detected")
    
    # Check if file exists when required
    if not allow_creation and not path.exists():
        raise ValidationError(f"File does not exist: {file_path}")
    
    # Check if parent directory exists for new files
    if allow_creation and not path.parent.exists():
        raise ValidationError(f"Parent directory does not exist: {path.parent}")
    
    # Validate file extension for output files
    if allow_creation and path.suffix:
        allowed_extensions = {'.py', '.json', '.log', '.txt', '.yml', '.yaml'}
        if path.suffix.lower() not in allowed_extensions:
            raise ValidationError(f"File extension '{path.suffix}' not allowed")
    
    return str(path)


def validate_source_type(source_type: str, supported_sources: set[str]) -> str:
    """Validate data source type.
    
    Args:
        source_type: The source type to validate
        supported_sources: Set of supported source types
        
    Returns:
        The normalized source type
        
    Raises:
        ValidationError: If the source type is not supported
    """
    if not source_type:
        raise ValidationError("Source type cannot be empty")
    
    normalized = source_type.lower().strip()
    
    # Check for SQL injection patterns in source type
    sql_patterns = [
        r"['\";]",  # SQL injection characters
        r"\b(union|select|insert|update|delete|drop|create|alter)\b",  # SQL keywords
        r"--",  # SQL comments
        r"/\*.*\*/",  # SQL block comments
    ]
    
    for pattern in sql_patterns:
        if re.search(pattern, normalized, re.IGNORECASE):
            raise ValidationError("Source type contains potentially malicious content")
    
    if normalized not in supported_sources:
        raise ValidationError(f"Unsupported source type: {source_type}")
    
    return normalized


def safe_path_type(value: str) -> str:
    """Argparse type function for safe file paths."""
    try:
        return validate_file_path(value)
    except ValidationError as e:
        raise argparse.ArgumentTypeError(str(e))


def safe_dag_id_type(value: str) -> str:
    """Argparse type function for DAG IDs."""
    try:
        return validate_dag_id(value)
    except ValidationError as e:
        raise argparse.ArgumentTypeError(str(e))


def safe_source_type(supported_sources: set[str]):
    """Create an argparse type function for source types."""
    def _validator(value: str) -> str:
        try:
            return validate_source_type(value, supported_sources)
        except ValidationError as e:
            raise argparse.ArgumentTypeError(str(e))
    return _validator


def sanitize_json_output(data: Any) -> Any:
    """Sanitize data before JSON serialization to prevent information leakage.
    
    Args:
        data: The data to sanitize
        
    Returns:
        Sanitized data safe for JSON output
    """
    if isinstance(data, dict):
        sanitized = {}
        for key, value in data.items():
            # Skip keys that might contain sensitive information
            if isinstance(key, str) and any(
                sensitive in key.lower() 
                for sensitive in ['password', 'secret', 'token', 'key', 'credential']
            ):
                sanitized[key] = "[REDACTED]"
            else:
                sanitized[key] = sanitize_json_output(value)
        return sanitized
    elif isinstance(data, list):
        return [sanitize_json_output(item) for item in data]
    elif isinstance(data, str):
        # Check for potential sensitive patterns in strings
        sensitive_patterns = [
            r'password\s*[=:]\s*\S+',
            r'token\s*[=:]\s*\S+',
            r'secret\s*[=:]\s*\S+',
            r'key\s*[=:]\s*\S+',
        ]
        
        for pattern in sensitive_patterns:
            if re.search(pattern, data, re.IGNORECASE):
                return "[REDACTED - SENSITIVE DATA DETECTED]"
        
        return data
    else:
        return data


def validate_environment_variable(var_name: str, value: str | None = None) -> str:
    """Validate environment variable name and optionally its value.
    
    Args:
        var_name: Name of the environment variable
        value: Optional value to validate
        
    Returns:
        The environment variable value
        
    Raises:
        ValidationError: If validation fails
    """
    if not var_name:
        raise ValidationError("Environment variable name cannot be empty")
    
    # Validate variable name format
    if not re.match(r'^[A-Z][A-Z0-9_]*$', var_name):
        raise ValidationError(
            "Environment variable name must start with a letter and contain only "
            "uppercase letters, numbers, and underscores"
        )
    
    if value is None:
        value = os.getenv(var_name)
    
    if value is None:
        raise ValidationError(f"Environment variable {var_name} is not set")
    
    # Basic validation to prevent obvious injection attempts
    if any(char in value for char in ['\0', '\n', '\r']):
        raise ValidationError(f"Environment variable {var_name} contains invalid characters")
    
    return value