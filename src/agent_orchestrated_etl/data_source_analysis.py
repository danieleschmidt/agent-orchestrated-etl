"""Simple data source analysis utilities."""

from __future__ import annotations

import re
from typing import Dict, List

from .validation import ValidationError


SUPPORTED_SOURCES = {"s3", "postgresql", "api", "test_db", "test_database", "integration_test_db"}


def supported_sources_text() -> str:
    """Return supported source types as newline separated text."""
    return "\n".join(sorted(SUPPORTED_SOURCES))


def _validate_metadata_security(metadata: Dict[str, List[str]]) -> None:
    """Validate metadata for potential security issues.
    
    Args:
        metadata: The metadata dictionary to validate
        
    Raises:
        ValidationError: If security issues are detected
    """
    # Check for SQL injection patterns in table names
    sql_injection_patterns = [
        r"['\";]",  # SQL injection characters
        r"\b(union|select|insert|update|delete|drop|create|alter|truncate)\b",  # SQL keywords
        r"--",  # SQL line comments
        r"/\*.*\*/",  # SQL block comments
        r"<script.*>",  # XSS attempt
    ]
    
    for table in metadata.get("tables", []):
        for pattern in sql_injection_patterns:
            if re.search(pattern, str(table), re.IGNORECASE):
                raise ValidationError(f"Table name '{table}' contains potentially malicious content")
    
    for field in metadata.get("fields", []):
        for pattern in sql_injection_patterns:
            if re.search(pattern, str(field), re.IGNORECASE):
                raise ValidationError(f"Field name '{field}' contains potentially malicious content")


def analyze_source(source_type: str) -> Dict[str, List[str]]:
    """Return basic metadata for the given source type.

    Parameters
    ----------
    source_type:
        A string representing the data source type. Currently supports
        ``"s3"``, ``"postgresql"``, and ``"api"``.

    Returns
    -------
    dict
        Dictionary with ``tables`` and ``fields`` keys. Database sources may
        include multiple tables to allow per-table DAG generation.

    Raises
    ------
    ValueError
        If ``source_type`` is not supported.
    ValidationError
        If the source type or resulting metadata contains malicious content.
    """
    normalized = source_type.lower().strip()
    
    # Additional security validation for source type
    if not re.match(r'^[a-z0-9_]+$', normalized):
        raise ValidationError("Source type contains invalid characters")
    
    if normalized not in SUPPORTED_SOURCES:
        raise ValueError(f"Unsupported source type: {source_type}")

    if normalized == "s3":
        # In a real implementation this would inspect the bucket to infer
        # structure. For now we simply return a single objects table.
        metadata = {"tables": ["objects"], "fields": ["key", "size"]}
        
    elif normalized == "api":
        # Placeholder metadata for a generic REST API source. In a real
        # implementation this would introspect available endpoints.
        metadata = {"tables": ["records"], "fields": ["id", "data"]}
        
    elif normalized == "postgresql":
        # Simulate a database with multiple tables so the DAG generator can
        # create per-table tasks. The specific table names are not important
        # for current tests but provide a more realistic example.
        metadata = {"tables": ["users", "orders"], "fields": ["id", "value"]}
        
    elif normalized in ["test_db", "test_database", "integration_test_db"]:
        # Test database sources for unit/integration testing
        # Provide predictable metadata that tests can rely on
        metadata = {"tables": ["test_table"], "fields": ["test_id", "test_data", "test_timestamp"]}
    
    else:
        # This should not happen since we validate supported sources earlier,
        # but add explicit handling for completeness
        raise ValueError(f"Unsupported source type: {source_type}")
    
    # Validate the metadata for security issues
    _validate_metadata_security(metadata)
    
    return metadata
