"""Simple data source analysis utilities."""

from __future__ import annotations

from typing import Dict, List


SUPPORTED_SOURCES = {"s3", "postgresql", "api"}


def supported_sources_text() -> str:
    """Return supported source types as newline separated text."""
    return "\n".join(sorted(SUPPORTED_SOURCES))


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
    """
    normalized = source_type.lower()
    if normalized not in SUPPORTED_SOURCES:
        raise ValueError(f"Unsupported source type: {source_type}")

    if normalized == "s3":
        # In a real implementation this would inspect the bucket to infer
        # structure. For now we simply return a single objects table.
        return {"tables": ["objects"], "fields": ["key", "size"]}

    if normalized == "api":
        # Placeholder metadata for a generic REST API source. In a real
        # implementation this would introspect available endpoints.
        return {"tables": ["records"], "fields": ["id", "data"]}

    # Simulate a database with multiple tables so the DAG generator can
    # create per-table tasks. The specific table names are not important
    # for current tests but provide a more realistic example.
    return {"tables": ["users", "orders"], "fields": ["id", "value"]}
