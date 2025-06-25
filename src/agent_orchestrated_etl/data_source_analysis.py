"""Simple data source analysis utilities."""

from __future__ import annotations

from typing import Dict, List


SUPPORTED_SOURCES = {"s3", "postgresql"}


def analyze_source(source_type: str) -> Dict[str, List[str]]:
    """Return basic metadata for the given source type.

    Parameters
    ----------
    source_type:
        A string representing the data source type. Currently supports
        ``"s3"`` and ``"postgresql"``.

    Returns
    -------
    dict
        Dictionary with ``tables`` and ``fields`` keys.

    Raises
    ------
    ValueError
        If ``source_type`` is not supported.
    """
    normalized = source_type.lower()
    if normalized not in SUPPORTED_SOURCES:
        raise ValueError(f"Unsupported source type: {source_type}")

    if normalized == "s3":
        return {"tables": ["objects"], "fields": ["key", "size"]}
    return {"tables": ["example_table"], "fields": ["id", "value"]}
