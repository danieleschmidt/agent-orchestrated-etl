import pytest
from agent_orchestrated_etl import data_source_analysis


def test_metadata_for_supported_sources():
    """Module returns metadata for all supported sources."""
    for source in ("s3", "postgresql", "api"):
        metadata = data_source_analysis.analyze_source(source)
        assert "tables" in metadata  # nosec B101
        assert "fields" in metadata  # nosec B101

def test_unsupported_source():
    """Raises ValueError when the source type is unsupported"""
    with pytest.raises(ValueError):
        data_source_analysis.analyze_source('mongodb')  # nosec B101


def test_supported_sources_text():
    """Utility returns newline-separated supported sources."""
    text = data_source_analysis.supported_sources_text()
    lines = text.splitlines()
    assert lines == sorted(data_source_analysis.SUPPORTED_SOURCES)  # nosec B101
