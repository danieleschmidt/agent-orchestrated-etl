import pytest
from agent_orchestrated_etl import data_source_analysis


def test_metadata_for_s3_and_postgresql():
    """Module returns metadata about tables and fields for S3 and PostgreSQL sources"""
    assert "tables" in data_source_analysis.analyze_source('s3')  # nosec B101
    assert "fields" in data_source_analysis.analyze_source('s3')  # nosec B101
    assert "tables" in data_source_analysis.analyze_source('postgresql')  # nosec B101
    assert "fields" in data_source_analysis.analyze_source('postgresql')  # nosec B101

def test_unsupported_source():
    """Raises ValueError when the source type is unsupported"""
    with pytest.raises(ValueError):
        data_source_analysis.analyze_source('mongodb')  # nosec B101
