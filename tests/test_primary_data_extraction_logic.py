from agent_orchestrated_etl.core import primary_data_extraction


def test_primary_data_extraction():
    """Ensure the primary data extraction returns a list of integers."""
    assert primary_data_extraction() == [1, 2, 3]  # nosec B101
