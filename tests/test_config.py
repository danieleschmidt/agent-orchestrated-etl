
import pytest
from agent_orchestrated_etl import config

def test_default_log_level():
    """Tests that the config module provides a default log level."""
    assert config.LOG_LEVEL == 'INFO'
