
import importlib
from agent_orchestrated_etl import config

def test_default_log_level():
    """Tests that the config module provides a default log level."""
    assert config.LOG_LEVEL == 'INFO'  # nosec B101


def test_env_override(monkeypatch):
    """Environment variables should override defaults when reloaded."""
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    importlib.reload(config)
    assert config.LOG_LEVEL == "DEBUG"
