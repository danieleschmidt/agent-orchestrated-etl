
"""Application configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(key: str, default: str | None = None) -> str:
    """Return the value of ``key`` from the environment or ``default``."""
    return os.environ.get(key, default) or ""


@dataclass
class Settings:
    """Container for environment driven settings."""

    LOG_LEVEL: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        """Create a :class:`Settings` instance from environment variables."""
        return cls(LOG_LEVEL=_env("LOG_LEVEL", "INFO"))


SETTINGS = Settings.from_env()
LOG_LEVEL: str = SETTINGS.LOG_LEVEL
