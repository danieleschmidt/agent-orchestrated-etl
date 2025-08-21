"""Database layer for Agent-Orchestrated-ETL."""

from .connection import DatabaseManager, get_database_manager
from .migrations import MigrationManager
from .repositories import (
    AgentRepository,
    BaseRepository,
    PipelineRepository,
    QualityMetricsRepository,
)


def initialize_database():
    """Initialize database connection and schema."""
    db_manager = get_database_manager()
    db_manager.initialize()
    return db_manager


def close_database():
    """Close database connections."""
    db_manager = get_database_manager()
    db_manager.close()


__all__ = [
    "DatabaseManager",
    "get_database_manager",
    "initialize_database",
    "close_database",
    "MigrationManager",
    "BaseRepository",
    "PipelineRepository",
    "AgentRepository",
    "QualityMetricsRepository",
]
