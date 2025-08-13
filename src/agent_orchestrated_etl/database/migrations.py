"""Database migration management for Agent-Orchestrated-ETL."""

from __future__ import annotations

import hashlib
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..exceptions import DatabaseException
from ..logging_config import get_logger
from .connection import DatabaseManager, get_database_manager


class Migration:
    """Represents a database migration."""

    def __init__(
        self,
        version: str,
        name: str,
        up_sql: str,
        down_sql: Optional[str] = None,
        description: Optional[str] = None,
    ):
        """Initialize migration.
        
        Args:
            version: Migration version (e.g., "001", "002_001")
            name: Migration name
            up_sql: SQL to apply the migration
            down_sql: SQL to rollback the migration
            description: Migration description
        """
        self.version = version
        self.name = name
        self.up_sql = up_sql
        self.down_sql = down_sql
        self.description = description or f"Migration {version}: {name}"

        # Generate checksum for integrity verification
        self.checksum = self._calculate_checksum()

    def _calculate_checksum(self) -> str:
        """Calculate migration checksum."""
        content = f"{self.version}:{self.name}:{self.up_sql}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    def __repr__(self) -> str:
        return f"Migration({self.version}, {self.name})"


class MigrationManager:
    """Manages database migrations."""

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        migrations_dir: Optional[str] = None,
    ):
        """Initialize migration manager.
        
        Args:
            db_manager: Database manager instance
            migrations_dir: Directory containing migration files
        """
        self.db_manager = db_manager or get_database_manager()
        self.logger = get_logger("agent_etl.migrations")

        # Set migrations directory
        if migrations_dir:
            self.migrations_dir = Path(migrations_dir)
        else:
            # Default to migrations directory relative to this file
            current_dir = Path(__file__).parent
            self.migrations_dir = current_dir / "migrations"

        self.migrations_dir.mkdir(exist_ok=True)

        # Migration tracking table
        self.migrations_table = "schema_migrations"

        self._migrations: List[Migration] = []
        self._loaded = False

    async def initialize(self) -> None:
        """Initialize migration tracking."""
        await self._ensure_migrations_table()
        await self._load_migrations()
        self._loaded = True

    async def _ensure_migrations_table(self) -> None:
        """Ensure the migrations tracking table exists."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.migrations_table} (
            version VARCHAR(50) PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            checksum VARCHAR(32) NOT NULL,
            applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            execution_time_ms INTEGER,
            description TEXT
        );
        
        CREATE INDEX IF NOT EXISTS idx_migrations_applied_at 
        ON {self.migrations_table}(applied_at);
        """

        try:
            await self.db_manager.execute_query(
                query=create_table_sql,
                fetch_mode="none"
            )
            self.logger.debug("Migrations table ensured")

        except Exception as e:
            self.logger.error(f"Failed to create migrations table: {e}")
            raise DatabaseException(f"Migration table creation failed: {e}") from e

    async def _load_migrations(self) -> None:
        """Load migrations from files and code."""
        self._migrations.clear()

        # Load built-in migrations
        self._add_builtin_migrations()

        # Load migrations from files
        await self._load_migration_files()

        # Sort by version
        self._migrations.sort(key=lambda m: m.version)

        self.logger.info(f"Loaded {len(self._migrations)} migrations")

    def _add_builtin_migrations(self) -> None:
        """Add built-in migrations."""

        # Migration 001: Initial schema
        initial_schema = Migration(
            version="001",
            name="initial_schema",
            description="Create initial database schema",
            up_sql="""
            -- Create schemas
            CREATE SCHEMA IF NOT EXISTS pipeline_metadata;
            CREATE SCHEMA IF NOT EXISTS agent_state;
            CREATE SCHEMA IF NOT EXISTS data_quality;
            
            -- Pipeline execution tracking
            CREATE TABLE IF NOT EXISTS pipeline_metadata.pipeline_executions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                pipeline_id VARCHAR(255) NOT NULL,
                status VARCHAR(50) NOT NULL,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                duration_seconds NUMERIC,
                configuration JSONB,
                results JSONB,
                error_summary JSONB,
                orchestrated_by VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Task execution tracking
            CREATE TABLE IF NOT EXISTS pipeline_metadata.task_executions (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                task_id VARCHAR(255) NOT NULL,
                pipeline_execution_id UUID REFERENCES pipeline_metadata.pipeline_executions(id),
                status VARCHAR(50) NOT NULL,
                attempt_number INTEGER DEFAULT 1,
                started_at TIMESTAMP WITH TIME ZONE,
                completed_at TIMESTAMP WITH TIME ZONE,
                duration_seconds NUMERIC,
                result JSONB,
                error_message TEXT,
                error_type VARCHAR(100),
                executed_by_agent VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Agent state tracking
            CREATE TABLE IF NOT EXISTS agent_state.agent_status (
                id VARCHAR(255) PRIMARY KEY,
                status VARCHAR(50) NOT NULL,
                last_heartbeat TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                configuration JSONB,
                metrics JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Data quality metrics
            CREATE TABLE IF NOT EXISTS data_quality.quality_metrics (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                data_source_id VARCHAR(255) NOT NULL,
                measurement_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                overall_score NUMERIC(3,2),
                dimension_scores JSONB,
                detailed_metrics JSONB,
                issues JSONB,
                record_count INTEGER,
                field_profiles JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            -- Create indexes
            CREATE INDEX IF NOT EXISTS idx_pipeline_executions_pipeline_id 
                ON pipeline_metadata.pipeline_executions(pipeline_id);
            CREATE INDEX IF NOT EXISTS idx_pipeline_executions_status 
                ON pipeline_metadata.pipeline_executions(status);
            CREATE INDEX IF NOT EXISTS idx_pipeline_executions_created 
                ON pipeline_metadata.pipeline_executions(created_at);
                
            CREATE INDEX IF NOT EXISTS idx_task_executions_pipeline_id 
                ON pipeline_metadata.task_executions(pipeline_execution_id);
            CREATE INDEX IF NOT EXISTS idx_task_executions_task_id 
                ON pipeline_metadata.task_executions(task_id);
            CREATE INDEX IF NOT EXISTS idx_task_executions_status 
                ON pipeline_metadata.task_executions(status);
                
            CREATE INDEX IF NOT EXISTS idx_agent_status_updated 
                ON agent_state.agent_status(updated_at);
            CREATE INDEX IF NOT EXISTS idx_agent_status_heartbeat 
                ON agent_state.agent_status(last_heartbeat);
                
            CREATE INDEX IF NOT EXISTS idx_quality_metrics_source 
                ON data_quality.quality_metrics(data_source_id);
            CREATE INDEX IF NOT EXISTS idx_quality_metrics_timestamp 
                ON data_quality.quality_metrics(measurement_timestamp);
            """,
            down_sql="""
            DROP SCHEMA IF EXISTS pipeline_metadata CASCADE;
            DROP SCHEMA IF EXISTS agent_state CASCADE;
            DROP SCHEMA IF EXISTS data_quality CASCADE;
            """
        )
        self._migrations.append(initial_schema)

        # Migration 002: Updated triggers
        triggers_migration = Migration(
            version="002",
            name="updated_at_triggers",
            description="Add updated_at triggers",
            up_sql="""
            -- Create updated_at trigger function
            CREATE OR REPLACE FUNCTION update_updated_at_column()
            RETURNS TRIGGER AS $$
            BEGIN
                NEW.updated_at = NOW();
                RETURN NEW;
            END;
            $$ language 'plpgsql';
            
            -- Apply triggers
            DROP TRIGGER IF EXISTS update_pipeline_executions_updated_at 
                ON pipeline_metadata.pipeline_executions;
            CREATE TRIGGER update_pipeline_executions_updated_at 
                BEFORE UPDATE ON pipeline_metadata.pipeline_executions 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
                
            DROP TRIGGER IF EXISTS update_agent_status_updated_at 
                ON agent_state.agent_status;
            CREATE TRIGGER update_agent_status_updated_at 
                BEFORE UPDATE ON agent_state.agent_status 
                FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
            """,
            down_sql="""
            DROP TRIGGER IF EXISTS update_pipeline_executions_updated_at 
                ON pipeline_metadata.pipeline_executions;
            DROP TRIGGER IF EXISTS update_agent_status_updated_at 
                ON agent_state.agent_status;
            DROP FUNCTION IF EXISTS update_updated_at_column();
            """
        )
        self._migrations.append(triggers_migration)

        # Migration 003: Agent metrics table
        agent_metrics_migration = Migration(
            version="003",
            name="agent_metrics_table",
            description="Add dedicated agent metrics table",
            up_sql="""
            CREATE TABLE IF NOT EXISTS agent_state.agent_metrics (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                agent_id VARCHAR(255) NOT NULL,
                measurement_period_start TIMESTAMP WITH TIME ZONE NOT NULL,
                measurement_period_end TIMESTAMP WITH TIME ZONE NOT NULL,
                tasks_started INTEGER DEFAULT 0,
                tasks_completed INTEGER DEFAULT 0,
                tasks_failed INTEGER DEFAULT 0,
                average_task_duration NUMERIC,
                success_rate NUMERIC(3,2),
                quality_score NUMERIC(3,2),
                resource_utilization JSONB,
                error_types JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            
            CREATE INDEX IF NOT EXISTS idx_agent_metrics_agent_id 
                ON agent_state.agent_metrics(agent_id);
            CREATE INDEX IF NOT EXISTS idx_agent_metrics_period_end 
                ON agent_state.agent_metrics(measurement_period_end);
            """,
            down_sql="""
            DROP TABLE IF EXISTS agent_state.agent_metrics;
            """
        )
        self._migrations.append(agent_metrics_migration)

    async def _load_migration_files(self) -> None:
        """Load migrations from .sql files."""
        if not self.migrations_dir.exists():
            return

        sql_files = sorted(self.migrations_dir.glob("*.sql"))

        for sql_file in sql_files:
            try:
                # Parse filename for version and name
                filename = sql_file.stem
                if "_" in filename:
                    version, name = filename.split("_", 1)
                else:
                    version = filename
                    name = filename

                # Read SQL content
                content = sql_file.read_text()

                # Look for -- DOWN comment to separate up and down SQL
                if "-- DOWN" in content:
                    up_sql, down_sql = content.split("-- DOWN", 1)
                    up_sql = up_sql.strip()
                    down_sql = down_sql.strip()
                else:
                    up_sql = content.strip()
                    down_sql = None

                migration = Migration(
                    version=version,
                    name=name,
                    up_sql=up_sql,
                    down_sql=down_sql,
                    description=f"File migration: {filename}"
                )

                self._migrations.append(migration)
                self.logger.debug(f"Loaded migration from file: {sql_file}")

            except Exception as e:
                self.logger.warning(f"Failed to load migration file {sql_file}: {e}")

    async def get_applied_migrations(self) -> List[Dict[str, Any]]:
        """Get list of applied migrations.
        
        Returns:
            List of applied migration records
        """
        query = f"""
        SELECT version, name, checksum, applied_at, execution_time_ms, description
        FROM {self.migrations_table}
        ORDER BY applied_at ASC
        """

        try:
            results = await self.db_manager.execute_query(
                query=query,
                fetch_mode="all"
            )
            return results or []

        except Exception as e:
            self.logger.error(f"Failed to get applied migrations: {e}")
            return []

    async def get_pending_migrations(self) -> List[Migration]:
        """Get list of pending migrations.
        
        Returns:
            List of migrations that haven't been applied
        """
        if not self._loaded:
            await self.initialize()

        applied_migrations = await self.get_applied_migrations()
        applied_versions = {m['version'] for m in applied_migrations}

        pending = [m for m in self._migrations if m.version not in applied_versions]

        self.logger.info(f"Found {len(pending)} pending migrations")
        return pending

    async def apply_migration(self, migration: Migration) -> bool:
        """Apply a single migration.
        
        Args:
            migration: Migration to apply
            
        Returns:
            True if migration was applied successfully
        """
        self.logger.info(f"Applying migration {migration.version}: {migration.name}")

        start_time = time.time()

        try:
            # Execute migration SQL
            await self.db_manager.execute_query(
                query=migration.up_sql,
                fetch_mode="none"
            )

            execution_time_ms = int((time.time() - start_time) * 1000)

            # Record migration as applied
            record_sql = f"""
            INSERT INTO {self.migrations_table} 
            (version, name, checksum, applied_at, execution_time_ms, description)
            VALUES ($1, $2, $3, $4, $5, $6)
            """

            await self.db_manager.execute_query(
                query=record_sql,
                parameters={
                    'version': migration.version,
                    'name': migration.name,
                    'checksum': migration.checksum,
                    'applied_at': time.time(),
                    'execution_time_ms': execution_time_ms,
                    'description': migration.description,
                },
                fetch_mode="none"
            )

            self.logger.info(
                f"Migration {migration.version} applied successfully "
                f"({execution_time_ms}ms)"
            )
            return True

        except Exception as e:
            self.logger.error(f"Failed to apply migration {migration.version}: {e}")
            raise DatabaseException(f"Migration {migration.version} failed: {e}") from e

    async def migrate(self) -> int:
        """Apply all pending migrations.
        
        Returns:
            Number of migrations applied
        """
        if not self._loaded:
            await self.initialize()

        pending_migrations = await self.get_pending_migrations()

        if not pending_migrations:
            self.logger.info("No pending migrations")
            return 0

        self.logger.info(f"Applying {len(pending_migrations)} pending migrations")

        applied_count = 0

        for migration in pending_migrations:
            try:
                await self.apply_migration(migration)
                applied_count += 1

            except Exception as e:
                self.logger.error(f"Migration failed, stopping at {migration.version}: {e}")
                break

        self.logger.info(f"Applied {applied_count} migrations successfully")
        return applied_count

    async def rollback_migration(self, migration: Migration) -> bool:
        """Rollback a single migration.
        
        Args:
            migration: Migration to rollback
            
        Returns:
            True if migration was rolled back successfully
        """
        if not migration.down_sql:
            raise DatabaseException(f"Migration {migration.version} has no rollback SQL")

        self.logger.info(f"Rolling back migration {migration.version}: {migration.name}")

        try:
            # Execute rollback SQL
            await self.db_manager.execute_query(
                query=migration.down_sql,
                fetch_mode="none"
            )

            # Remove migration record
            delete_sql = f"DELETE FROM {self.migrations_table} WHERE version = $1"
            await self.db_manager.execute_query(
                query=delete_sql,
                parameters={'version': migration.version},
                fetch_mode="none"
            )

            self.logger.info(f"Migration {migration.version} rolled back successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to rollback migration {migration.version}: {e}")
            raise DatabaseException(f"Migration rollback {migration.version} failed: {e}") from e

    async def get_migration_status(self) -> Dict[str, Any]:
        """Get overall migration status.
        
        Returns:
            Migration status information
        """
        if not self._loaded:
            await self.initialize()

        applied_migrations = await self.get_applied_migrations()
        pending_migrations = await self.get_pending_migrations()

        return {
            "total_migrations": len(self._migrations),
            "applied_count": len(applied_migrations),
            "pending_count": len(pending_migrations),
            "latest_applied": applied_migrations[-1] if applied_migrations else None,
            "next_pending": pending_migrations[0] if pending_migrations else None,
            "status": "up_to_date" if not pending_migrations else "pending_migrations",
        }

    def create_migration_file(
        self,
        name: str,
        up_sql: str,
        down_sql: Optional[str] = None
    ) -> Path:
        """Create a new migration file.
        
        Args:
            name: Migration name
            up_sql: SQL to apply the migration
            down_sql: SQL to rollback the migration
            
        Returns:
            Path to the created migration file
        """
        # Generate version based on timestamp
        version = str(int(time.time()))
        filename = f"{version}_{name}.sql"
        filepath = self.migrations_dir / filename

        # Create migration content
        content = f"-- Migration: {name}\n-- Created: {time.ctime()}\n\n{up_sql}"

        if down_sql:
            content += f"\n\n-- DOWN\n{down_sql}"

        # Write file
        filepath.write_text(content)

        self.logger.info(f"Created migration file: {filepath}")
        return filepath
