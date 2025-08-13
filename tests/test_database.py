"""Tests for database layer functionality."""

import time
from unittest.mock import AsyncMock, Mock, patch

import pytest

from agent_orchestrated_etl.database import (
    AgentRepository,
    BaseRepository,
    DatabaseManager,
    MigrationManager,
    PipelineRepository,
    QualityMetricsRepository,
)
from agent_orchestrated_etl.exceptions import ConfigurationError, DatabaseException
from tests.utils.test_factories import (
    AgentConfigurationFactory,
    DataQualityMetricsFactory,
    PipelineConfigFactory,
)


class TestDatabaseManager:
    """Test DatabaseManager functionality."""

    def test_database_manager_initialization(self):
        """Test DatabaseManager initialization."""
        db_manager = DatabaseManager(
            database_url="postgresql://user:pass@localhost:5432/test",
            pool_size=5,
            max_overflow=10
        )

        assert db_manager.database_url == "postgresql://user:pass@localhost:5432/test"
        assert db_manager.pool_size == 5
        assert db_manager.max_overflow == 10
        assert not db_manager._is_initialized

    def test_database_manager_invalid_url(self):
        """Test DatabaseManager with invalid URL."""
        with pytest.raises(ConfigurationError):
            DatabaseManager(database_url="")

        with pytest.raises(ConfigurationError):
            DatabaseManager(database_url="invalid-url")

    @pytest.mark.asyncio
    async def test_initialize_database_manager(self):
        """Test database manager initialization."""
        db_manager = DatabaseManager(
            database_url="sqlite:///:memory:"
        )

        # Mock the setup methods to avoid actual connections
        with patch.object(db_manager, '_setup_sqlalchemy') as mock_sqlalchemy, \
             patch.object(db_manager, '_setup_asyncpg_pool') as mock_asyncpg, \
             patch.object(db_manager, '_test_connection') as mock_test:

            await db_manager.initialize()

            assert db_manager._is_initialized
            mock_sqlalchemy.assert_called_once()
            mock_asyncpg.assert_called_once()
            mock_test.assert_called_once()

    @pytest.mark.asyncio
    async def test_execute_query_mock(self):
        """Test query execution with mocked connection."""
        db_manager = DatabaseManager(
            database_url="sqlite:///:memory:"
        )

        # Initialize without actual connections
        with patch.object(db_manager, '_setup_sqlalchemy'), \
             patch.object(db_manager, '_setup_asyncpg_pool'), \
             patch.object(db_manager, '_test_connection'):
            await db_manager.initialize()

        # Test query execution with mock connection
        result = await db_manager.execute_query(
            "SELECT 1 as test",
            fetch_mode="one"
        )

        # With mock connection, result should be None
        assert result is None

    @pytest.mark.asyncio
    async def test_execute_transaction_mock(self):
        """Test transaction execution with mocked connection."""
        db_manager = DatabaseManager(
            database_url="sqlite:///:memory:"
        )

        with patch.object(db_manager, '_setup_sqlalchemy'), \
             patch.object(db_manager, '_setup_asyncpg_pool'), \
             patch.object(db_manager, '_test_connection'):
            await db_manager.initialize()

        operations = [
            {"query": "INSERT INTO test (id) VALUES (1)", "fetch_mode": "none"},
            {"query": "SELECT * FROM test", "fetch_mode": "all"}
        ]

        results = await db_manager.execute_transaction(operations)

        # With mock connection, results should be None for each operation
        assert len(results) == 2
        assert all(result is None for result in results)

    @pytest.mark.asyncio
    async def test_get_health_status(self):
        """Test health status check."""
        db_manager = DatabaseManager(
            database_url="sqlite:///:memory:"
        )

        with patch.object(db_manager, '_setup_sqlalchemy'), \
             patch.object(db_manager, '_setup_asyncpg_pool'), \
             patch.object(db_manager, '_test_connection'):
            await db_manager.initialize()

        status = await db_manager.get_health_status()

        assert "status" in status
        assert "timestamp" in status
        assert status["status"] in ["healthy", "unhealthy"]

    @pytest.mark.asyncio
    async def test_close_database_manager(self):
        """Test database manager cleanup."""
        db_manager = DatabaseManager(
            database_url="sqlite:///:memory:"
        )

        with patch.object(db_manager, '_setup_sqlalchemy'), \
             patch.object(db_manager, '_setup_asyncpg_pool'), \
             patch.object(db_manager, '_test_connection'):
            await db_manager.initialize()

        await db_manager.close()

        assert not db_manager._is_initialized
        assert db_manager._engine is None
        assert db_manager._session_factory is None


class TestBaseRepository:
    """Test BaseRepository functionality."""

    class TestRepository(BaseRepository):
        """Test repository implementation."""

        def get_table_name(self) -> str:
            return "test_table"

    @pytest.fixture
    def repository(self):
        """Create test repository."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock()
        return self.TestRepository(db_manager=mock_db)

    @pytest.mark.asyncio
    async def test_create_record(self, repository):
        """Test creating a record."""
        test_data = {"name": "test", "value": 123}

        repository.db_manager.execute_query.return_value = None

        record_id = await repository.create(test_data)

        assert record_id is not None
        repository.db_manager.execute_query.assert_called_once()

        # Check that call includes the data plus id, created_at, updated_at
        call_args = repository.db_manager.execute_query.call_args
        assert "name" in str(call_args)
        assert "value" in str(call_args)

    @pytest.mark.asyncio
    async def test_get_by_id(self, repository):
        """Test getting a record by ID."""
        test_id = "test-id-123"
        expected_result = {"id": test_id, "name": "test"}

        repository.db_manager.execute_query.return_value = expected_result

        result = await repository.get_by_id(test_id)

        assert result == expected_result
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_record(self, repository):
        """Test updating a record."""
        test_id = "test-id-123"
        update_data = {"name": "updated"}

        repository.db_manager.execute_query.return_value = None

        result = await repository.update(test_id, update_data)

        assert result is True
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_delete_record(self, repository):
        """Test deleting a record."""
        test_id = "test-id-123"

        repository.db_manager.execute_query.return_value = None

        result = await repository.delete(test_id)

        assert result is True
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_all_records(self, repository):
        """Test listing all records."""
        expected_results = [
            {"id": "1", "name": "test1"},
            {"id": "2", "name": "test2"}
        ]

        repository.db_manager.execute_query.return_value = expected_results

        results = await repository.list_all(limit=10)

        assert results == expected_results
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_count_records(self, repository):
        """Test counting records."""
        repository.db_manager.execute_query.return_value = {"count": 5}

        count = await repository.count()

        assert count == 5
        repository.db_manager.execute_query.assert_called_once()


class TestPipelineRepository:
    """Test PipelineRepository functionality."""

    @pytest.fixture
    def repository(self):
        """Create pipeline repository."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock()
        return PipelineRepository(db_manager=mock_db)

    @pytest.mark.asyncio
    async def test_create_pipeline_execution(self, repository):
        """Test creating a pipeline execution."""
        pipeline_config = PipelineConfigFactory.create()

        repository.db_manager.execute_query.return_value = None

        execution_id = await repository.create_pipeline_execution(pipeline_config)

        assert execution_id is not None
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_pipeline_execution(self, repository):
        """Test getting a pipeline execution."""
        execution_id = "exec-123"
        mock_record = {
            "id": execution_id,
            "pipeline_id": "pipeline-123",
            "status": "running",
            "started_at": time.time(),
            "current_tasks": ["task1", "task2"],
            "completed_tasks": [],
            "failed_tasks": []
        }

        repository.db_manager.execute_query.return_value = mock_record

        execution = await repository.get_pipeline_execution(execution_id)

        assert execution is not None
        assert execution.execution_id == execution_id
        assert execution.pipeline_id == "pipeline-123"
        assert execution.status.value == "running"

    @pytest.mark.asyncio
    async def test_list_pipeline_executions(self, repository):
        """Test listing pipeline executions."""
        mock_records = [
            {
                "id": "exec-1",
                "pipeline_id": "pipeline-1",
                "status": "completed"
            },
            {
                "id": "exec-2",
                "pipeline_id": "pipeline-2",
                "status": "running"
            }
        ]

        repository.db_manager.execute_query.return_value = mock_records

        executions = await repository.list_pipeline_executions()

        assert len(executions) == 2
        assert executions[0].execution_id == "exec-1"
        assert executions[1].execution_id == "exec-2"


class TestAgentRepository:
    """Test AgentRepository functionality."""

    @pytest.fixture
    def repository(self):
        """Create agent repository."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock()
        return AgentRepository(db_manager=mock_db)

    @pytest.mark.asyncio
    async def test_create_agent(self, repository):
        """Test creating an agent."""
        agent_config = AgentConfigurationFactory.create()

        repository.db_manager.execute_query.return_value = None

        agent_id = await repository.create_agent(agent_config)

        assert agent_id == agent_config.agent_id
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_agent_status(self, repository):
        """Test updating agent status."""
        agent_id = "agent-123"
        status = "active"
        metrics = {"tasks_completed": 10}

        repository.db_manager.execute_query.return_value = None

        result = await repository.update_agent_status(agent_id, status, metrics)

        assert result is True
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_list_active_agents(self, repository):
        """Test listing active agents."""
        mock_agents = [
            {
                "id": "agent-1",
                "status": "active",
                "last_heartbeat": time.time()
            },
            {
                "id": "agent-2",
                "status": "active",
                "last_heartbeat": time.time() - 100
            }
        ]

        repository.db_manager.execute_query.return_value = mock_agents

        agents = await repository.list_active_agents()

        assert len(agents) == 2
        assert agents[0]["id"] == "agent-1"
        assert agents[1]["id"] == "agent-2"


class TestQualityMetricsRepository:
    """Test QualityMetricsRepository functionality."""

    @pytest.fixture
    def repository(self):
        """Create quality metrics repository."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock()
        return QualityMetricsRepository(db_manager=mock_db)

    @pytest.mark.asyncio
    async def test_record_quality_metrics(self, repository):
        """Test recording quality metrics."""
        data_source_id = "source-123"
        metrics = DataQualityMetricsFactory.create(data_source_id=data_source_id)

        repository.db_manager.execute_query.return_value = None

        metrics_id = await repository.record_quality_metrics(data_source_id, metrics)

        assert metrics_id is not None
        repository.db_manager.execute_query.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_latest_quality_metrics(self, repository):
        """Test getting latest quality metrics."""
        data_source_id = "source-123"
        mock_record = {
            "id": "metrics-123",
            "data_source_id": data_source_id,
            "measurement_timestamp": time.time(),
            "overall_score": 0.85,
            "dimension_scores": {
                "completeness": 0.9,
                "accuracy": 0.8
            },
            "record_count": 1000,
            "field_profiles": {}
        }

        repository.db_manager.execute_query.return_value = mock_record

        metrics = await repository.get_latest_quality_metrics(data_source_id)

        assert metrics is not None
        assert metrics.data_source_id == data_source_id
        assert metrics.overall_quality_score == 0.85

    @pytest.mark.asyncio
    async def test_get_quality_trends(self, repository):
        """Test getting quality trends."""
        data_source_id = "source-123"
        mock_trends = [
            {
                "measurement_timestamp": time.time() - 86400,
                "overall_score": 0.83,
                "record_count": 950
            },
            {
                "measurement_timestamp": time.time(),
                "overall_score": 0.85,
                "record_count": 1000
            }
        ]

        repository.db_manager.execute_query.return_value = mock_trends

        trends = await repository.get_quality_trends(data_source_id, days=7)

        assert len(trends) == 2
        assert trends[0]["overall_score"] == 0.83
        assert trends[1]["overall_score"] == 0.85


class TestMigrationManager:
    """Test MigrationManager functionality."""

    @pytest.fixture
    def migration_manager(self, tmp_path):
        """Create migration manager."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock()

        return MigrationManager(
            db_manager=mock_db,
            migrations_dir=str(tmp_path)
        )

    @pytest.mark.asyncio
    async def test_initialize_migration_manager(self, migration_manager):
        """Test migration manager initialization."""
        await migration_manager.initialize()

        assert migration_manager._loaded
        assert len(migration_manager._migrations) > 0  # Should have built-in migrations

    @pytest.mark.asyncio
    async def test_get_pending_migrations(self, migration_manager):
        """Test getting pending migrations."""
        # Mock applied migrations (empty)
        migration_manager.db_manager.execute_query.return_value = []

        await migration_manager.initialize()
        pending = await migration_manager.get_pending_migrations()

        # Should have built-in migrations as pending
        assert len(pending) > 0

    @pytest.mark.asyncio
    async def test_apply_migration(self, migration_manager):
        """Test applying a migration."""
        from agent_orchestrated_etl.database.migrations import Migration

        migration = Migration(
            version="test_001",
            name="test_migration",
            up_sql="CREATE TABLE test (id INTEGER);",
            description="Test migration"
        )

        result = await migration_manager.apply_migration(migration)

        assert result is True
        # Check that both migration SQL and recording were called
        assert migration_manager.db_manager.execute_query.call_count == 2

    @pytest.mark.asyncio
    async def test_get_migration_status(self, migration_manager):
        """Test getting migration status."""
        migration_manager.db_manager.execute_query.return_value = []

        await migration_manager.initialize()
        status = await migration_manager.get_migration_status()

        assert "total_migrations" in status
        assert "applied_count" in status
        assert "pending_count" in status
        assert "status" in status

    def test_create_migration_file(self, migration_manager):
        """Test creating a migration file."""
        name = "test_migration"
        up_sql = "CREATE TABLE test (id INTEGER);"
        down_sql = "DROP TABLE test;"

        filepath = migration_manager.create_migration_file(name, up_sql, down_sql)

        assert filepath.exists()
        content = filepath.read_text()
        assert up_sql in content
        assert down_sql in content
        assert "-- DOWN" in content


class TestDatabaseExceptions:
    """Test database exception handling."""

    @pytest.mark.asyncio
    async def test_database_exception_on_connection_failure(self):
        """Test DatabaseException on connection failure."""
        db_manager = DatabaseManager(
            database_url="postgresql://invalid:invalid@invalid:5432/invalid"
        )

        # Test connection should fail
        with pytest.raises(DatabaseException):
            await db_manager._test_connection()

    @pytest.mark.asyncio
    async def test_repository_exception_on_query_failure(self):
        """Test exception handling in repository operations."""
        mock_db = Mock()
        mock_db.execute_query = AsyncMock(side_effect=Exception("Query failed"))

        repo = PipelineRepository(db_manager=mock_db)

        with pytest.raises(DatabaseException):
            await repo.get_by_id("test-id")


class TestDatabaseWithoutDependencies:
    """Test database functionality without external dependencies."""

    def test_database_manager_without_sqlalchemy(self):
        """Test DatabaseManager when SQLAlchemy is not available."""
        # This would test the fallback behavior when SQLAlchemy is not installed
        # For now, we assume SQLAlchemy is available in the test environment
        pass

    @pytest.mark.asyncio
    async def test_mock_session_operations(self):
        """Test mock session operations."""
        from agent_orchestrated_etl.database.connection import MockSession

        session = MockSession()

        # These should not raise exceptions
        await session.commit()
        await session.rollback()
        await session.close()
        session.add("test_object")
        await session.flush()
        await session.refresh("test_object")

    @pytest.mark.asyncio
    async def test_mock_connection_operations(self):
        """Test mock connection operations."""
        from agent_orchestrated_etl.database.connection import MockConnection

        conn = MockConnection()

        # These should not raise exceptions
        rows = await conn.fetch("SELECT 1")
        assert rows == []

        row = await conn.fetchrow("SELECT 1")
        assert row is None

        await conn.execute("INSERT INTO test VALUES (1)")

        async with conn.transaction():
            pass
