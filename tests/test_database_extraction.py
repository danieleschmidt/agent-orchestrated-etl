"""Tests for database extraction functionality."""
import pytest
import sqlite3
import tempfile
import os

from agent_orchestrated_etl.agents.etl_agent import ETLAgent
from agent_orchestrated_etl.agents.base_agent import AgentConfig


class TestDatabaseExtraction:
    """Test database extraction methods."""

    @pytest.fixture
    def etl_agent(self):
        """Create ETL agent for testing."""
        config = AgentConfig()
        return ETLAgent(config=config)

    @pytest.fixture
    def sqlite_db(self):
        """Create a temporary SQLite database for testing."""
        db_fd, db_path = tempfile.mkstemp(suffix='.db')
        os.close(db_fd)
        
        # Create test data
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE test_table (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                value REAL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.executemany(
            'INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)',
            [(1, 'test1', 10.5), (2, 'test2', 20.3), (3, 'test3', 30.7)]
        )
        conn.commit()
        conn.close()
        
        yield db_path
        os.unlink(db_path)

    @pytest.mark.asyncio
    async def test_extract_from_database_sqlite_basic(self, etl_agent, sqlite_db):
        """Test basic SQLite database extraction."""
        source_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT * FROM test_table',
            'database_type': 'sqlite'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        # Should return actual data, not placeholder
        assert result['extraction_method'] == 'database'
        assert result['status'] == 'completed'
        assert 'data' in result
        assert len(result['data']) == 3
        assert result['record_count'] == 3
        
        # Verify data structure
        first_row = result['data'][0]
        assert 'id' in first_row
        assert 'name' in first_row
        assert 'value' in first_row

    @pytest.mark.asyncio
    async def test_extract_from_database_with_parameters(self, etl_agent, sqlite_db):
        """Test parameterized query execution."""
        source_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT * FROM test_table WHERE id > :min_id',
            'parameters': {'min_id': 1},
            'database_type': 'sqlite'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'completed'
        assert len(result['data']) == 2  # Only id 2 and 3
        assert result['record_count'] == 2

    @pytest.mark.asyncio
    async def test_extract_from_database_batch_processing(self, etl_agent, sqlite_db):
        """Test batch processing for large datasets."""
        source_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT * FROM test_table',
            'batch_size': 2,
            'database_type': 'sqlite'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'completed'
        assert result['record_count'] == 3
        assert 'batch_info' in result
        assert result['batch_info']['total_batches'] >= 1

    @pytest.mark.asyncio
    async def test_extract_from_database_connection_error(self, etl_agent):
        """Test handling of connection errors."""
        source_config = {
            'connection_string': 'postgresql://baduser:badpass@nonexistent:5432/baddb',
            'query': 'SELECT * FROM test_table',
            'database_type': 'postgresql'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'error'
        assert 'error_message' in result

    @pytest.mark.asyncio 
    async def test_extract_from_database_sql_injection_prevention(self, etl_agent, sqlite_db):
        """Test SQL injection prevention with parameterized queries."""
        # This should be safe because we use parameterized queries
        malicious_input = "'; DROP TABLE test_table; --"
        
        source_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT * FROM test_table WHERE name = :name',
            'parameters': {'name': malicious_input},
            'database_type': 'sqlite'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        # Should execute safely without dropping table
        assert result['status'] == 'completed'
        assert result['record_count'] == 0  # No matching records
        
        # Verify table still exists by running another query
        verify_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT COUNT(*) as count FROM test_table',
            'database_type': 'sqlite'
        }
        verify_result = await etl_agent._extract_from_database(verify_config)
        assert verify_result['status'] == 'completed'
        assert verify_result['data'][0]['count'] == 3  # Table still intact

    @pytest.mark.asyncio
    async def test_extract_from_database_timeout_handling(self, etl_agent):
        """Test query timeout handling."""
        source_config = {
            'connection_string': 'sqlite:///:memory:',
            'query': 'SELECT * FROM sqlite_master',  # Simple query
            'timeout': 0.001,  # Very short timeout
            'database_type': 'sqlite'
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        # Should handle timeout gracefully
        assert result['status'] in ['completed', 'timeout', 'error']
        if result['status'] == 'timeout':
            assert 'timeout' in result['error_message'].lower()

    @pytest.mark.asyncio
    async def test_extract_from_database_postgresql_config(self, etl_agent):
        """Test PostgreSQL connection configuration."""
        source_config = {
            'connection_string': 'postgresql://user:pass@localhost:5432/test_db',
            'query': 'SELECT version()',
            'database_type': 'postgresql'
        }
        
        # Should fail gracefully when PostgreSQL is not available
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'error'
        assert 'error_message' in result

    @pytest.mark.asyncio
    async def test_extract_from_database_mysql_config(self, etl_agent):
        """Test MySQL connection configuration."""
        source_config = {
            'connection_string': 'mysql://user:pass@localhost:3306/test_db',
            'query': 'SELECT version()',
            'database_type': 'mysql'
        }
        
        # Should fail gracefully when MySQL is not available
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'error'
        assert 'error_message' in result

    @pytest.mark.asyncio
    async def test_extract_from_database_connection_pooling(self, etl_agent, sqlite_db):
        """Test connection pooling configuration."""
        source_config = {
            'connection_string': f'sqlite:///{sqlite_db}',
            'query': 'SELECT * FROM test_table',
            'database_type': 'sqlite',
            'pool_size': 5,
            'max_overflow': 10
        }
        
        result = await etl_agent._extract_from_database(source_config)
        
        assert result['status'] == 'completed'
        assert 'connection_info' in result
        assert 'pool_configured' in result['connection_info']