"""Tests for SQL injection security fixes."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from sqlalchemy import create_engine, text

from agent_orchestrated_etl.orchestrator import DataLoader


class TestSQLInjectionSecurity:
    """Test SQL injection vulnerability fixes."""

    def setup_method(self):
        """Set up test database."""
        # Create in-memory SQLite database for testing
        self.db_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.db_file.close()
        self.connection_string = f"sqlite:///{self.db_file.name}"

        # Create test table
        engine = create_engine(self.connection_string)
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE test_table (
                    id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT
                )
            """))
            # Insert test data
            conn.execute(text("""
                INSERT INTO test_table (id, name, email) VALUES 
                (1, 'John Doe', 'john@example.com'),
                (2, 'Jane Smith', 'jane@example.com')
            """))

    def teardown_method(self):
        """Clean up test database."""
        try:
            os.unlink(self.db_file.name)
        except FileNotFoundError:
            pass

    def test_malicious_table_name_injection_attempt(self):
        """Test that malicious table names are properly handled."""
        loader = DataLoader()

        # Attempt SQL injection through table name
        malicious_table_name = "test_table; DROP TABLE test_table; --"

        data = [{"id": 999, "name": "Evil User", "email": "evil@example.com"}]
        config = {
            "connection_string": self.connection_string,
            "table_name": malicious_table_name,
            "operation": "upsert",
            "primary_key": ["id"]
        }

        # This should fail safely without executing the injection
        result = loader.load(data, config)

        # Verify the operation failed safely
        assert result["status"] == "error"

        # Verify original table still exists by querying it
        engine = create_engine(self.connection_string)
        with engine.begin() as conn:
            result_check = conn.execute(text("SELECT COUNT(*) FROM test_table")).fetchone()
            assert result_check[0] == 2  # Original data should still exist

    def test_malicious_column_name_injection_attempt(self):
        """Test that malicious column names are properly handled."""
        loader = DataLoader()

        # Attempt SQL injection through column names
        data = [{
            "id": 999,
            "name; DROP TABLE test_table; --": "Evil User",
            "email": "evil@example.com"
        }]
        config = {
            "connection_string": self.connection_string,
            "table_name": "test_table",
            "operation": "upsert",
            "primary_key": ["id"]
        }

        # This should fail safely without executing the injection
        result = loader.load(data, config)

        # Should either succeed with escaped column name or fail safely
        # In either case, table should not be dropped
        engine = create_engine(self.connection_string)
        with engine.begin() as conn:
            result_check = conn.execute(text("SELECT COUNT(*) FROM test_table")).fetchone()
            assert result_check[0] == 2  # Original data should still exist

    def test_input_validation_prevents_injection(self):
        """Test that input validation prevents SQL injection attempts."""
        loader = DataLoader()

        # Test with invalid table name characters
        invalid_table_names = [
            "table'; DROP TABLE users; --",
            "table UNION SELECT * FROM secrets",
            "table; INSERT INTO logs VALUES ('hacked')",
            "../../../etc/passwd",
            "table\x00DROP",
        ]

        for malicious_name in invalid_table_names:
            config = {
                "connection_string": self.connection_string,
                "table_name": malicious_name,
                "operation": "insert"
            }

            result = loader.load([{"id": 1, "name": "test"}], config)

            # Should fail with validation error, not execute malicious SQL
            assert result["status"] == "error"
            assert "validation" in result.get("error_message", "").lower() or \
                   "invalid" in result.get("error_message", "").lower()

    def test_safe_upsert_with_valid_data(self):
        """Test that legitimate upsert operations work correctly."""
        loader = DataLoader()

        # Test legitimate upsert
        data = [
            {"id": 1, "name": "John Updated", "email": "john.updated@example.com"},
            {"id": 3, "name": "New User", "email": "new@example.com"}
        ]
        config = {
            "connection_string": self.connection_string,
            "table_name": "test_table",
            "operation": "upsert",
            "primary_key": ["id"]
        }

        result = loader.load(data, config)

        # Should succeed
        assert result["status"] == "completed"
        assert result["records_loaded"] == 2

        # Verify data was properly upserted
        engine = create_engine(self.connection_string)
        with engine.begin() as conn:
            # Check updated record
            updated = conn.execute(text(
                "SELECT name FROM test_table WHERE id = 1"
            )).fetchone()
            assert updated[0] == "John Updated"

            # Check new record
            new_record = conn.execute(text(
                "SELECT name FROM test_table WHERE id = 3"
            )).fetchone()
            assert new_record[0] == "New User"

            # Check total count
            total = conn.execute(text("SELECT COUNT(*) FROM test_table")).fetchone()
            assert total[0] == 3

    def test_column_name_validation(self):
        """Test that column names are properly validated."""
        loader = DataLoader()

        # Test with suspicious column names that should be rejected
        suspicious_columns = [
            "name'; DROP TABLE test_table; --",
            "UNION SELECT password FROM users",
            "name, (SELECT secret FROM admin_table)",
        ]

        for suspicious_col in suspicious_columns:
            data = [{suspicious_col: "some_value", "id": 999}]
            config = {
                "connection_string": self.connection_string,
                "table_name": "test_table",
                "operation": "insert"
            }

            result = loader.load(data, config)

            # Should handle gracefully - either escape properly or reject
            # Main requirement: should not execute injection
            if result["status"] == "error":
                # If rejected, that's fine for security
                continue
            elif result["status"] == "completed":
                # If accepted, verify table integrity wasn't compromised
                engine = create_engine(self.connection_string)
                with engine.begin() as conn:
                    count = conn.execute(text("SELECT COUNT(*) FROM test_table")).fetchone()
                    # Table should still exist and have reasonable record count
                    assert count[0] >= 2

    def test_parametrized_queries_used(self):
        """Test that parametrized queries are actually being used."""
        loader = DataLoader()

        with patch('sqlalchemy.engine.base.Connection.execute') as mock_execute:
            mock_execute.return_value = MagicMock()

            data = [{"id": 1, "name": "Test User", "email": "test@example.com"}]
            config = {
                "connection_string": self.connection_string,
                "table_name": "test_table",
                "operation": "upsert",
                "primary_key": ["id"]
            }

            try:
                loader._load_to_database(data, config)
            except Exception:
                # We expect this to fail due to mocking, but we want to check the SQL
                pass

            # Verify that execute was called with text() object (parameterized)
            assert mock_execute.called
            call_args = mock_execute.call_args_list

            # Should be called with sqlalchemy.text() objects, not raw strings
            for call in call_args:
                if call[0]:  # If there are positional arguments
                    sql_arg = call[0][0]
                    # Should be a text() object or similar, not a raw string
                    assert hasattr(sql_arg, '_bindparams') or hasattr(sql_arg, 'params'), \
                        f"Expected parameterized query, got: {type(sql_arg)}"
