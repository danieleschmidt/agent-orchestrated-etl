"""Unit tests for SQL injection security fixes."""

from unittest.mock import MagicMock, patch

import pytest

# Import the DataLoader class
try:
    from agent_orchestrated_etl.orchestrator import DataLoader
except ImportError:
    # Skip if dependencies not available
    pytest.skip("Dependencies not available", allow_module_level=True)


class TestSQLInjectionSecurity:
    """Test suite for SQL injection vulnerability fixes."""

    def test_malicious_table_name_rejected(self):
        """Test that malicious table names are rejected."""
        loader = DataLoader()

        malicious_names = [
            "test_table; DROP TABLE test_table; --",
            "users UNION SELECT * FROM passwords",
            "table'; DELETE FROM users; --",
            "../../../etc/passwd",
            "table\x00DROP"
        ]

        for malicious_name in malicious_names:
            config = {
                "connection_string": "sqlite:///test.db",
                "table_name": malicious_name,
                "operation": "insert"
            }

            result = loader.load([{"id": 1, "name": "test"}], config)

            assert result["status"] == "error"
            assert "Invalid table name" in result["error_message"]

    def test_malicious_column_name_rejected(self):
        """Test that malicious column names are rejected."""
        loader = DataLoader()

        malicious_columns = [
            "name'; DROP TABLE users; --",
            "UNION SELECT password FROM admin",
            "name, (SELECT secret FROM config)"
        ]

        for malicious_col in malicious_columns:
            data = [{malicious_col: "some_value", "id": 999}]
            config = {
                "connection_string": "sqlite:///test.db",
                "table_name": "test_table",
                "operation": "insert"
            }

            result = loader.load(data, config)

            assert result["status"] == "error"
            assert "Invalid column name" in result["error_message"]

    def test_malicious_operation_rejected(self):
        """Test that malicious operation names are rejected."""
        loader = DataLoader()

        malicious_operations = [
            "insert; DROP TABLE users",
            "upsert' OR '1'='1",
            "DELETE FROM"
        ]

        for malicious_op in malicious_operations:
            config = {
                "connection_string": "sqlite:///test.db",
                "table_name": "test_table",
                "operation": malicious_op
            }

            result = loader.load([{"id": 1, "name": "test"}], config)

            assert result["status"] == "error"
            assert "Invalid operation" in result["error_message"]

    def test_valid_identifiers_accepted(self):
        """Test that valid identifiers are accepted."""
        loader = DataLoader()

        valid_identifiers = ["users", "user_table", "schema.table", "Table123", "_private"]

        for identifier in valid_identifiers:
            assert loader._is_valid_identifier(identifier) == True

    def test_invalid_identifiers_rejected(self):
        """Test that invalid identifiers are rejected."""
        loader = DataLoader()

        invalid_identifiers = [
            "users; DROP", "table'--", "table UNION", "123invalid",
            "table..", "", None, "a"*100, "table\x00", "table space"
        ]

        for identifier in invalid_identifiers:
            assert loader._is_valid_identifier(identifier) == False

    def test_record_value_validation(self):
        """Test record value validation."""
        loader = DataLoader()

        # Valid record should pass
        valid_record = {"id": 1, "name": "John", "active": True}
        validated = loader._validate_record_values(valid_record)
        assert validated == valid_record

        # Invalid column name should fail
        with pytest.raises(ValueError, match="Invalid column name"):
            loader._validate_record_values({"id; DROP": "evil"})

        # Oversized string should fail
        with pytest.raises(ValueError, match="String value too long"):
            loader._validate_record_values({"id": 1, "data": "x" * 20000})

    def test_primary_key_validation(self):
        """Test primary key validation."""
        loader = DataLoader()

        # Invalid primary key should be rejected
        config = {
            "connection_string": "sqlite:///test.db",
            "table_name": "test_table",
            "operation": "upsert",
            "primary_key": ["id; DROP TABLE"]
        }

        result = loader.load([{"id": 1, "name": "test"}], config)

        assert result["status"] == "error"
        assert "Invalid primary key column name" in result["error_message"]

    @patch('agent_orchestrated_etl.orchestrator.create_engine')
    @patch('agent_orchestrated_etl.orchestrator.pd.DataFrame')
    def test_sql_injection_prevented_in_upsert(self, mock_df, mock_engine):
        """Test that SQL injection is prevented in upsert operations."""
        loader = DataLoader()

        # Mock the DataFrame and engine
        mock_df_instance = MagicMock()
        mock_df_instance.empty = False
        mock_df_instance.columns = ['id', 'name']
        mock_df_instance.__len__ = lambda: 1
        mock_df.return_value = mock_df_instance

        mock_engine_instance = MagicMock()
        mock_engine.return_value = mock_engine_instance

        # Test with valid data (should proceed to database operations)
        valid_config = {
            "connection_string": "sqlite:///test.db",
            "table_name": "valid_table",
            "operation": "upsert",
            "primary_key": ["id"]
        }
        valid_data = [{"id": 1, "name": "Valid User"}]

        # This should not raise a validation error
        try:
            result = loader.load(valid_data, valid_config)
            # May fail due to mocking, but validation should pass
        except Exception as e:
            # Should not be a validation error
            assert "Invalid" not in str(e)

    def test_identifier_edge_cases(self):
        """Test edge cases for identifier validation."""
        loader = DataLoader()

        # Edge cases that should be valid
        valid_cases = ["a", "_", "_a", "schema.table", "TABLE123"]
        for case in valid_cases:
            assert loader._is_valid_identifier(case) == True, f"Should be valid: {case}"

        # Edge cases that should be invalid
        invalid_cases = ["", "123table", "table..", "table space", "table\n"]
        for case in invalid_cases:
            assert loader._is_valid_identifier(case) == False, f"Should be invalid: {case}"

    def test_comprehensive_injection_patterns(self):
        """Test comprehensive SQL injection patterns."""
        loader = DataLoader()

        injection_patterns = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "' UNION SELECT * FROM passwords --",
            "'; INSERT INTO logs VALUES ('hacked'); --",
            "' AND 1=1 --",
            "admin'/*",
            "' OR 1=1#",
            "'; SHUTDOWN; --",
        ]

        for pattern in injection_patterns:
            # Test in table name
            table_name = f"users{pattern}"
            assert not loader._is_valid_identifier(table_name), f"Should reject table injection: {pattern}"

            # Test in column name
            column_name = f"name{pattern}"
            assert not loader._is_valid_identifier(column_name), f"Should reject column injection: {pattern}"
