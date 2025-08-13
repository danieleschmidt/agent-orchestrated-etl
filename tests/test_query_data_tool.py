"""Tests for QueryDataTool - ETL-012 implementation."""

import os
import sqlite3
import tempfile
from unittest.mock import MagicMock, patch

import pytest

from src.agent_orchestrated_etl.agents.tools import QueryDataTool, ToolException


class TestQueryDataTool:
    """Test suite for QueryDataTool implementation."""

    @pytest.fixture
    def query_tool(self):
        """Create QueryDataTool instance for testing."""
        return QueryDataTool()

    @pytest.fixture
    def test_database(self):
        """Create test SQLite database."""
        db_file = tempfile.mktemp(suffix='.db')
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()

        cursor.execute('''
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                department TEXT,
                salary REAL,
                hire_date TEXT
            )
        ''')

        test_data = [
            (1, 'Alice Johnson', 'Engineering', 95000.0, '2023-01-15'),
            (2, 'Bob Smith', 'Marketing', 75000.0, '2023-02-20'),
            (3, 'Carol Davis', 'Engineering', 105000.0, '2023-01-10'),
            (4, 'David Wilson', 'Sales', 68000.0, '2023-03-05'),
            (5, 'Eve Brown', 'Engineering', 89000.0, '2023-01-25'),
        ]

        cursor.executemany('INSERT INTO employees VALUES (?, ?, ?, ?, ?)', test_data)
        conn.commit()
        conn.close()

        yield f"sqlite:///{db_file}"

        # Cleanup
        if os.path.exists(db_file):
            os.unlink(db_file)

    def test_query_tool_initialization(self, query_tool):
        """Test QueryDataTool initialization."""
        assert query_tool.name == "query_data"
        assert query_tool.description == "Execute queries against data sources"
        assert hasattr(query_tool, '_query_cache')
        assert hasattr(query_tool, '_query_history')
        assert hasattr(query_tool, '_audit_log')
        assert isinstance(query_tool._query_cache, dict)
        assert isinstance(query_tool._query_history, list)
        assert isinstance(query_tool._audit_log, list)

    def test_sql_query_execution(self, query_tool, test_database):
        """Test basic SQL query execution."""
        # Mock the SQLAlchemy dependencies to avoid import issues
        with patch('src.agent_orchestrated_etl.agents.tools.create_engine') as mock_engine, \
             patch('src.agent_orchestrated_etl.agents.tools.text') as mock_text:

            # Setup mock engine and connection
            mock_conn = MagicMock()
            mock_engine.return_value.connect.return_value.__enter__.return_value = mock_conn
            mock_engine.return_value.connect.return_value.__exit__.return_value = None

            # Setup mock query result
            mock_result = MagicMock()
            mock_result.keys.return_value = ['name', 'department', 'salary']
            mock_result.fetchall.return_value = [
                ('Alice Johnson', 'Engineering', 95000.0),
                ('Carol Davis', 'Engineering', 105000.0)
            ]
            mock_conn.execute.return_value = mock_result

            # Execute query
            result = query_tool._execute(
                data_source=test_database,
                query="SELECT name, department, salary FROM employees WHERE department = 'Engineering'",
                limit=10,
                format="json"
            )

            # Verify result structure
            assert result["status"] == "completed"
            assert result["results"]["row_count"] == 2
            assert result["results"]["columns"] == ['name', 'department', 'salary']
            assert len(result["results"]["data"]) == 2
            assert result["cache_hit"] is False
            assert "performance_metrics" in result
            assert "execution_time_ms" in result["performance_metrics"]

    def test_query_caching(self, query_tool, test_database):
        """Test query result caching functionality."""
        query = "SELECT * FROM employees WHERE salary > 80000"

        with patch.object(query_tool, '_execute_sql_query') as mock_execute:
            mock_execute.return_value = {
                "status": "completed",
                "results": {
                    "row_count": 3,
                    "columns": ["id", "name", "salary"],
                    "data": [{"id": 1, "name": "Alice", "salary": 95000}]
                }
            }

            # First execution - should call _execute_sql_query
            result1 = query_tool._execute(
                data_source=test_database,
                query=query,
                limit=100,
                use_cache=True
            )

            assert result1["cache_hit"] is False
            assert mock_execute.call_count == 1

            # Second execution - should use cache
            result2 = query_tool._execute(
                data_source=test_database,
                query=query,
                limit=100,
                use_cache=True
            )

            assert result2["cache_hit"] is True
            assert mock_execute.call_count == 1  # Should not call again

    def test_security_validation_safe_query(self, query_tool):
        """Test security validation for safe queries."""
        safe_queries = [
            "SELECT * FROM employees",
            "SELECT name, salary FROM employees WHERE department = 'Engineering'",
            "SELECT COUNT(*) FROM orders WHERE date > '2024-01-01'"
        ]

        for query in safe_queries:
            result = query_tool._validate_query_security(query, "standard")
            assert result["is_safe"] is True
            assert result["violation_reason"] is None

    def test_security_validation_dangerous_query(self, query_tool):
        """Test security validation for dangerous queries."""
        dangerous_queries = [
            "DROP TABLE employees",
            "DELETE FROM employees WHERE 1=1",
            "SELECT * FROM users; DROP TABLE users; --",
            "UPDATE employees SET salary = 0",
            "INSERT INTO admin_users VALUES ('hacker', 'password')"
        ]

        for query in dangerous_queries:
            result = query_tool._validate_query_security(query, "standard")
            assert result["is_safe"] is False
            assert result["violation_reason"] is not None
            assert "pattern detected" in result["violation_reason"]

    def test_security_strict_mode(self, query_tool):
        """Test strict security mode."""
        # Non-SELECT queries should be blocked in strict mode
        non_select_queries = [
            "SHOW TABLES",
            "DESCRIBE employees",
            "EXPLAIN SELECT * FROM employees"
        ]

        for query in non_select_queries:
            result = query_tool._validate_query_security(query, "strict")
            assert result["is_safe"] is False
            assert "Only SELECT queries allowed" in result["violation_reason"]

    def test_query_blocked_by_security(self, query_tool, test_database):
        """Test that dangerous queries are blocked."""
        dangerous_query = "SELECT * FROM employees; DROP TABLE employees; --"

        result = query_tool._execute(
            data_source=test_database,
            query=dangerous_query,
            limit=100
        )

        assert result["status"] == "blocked"
        assert "security_violation" in result

        # Check audit log
        audit_log = query_tool.get_audit_log()
        assert len(audit_log) > 0
        assert any("security_violation" in entry["event_type"] for entry in audit_log)

    def test_add_limit_to_query(self, query_tool):
        """Test automatic LIMIT addition to queries."""
        # Query without LIMIT
        query1 = "SELECT * FROM employees"
        result1 = query_tool._add_limit_to_query(query1, 50)
        assert "LIMIT 50" in result1

        # Query with existing LIMIT
        query2 = "SELECT * FROM employees LIMIT 10"
        result2 = query_tool._add_limit_to_query(query2, 50)
        assert result2 == query2  # Should not modify

        # Query with semicolon
        query3 = "SELECT * FROM employees;"
        result3 = query_tool._add_limit_to_query(query3, 25)
        assert "LIMIT 25;" in result3

    def test_cache_key_generation(self, query_tool):
        """Test cache key generation."""
        key1 = query_tool._generate_cache_key("db1", "SELECT * FROM table1", 100)
        key2 = query_tool._generate_cache_key("db1", "SELECT * FROM table1", 100)
        key3 = query_tool._generate_cache_key("db1", "SELECT * FROM table2", 100)

        assert key1 == key2  # Same inputs should generate same key
        assert key1 != key3  # Different inputs should generate different keys
        assert len(key1) == 32  # MD5 hash length

    def test_query_history_tracking(self, query_tool, test_database):
        """Test query history tracking."""
        with patch.object(query_tool, '_execute_sql_query') as mock_execute:
            mock_execute.return_value = {
                "status": "completed",
                "results": {"row_count": 1, "columns": [], "data": []}
            }

            # Execute a few queries
            query_tool._execute(test_database, "SELECT * FROM employees", 10)
            query_tool._execute(test_database, "SELECT name FROM employees", 5)

            history = query_tool.get_query_history()
            assert len(history) == 2

            # Check history entry structure
            entry = history[0]
            assert "query_id" in entry
            assert "query" in entry
            assert "data_source" in entry
            assert "status" in entry
            assert "execution_time" in entry
            assert "timestamp" in entry

    def test_audit_log_tracking(self, query_tool):
        """Test audit log functionality."""
        query_tool._log_audit_event("test_event", "SELECT * FROM test", "test_db", "test details")

        audit_log = query_tool.get_audit_log()
        assert len(audit_log) == 1

        entry = audit_log[0]
        assert entry["event_type"] == "test_event"
        assert entry["query"] == "SELECT * FROM test"
        assert entry["data_source"] == "test_db"
        assert entry["details"] == "test details"
        assert "timestamp" in entry

    def test_csv_formatting(self, query_tool):
        """Test CSV result formatting."""
        test_result = {
            "results": {
                "columns": ["name", "age"],
                "data": [
                    {"name": "Alice", "age": 30},
                    {"name": "Bob", "age": 25}
                ]
            }
        }

        formatted = query_tool._format_as_csv(test_result)

        assert formatted["format"] == "csv"
        assert "csv_data" in formatted
        csv_content = formatted["csv_data"]
        assert "name,age" in csv_content  # Header
        assert "Alice,30" in csv_content  # Data
        assert "Bob,25" in csv_content    # Data

    def test_excel_formatting_placeholder(self, query_tool):
        """Test Excel formatting placeholder."""
        test_result = {"results": {"columns": [], "data": []}}

        formatted = query_tool._format_as_excel(test_result)

        assert formatted["format"] == "xlsx"
        assert "excel_note" in formatted

    def test_export_results(self, query_tool):
        """Test result export functionality."""
        # Add a query to history first
        query_tool._add_to_history("test123", "SELECT * FROM test", "test_db", "completed", 0.1)

        export_result = query_tool.export_results("test123", "csv")

        assert export_result["query_id"] == "test123"
        assert export_result["format"] == "csv"
        assert export_result["status"] == "ready"
        assert "download_url" in export_result
        assert "expires_at" in export_result

    def test_export_results_not_found(self, query_tool):
        """Test export for non-existent query ID."""
        export_result = query_tool.export_results("nonexistent", "csv")

        assert export_result["status"] == "error"
        assert "not found" in export_result["error_message"]

    def test_cache_cleanup(self, query_tool):
        """Test cache cleanup functionality."""
        # Fill cache with test entries
        for i in range(1500):  # More than max_entries (1000)
            cache_key = f"key_{i}"
            query_tool._query_cache[cache_key] = {
                "cache_timestamp": i,
                "data": f"result_{i}"
            }

        # Trigger cleanup
        query_tool._cleanup_cache(max_entries=1000)

        # Should keep only newest 80% (800 entries)
        assert len(query_tool._query_cache) == 800

    def test_query_plan_extraction(self, query_tool):
        """Test query plan extraction."""
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.fetchall.return_value = [
            ("SCAN TABLE employees",),
            ("USE INDEX idx_department",)
        ]
        mock_conn.execute.return_value = mock_result

        plan = query_tool._get_query_plan(mock_conn, "SELECT * FROM employees")

        assert "SCAN TABLE employees" in plan
        assert "USE INDEX idx_department" in plan

    def test_query_plan_not_available(self, query_tool):
        """Test query plan when not available."""
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("Plan not supported")

        plan = query_tool._get_query_plan(mock_conn, "SELECT * FROM employees")

        assert plan == "Query plan not available"

    def test_tool_exception_handling(self, query_tool):
        """Test tool exception handling."""
        with patch.object(query_tool, '_validate_query_security', side_effect=Exception("Security check failed")):
            with pytest.raises(ToolException) as exc_info:
                query_tool._execute("test_db", "SELECT * FROM test", 10)

            assert "Data query failed" in str(exc_info.value)
            assert "Security check failed" in str(exc_info.value)


class TestQueryDataToolIntegration:
    """Integration tests for QueryDataTool."""

    @pytest.fixture
    def query_tool(self):
        """Create QueryDataTool instance."""
        return QueryDataTool()

    def test_end_to_end_query_workflow(self, query_tool):
        """Test complete query workflow with mocked dependencies."""
        with patch.object(query_tool, '_execute_sql_query') as mock_execute:
            mock_execute.return_value = {
                "status": "completed",
                "results": {
                    "row_count": 2,
                    "columns": ["name", "department"],
                    "data": [
                        {"name": "Alice", "department": "Engineering"},
                        {"name": "Bob", "department": "Marketing"}
                    ]
                },
                "query_plan": "SCAN TABLE employees"
            }

            # Execute query
            result = query_tool._execute(
                data_source="sqlite:///test.db",
                query="SELECT name, department FROM employees",
                limit=100,
                format="json",
                use_cache=True,
                security_level="standard"
            )

            # Verify comprehensive result
            assert result["status"] == "completed"
            assert result["cache_hit"] is False
            assert result["results"]["row_count"] == 2
            assert "performance_metrics" in result
            assert result["performance_metrics"]["execution_time_ms"] > 0

            # Verify history and audit log
            history = query_tool.get_query_history()
            assert len(history) == 1

            audit_log = query_tool.get_audit_log()
            assert any("query_executed" in entry["event_type"] for entry in audit_log)

    def test_multiple_format_support(self, query_tool):
        """Test support for multiple output formats."""
        with patch.object(query_tool, '_execute_sql_query') as mock_execute:
            mock_execute.return_value = {
                "status": "completed",
                "results": {
                    "row_count": 1,
                    "columns": ["name"],
                    "data": [{"name": "Test"}]
                }
            }

            # Test JSON format (default)
            json_result = query_tool._execute("test_db", "SELECT name FROM test", format="json")
            assert json_result["format"] == "json"

            # Test CSV format
            csv_result = query_tool._execute("test_db", "SELECT name FROM test", format="csv")
            assert csv_result["format"] == "csv"
            assert "csv_data" in csv_result

            # Test Excel format
            xlsx_result = query_tool._execute("test_db", "SELECT name FROM test", format="xlsx")
            assert xlsx_result["format"] == "xlsx"
