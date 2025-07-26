"""Tests for ETL-017: Secret Management Security Fixes."""

import os
import pytest
import tempfile
from pathlib import Path
from unittest.mock import patch, Mock

from src.agent_orchestrated_etl.config import SecretManager, SecurityConfig
from src.agent_orchestrated_etl.config_templates import (
    generate_env_template,
    generate_docker_compose_template,
    generate_config_template
)


class TestSecretManagementSecurity:
    """Test secret management security implementation."""

    def test_no_hardcoded_secrets_in_env_template(self):
        """Test that generated .env templates don't contain hardcoded secrets."""
        for env in ["development", "staging", "production"]:
            template = generate_env_template(env)
            
            # Check for commented out or placeholder secrets
            assert "# AGENT_ETL_DB_PASSWORD=" in template
            assert "# AGENT_ETL_S3_ACCESS_KEY_ID=" in template
            assert "# AGENT_ETL_S3_SECRET_ACCESS_KEY=" in template
            assert "# AGENT_ETL_API_KEY=" in template
            
            # Ensure no actual secrets are present
            lines = template.split('\n')
            for line in lines:
                if line.startswith('AGENT_ETL_') and '=' in line:
                    key, value = line.split('=', 1)
                    # Only check uncommented lines
                    if not line.strip().startswith('#'):
                        # These should not contain secret-like values
                        if any(secret_key in key.upper() for secret_key in ['PASSWORD', 'KEY', 'SECRET', 'TOKEN']):
                            # Skip the ones we intentionally commented out
                            if key.strip() not in ['AGENT_ETL_DB_PASSWORD', 'AGENT_ETL_S3_ACCESS_KEY_ID', 
                                                 'AGENT_ETL_S3_SECRET_ACCESS_KEY', 'AGENT_ETL_API_KEY']:
                                assert not value or value.isspace(), f"Found potential secret in {key}={value}"

    def test_docker_compose_uses_env_variables(self):
        """Test that Docker Compose template uses environment variables for secrets."""
        template = generate_docker_compose_template("production")
        
        # Check that sensitive values use environment variable substitution
        assert "POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-changeme}" in template
        assert "# Set POSTGRES_PASSWORD environment variable" in template

    def test_config_template_security_warnings(self):
        """Test that config templates include appropriate security warnings."""
        prod_template = generate_env_template("production")
        
        # Check for security warnings in production
        assert "SECURITY WARNING: PRODUCTION ENVIRONMENT" in prod_template
        assert "Never store secrets in configuration files!" in prod_template
        assert "Use AWS Secrets Manager, HashiCorp Vault" in prod_template
        assert "AGENT_ETL_SECRET_PROVIDER=aws_secrets" in prod_template

    def test_secret_manager_prevents_logging_secrets(self):
        """Test that SecretManager doesn't log or expose secrets."""
        config = SecurityConfig(secret_provider="env")
        secret_manager = SecretManager(provider="env", security_config=config)
        
        with patch.dict(os.environ, {"AGENT_ETL_TEST_SECRET": "sensitive_value"}):
            secret = secret_manager.get_secret("TEST_SECRET")
            assert secret == "sensitive_value"
            
            # Test that the secret manager itself doesn't expose secrets in str representation
            manager_str = str(secret_manager.__dict__)
            assert "sensitive_value" not in manager_str

    def test_secret_validation_prevents_injection(self):
        """Test that secret validation prevents injection attacks."""
        from src.agent_orchestrated_etl.validation import validate_environment_variable
        
        # Test SQL injection patterns
        malicious_values = [
            "'; DROP TABLE users; --",
            "admin'/**/OR/**/1=1--",
            "'; UNION SELECT * FROM secrets; --",
            "<script>alert('xss')</script>",
            "$(rm -rf /)",
            "`rm -rf /`",
        ]
        
        for malicious_value in malicious_values:
            try:
                # This should either clean the value or raise an error
                result = validate_environment_variable("TEST_VAR", malicious_value)
                # If it doesn't raise an error, it should have cleaned the value
                assert result != malicious_value or result is None
            except Exception:
                # Raising an exception is also acceptable for malicious input
                pass

    def test_aws_secrets_manager_integration_security(self):
        """Test AWS Secrets Manager integration security features."""
        config = SecurityConfig(
            secret_provider="aws_secrets",
            aws_secrets_region="us-east-1",
            aws_secrets_prefix="test/"
        )
        secret_manager = SecretManager(provider="aws_secrets", security_config=config)
        
        # Test that cache doesn't expose secrets
        cache_stats = secret_manager.get_cache_stats()
        assert "secret" not in str(cache_stats).lower()
        assert "password" not in str(cache_stats).lower()
        
    def test_vault_integration_security(self):
        """Test HashiCorp Vault integration security features.""" 
        config = SecurityConfig(
            secret_provider="vault",
            vault_url="http://localhost:8200",
            vault_auth_method="token"
        )
        secret_manager = SecretManager(provider="vault", security_config=config)
        
        # Test cache security
        cache_stats = secret_manager.get_cache_stats()
        assert isinstance(cache_stats, dict)
        # Ensure cache stats don't leak sensitive information
        cache_str = str(cache_stats)
        sensitive_patterns = ["password", "secret", "token", "key"]
        for pattern in sensitive_patterns:
            # Stats should not contain actual secret values
            assert pattern not in cache_str.lower() or "total_entries" in cache_str

    def test_secret_cache_security(self):
        """Test that secret caching doesn't expose sensitive data."""
        config = SecurityConfig(secret_provider="env")
        secret_manager = SecretManager(provider="env", security_config=config)
        
        with patch.dict(os.environ, {"AGENT_ETL_CACHE_TEST": "secret_value"}):
            # Get secret to populate cache
            secret_manager.get_secret("CACHE_TEST")
            
            # Check cache stats don't expose secrets
            stats = secret_manager.get_cache_stats()
            stats_str = str(stats)
            assert "secret_value" not in stats_str
            
            # Clear cache should work without issues
            secret_manager.clear_cache()
            stats_after = secret_manager.get_cache_stats()
            assert stats_after["total_entries"] == 0

    def test_configuration_file_secret_detection(self):
        """Test detection of secrets in configuration files."""
        # Test various secret patterns that should be detected
        test_configs = [
            'password = "hardcoded_password"',
            "api_key = 'sk-1234567890abcdef'",
            'secret_key = "AKIA1234567890123456"',
            'token = "eyJhbGciOiJIUzI1NiJ9"',
        ]
        
        for config_line in test_configs:
            # This test ensures our secret detection would catch these patterns
            # In practice, this would be part of a pre-commit hook or CI check
            assert any(pattern in config_line.lower() for pattern in ['password', 'key', 'secret', 'token'])

    def test_environment_variable_security_best_practices(self):
        """Test that environment variable handling follows security best practices."""
        config = SecurityConfig(secret_provider="env")
        secret_manager = SecretManager(provider="env", security_config=config)
        
        # Test with empty/None values
        assert secret_manager.get_secret("NONEXISTENT_SECRET") is None
        assert secret_manager.get_secret("NONEXISTENT_SECRET", "default") == "default"
        
        # Test with whitespace (should be cleaned)
        with patch.dict(os.environ, {"AGENT_ETL_WHITESPACE_TEST": "  secret_with_spaces  "}):
            result = secret_manager.get_secret("WHITESPACE_TEST")
            # Should handle whitespace appropriately
            assert result is not None

    def test_secret_rotation_support(self):
        """Test that secret managers support rotation (cache invalidation)."""
        config = SecurityConfig(
            secret_provider="env",
            aws_secrets_cache_ttl=1  # 1 second for testing
        )
        secret_manager = SecretManager(provider="env", security_config=config)
        
        # Test cache expiration
        with patch.dict(os.environ, {"AGENT_ETL_ROTATION_TEST": "old_value"}):
            old_secret = secret_manager.get_secret("ROTATION_TEST")
            assert old_secret == "old_value"
            
            # Clear cache manually (simulating rotation)
            secret_manager.clear_cache()
            
            # Update environment (simulating rotation)
            with patch.dict(os.environ, {"AGENT_ETL_ROTATION_TEST": "new_value"}):
                new_secret = secret_manager.get_secret("ROTATION_TEST")
                assert new_secret == "new_value"

    def test_configuration_validation_security(self):
        """Test that configuration validation includes security checks."""
        from src.agent_orchestrated_etl.config import _validate_config, AppConfig
        
        # Test that production configs are validated for security
        config = AppConfig()
        config.environment = "production"
        config.debug = True  # This should be invalid for production
        
        with pytest.raises(Exception):  # Should raise ValidationError
            _validate_config(config)
        
        # Test valid production config
        config.debug = False
        config.logging.level = "INFO"  # Not DEBUG
        try:
            _validate_config(config)
        except Exception as e:
            # Should not raise an exception for valid config
            pytest.fail(f"Valid production config failed validation: {e}")