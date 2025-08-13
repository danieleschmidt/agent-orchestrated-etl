"""Tests for HashiCorp Vault integration."""

import time
from unittest.mock import Mock, patch

import pytest

from src.agent_orchestrated_etl.config import (
    HVAC_AVAILABLE,
    SecretManager,
    SecurityConfig,
)
from src.agent_orchestrated_etl.validation import ValidationError


class TestHashiCorpVaultIntegration:
    """Test HashiCorp Vault integration."""

    def setup_method(self):
        """Set up test fixtures."""
        self.security_config = SecurityConfig(
            secret_provider="vault",
            vault_url="http://localhost:8200",
            vault_auth_method="token",
            vault_mount_path="secret",
            vault_secret_path="test-app/",
            vault_cache_ttl=300,
            vault_timeout=30
        )
        self.secret_manager = SecretManager(
            provider="vault",
            prefix="TEST_",
            security_config=self.security_config
        )

    def test_init_with_security_config(self):
        """Test SecretManager initialization with vault config."""
        assert self.secret_manager.provider == "vault"
        assert self.secret_manager.prefix == "TEST_"
        assert self.secret_manager.security_config.vault_url == "http://localhost:8200"
        assert self.secret_manager.security_config.vault_auth_method == "token"
        assert self.secret_manager.security_config.vault_mount_path == "secret"
        assert self.secret_manager.security_config.vault_secret_path == "test-app/"
        assert self.secret_manager.security_config.vault_cache_ttl == 300

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_token_auth(self, mock_getenv, mock_hvac_client):
        """Test retrieving a secret from Vault with token authentication."""
        # Mock environment variable
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        # Mock KV v2 response
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'db_password': 'secret123'
                }
            }
        }

        # Test retrieval
        result = self.secret_manager.get_secret("db_password")

        # Verify calls
        mock_hvac_client.assert_called_once_with(
            url="http://localhost:8200",
            timeout=30
        )
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="test-app/db_password",
            mount_point="secret"
        )

        assert result == 'secret123'
        assert mock_client.token == "test-token"

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_approle_auth(self, mock_getenv, mock_hvac_client):
        """Test retrieving a secret from Vault with AppRole authentication."""
        # Configure for AppRole auth
        self.security_config.vault_auth_method = "approle"

        # Mock environment variables
        def getenv_side_effect(key, default=None):
            env_vars = {
                "VAULT_ROLE_ID": "test-role-id",
                "VAULT_SECRET_ID": "test-secret-id"
            }
            return env_vars.get(key, default)

        mock_getenv.side_effect = getenv_side_effect

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = False

        # Mock AppRole auth response
        mock_client.auth.approle.login.return_value = {
            'auth': {
                'client_token': 'test-client-token'
            }
        }

        # Mock KV v1/generic response
        mock_client.read.return_value = {
            'data': {
                'api_key': 'key456'
            }
        }

        # Test retrieval
        result = self.secret_manager.get_secret("api_key")

        # Verify AppRole auth was called
        mock_client.auth.approle.login.assert_called_once_with(
            role_id="test-role-id",
            secret_id="test-secret-id"
        )

        assert result == 'key456'
        assert mock_client.token == "test-client-token"

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    @patch('builtins.open')
    def test_get_vault_secret_kubernetes_auth(self, mock_open, mock_getenv, mock_hvac_client):
        """Test retrieving a secret from Vault with Kubernetes authentication."""
        # Configure for Kubernetes auth
        self.security_config.vault_auth_method = "kubernetes"

        # Mock environment variables
        mock_getenv.side_effect = lambda key, default=None: {
            "VAULT_K8S_ROLE": "test-k8s-role"
        }.get(key, default)

        # Mock JWT file reading
        mock_file = Mock()
        mock_file.read.return_value = "test-jwt-token   \n"
        mock_open.return_value.__enter__.return_value = mock_file

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        # Mock Kubernetes auth response
        mock_client.auth.kubernetes.login.return_value = {
            'auth': {
                'client_token': 'k8s-client-token'
            }
        }

        # Mock secret response with single key-value
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'password': 'single-secret-value'
                }
            }
        }

        # Test retrieval - should return single value when only one key exists
        result = self.secret_manager.get_secret("anything")

        # Verify Kubernetes auth was called
        mock_client.auth.kubernetes.login.assert_called_once_with(
            role="test-k8s-role",
            jwt="test-jwt-token"
        )

        assert result == 'single-secret-value'
        assert mock_client.token == "k8s-client-token"

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_caching(self, mock_getenv, mock_hvac_client):
        """Test that Vault secrets are cached properly."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'cached_value': 'test-cache-value'
                }
            }
        }

        # First call - should hit Vault
        result1 = self.secret_manager.get_secret("cached_value")
        assert result1 == 'test-cache-value'
        assert mock_client.secrets.kv.v2.read_secret_version.call_count == 1

        # Second call - should use cache
        result2 = self.secret_manager.get_secret("cached_value")
        assert result2 == 'test-cache-value'
        assert mock_client.secrets.kv.v2.read_secret_version.call_count == 1  # No additional call

        # Verify cache statistics
        stats = self.secret_manager.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["expired_entries"] == 0
        assert stats["vault_secrets"]["total_entries"] == 1

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('src.agent_orchestrated_etl.config.time.time')
    @patch('os.getenv')
    def test_get_vault_secret_cache_expiry(self, mock_getenv, mock_time, mock_hvac_client):
        """Test that cache expires after TTL."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'expiry_test': 'cached-value'
                }
            }
        }

        # Mock time progression
        mock_time.side_effect = [1000, 1400, 1400]  # Cache, expire check, new cache

        # First call - should hit Vault and cache
        result1 = self.secret_manager.get_secret("expiry_test")
        assert result1 == 'cached-value'
        assert mock_client.secrets.kv.v2.read_secret_version.call_count == 1

        # Second call after cache expiry - should hit Vault again
        result2 = self.secret_manager.get_secret("expiry_test")
        assert result2 == 'cached-value'
        assert mock_client.secrets.kv.v2.read_secret_version.call_count == 2

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_not_found(self, mock_getenv, mock_hvac_client):
        """Test handling when secret is not found in Vault."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        # Mock InvalidPath exception
        import hvac.exceptions
        mock_client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.InvalidPath()

        # Test with default value
        result = self.secret_manager.get_secret("nonexistent", default="default-value")
        assert result == "default-value"

        # Test without default value
        result = self.secret_manager.get_secret("nonexistent")
        assert result is None

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_authentication_errors(self, mock_getenv, mock_hvac_client):
        """Test handling of Vault authentication errors."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = False

        # Test that ValidationError is raised when authentication fails
        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")

        assert "Failed to authenticate with Vault" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_forbidden_error(self, mock_getenv, mock_hvac_client):
        """Test handling of Vault forbidden errors."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        # Mock Forbidden exception
        import hvac.exceptions
        mock_client.secrets.kv.v2.read_secret_version.side_effect = hvac.exceptions.Forbidden("Access denied")

        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("forbidden_secret")

        assert "Access denied to Vault secret" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_get_vault_secret_multiple_keys_fallback(self, mock_getenv, mock_hvac_client):
        """Test fallback to common key patterns when multiple keys exist."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        # Mock response with multiple keys, including a common pattern
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'username': 'testuser',
                    'password': 'secret123',  # Should match common pattern
                    'other_field': 'other_value'
                }
            }
        }

        # Test retrieval - should find 'password' from common patterns
        result = self.secret_manager.get_secret("some_key")
        assert result == 'secret123'

    def test_get_vault_secret_hvac_not_available(self):
        """Test error when hvac is not available."""
        with patch('src.agent_orchestrated_etl.config.HVAC_AVAILABLE', False):
            secret_manager = SecretManager(
                provider="vault",
                security_config=self.security_config
            )

            with pytest.raises(ValidationError) as exc_info:
                secret_manager.get_secret("test_key")

            assert "requires hvac library" in str(exc_info.value)
            assert "pip install hvac" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('os.getenv')
    def test_vault_token_auth_missing_token(self, mock_getenv):
        """Test error when VAULT_TOKEN is missing for token auth."""
        # Mock missing token
        mock_getenv.return_value = None

        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")

        assert "VAULT_TOKEN environment variable required" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('os.getenv')
    def test_vault_approle_auth_missing_credentials(self, mock_getenv):
        """Test error when AppRole credentials are missing."""
        # Configure for AppRole auth
        self.security_config.vault_auth_method = "approle"

        # Mock missing credentials
        mock_getenv.return_value = None

        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")

        assert "VAULT_ROLE_ID and VAULT_SECRET_ID environment variables required" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    def test_vault_unsupported_auth_method(self):
        """Test error for unsupported authentication method."""
        # Configure unsupported auth method
        self.security_config.vault_auth_method = "unsupported"

        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")

        assert "Unsupported Vault authentication method: unsupported" in str(exc_info.value)

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    def test_clear_cache_includes_vault(self):
        """Test that cache clearing includes Vault cache."""
        # Add some mock cache entries
        self.secret_manager._vault_secrets_cache = {
            'secret/test-app/key1': ('value1', time.time()),
            'secret/test-app/key2': ('value2', time.time()),
        }

        # Add AWS cache too
        self.secret_manager._aws_secrets_cache = {
            'aws-key': ('aws-value', time.time()),
        }

        # Verify caches have entries
        assert len(self.secret_manager._vault_secrets_cache) == 2
        assert len(self.secret_manager._aws_secrets_cache) == 1

        # Clear cache
        self.secret_manager.clear_cache()

        # Verify both caches are empty
        assert len(self.secret_manager._vault_secrets_cache) == 0
        assert len(self.secret_manager._aws_secrets_cache) == 0

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    def test_get_cache_stats_includes_vault(self):
        """Test that cache statistics include Vault cache."""
        current_time = time.time()

        # Add mock cache entries with different ages
        self.secret_manager._vault_secrets_cache = {
            'secret/test-app/key1': ('value1', current_time),  # Valid
            'secret/test-app/key2': ('value2', current_time - 400),  # Expired (TTL is 300)
            'secret/test-app/key3': ('value3', current_time - 100),  # Valid
        }

        # Add AWS cache entries too
        self.secret_manager._aws_secrets_cache = {
            'aws-key1': ('aws-value1', current_time),  # Valid
            'aws-key2': ('aws-value2', current_time - 400),  # Expired
        }

        # Get cache statistics
        stats = self.secret_manager.get_cache_stats()

        # Verify combined statistics
        assert stats["total_entries"] == 5
        assert stats["valid_entries"] == 3
        assert stats["expired_entries"] == 2

        # Verify Vault-specific statistics
        assert stats["vault_secrets"]["total_entries"] == 3
        assert stats["vault_secrets"]["valid_entries"] == 2
        assert stats["vault_secrets"]["expired_entries"] == 1

        # Verify AWS-specific statistics
        assert stats["aws_secrets"]["total_entries"] == 2
        assert stats["aws_secrets"]["valid_entries"] == 1
        assert stats["aws_secrets"]["expired_entries"] == 1


class TestVaultClientReuse:
    """Test Vault client reuse and connection management."""

    @pytest.mark.skipif(not HVAC_AVAILABLE, reason="hvac not available")
    @patch('src.agent_orchestrated_etl.config.hvac.Client')
    @patch('os.getenv')
    def test_vault_client_reuse(self, mock_getenv, mock_hvac_client):
        """Test that Vault client is reused across calls."""
        # Mock token
        mock_getenv.return_value = "test-token"

        # Mock Vault client
        mock_client = Mock()
        mock_hvac_client.return_value = mock_client
        mock_client.is_authenticated.return_value = True
        mock_client.is_secret_backend_mounted.return_value = True

        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            'data': {
                'data': {
                    'test_value': 'reuse-test'
                }
            }
        }

        security_config = SecurityConfig(secret_provider="vault")
        secret_manager = SecretManager(provider="vault", security_config=security_config)

        # Make multiple calls
        secret_manager.get_secret("key1")
        secret_manager.clear_cache()  # Clear cache to force new Vault calls
        secret_manager.get_secret("key2")

        # Verify hvac.Client was called only once (client reused)
        assert mock_hvac_client.call_count == 1

        # But read operations were called twice
        assert mock_client.secrets.kv.v2.read_secret_version.call_count == 2
