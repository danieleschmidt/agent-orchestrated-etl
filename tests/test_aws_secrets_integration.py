"""Tests for AWS Secrets Manager integration."""

import json
import os
import time
from unittest.mock import Mock, patch, MagicMock
import pytest

from src.agent_orchestrated_etl.config import (
    SecretManager, 
    SecurityConfig, 
    BOTO3_AVAILABLE
)
from src.agent_orchestrated_etl.validation import ValidationError


class TestAWSSecretsManagerIntegration:
    """Test AWS Secrets Manager integration."""
    
    def setup_method(self):
        """Set up test fixtures."""
        self.security_config = SecurityConfig(
            secret_provider="aws_secrets",
            aws_secrets_region="us-west-2",
            aws_secrets_prefix="test-app/",
            aws_secrets_cache_ttl=300
        )
        self.secret_manager = SecretManager(
            provider="aws_secrets",
            prefix="TEST_",
            security_config=self.security_config
        )
    
    def test_init_with_security_config(self):
        """Test SecretManager initialization with security config."""
        assert self.secret_manager.provider == "aws_secrets"
        assert self.secret_manager.prefix == "TEST_"
        assert self.secret_manager.security_config.aws_secrets_region == "us-west-2"
        assert self.secret_manager.security_config.aws_secrets_prefix == "test-app/"
        assert self.secret_manager.security_config.aws_secrets_cache_ttl == 300
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_string_value(self, mock_boto_client):
        """Test retrieving a simple string secret from AWS Secrets Manager."""
        # Mock AWS client response
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'my-secret-value'
        }
        
        # Test retrieval
        result = self.secret_manager.get_secret("db_password")
        
        # Verify calls
        mock_boto_client.assert_called_once_with(
            'secretsmanager',
            region_name='us-west-2'
        )
        mock_client.get_secret_value.assert_called_once_with(
            SecretName='test-app/db_password'
        )
        
        assert result == 'my-secret-value'
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_json_value_specific_key(self, mock_boto_client):
        """Test retrieving a specific key from JSON secret."""
        # Mock AWS client response with JSON
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': json.dumps({
                'db_password': 'secret123',
                'api_key': 'key456'
            })
        }
        
        # Test retrieval of specific key
        result = self.secret_manager.get_secret("db_password")
        
        assert result == 'secret123'
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_json_value_single_key(self, mock_boto_client):
        """Test retrieving from JSON secret with single key-value pair."""
        # Mock AWS client response with single JSON key
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': json.dumps({'password': 'secret123'})
        }
        
        # Test retrieval - should return the single value
        result = self.secret_manager.get_secret("anything")
        
        assert result == 'secret123'
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_caching(self, mock_boto_client):
        """Test that AWS secrets are cached properly."""
        # Mock AWS client response
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'cached-value'
        }
        
        # First call - should hit AWS
        result1 = self.secret_manager.get_secret("test_key")
        assert result1 == 'cached-value'
        assert mock_client.get_secret_value.call_count == 1
        
        # Second call - should use cache
        result2 = self.secret_manager.get_secret("test_key")
        assert result2 == 'cached-value'
        assert mock_client.get_secret_value.call_count == 1  # No additional call
        
        # Verify cache statistics
        stats = self.secret_manager.get_cache_stats()
        assert stats["total_entries"] == 1
        assert stats["valid_entries"] == 1
        assert stats["expired_entries"] == 0
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    @patch('src.agent_orchestrated_etl.config.time.time')
    def test_get_aws_secret_cache_expiry(self, mock_time, mock_boto_client):
        """Test that cache expires after TTL."""
        # Mock AWS client response
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'cached-value'
        }
        
        # Mock time progression
        mock_time.side_effect = [1000, 1000, 1400]  # Cache, check, expire check
        
        # First call - should hit AWS and cache
        result1 = self.secret_manager.get_secret("test_key")
        assert result1 == 'cached-value'
        assert mock_client.get_secret_value.call_count == 1
        
        # Second call after cache expiry - should hit AWS again
        result2 = self.secret_manager.get_secret("test_key")
        assert result2 == 'cached-value'
        assert mock_client.get_secret_value.call_count == 2
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_not_found(self, mock_boto_client):
        """Test handling when secret is not found in AWS."""
        from botocore.exceptions import ClientError
        
        # Mock AWS client to raise ResourceNotFoundException
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        error_response = {'Error': {'Code': 'ResourceNotFoundException'}}
        mock_client.get_secret_value.side_effect = ClientError(error_response, 'GetSecretValue')
        
        # Test with default value
        result = self.secret_manager.get_secret("nonexistent", default="default-value")
        assert result == "default-value"
        
        # Test without default value
        result = self.secret_manager.get_secret("nonexistent")
        assert result is None
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_authentication_error(self, mock_boto_client):
        """Test handling of AWS authentication errors."""
        from botocore.exceptions import NoCredentialsError
        
        # Mock AWS client to raise authentication error
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.side_effect = NoCredentialsError()
        
        # Test that ValidationError is raised with helpful message
        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")
        
        assert "AWS credentials not configured properly" in str(exc_info.value)
        assert "environment variables" in str(exc_info.value)
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_other_client_errors(self, mock_boto_client):
        """Test handling of various AWS client errors."""
        from botocore.exceptions import ClientError
        
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        
        # Test InvalidRequestException
        error_response = {'Error': {'Code': 'InvalidRequestException'}}
        mock_client.get_secret_value.side_effect = ClientError(error_response, 'GetSecretValue')
        
        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")
        assert "Invalid secret name" in str(exc_info.value)
        
        # Test DecryptionFailure
        error_response = {'Error': {'Code': 'DecryptionFailure'}}
        mock_client.get_secret_value.side_effect = ClientError(error_response, 'GetSecretValue')
        
        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("test_key")
        assert "Failed to decrypt secret" in str(exc_info.value)
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_get_aws_secret_binary_not_supported(self, mock_boto_client):
        """Test that binary secrets are not supported."""
        # Mock AWS client response with binary data
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretBinary': b'binary-data'
        }
        
        # Test that ValidationError is raised
        with pytest.raises(ValidationError) as exc_info:
            self.secret_manager.get_secret("binary_secret")
        
        assert "Binary secret" in str(exc_info.value)
        assert "not supported" in str(exc_info.value)
    
    def test_get_aws_secret_boto3_not_available(self):
        """Test error when boto3 is not available."""
        # Create secret manager and patch BOTO3_AVAILABLE to False
        with patch('src.agent_orchestrated_etl.config.BOTO3_AVAILABLE', False):
            secret_manager = SecretManager(
                provider="aws_secrets",
                security_config=self.security_config
            )
            
            with pytest.raises(ValidationError) as exc_info:
                secret_manager.get_secret("test_key")
            
            assert "requires boto3" in str(exc_info.value)
            assert "pip install boto3" in str(exc_info.value)
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    def test_clear_cache(self):
        """Test cache clearing functionality."""
        # Add some mock cache entries
        self.secret_manager._aws_secrets_cache = {
            'test-app/key1': ('value1', time.time()),
            'test-app/key2': ('value2', time.time()),
        }
        
        # Verify cache has entries
        assert len(self.secret_manager._aws_secrets_cache) == 2
        
        # Clear cache
        self.secret_manager.clear_cache()
        
        # Verify cache is empty
        assert len(self.secret_manager._aws_secrets_cache) == 0
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    def test_get_cache_stats(self):
        """Test cache statistics functionality."""
        current_time = time.time()
        
        # Add mock cache entries with different ages
        self.secret_manager._aws_secrets_cache = {
            'test-app/key1': ('value1', current_time),  # Valid
            'test-app/key2': ('value2', current_time - 400),  # Expired (TTL is 300)
            'test-app/key3': ('value3', current_time - 100),  # Valid
        }
        
        # Get cache statistics
        stats = self.secret_manager.get_cache_stats()
        
        # Verify statistics
        assert stats["total_entries"] == 3
        assert stats["valid_entries"] == 2
        assert stats["expired_entries"] == 1
    
    @pytest.mark.skipif(not BOTO3_AVAILABLE, reason="boto3 not available")
    @patch('src.agent_orchestrated_etl.config.boto3.client')
    def test_aws_client_reuse(self, mock_boto_client):
        """Test that AWS client is reused across calls."""
        # Mock AWS client response
        mock_client = Mock()
        mock_boto_client.return_value = mock_client
        mock_client.get_secret_value.return_value = {
            'SecretString': 'test-value'
        }
        
        # Make multiple calls
        self.secret_manager.get_secret("key1")
        self.secret_manager.clear_cache()  # Clear cache to force new AWS calls
        self.secret_manager.get_secret("key2")
        
        # Verify boto3.client was called only once (client reused)
        assert mock_boto_client.call_count == 1
        
        # But get_secret_value was called twice
        assert mock_client.get_secret_value.call_count == 2


class TestSecretManagerProviderSelection:
    """Test provider selection in SecretManager."""
    
    def test_unsupported_provider(self):
        """Test error for unsupported secret provider."""
        secret_manager = SecretManager(provider="unsupported_provider")
        
        with pytest.raises(ValidationError) as exc_info:
            secret_manager.get_secret("test_key")
        
        assert "Unsupported secret provider" in str(exc_info.value)
        assert "unsupported_provider" in str(exc_info.value)
    
    def test_env_provider_fallback_to_default(self):
        """Test environment provider with default values."""
        secret_manager = SecretManager(provider="env", prefix="NONEXISTENT_")
        
        # Test with default value
        result = secret_manager.get_secret("key", default="default-value")
        assert result == "default-value"
        
        # Test without default value
        result = secret_manager.get_secret("key")
        assert result is None


class TestSecretManagerConfiguration:
    """Test SecretManager configuration handling."""
    
    def test_default_security_config(self):
        """Test SecretManager with default security config."""
        secret_manager = SecretManager()
        
        assert secret_manager.security_config.aws_secrets_region == "us-east-1"
        assert secret_manager.security_config.aws_secrets_prefix == "agent-etl/"
        assert secret_manager.security_config.aws_secrets_cache_ttl == 300
    
    def test_custom_security_config(self):
        """Test SecretManager with custom security config."""
        custom_config = SecurityConfig(
            aws_secrets_region="eu-west-1",
            aws_secrets_prefix="custom/",
            aws_secrets_cache_ttl=600
        )
        
        secret_manager = SecretManager(security_config=custom_config)
        
        assert secret_manager.security_config.aws_secrets_region == "eu-west-1"
        assert secret_manager.security_config.aws_secrets_prefix == "custom/"
        assert secret_manager.security_config.aws_secrets_cache_ttl == 600