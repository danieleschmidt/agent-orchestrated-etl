
"""Configuration management for Agent-Orchestrated ETL."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

from .validation import validate_environment_variable, ValidationError


@dataclass
class SecurityConfig:
    """Security-related configuration settings."""
    
    # Secret management
    secret_provider: str = "env"  # env, aws_secrets, vault, etc.
    secret_prefix: str = "AGENT_ETL_"
    
    # Input validation
    max_dag_id_length: int = 200
    max_task_name_length: int = 250
    max_file_path_length: int = 4096
    
    # Rate limiting
    max_pipeline_executions_per_minute: int = 60
    max_dag_generation_per_minute: int = 120


@dataclass
class DatabaseConfig:
    """Database connection configuration."""
    
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    ssl_mode: str = "require"
    connection_timeout: int = 30
    
    def get_connection_string(self) -> str:
        """Get database connection string with password redacted for logging."""
        if not all([self.host, self.database, self.username]):
            raise ValidationError("Missing required database connection parameters")
        
        return f"postgresql://{self.username}:***@{self.host}:{self.port or 5432}/{self.database}"


@dataclass
class S3Config:
    """S3 configuration settings."""
    
    region: str = "us-east-1"
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    endpoint_url: Optional[str] = None  # For S3-compatible services
    use_ssl: bool = True


@dataclass
class APIConfig:
    """API configuration settings."""
    
    base_url: Optional[str] = None
    api_key: Optional[str] = None
    timeout: int = 30
    max_retries: int = 3
    user_agent: str = "agent-orchestrated-etl/1.0"


@dataclass
class LoggingConfig:
    """Logging configuration."""
    
    level: str = "INFO"
    format: str = "json"  # json or text
    output: str = "console"  # console, file, or both
    file_path: Optional[str] = None
    max_file_size_mb: int = 100
    backup_count: int = 5


@dataclass
class AppConfig:
    """Main application configuration."""
    
    # Core settings
    environment: str = "development"
    debug: bool = False
    
    # Component configurations
    security: SecurityConfig = field(default_factory=SecurityConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    s3: S3Config = field(default_factory=S3Config)
    api: APIConfig = field(default_factory=APIConfig)
    logging: LoggingConfig = field(default_factory=LoggingConfig)
    
    # Paths
    temp_dir: str = "/tmp/agent_etl"
    data_dir: str = "./data"
    output_dir: str = "./output"


class SecretManager:
    """Manages secret retrieval from various sources."""
    
    def __init__(self, provider: str = "env", prefix: str = "AGENT_ETL_"):
        self.provider = provider
        self.prefix = prefix
    
    def get_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Retrieve a secret value.
        
        Args:
            key: The secret key (without prefix)
            default: Default value if secret is not found
            
        Returns:
            The secret value or default
            
        Raises:
            ValidationError: If secret validation fails
        """
        if self.provider == "env":
            return self._get_env_secret(key, default)
        elif self.provider == "aws_secrets":
            return self._get_aws_secret(key, default)
        elif self.provider == "vault":
            return self._get_vault_secret(key, default)
        else:
            raise ValidationError(f"Unsupported secret provider: {self.provider}")
    
    def _get_env_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from environment variable."""
        env_key = f"{self.prefix}{key.upper()}"
        try:
            return validate_environment_variable(env_key, os.getenv(env_key, default))
        except ValidationError:
            if default is not None:
                return default
            return None
    
    def _get_aws_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from AWS Secrets Manager."""
        # Placeholder for AWS Secrets Manager integration
        # In a real implementation, this would use boto3
        raise NotImplementedError("AWS Secrets Manager integration not implemented yet")
    
    def _get_vault_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from HashiCorp Vault."""
        # Placeholder for Vault integration
        # In a real implementation, this would use hvac
        raise NotImplementedError("HashiCorp Vault integration not implemented yet")


def load_config() -> AppConfig:
    """Load configuration from environment variables and defaults.
    
    Returns:
        Configured AppConfig instance
        
    Raises:
        ValidationError: If configuration validation fails
    """
    config = AppConfig()
    secret_manager = SecretManager()
    
    # Load environment
    config.environment = os.getenv("AGENT_ETL_ENVIRONMENT", "development")
    config.debug = os.getenv("AGENT_ETL_DEBUG", "false").lower() == "true"
    
    # Load security config
    config.security.secret_provider = os.getenv("AGENT_ETL_SECRET_PROVIDER", "env")
    config.security.secret_prefix = os.getenv("AGENT_ETL_SECRET_PREFIX", "AGENT_ETL_")
    
    # Load database config
    config.database.host = secret_manager.get_secret("DB_HOST")
    config.database.port = int(os.getenv("AGENT_ETL_DB_PORT", "5432"))
    config.database.database = secret_manager.get_secret("DB_NAME")
    config.database.username = secret_manager.get_secret("DB_USER")
    config.database.password = secret_manager.get_secret("DB_PASSWORD")
    config.database.ssl_mode = os.getenv("AGENT_ETL_DB_SSL_MODE", "require")
    
    # Load S3 config
    config.s3.region = os.getenv("AGENT_ETL_S3_REGION", "us-east-1")
    config.s3.access_key_id = secret_manager.get_secret("S3_ACCESS_KEY_ID")
    config.s3.secret_access_key = secret_manager.get_secret("S3_SECRET_ACCESS_KEY")
    config.s3.session_token = secret_manager.get_secret("S3_SESSION_TOKEN")
    config.s3.endpoint_url = os.getenv("AGENT_ETL_S3_ENDPOINT_URL")
    
    # Load API config
    config.api.base_url = os.getenv("AGENT_ETL_API_BASE_URL")
    config.api.api_key = secret_manager.get_secret("API_KEY")
    config.api.timeout = int(os.getenv("AGENT_ETL_API_TIMEOUT", "30"))
    
    # Load logging config
    config.logging.level = os.getenv("AGENT_ETL_LOG_LEVEL", "INFO")
    config.logging.format = os.getenv("AGENT_ETL_LOG_FORMAT", "json")
    config.logging.output = os.getenv("AGENT_ETL_LOG_OUTPUT", "console")
    config.logging.file_path = os.getenv("AGENT_ETL_LOG_FILE")
    
    # Load paths
    config.temp_dir = os.getenv("AGENT_ETL_TEMP_DIR", "/tmp/agent_etl")
    config.data_dir = os.getenv("AGENT_ETL_DATA_DIR", "./data")
    config.output_dir = os.getenv("AGENT_ETL_OUTPUT_DIR", "./output")
    
    # Validate configuration
    _validate_config(config)
    
    return config


def _validate_config(config: AppConfig) -> None:
    """Validate the loaded configuration.
    
    Args:
        config: The configuration to validate
        
    Raises:
        ValidationError: If configuration is invalid
    """
    # Validate environment
    valid_environments = {"development", "staging", "production"}
    if config.environment not in valid_environments:
        raise ValidationError(f"Invalid environment: {config.environment}")
    
    # Validate logging level
    valid_log_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
    if config.logging.level.upper() not in valid_log_levels:
        raise ValidationError(f"Invalid log level: {config.logging.level}")
    
    # Validate logging format
    valid_log_formats = {"json", "text"}
    if config.logging.format not in valid_log_formats:
        raise ValidationError(f"Invalid log format: {config.logging.format}")
    
    # Validate paths
    for path_name, path_value in [
        ("temp_dir", config.temp_dir),
        ("data_dir", config.data_dir),
        ("output_dir", config.output_dir),
    ]:
        if not path_value:
            raise ValidationError(f"{path_name} cannot be empty")
        
        # Create directories if they don't exist
        try:
            Path(path_value).mkdir(parents=True, exist_ok=True)
        except (OSError, PermissionError) as e:
            raise ValidationError(f"Cannot create {path_name} '{path_value}': {e}")


# Global configuration instance
_config: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get the global configuration instance.
    
    Returns:
        The global AppConfig instance
    """
    global _config
    if _config is None:
        _config = load_config()
    return _config


def reset_config() -> None:
    """Reset the global configuration instance (useful for testing)."""
    global _config
    _config = None


# Legacy support
@dataclass
class Settings:
    """Container for environment driven settings (legacy)."""

    LOG_LEVEL: str = "INFO"

    @classmethod
    def from_env(cls) -> "Settings":
        """Create a :class:`Settings` instance from environment variables."""
        config = get_config()
        return cls(LOG_LEVEL=config.logging.level)


SETTINGS = Settings.from_env()
LOG_LEVEL: str = SETTINGS.LOG_LEVEL
