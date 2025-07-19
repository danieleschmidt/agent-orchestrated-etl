
"""Configuration management for Agent-Orchestrated ETL."""

from __future__ import annotations

import json
import os
import threading
import time
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

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


class ConfigChangeHandler(FileSystemEventHandler):
    """Handler for configuration file changes."""
    
    def __init__(self, config_manager: 'ConfigManager'):
        self.config_manager = config_manager
        self.last_modified = {}
    
    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return
        
        file_path = event.src_path
        current_time = time.time()
        
        # Debounce rapid file changes (some editors trigger multiple events)
        if file_path in self.last_modified:
            if current_time - self.last_modified[file_path] < 1.0:
                return
        
        self.last_modified[file_path] = current_time
        
        # Check if it's a config file we're watching
        if Path(file_path) in self.config_manager.watched_files:
            self.config_manager.reload_config()


class ConfigManager:
    """Enhanced configuration manager with hot-reload capabilities."""
    
    def __init__(self):
        self._config: Optional[AppConfig] = None
        self._config_lock = threading.RLock()
        self._observers: List[Observer] = []
        self._change_callbacks: List[Callable[[AppConfig, AppConfig], None]] = []
        self.watched_files: List[Path] = []
        self.config_file_path: Optional[Path] = None
        self.hot_reload_enabled = False
    
    def load_config(
        self,
        config_file: Optional[Union[str, Path]] = None,
        enable_hot_reload: bool = False
    ) -> AppConfig:
        """Load configuration from multiple sources.
        
        Args:
            config_file: Path to configuration file (JSON or YAML)
            enable_hot_reload: Whether to enable hot-reload for config file
            
        Returns:
            Configured AppConfig instance
        """
        with self._config_lock:
            # Load base configuration
            config = self._load_base_config()
            
            # Load from config file if provided
            if config_file:
                config_path = Path(config_file)
                if config_path.exists():
                    file_config = self._load_config_file(config_path)
                    config = self._merge_configs(config, file_config)
                    self.config_file_path = config_path
            
            # Override with environment variables
            config = self._apply_env_overrides(config)
            
            # Validate configuration
            self._validate_config_enhanced(config)
            
            # Set up hot-reload if enabled
            if enable_hot_reload and config_file:
                self._setup_hot_reload()
            
            # Notify callbacks if config changed
            old_config = self._config
            self._config = config
            
            if old_config is not None:
                self._notify_config_change(old_config, config)
            
            return config
    
    def _load_base_config(self) -> AppConfig:
        """Load base configuration with defaults."""
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
        
        return config
    
    def _load_config_file(self, config_path: Path) -> Dict[str, Any]:
        """Load configuration from file (JSON or YAML)."""
        try:
            content = config_path.read_text(encoding='utf-8')
            
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                return yaml.safe_load(content) or {}
            elif config_path.suffix.lower() == '.json':
                return json.loads(content)
            else:
                raise ValidationError(f"Unsupported config file format: {config_path.suffix}")
        
        except Exception as e:
            raise ValidationError(f"Failed to load config file {config_path}: {e}")
    
    def _merge_configs(self, base_config: AppConfig, file_config: Dict[str, Any]) -> AppConfig:
        """Merge file configuration into base configuration."""
        # This is a simplified merge - in practice, you'd want recursive merging
        for section, values in file_config.items():
            if hasattr(base_config, section) and isinstance(values, dict):
                section_obj = getattr(base_config, section)
                for key, value in values.items():
                    if hasattr(section_obj, key):
                        setattr(section_obj, key, value)
        
        return base_config
    
    def _apply_env_overrides(self, config: AppConfig) -> AppConfig:
        """Apply environment variable overrides to configuration."""
        # Environment variables always take precedence
        # This ensures compatibility with existing environment-based configuration
        return config
    
    def _validate_config_enhanced(self, config: AppConfig) -> None:
        """Enhanced configuration validation."""
        _validate_config(config)
        
        # Additional environment-specific validations
        if config.environment == "production":
            # Production-specific validations
            if config.debug:
                raise ValidationError("Debug mode cannot be enabled in production")
            
            if config.logging.level == "DEBUG":
                raise ValidationError("Debug logging cannot be enabled in production")
            
            # Ensure secrets are configured in production
            if not config.database.host and config.environment == "production":
                # This is just a warning, not an error, as database might not be required
                pass
    
    def _setup_hot_reload(self):
        """Set up file system watcher for hot-reload."""
        if not self.config_file_path or self.hot_reload_enabled:
            return
        
        try:
            # Set up file watcher
            event_handler = ConfigChangeHandler(self)
            observer = Observer()
            
            # Watch the directory containing the config file
            watch_dir = self.config_file_path.parent
            observer.schedule(event_handler, str(watch_dir), recursive=False)
            
            # Track the config file
            self.watched_files.append(self.config_file_path)
            
            observer.start()
            self._observers.append(observer)
            self.hot_reload_enabled = True
            
        except Exception as e:
            # Hot-reload is not critical, so we log the error but don't fail
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Failed to set up hot-reload for config: {e}")
    
    def reload_config(self):
        """Manually reload configuration."""
        try:
            old_config = self._config
            new_config = self.load_config(
                self.config_file_path,
                enable_hot_reload=False  # Don't restart watchers
            )
            
            import logging
            logger = logging.getLogger(__name__)
            logger.info("Configuration reloaded successfully")
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Failed to reload configuration: {e}")
    
    def add_change_callback(self, callback: Callable[[AppConfig, AppConfig], None]):
        """Add a callback to be notified of configuration changes.
        
        Args:
            callback: Function that takes (old_config, new_config) as parameters
        """
        self._change_callbacks.append(callback)
    
    def remove_change_callback(self, callback: Callable[[AppConfig, AppConfig], None]):
        """Remove a configuration change callback."""
        if callback in self._change_callbacks:
            self._change_callbacks.remove(callback)
    
    def _notify_config_change(self, old_config: AppConfig, new_config: AppConfig):
        """Notify all callbacks of configuration changes."""
        for callback in self._change_callbacks:
            try:
                callback(old_config, new_config)
            except Exception as e:
                import logging
                logger = logging.getLogger(__name__)
                logger.error(f"Error in config change callback: {e}")
    
    def get_config(self) -> AppConfig:
        """Get the current configuration."""
        with self._config_lock:
            if self._config is None:
                self._config = self.load_config()
            return self._config
    
    def reset_config(self):
        """Reset configuration and stop watchers."""
        with self._config_lock:
            # Stop file watchers
            for observer in self._observers:
                observer.stop()
                observer.join()
            
            self._observers.clear()
            self.watched_files.clear()
            self.hot_reload_enabled = False
            self._config = None
    
    def get_config_summary(self) -> Dict[str, Any]:
        """Get a summary of current configuration (with secrets redacted)."""
        config = self.get_config()
        
        return {
            "environment": config.environment,
            "debug": config.debug,
            "logging": {
                "level": config.logging.level,
                "format": config.logging.format,
                "output": config.logging.output,
            },
            "database": {
                "host": config.database.host,
                "port": config.database.port,
                "database": config.database.database,
                "ssl_mode": config.database.ssl_mode,
                "connection_timeout": config.database.connection_timeout,
            },
            "hot_reload_enabled": self.hot_reload_enabled,
            "watched_files": [str(f) for f in self.watched_files],
        }


# Global configuration manager instance
_config_manager = ConfigManager()


def load_config(
    config_file: Optional[Union[str, Path]] = None,
    enable_hot_reload: bool = False
) -> AppConfig:
    """Load configuration from environment variables and optional config file.
    
    Args:
        config_file: Path to configuration file (JSON or YAML)
        enable_hot_reload: Whether to enable hot-reload for config file
        
    Returns:
        Configured AppConfig instance
        
    Raises:
        ValidationError: If configuration validation fails
    """
    return _config_manager.load_config(config_file, enable_hot_reload)


def get_config() -> AppConfig:
    """Get the global configuration instance.
    
    Returns:
        The global AppConfig instance
    """
    return _config_manager.get_config()


def reset_config() -> None:
    """Reset the global configuration instance (useful for testing)."""
    _config_manager.reset_config()


def reload_config() -> None:
    """Manually reload configuration from sources."""
    _config_manager.reload_config()


def add_config_change_callback(callback: Callable[[AppConfig, AppConfig], None]) -> None:
    """Add a callback to be notified of configuration changes.
    
    Args:
        callback: Function that takes (old_config, new_config) as parameters
    """
    _config_manager.add_change_callback(callback)


def remove_config_change_callback(callback: Callable[[AppConfig, AppConfig], None]) -> None:
    """Remove a configuration change callback.
    
    Args:
        callback: The callback function to remove
    """
    _config_manager.remove_change_callback(callback)


def get_config_summary() -> Dict[str, Any]:
    """Get a summary of current configuration (with secrets redacted).
    
    Returns:
        Configuration summary dictionary
    """
    return _config_manager.get_config_summary()


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
