
"""Configuration management for Agent-Orchestrated ETL."""

from __future__ import annotations

import json
import os
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

try:
    import yaml
except ImportError:
    yaml = None

try:
    from watchdog.events import FileSystemEventHandler
    from watchdog.observers import Observer
except ImportError:
    FileSystemEventHandler = None
    Observer = None

from .validation import ValidationError, validate_environment_variable

try:
    import boto3
    from botocore.exceptions import (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
    )
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

try:
    import hvac
    from requests.exceptions import ConnectionError, RequestException, Timeout
    HVAC_AVAILABLE = True
except ImportError:
    HVAC_AVAILABLE = False


@dataclass
class SecurityConfig:
    """Security-related configuration settings."""

    # Secret management
    secret_provider: str = "env"  # env, aws_secrets, vault, etc.
    secret_prefix: str = "AGENT_ETL_"

    # AWS Secrets Manager settings
    aws_secrets_region: str = "us-east-1"
    aws_secrets_prefix: str = "agent-etl/"
    aws_secrets_cache_ttl: int = 300  # 5 minutes

    # HashiCorp Vault settings
    vault_url: str = "http://localhost:8200"
    vault_auth_method: str = "token"  # token, approle, kubernetes, etc.
    vault_mount_path: str = "secret"
    vault_secret_path: str = "agent-etl/"
    vault_cache_ttl: int = 300  # 5 minutes
    vault_timeout: int = 30  # Request timeout in seconds

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

    def __init__(self, provider: str = "env", prefix: str = "AGENT_ETL_", security_config: Optional[SecurityConfig] = None):
        self.provider = provider
        self.prefix = prefix
        self.security_config = security_config or SecurityConfig()

        # AWS Secrets Manager caching
        self._aws_secrets_cache: Dict[str, tuple[str, float]] = {}
        self._aws_client: Optional[Any] = None

        # HashiCorp Vault caching and client
        self._vault_secrets_cache: Dict[str, tuple[str, float]] = {}
        self._vault_client: Optional[Any] = None

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
        """Get secret from AWS Secrets Manager.
        
        Args:
            key: The secret key (without prefix)
            default: Default value if secret is not found
            
        Returns:
            The secret value or default
            
        Raises:
            ValidationError: If AWS Secrets Manager is not available or authentication fails
        """
        if not BOTO3_AVAILABLE:
            raise ValidationError(
                "AWS Secrets Manager integration requires boto3. "
                "Install with: pip install boto3"
            )

        # Construct the secret name with prefix
        secret_name = f"{self.security_config.aws_secrets_prefix}{key.lower()}"

        # Check cache first
        if secret_name in self._aws_secrets_cache:
            cached_value, cache_time = self._aws_secrets_cache[secret_name]
            if time.time() - cache_time < self.security_config.aws_secrets_cache_ttl:
                return cached_value
            else:
                # Cache expired, remove entry
                del self._aws_secrets_cache[secret_name]

        try:
            # Initialize AWS client if needed
            if self._aws_client is None:
                self._aws_client = boto3.client(
                    'secretsmanager',
                    region_name=self.security_config.aws_secrets_region
                )

            # Retrieve the secret
            response = self._aws_client.get_secret_value(SecretName=secret_name)

            # Extract the secret value
            if 'SecretString' in response:
                secret_value = response['SecretString']

                # Try to parse as JSON in case it's a key-value secret
                try:
                    secret_dict = json.loads(secret_value)
                    if isinstance(secret_dict, dict) and key in secret_dict:
                        secret_value = secret_dict[key]
                    elif isinstance(secret_dict, dict) and len(secret_dict) == 1:
                        # Single key-value pair, use the value
                        secret_value = next(iter(secret_dict.values()))
                except json.JSONDecodeError:
                    # Not JSON, use the raw string value
                    pass

                # Cache the result
                self._aws_secrets_cache[secret_name] = (secret_value, time.time())

                return secret_value
            else:
                # Binary secret (not typical for configuration)
                raise ValidationError(f"Binary secret '{secret_name}' is not supported")

        except ClientError as e:
            error_code = e.response['Error']['Code']

            if error_code == 'ResourceNotFoundException':
                # Secret not found, return default
                if default is not None:
                    return default
                return None
            elif error_code == 'InvalidRequestException':
                raise ValidationError(f"Invalid secret name '{secret_name}': {e}")
            elif error_code == 'InvalidParameterException':
                raise ValidationError(f"Invalid parameter for secret '{secret_name}': {e}")
            elif error_code == 'DecryptionFailure':
                raise ValidationError(f"Failed to decrypt secret '{secret_name}': {e}")
            elif error_code == 'InternalServiceError':
                raise ValidationError(f"AWS Secrets Manager internal error for '{secret_name}': {e}")
            else:
                raise ValidationError(f"AWS Secrets Manager error for '{secret_name}': {e}")

        except (NoCredentialsError, PartialCredentialsError) as e:
            raise ValidationError(
                f"AWS credentials not configured properly for Secrets Manager: {e}. "
                "Ensure AWS credentials are set via environment variables, "
                "IAM role, or AWS credentials file."
            )
        except Exception as e:
            raise ValidationError(f"Unexpected error retrieving secret '{secret_name}': {e}")

    def _get_vault_secret(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """Get secret from HashiCorp Vault.
        
        Args:
            key: The secret key (without prefix)
            default: Default value if secret is not found
            
        Returns:
            The secret value or default
            
        Raises:
            ValidationError: If Vault is not available or authentication fails
        """
        if not HVAC_AVAILABLE:
            raise ValidationError(
                "HashiCorp Vault integration requires hvac library. "
                "Install with: pip install hvac"
            )

        # Construct full secret path
        secret_path = f"{self.security_config.vault_secret_path}{key}"
        cache_key = f"{self.security_config.vault_mount_path}/{secret_path}"

        # Check cache first
        if cache_key in self._vault_secrets_cache:
            cached_value, cache_time = self._vault_secrets_cache[cache_key]
            if time.time() - cache_time < self.security_config.vault_cache_ttl:
                return cached_value
            else:
                # Cache expired, remove entry
                del self._vault_secrets_cache[cache_key]

        try:
            # Get or create Vault client
            vault_client = self._get_vault_client()

            # Read secret from Vault
            try:
                # For KV v2 secrets engine
                if vault_client.is_secret_backend_mounted(self.security_config.vault_mount_path):
                    response = vault_client.secrets.kv.v2.read_secret_version(
                        path=secret_path,
                        mount_point=self.security_config.vault_mount_path
                    )
                    secret_data = response['data']['data']
                else:
                    # Fallback to KV v1 or generic read
                    response = vault_client.read(f"{self.security_config.vault_mount_path}/{secret_path}")
                    secret_data = response['data'] if response else None

                if not secret_data:
                    return default

                # Extract secret value
                secret_value = None
                if key in secret_data:
                    # Direct key match
                    secret_value = str(secret_data[key])
                elif len(secret_data) == 1:
                    # Single key-value pair, return the value
                    secret_value = str(next(iter(secret_data.values())))
                else:
                    # Multiple keys, try common patterns
                    for common_key in ['value', 'password', 'secret', 'token']:
                        if common_key in secret_data:
                            secret_value = str(secret_data[common_key])
                            break

                if secret_value is None:
                    return default

                # Cache the result
                self._vault_secrets_cache[cache_key] = (secret_value, time.time())
                return secret_value

            except hvac.exceptions.InvalidPath:
                # Secret path not found
                return default
            except hvac.exceptions.Forbidden as e:
                raise ValidationError(f"Access denied to Vault secret '{secret_path}': {e}")
            except hvac.exceptions.Unauthorized as e:
                raise ValidationError(f"Unauthorized access to Vault: {e}. Check authentication credentials.")

        except (RequestException, Timeout, ConnectionError) as e:
            raise ValidationError(f"Network error connecting to Vault: {e}")
        except hvac.exceptions.VaultError as e:
            raise ValidationError(f"Vault error retrieving secret '{secret_path}': {e}")
        except Exception as e:
            raise ValidationError(f"Unexpected error retrieving Vault secret '{secret_path}': {e}")

    def _get_vault_client(self):
        """Get or create Vault client with proper authentication."""
        if self._vault_client is None:
            if not HVAC_AVAILABLE:
                raise ValidationError("hvac library not available")

            # Create Vault client
            self._vault_client = hvac.Client(
                url=self.security_config.vault_url,
                timeout=self.security_config.vault_timeout
            )

            # Authenticate based on configured method
            auth_method = self.security_config.vault_auth_method.lower()

            if auth_method == "token":
                # Token-based authentication
                token = os.getenv("VAULT_TOKEN")
                if not token:
                    raise ValidationError(
                        "VAULT_TOKEN environment variable required for token authentication"
                    )
                self._vault_client.token = token

            elif auth_method == "approle":
                # AppRole authentication
                role_id = os.getenv("VAULT_ROLE_ID")
                secret_id = os.getenv("VAULT_SECRET_ID")
                if not role_id or not secret_id:
                    raise ValidationError(
                        "VAULT_ROLE_ID and VAULT_SECRET_ID environment variables required for AppRole authentication"
                    )

                auth_response = self._vault_client.auth.approle.login(
                    role_id=role_id,
                    secret_id=secret_id
                )
                self._vault_client.token = auth_response['auth']['client_token']

            elif auth_method == "kubernetes":
                # Kubernetes authentication
                jwt_path = os.getenv("VAULT_K8S_JWT_PATH", "/var/run/secrets/kubernetes.io/serviceaccount/token")
                role = os.getenv("VAULT_K8S_ROLE")

                if not role:
                    raise ValidationError("VAULT_K8S_ROLE environment variable required for Kubernetes authentication")

                try:
                    with open(jwt_path) as f:
                        jwt = f.read().strip()
                except FileNotFoundError:
                    raise ValidationError(f"Kubernetes JWT not found at {jwt_path}")

                auth_response = self._vault_client.auth.kubernetes.login(
                    role=role,
                    jwt=jwt
                )
                self._vault_client.token = auth_response['auth']['client_token']

            else:
                raise ValidationError(f"Unsupported Vault authentication method: {auth_method}")

            # Verify authentication worked
            if not self._vault_client.is_authenticated():
                raise ValidationError("Failed to authenticate with Vault")

        return self._vault_client

    def clear_cache(self) -> None:
        """Clear the secrets cache (useful for testing or forced refresh)."""
        self._aws_secrets_cache.clear()
        self._vault_secrets_cache.clear()

    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring."""
        current_time = time.time()

        # AWS Secrets Manager cache stats
        aws_stats = {
            "total_entries": len(self._aws_secrets_cache),
            "expired_entries": 0,
            "valid_entries": 0,
        }

        for _, (_, cache_time) in self._aws_secrets_cache.items():
            if current_time - cache_time >= self.security_config.aws_secrets_cache_ttl:
                aws_stats["expired_entries"] += 1
            else:
                aws_stats["valid_entries"] += 1

        # Vault cache stats
        vault_stats = {
            "total_entries": len(self._vault_secrets_cache),
            "expired_entries": 0,
            "valid_entries": 0,
        }

        for _, (_, cache_time) in self._vault_secrets_cache.items():
            if current_time - cache_time >= self.security_config.vault_cache_ttl:
                vault_stats["expired_entries"] += 1
            else:
                vault_stats["valid_entries"] += 1

        # Combined stats
        total_entries = aws_stats["total_entries"] + vault_stats["total_entries"]
        total_expired = aws_stats["expired_entries"] + vault_stats["expired_entries"]
        total_valid = aws_stats["valid_entries"] + vault_stats["valid_entries"]

        return {
            "total_entries": total_entries,
            "expired_entries": total_expired,
            "valid_entries": total_valid,
            "aws_secrets": aws_stats,
            "vault_secrets": vault_stats
        }


def load_default_config() -> AppConfig:
    """Load configuration from environment variables and defaults.
    
    Returns:
        Configured AppConfig instance
        
    Raises:
        ValidationError: If configuration validation fails
    """
    config = AppConfig()

    # Initialize secret manager with security config
    config.security.secret_provider = os.getenv("AGENT_ETL_SECRET_PROVIDER", "env")
    config.security.secret_prefix = os.getenv("AGENT_ETL_SECRET_PREFIX", "AGENT_ETL_")
    config.security.aws_secrets_region = os.getenv("AGENT_ETL_AWS_SECRETS_REGION", "us-east-1")
    config.security.aws_secrets_prefix = os.getenv("AGENT_ETL_AWS_SECRETS_PREFIX", "agent-etl/")

    secret_manager = SecretManager(
        provider=config.security.secret_provider,
        prefix=config.security.secret_prefix,
        security_config=config.security
    )

    # Load environment
    config.environment = os.getenv("AGENT_ETL_ENVIRONMENT", "development")
    config.debug = os.getenv("AGENT_ETL_DEBUG", "false").lower() == "true"

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


class ConfigChangeHandler(FileSystemEventHandler if FileSystemEventHandler else object):
    """Handler for configuration file changes."""

    def __init__(self, config_manager: ConfigManager):
        self.config_manager = config_manager
        self.last_modified: Dict[str, float] = {}

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

        # Initialize secret manager with security config
        config.security.secret_provider = os.getenv("AGENT_ETL_SECRET_PROVIDER", "env")
        config.security.secret_prefix = os.getenv("AGENT_ETL_SECRET_PREFIX", "AGENT_ETL_")
        config.security.aws_secrets_region = os.getenv("AGENT_ETL_AWS_SECRETS_REGION", "us-east-1")
        config.security.aws_secrets_prefix = os.getenv("AGENT_ETL_AWS_SECRETS_PREFIX", "agent-etl/")

        secret_manager = SecretManager(
            provider=config.security.secret_provider,
            prefix=config.security.secret_prefix,
            security_config=config.security
        )

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

        # Load logging config - support both prefixed and simple env vars for compatibility
        config.logging.level = os.getenv("AGENT_ETL_LOG_LEVEL") or os.getenv("LOG_LEVEL") or "INFO"
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
                if yaml is None:
                    raise ValidationError("PyYAML is required for YAML config files")
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
            self.load_config(
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
    def from_env(cls) -> Settings:
        """Create a :class:`Settings` instance from environment variables."""
        config = get_config()
        return cls(LOG_LEVEL=config.logging.level)


SETTINGS = Settings.from_env()
LOG_LEVEL: str = SETTINGS.LOG_LEVEL
