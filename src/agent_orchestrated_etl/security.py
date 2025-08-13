"""Security module for agent-orchestrated ETL system."""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .exceptions import SecurityException
from .logging_config import get_logger


@dataclass
class SecurityConfig:
    """Configuration for security features."""
    enable_encryption: bool = True
    enable_audit_log: bool = True
    max_request_size: int = 10_000_000  # 10MB
    rate_limit_per_minute: int = 100
    require_authentication: bool = True
    allowed_origins: List[str] = None

    def __post_init__(self):
        if self.allowed_origins is None:
            self.allowed_origins = ["localhost", "127.0.0.1"]


class SecurityValidator:
    """Validates security requirements for ETL operations."""

    def __init__(self, config: Optional[SecurityConfig] = None):
        self.config = config or SecurityConfig()
        self.logger = get_logger("security_validator")
        self.request_counts: Dict[str, List[float]] = {}
        self.api_keys: Dict[str, Dict[str, Any]] = {}

    def validate_input(self, data: Any, context: str = "general") -> bool:
        """Validate input data for security issues."""
        try:
            # Check for SQL injection patterns
            if isinstance(data, str):
                dangerous_patterns = [
                    "'; DROP TABLE", "'; DELETE FROM", "'; INSERT INTO",
                    "'; UPDATE ", "UNION SELECT", "OR 1=1", "OR '1'='1'",
                    "<script>", "</script>", "javascript:", "vbscript:",
                    "eval(", "exec(", "__import__"
                ]

                data_upper = data.upper()
                for pattern in dangerous_patterns:
                    if pattern in data_upper:
                        self.logger.warning(f"Dangerous pattern detected in {context}: {pattern}")
                        raise SecurityException(f"Invalid input detected: {pattern}")

            # Check for path traversal
            if isinstance(data, str) and ("../" in data or "..\\" in data):
                raise SecurityException("Path traversal attempt detected")

            # Validate data size
            if isinstance(data, (str, bytes)):
                if len(data) > self.config.max_request_size:
                    raise SecurityException("Input data exceeds maximum allowed size")

            return True

        except Exception as e:
            self.logger.error(f"Security validation failed: {str(e)}")
            raise SecurityException(f"Security validation failed: {str(e)}")

    def check_rate_limit(self, client_id: str) -> bool:
        """Check if client has exceeded rate limits."""
        current_time = time.time()

        # Clean old requests (older than 1 minute)
        if client_id in self.request_counts:
            self.request_counts[client_id] = [
                req_time for req_time in self.request_counts[client_id]
                if current_time - req_time < 60
            ]
        else:
            self.request_counts[client_id] = []

        # Check current request count
        if len(self.request_counts[client_id]) >= self.config.rate_limit_per_minute:
            self.logger.warning(f"Rate limit exceeded for client: {client_id}")
            return False

        # Add current request
        self.request_counts[client_id].append(current_time)
        return True

    def generate_api_key(self, client_name: str) -> str:
        """Generate a secure API key for client authentication."""
        key = secrets.token_urlsafe(32)

        self.api_keys[key] = {
            "client_name": client_name,
            "created_at": time.time(),
            "is_active": True
        }

        self.logger.info(f"Generated API key for client: {client_name}")
        return key

    def validate_api_key(self, api_key: str) -> bool:
        """Validate API key."""
        if not self.config.require_authentication:
            return True

        if api_key not in self.api_keys:
            self.logger.warning(f"Invalid API key used: {api_key[:8]}...")
            return False

        key_info = self.api_keys[api_key]
        if not key_info.get("is_active", False):
            self.logger.warning(f"Inactive API key used: {api_key[:8]}...")
            return False

        return True

    def hash_sensitive_data(self, data: str, salt: Optional[str] = None) -> str:
        """Hash sensitive data with salt."""
        if salt is None:
            salt = secrets.token_hex(16)

        return hashlib.pbkdf2_hmac(
            'sha256',
            data.encode('utf-8'),
            salt.encode('utf-8'),
            100000  # iterations
        ).hex()

    def verify_signature(self, data: str, signature: str, secret: str) -> bool:
        """Verify HMAC signature."""
        expected_signature = hmac.new(
            secret.encode('utf-8'),
            data.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        return hmac.compare_digest(signature, expected_signature)


class AuditLogger:
    """Audit logging for security events."""

    def __init__(self):
        self.logger = get_logger("audit_log")

    def log_access(self, user_id: str, resource: str, action: str, success: bool):
        """Log access attempt."""
        status = "SUCCESS" if success else "FAILED"
        self.logger.info(f"AUDIT: {user_id} {action} {resource} - {status}")

    def log_data_access(self, user_id: str, data_source: str, record_count: int):
        """Log data access event."""
        self.logger.info(f"AUDIT: {user_id} accessed {record_count} records from {data_source}")

    def log_security_event(self, event_type: str, details: Dict[str, Any]):
        """Log security-related events."""
        self.logger.warning(f"SECURITY: {event_type} - {details}")


class DataEncryption:
    """Data encryption utilities."""

    @staticmethod
    def encrypt_sensitive_fields(data: Dict[str, Any], sensitive_fields: List[str]) -> Dict[str, Any]:
        """Encrypt sensitive fields in data dictionary."""
        encrypted_data = data.copy()

        for field in sensitive_fields:
            if field in encrypted_data:
                # Simple XOR encryption for demonstration
                # In production, use proper encryption like AES
                value = str(encrypted_data[field])
                key = "secure_key_placeholder"

                encrypted_value = "".join(
                    chr(ord(c) ^ ord(key[i % len(key)]))
                    for i, c in enumerate(value)
                )

                encrypted_data[field] = f"ENCRYPTED:{encrypted_value}"

        return encrypted_data

    @staticmethod
    def decrypt_sensitive_fields(data: Dict[str, Any], sensitive_fields: List[str]) -> Dict[str, Any]:
        """Decrypt sensitive fields in data dictionary."""
        decrypted_data = data.copy()

        for field in sensitive_fields:
            if field in decrypted_data:
                value = str(decrypted_data[field])

                if value.startswith("ENCRYPTED:"):
                    encrypted_value = value[10:]  # Remove "ENCRYPTED:" prefix
                    key = "secure_key_placeholder"

                    decrypted_value = "".join(
                        chr(ord(c) ^ ord(key[i % len(key)]))
                        for i, c in enumerate(encrypted_value)
                    )

                    decrypted_data[field] = decrypted_value

        return decrypted_data


def secure_operation(func):
    """Decorator to add security validation to operations."""
    def wrapper(*args, **kwargs):
        validator = SecurityValidator()

        # Validate all string arguments
        for arg in args:
            if isinstance(arg, (str, dict)):
                validator.validate_input(arg, func.__name__)

        for key, value in kwargs.items():
            if isinstance(value, (str, dict)):
                validator.validate_input(value, f"{func.__name__}.{key}")

        return func(*args, **kwargs)

    return wrapper
