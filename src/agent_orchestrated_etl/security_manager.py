"""Security management and threat detection for Agent-Orchestrated-ETL."""

from __future__ import annotations

import hashlib
import re
import secrets
import time
from typing import Any, Optional

from .logging_config import get_logger


class SecurityManager:
    """Manages security policies and threat detection."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.security")
        self.threat_patterns = [
            # SQL injection patterns
            r"(\b(SELECT|INSERT|UPDATE|DELETE|DROP|CREATE|ALTER)\b.*\b(FROM|INTO|SET|WHERE)\b)",
            r"(--|/\*|\*/|;)",
            # Script injection patterns  
            r"(<script[^>]*>.*?</script>)",
            r"(javascript:|vbscript:|onload=|onerror=)",
            # Path traversal patterns
            r"(\.\./|\.\.\\|%2e%2e%2f|%2e%2e%5c)",
            # Command injection patterns
            r"(\b(exec|eval|system|shell_exec|passthru|popen|proc_open)\b)",
        ]
        self.compiled_patterns = [re.compile(pattern, re.IGNORECASE) for pattern in self.threat_patterns]
        
    def sanitize_input(self, data: Any) -> Any:
        """Sanitize input data to prevent injection attacks."""
        if isinstance(data, str):
            return self._sanitize_string(data)
        elif isinstance(data, dict):
            return {k: self.sanitize_input(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.sanitize_input(item) for item in data]
        else:
            return data
    
    def _sanitize_string(self, text: str) -> str:
        """Sanitize a string by removing/escaping dangerous characters."""
        # Remove null bytes
        text = text.replace('\x00', '')
        
        # Escape SQL special characters
        sql_escapes = {
            "'": "''",
            '"': '""',
            "\\": "\\\\",
        }
        
        for char, escape in sql_escapes.items():
            text = text.replace(char, escape)
        
        return text
    
    def detect_threats(self, data: str) -> list[dict[str, Any]]:
        """Detect potential security threats in input data."""
        threats = []
        
        for i, pattern in enumerate(self.compiled_patterns):
            matches = pattern.findall(data)
            if matches:
                threat = {
                    'pattern_id': i,
                    'threat_type': self._get_threat_type(i),
                    'matches': matches,
                    'severity': self._get_severity(i),
                    'timestamp': time.time()
                }
                threats.append(threat)
                
                self.logger.warning(
                    f"Security threat detected: {threat['threat_type']}",
                    extra={
                        'threat_type': threat['threat_type'],
                        'severity': threat['severity'],
                        'matches_count': len(matches),
                        'data_preview': data[:100] + '...' if len(data) > 100 else data
                    }
                )
        
        return threats
    
    def _get_threat_type(self, pattern_id: int) -> str:
        """Get threat type based on pattern ID."""
        threat_types = [
            'sql_injection',
            'sql_injection', 
            'script_injection',
            'script_injection',
            'path_traversal',
            'command_injection'
        ]
        return threat_types[pattern_id] if pattern_id < len(threat_types) else 'unknown'
    
    def _get_severity(self, pattern_id: int) -> str:
        """Get threat severity based on pattern ID."""
        severities = ['high', 'high', 'high', 'medium', 'high', 'critical']
        return severities[pattern_id] if pattern_id < len(severities) else 'medium'
    
    def validate_data_access(self, source: str, user_context: Optional[dict[str, Any]] = None) -> bool:
        """Validate if data access is authorized."""
        # Basic validation - could be extended with RBAC
        if not source:
            return False
            
        # Check for suspicious sources
        suspicious_patterns = [
            r'file:///(etc|proc|sys)/',
            r'\\\\.*\\c\$',
            r'jdbc:.*password=',
        ]
        
        for pattern in suspicious_patterns:
            if re.search(pattern, source, re.IGNORECASE):
                self.logger.error(
                    f"Suspicious data source detected: {source}",
                    extra={'source': source, 'user_context': user_context}
                )
                return False
        
        return True
    
    def generate_secure_token(self, length: int = 32) -> str:
        """Generate a cryptographically secure token."""
        return secrets.token_urlsafe(length)
    
    def hash_sensitive_data(self, data: str) -> str:
        """Hash sensitive data for logging/storage."""
        return hashlib.sha256(data.encode()).hexdigest()


class AuditLogger:
    """Handles security audit logging."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.audit")
    
    def log_data_access(
        self, 
        source: str, 
        operation: str, 
        user_id: Optional[str] = None,
        success: bool = True,
        details: Optional[dict[str, Any]] = None
    ) -> None:
        """Log data access events."""
        self.logger.info(
            f"Data access: {operation} on {source}",
            extra={
                'event_type': 'data_access',
                'source': source,
                'operation': operation,
                'user_id': user_id,
                'success': success,
                'details': details or {},
                'timestamp': time.time()
            }
        )
    
    def log_security_event(
        self,
        event_type: str,
        description: str,
        severity: str = 'medium',
        details: Optional[dict[str, Any]] = None
    ) -> None:
        """Log security-related events."""
        self.logger.warning(
            f"Security event: {event_type} - {description}",
            extra={
                'event_type': 'security_event',
                'security_event_type': event_type,
                'description': description,
                'severity': severity,
                'details': details or {},
                'timestamp': time.time()
            }
        )
    
    def log_pipeline_execution(
        self,
        pipeline_id: str,
        status: str,
        duration: float,
        records_processed: int,
        user_id: Optional[str] = None
    ) -> None:
        """Log pipeline execution events."""
        self.logger.info(
            f"Pipeline execution: {pipeline_id} - {status}",
            extra={
                'event_type': 'pipeline_execution',
                'pipeline_id': pipeline_id,
                'status': status,
                'duration': duration,
                'records_processed': records_processed,
                'user_id': user_id,
                'timestamp': time.time()
            }
        )


class RateLimiter:
    """Simple rate limiting for API protection."""
    
    def __init__(self, max_requests: int = 100, time_window: int = 60):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = {}
        self.logger = get_logger("agent_etl.rate_limiter")
    
    def is_allowed(self, identifier: str) -> bool:
        """Check if request is allowed under rate limits."""
        current_time = time.time()
        
        # Clean old entries
        cutoff_time = current_time - self.time_window
        self.requests = {
            k: [t for t in timestamps if t > cutoff_time]
            for k, timestamps in self.requests.items()
        }
        
        # Check current rate
        if identifier not in self.requests:
            self.requests[identifier] = []
        
        request_count = len(self.requests[identifier])
        
        if request_count >= self.max_requests:
            self.logger.warning(
                f"Rate limit exceeded for {identifier}",
                extra={
                    'identifier': identifier,
                    'request_count': request_count,
                    'max_requests': self.max_requests,
                    'time_window': self.time_window
                }
            )
            return False
        
        # Record this request
        self.requests[identifier].append(current_time)
        return True


class DataPrivacyManager:
    """Manages data privacy and PII protection."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.privacy")
        self.pii_patterns = {
            'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
            'phone': r'\b(\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}\b',
            'ssn': r'\b\d{3}-\d{2}-\d{4}\b',
            'credit_card': r'\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b'
        }
        self.compiled_pii_patterns = {
            name: re.compile(pattern, re.IGNORECASE)
            for name, pattern in self.pii_patterns.items()
        }
    
    def detect_pii(self, data: str) -> dict[str, list[str]]:
        """Detect personally identifiable information in data."""
        detected_pii = {}
        
        for pii_type, pattern in self.compiled_pii_patterns.items():
            matches = pattern.findall(data)
            if matches:
                detected_pii[pii_type] = matches
                self.logger.info(
                    f"PII detected: {pii_type}",
                    extra={
                        'pii_type': pii_type,
                        'count': len(matches)
                    }
                )
        
        return detected_pii
    
    def anonymize_data(self, data: str) -> str:
        """Anonymize PII in data."""
        anonymized = data
        
        for pii_type, pattern in self.compiled_pii_patterns.items():
            replacement = self._get_replacement(pii_type)
            anonymized = pattern.sub(replacement, anonymized)
        
        return anonymized
    
    def _get_replacement(self, pii_type: str) -> str:
        """Get replacement string for PII type."""
        replacements = {
            'email': '[EMAIL_REDACTED]',
            'phone': '[PHONE_REDACTED]',
            'ssn': '[SSN_REDACTED]',
            'credit_card': '[CARD_REDACTED]'
        }
        return replacements.get(pii_type, '[PII_REDACTED]')


# Global security instances
security_manager = SecurityManager()
audit_logger = AuditLogger()
rate_limiter = RateLimiter()
privacy_manager = DataPrivacyManager()