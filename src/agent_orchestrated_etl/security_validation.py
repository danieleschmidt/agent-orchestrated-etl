"""Comprehensive security validation and threat detection system."""

from __future__ import annotations

import hashlib
import hmac
import time
import re
import json
import asyncio
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
import ipaddress
from urllib.parse import urlparse
import base64

from .exceptions import ValidationError
from .logging_config import get_logger


class ThreatLevel(Enum):
    """Security threat levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ValidationRule(Enum):
    """Types of validation rules."""
    INPUT_SANITIZATION = "input_sanitization"
    SQL_INJECTION = "sql_injection"
    XSS_PREVENTION = "xss_prevention"
    PATH_TRAVERSAL = "path_traversal"
    COMMAND_INJECTION = "command_injection"
    DATA_LEAK_PREVENTION = "data_leak_prevention"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    RATE_LIMITING = "rate_limiting"
    IP_WHITELIST = "ip_whitelist"


@dataclass
class SecurityThreat:
    """Represents a detected security threat."""
    threat_id: str
    timestamp: float
    threat_type: ValidationRule
    threat_level: ThreatLevel
    source_ip: Optional[str]
    user_agent: Optional[str]
    payload: str
    matched_pattern: str
    remediation: str
    blocked: bool
    metadata: Dict[str, Any]


@dataclass
class SecurityConfig:
    """Security validation configuration."""
    enable_sql_injection_detection: bool = True
    enable_xss_detection: bool = True
    enable_path_traversal_detection: bool = True
    enable_command_injection_detection: bool = True
    enable_data_leak_detection: bool = True
    enable_rate_limiting: bool = True
    enable_ip_filtering: bool = True
    max_payload_size: int = 1024 * 1024  # 1MB
    rate_limit_requests_per_minute: int = 100
    blocked_ip_cache_duration: int = 3600  # 1 hour
    encryption_key: Optional[str] = None


class SecurityValidator:
    """Comprehensive security validation and threat detection system."""
    
    def __init__(self, config: Optional[SecurityConfig] = None):
        self.logger = get_logger("agent_etl.security")
        self.config = config or SecurityConfig()
        self.threats: List[SecurityThreat] = []
        self.blocked_ips: Set[str] = set()
        self.rate_limit_cache: Dict[str, List[float]] = {}
        self.auth_tokens: Dict[str, Dict[str, Any]] = {}
        
        # Security patterns for threat detection
        self._initialize_security_patterns()
        
        # Metrics
        self.metrics = {
            "total_validations": 0,
            "threats_detected": 0,
            "threats_blocked": 0,
            "sql_injections_detected": 0,
            "xss_attempts_detected": 0,
            "path_traversals_detected": 0,
            "rate_limit_violations": 0
        }
    
    def _initialize_security_patterns(self):
        """Initialize security threat detection patterns."""
        
        # SQL Injection patterns
        self.sql_injection_patterns = [
            r"(?i)(union\s+select)",
            r"(?i)(drop\s+table)",
            r"(?i)(delete\s+from)",
            r"(?i)(insert\s+into)",
            r"(?i)(update\s+.*\s+set)",
            r"(?i)(exec\s*\()",
            r"(?i)(execute\s*\()",
            r"(?i)(declare\s+@)",
            r"(?i)(sp_executesql)",
            r"(?i)(\bor\s+\d+\s*=\s*\d+)",
            r"(?i)(\band\s+\d+\s*=\s*\d+)",
            r"(?i)(;\s*drop\s)",
            r"(?i)(0x[0-9a-f]+)",
            r"(?i)(waitfor\s+delay)",
            r"(?i)(benchmark\s*\()"
        ]
        
        # XSS patterns
        self.xss_patterns = [
            r"(?i)(<script[^>]*>)",
            r"(?i)(</script>)",
            r"(?i)(javascript:)",
            r"(?i)(on\w+\s*=)",
            r"(?i)(<iframe[^>]*>)",
            r"(?i)(<object[^>]*>)",
            r"(?i)(<embed[^>]*>)",
            r"(?i)(<link[^>]*>)",
            r"(?i)(<meta[^>]*>)",
            r"(?i)(eval\s*\()",
            r"(?i)(document\.(write|writeln))",
            r"(?i)(window\.(open|location))",
            r"(?i)(<img[^>]*onerror)",
            r"(?i)(<svg[^>]*onload)"
        ]
        
        # Path traversal patterns
        self.path_traversal_patterns = [
            r"(\.\.[\\/])",
            r"(\.\.%2f)",
            r"(\.\.%5c)",
            r"(%2e%2e[\\/])",
            r"(%252e%252e)",
            r"(\\.\\.\\)",
            r"(\.\./.*etc/passwd)",
            r"(\.\./.*windows/system32)",
            r"(file://)",
            r"(\.\./.*\.\.)"
        ]
        
        # Command injection patterns
        self.command_injection_patterns = [
            r"(;\s*\w+)",
            r"(\|\s*\w+)",
            r"(&\s*\w+)",
            r"(`[^`]*`)",
            r"(\$\([^)]*\))",
            r"(>\s*/dev/null)",
            r"(<\s*/dev/null)",
            r"(2>&1)",
            r"(&&\s*\w+)",
            r"(\|\|\s*\w+)",
            r"(rm\s+-rf)",
            r"(curl\s+)",
            r"(wget\s+)",
            r"(nc\s+)",
            r"(netcat\s+)"
        ]
        
        # Data leak patterns (sensitive information)
        self.data_leak_patterns = [
            r"(\b\d{4}[-\s]?\d{4}[-\s]?\d{4}[-\s]?\d{4}\b)",  # Credit card
            r"(\b\d{3}-?\d{2}-?\d{4}\b)",  # SSN
            r"(\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b)",  # Email
            r"(BEGIN\s+(RSA\s+)?PRIVATE\s+KEY)",  # Private key
            r"(password\s*[:=]\s*['\"]?[^\s'\"]+)",  # Password
            r"(api[_-]?key\s*[:=]\s*['\"]?[^\s'\"]+)",  # API key
            r"(secret[_-]?key\s*[:=]\s*['\"]?[^\s'\"]+)",  # Secret key
            r"(access[_-]?token\s*[:=]\s*['\"]?[^\s'\"]+)",  # Access token
            r"(bearer\s+[a-zA-Z0-9_-]+)",  # Bearer token
            r"(\bAKIA[0-9A-Z]{16}\b)",  # AWS Access Key
            r"(\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b)"  # UUID/GUID
        ]
    
    async def validate_input(self,
                           data: Any,
                           source_ip: Optional[str] = None,
                           user_agent: Optional[str] = None,
                           user_id: Optional[str] = None) -> Tuple[bool, List[SecurityThreat]]:
        """Comprehensive input validation and threat detection."""
        
        self.metrics["total_validations"] += 1
        threats_detected = []
        
        # Convert data to string for pattern matching
        payload = self._serialize_data(data)
        
        # Check payload size
        if len(payload) > self.config.max_payload_size:
            threat = self._create_threat(
                ValidationRule.INPUT_SANITIZATION,
                ThreatLevel.MEDIUM,
                source_ip, user_agent, payload[:100] + "...",
                f"Payload size: {len(payload)} bytes",
                "Reject oversized payload",
                True
            )
            threats_detected.append(threat)
        
        # IP-based validation
        if source_ip:
            ip_threats = await self._validate_ip(source_ip, user_id)
            threats_detected.extend(ip_threats)
        
        # Rate limiting
        if self.config.enable_rate_limiting and source_ip:
            rate_limit_threat = await self._check_rate_limit(source_ip)
            if rate_limit_threat:
                threats_detected.append(rate_limit_threat)
        
        # SQL Injection detection
        if self.config.enable_sql_injection_detection:
            sql_threats = self._detect_sql_injection(payload, source_ip, user_agent)
            threats_detected.extend(sql_threats)
        
        # XSS detection
        if self.config.enable_xss_detection:
            xss_threats = self._detect_xss(payload, source_ip, user_agent)
            threats_detected.extend(xss_threats)
        
        # Path traversal detection
        if self.config.enable_path_traversal_detection:
            path_threats = self._detect_path_traversal(payload, source_ip, user_agent)
            threats_detected.extend(path_threats)
        
        # Command injection detection
        if self.config.enable_command_injection_detection:
            cmd_threats = self._detect_command_injection(payload, source_ip, user_agent)
            threats_detected.extend(cmd_threats)
        
        # Data leak detection
        if self.config.enable_data_leak_detection:
            leak_threats = self._detect_data_leaks(payload, source_ip, user_agent)
            threats_detected.extend(leak_threats)
        
        # Update metrics and store threats
        self.metrics["threats_detected"] += len(threats_detected)
        self.threats.extend(threats_detected)
        
        # Determine if request should be blocked
        should_block = any(
            threat.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL] 
            for threat in threats_detected
        )
        
        if should_block:
            self.metrics["threats_blocked"] += 1
            if source_ip:
                self.blocked_ips.add(source_ip)
                # Log security incident
                self.logger.critical(
                    f"Security threat blocked from IP {source_ip}: "
                    f"{len(threats_detected)} threats detected"
                )
        
        return not should_block, threats_detected
    
    def _serialize_data(self, data: Any) -> str:
        """Serialize data to string for pattern matching."""
        try:
            if isinstance(data, str):
                return data
            elif isinstance(data, (dict, list)):
                return json.dumps(data, default=str)
            else:
                return str(data)
        except Exception:
            return str(data)
    
    async def _validate_ip(self, source_ip: str, user_id: Optional[str]) -> List[SecurityThreat]:
        """Validate IP address and check against blacklists."""
        threats = []
        
        # Check if IP is already blocked
        if source_ip in self.blocked_ips:
            threat = self._create_threat(
                ValidationRule.IP_WHITELIST,
                ThreatLevel.HIGH,
                source_ip, None, source_ip,
                "IP in blocked list",
                "Block all requests from this IP",
                True
            )
            threats.append(threat)
            return threats
        
        # Validate IP format
        try:
            ip = ipaddress.ip_address(source_ip)
            
            # Check for private/internal IPs in suspicious contexts
            if isinstance(ip, ipaddress.IPv4Address) and ip.is_private:
                # Allow private IPs but log for monitoring
                self.logger.debug(f"Private IP detected: {source_ip}")
            
            # Check for suspicious IP ranges (example: known malicious ranges)
            # In production, integrate with threat intelligence feeds
            suspicious_ranges = [
                "10.0.0.0/8",  # Example - not actually suspicious
                # Add actual malicious IP ranges from threat intelligence
            ]
            
            for suspicious_range in suspicious_ranges:
                if ip in ipaddress.ip_network(suspicious_range):
                    threat = self._create_threat(
                        ValidationRule.IP_WHITELIST,
                        ThreatLevel.MEDIUM,
                        source_ip, None, source_ip,
                        f"IP in suspicious range: {suspicious_range}",
                        "Monitor closely",
                        False
                    )
                    threats.append(threat)
            
        except ValueError:
            # Invalid IP format
            threat = self._create_threat(
                ValidationRule.INPUT_SANITIZATION,
                ThreatLevel.HIGH,
                source_ip, None, source_ip,
                "Invalid IP address format",
                "Reject request",
                True
            )
            threats.append(threat)
        
        return threats
    
    async def _check_rate_limit(self, source_ip: str) -> Optional[SecurityThreat]:
        """Check rate limiting for source IP."""
        current_time = time.time()
        minute_ago = current_time - 60
        
        # Clean old entries
        if source_ip not in self.rate_limit_cache:
            self.rate_limit_cache[source_ip] = []
        
        requests = self.rate_limit_cache[source_ip]
        requests[:] = [req_time for req_time in requests if req_time > minute_ago]
        
        # Add current request
        requests.append(current_time)
        
        # Check if rate limit exceeded
        if len(requests) > self.config.rate_limit_requests_per_minute:
            self.metrics["rate_limit_violations"] += 1
            
            return self._create_threat(
                ValidationRule.RATE_LIMITING,
                ThreatLevel.MEDIUM,
                source_ip, None, f"{len(requests)} requests/minute",
                f"Rate limit exceeded: {len(requests)} > {self.config.rate_limit_requests_per_minute}",
                "Temporarily block IP",
                True
            )
        
        return None
    
    def _detect_sql_injection(self, payload: str, source_ip: Optional[str], user_agent: Optional[str]) -> List[SecurityThreat]:
        """Detect SQL injection attempts."""
        threats = []
        
        for pattern in self.sql_injection_patterns:
            matches = re.findall(pattern, payload, re.IGNORECASE)
            
            if matches:
                self.metrics["sql_injections_detected"] += 1
                
                threat = self._create_threat(
                    ValidationRule.SQL_INJECTION,
                    ThreatLevel.HIGH,
                    source_ip, user_agent, payload[:200],
                    f"SQL injection pattern: {pattern}",
                    "Block request and sanitize database queries",
                    True,
                    {"matches": matches, "pattern": pattern}
                )
                threats.append(threat)
        
        return threats
    
    def _detect_xss(self, payload: str, source_ip: Optional[str], user_agent: Optional[str]) -> List[SecurityThreat]:
        """Detect XSS (Cross-Site Scripting) attempts."""
        threats = []
        
        for pattern in self.xss_patterns:
            matches = re.findall(pattern, payload, re.IGNORECASE)
            
            if matches:
                self.metrics["xss_attempts_detected"] += 1
                
                threat = self._create_threat(
                    ValidationRule.XSS_PREVENTION,
                    ThreatLevel.HIGH,
                    source_ip, user_agent, payload[:200],
                    f"XSS pattern: {pattern}",
                    "Sanitize output and block script execution",
                    True,
                    {"matches": matches, "pattern": pattern}
                )
                threats.append(threat)
        
        return threats
    
    def _detect_path_traversal(self, payload: str, source_ip: Optional[str], user_agent: Optional[str]) -> List[SecurityThreat]:
        """Detect path traversal attempts."""
        threats = []
        
        for pattern in self.path_traversal_patterns:
            matches = re.findall(pattern, payload, re.IGNORECASE)
            
            if matches:
                self.metrics["path_traversals_detected"] += 1
                
                threat = self._create_threat(
                    ValidationRule.PATH_TRAVERSAL,
                    ThreatLevel.HIGH,
                    source_ip, user_agent, payload[:200],
                    f"Path traversal pattern: {pattern}",
                    "Validate and sanitize file paths",
                    True,
                    {"matches": matches, "pattern": pattern}
                )
                threats.append(threat)
        
        return threats
    
    def _detect_command_injection(self, payload: str, source_ip: Optional[str], user_agent: Optional[str]) -> List[SecurityThreat]:
        """Detect command injection attempts."""
        threats = []
        
        for pattern in self.command_injection_patterns:
            matches = re.findall(pattern, payload, re.IGNORECASE)
            
            if matches:
                threat = self._create_threat(
                    ValidationRule.COMMAND_INJECTION,
                    ThreatLevel.CRITICAL,
                    source_ip, user_agent, payload[:200],
                    f"Command injection pattern: {pattern}",
                    "Block execution and sanitize system commands",
                    True,
                    {"matches": matches, "pattern": pattern}
                )
                threats.append(threat)
        
        return threats
    
    def _detect_data_leaks(self, payload: str, source_ip: Optional[str], user_agent: Optional[str]) -> List[SecurityThreat]:
        """Detect potential data leaks (sensitive information)."""
        threats = []
        
        for pattern in self.data_leak_patterns:
            matches = re.findall(pattern, payload, re.IGNORECASE)
            
            if matches:
                # Redact matched content for logging
                redacted_matches = ["[REDACTED]" for _ in matches]
                
                threat = self._create_threat(
                    ValidationRule.DATA_LEAK_PREVENTION,
                    ThreatLevel.MEDIUM,
                    source_ip, user_agent, "[SENSITIVE_DATA_DETECTED]",
                    f"Sensitive data pattern: {pattern}",
                    "Encrypt or remove sensitive data before processing",
                    False,  # Don't block, but monitor
                    {"pattern": pattern, "redacted_count": len(matches)}
                )
                threats.append(threat)
        
        return threats
    
    def _create_threat(self,
                      threat_type: ValidationRule,
                      threat_level: ThreatLevel,
                      source_ip: Optional[str],
                      user_agent: Optional[str],
                      payload: str,
                      matched_pattern: str,
                      remediation: str,
                      blocked: bool,
                      metadata: Optional[Dict[str, Any]] = None) -> SecurityThreat:
        """Create a security threat record."""
        
        return SecurityThreat(
            threat_id=f"threat_{int(time.time() * 1000000)}",
            timestamp=time.time(),
            threat_type=threat_type,
            threat_level=threat_level,
            source_ip=source_ip,
            user_agent=user_agent,
            payload=payload,
            matched_pattern=matched_pattern,
            remediation=remediation,
            blocked=blocked,
            metadata=metadata or {}
        )
    
    def sanitize_input(self, data: Any) -> Any:
        """Sanitize input data to prevent security issues."""
        
        if isinstance(data, str):
            # HTML encode special characters
            sanitized = (data.replace("&", "&amp;")
                           .replace("<", "&lt;")
                           .replace(">", "&gt;")
                           .replace("\"", "&quot;")
                           .replace("'", "&#39;"))
            
            # Remove potentially dangerous characters
            sanitized = re.sub(r"[^\w\s\-.,@#$%^&*()+=\[\]{}|;:!?]", "", sanitized)
            
            return sanitized
            
        elif isinstance(data, dict):
            return {k: self.sanitize_input(v) for k, v in data.items()}
            
        elif isinstance(data, list):
            return [self.sanitize_input(item) for item in data]
            
        else:
            return data
    
    def generate_secure_token(self, user_id: str, expires_in: int = 3600) -> str:
        """Generate a secure authentication token."""
        
        if not self.config.encryption_key:
            raise ValidationError("Encryption key not configured")
        
        # Create token payload
        payload = {
            "user_id": user_id,
            "issued_at": time.time(),
            "expires_at": time.time() + expires_in,
            "nonce": hashlib.sha256(f"{user_id}{time.time()}".encode()).hexdigest()[:16]
        }
        
        # Sign token
        payload_str = json.dumps(payload, sort_keys=True)
        signature = hmac.new(
            self.config.encryption_key.encode(),
            payload_str.encode(),
            hashlib.sha256
        ).hexdigest()
        
        # Encode token
        token_data = {
            "payload": payload,
            "signature": signature
        }
        
        token = base64.urlsafe_b64encode(
            json.dumps(token_data).encode()
        ).decode().rstrip("=")
        
        # Store token
        self.auth_tokens[token] = payload
        
        return token
    
    def validate_token(self, token: str) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validate authentication token."""
        
        try:
            # Decode token
            padded_token = token + "=" * (4 - len(token) % 4)
            token_data = json.loads(base64.urlsafe_b64decode(padded_token).decode())
            
            payload = token_data["payload"]
            signature = token_data["signature"]
            
            # Verify signature
            payload_str = json.dumps(payload, sort_keys=True)
            expected_signature = hmac.new(
                self.config.encryption_key.encode(),
                payload_str.encode(),
                hashlib.sha256
            ).hexdigest()
            
            if not hmac.compare_digest(signature, expected_signature):
                return False, None
            
            # Check expiration
            if time.time() > payload.get("expires_at", 0):
                # Remove expired token
                if token in self.auth_tokens:
                    del self.auth_tokens[token]
                return False, None
            
            return True, payload
            
        except Exception as e:
            self.logger.error(f"Token validation error: {str(e)}")
            return False, None
    
    def get_security_report(self) -> Dict[str, Any]:
        """Generate comprehensive security report."""
        
        current_time = time.time()
        recent_threats = [
            threat for threat in self.threats
            if current_time - threat.timestamp < 3600  # Last hour
        ]
        
        # Threat analysis
        threat_by_type = {}
        threat_by_level = {}
        threat_by_ip = {}
        
        for threat in recent_threats:
            # By type
            threat_type = threat.threat_type.value
            threat_by_type[threat_type] = threat_by_type.get(threat_type, 0) + 1
            
            # By level
            threat_level = threat.threat_level.value
            threat_by_level[threat_level] = threat_by_level.get(threat_level, 0) + 1
            
            # By IP
            if threat.source_ip:
                ip = threat.source_ip
                threat_by_ip[ip] = threat_by_ip.get(ip, 0) + 1
        
        # Top attacking IPs
        top_attacking_ips = sorted(
            threat_by_ip.items(),
            key=lambda x: x[1],
            reverse=True
        )[:10]
        
        return {
            "timestamp": current_time,
            "metrics": self.metrics,
            "recent_threats_count": len(recent_threats),
            "blocked_ips_count": len(self.blocked_ips),
            "active_tokens_count": len(self.auth_tokens),
            "threat_analysis": {
                "by_type": threat_by_type,
                "by_level": threat_by_level,
                "top_attacking_ips": top_attacking_ips
            },
            "security_config": {
                "sql_injection_detection": self.config.enable_sql_injection_detection,
                "xss_detection": self.config.enable_xss_detection,
                "rate_limiting": self.config.enable_rate_limiting,
                "max_payload_size": self.config.max_payload_size,
                "rate_limit": self.config.rate_limit_requests_per_minute
            },
            "recommendations": self._generate_security_recommendations(recent_threats)
        }
    
    def _generate_security_recommendations(self, recent_threats: List[SecurityThreat]) -> List[str]:
        """Generate security recommendations based on threat patterns."""
        recommendations = []
        
        if len(recent_threats) > 10:
            recommendations.append("Consider implementing additional rate limiting")
        
        sql_threats = [t for t in recent_threats if t.threat_type == ValidationRule.SQL_INJECTION]
        if len(sql_threats) > 3:
            recommendations.append("Review database query parameterization")
        
        xss_threats = [t for t in recent_threats if t.threat_type == ValidationRule.XSS_PREVENTION]
        if len(xss_threats) > 3:
            recommendations.append("Implement Content Security Policy (CSP) headers")
        
        high_severity = [t for t in recent_threats if t.threat_level in [ThreatLevel.HIGH, ThreatLevel.CRITICAL]]
        if len(high_severity) > 5:
            recommendations.append("Consider implementing Web Application Firewall (WAF)")
        
        if len(self.blocked_ips) > 20:
            recommendations.append("Review and update IP blocking policies")
        
        return recommendations


# Usage example
async def demo_security_validation():
    """Demonstrate security validation capabilities."""
    logger = get_logger("agent_etl.security.demo")
    
    # Create security validator with custom config
    config = SecurityConfig(
        enable_sql_injection_detection=True,
        enable_xss_detection=True,
        enable_rate_limiting=True,
        rate_limit_requests_per_minute=5,  # Low limit for demo
        encryption_key="your-secret-encryption-key-here"
    )
    
    validator = SecurityValidator(config)
    
    # Test various security threats
    test_cases = [
        {
            "name": "Normal request",
            "data": {"user": "john", "message": "Hello world"},
            "expected_safe": True
        },
        {
            "name": "SQL Injection attempt",
            "data": {"query": "SELECT * FROM users WHERE id = 1 OR 1=1"},
            "expected_safe": False
        },
        {
            "name": "XSS attempt",
            "data": {"content": "<script>alert('xss')</script>"},
            "expected_safe": False
        },
        {
            "name": "Path traversal attempt",
            "data": {"file": "../../../etc/passwd"},
            "expected_safe": False
        },
        {
            "name": "Command injection attempt",
            "data": {"cmd": "ls; rm -rf /"},
            "expected_safe": False
        },
        {
            "name": "Data leak (credit card)",
            "data": {"payment": "4532-1234-5678-9012"},
            "expected_safe": True  # Detected but not blocked
        }
    ]
    
    for test_case in test_cases:
        logger.info(f"Testing: {test_case['name']}")
        
        is_safe, threats = await validator.validate_input(
            test_case["data"],
            source_ip="192.168.1.100",
            user_agent="TestAgent/1.0"
        )
        
        logger.info(f"  Safe: {is_safe}, Threats detected: {len(threats)}")
        for threat in threats:
            logger.info(f"    - {threat.threat_type.value}: {threat.threat_level.value}")
    
    # Test token generation and validation
    logger.info("Testing token authentication")
    token = validator.generate_secure_token("user123", expires_in=300)
    logger.info(f"Generated token: {token[:20]}...")
    
    is_valid, payload = validator.validate_token(token)
    logger.info(f"Token valid: {is_valid}, Payload: {payload}")
    
    # Generate security report
    report = validator.get_security_report()
    logger.info("Security Report:")
    logger.info(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(demo_security_validation())