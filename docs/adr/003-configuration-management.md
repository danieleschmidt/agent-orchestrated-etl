# ADR-003: Configuration Management Strategy

## Status
Proposed

## Context
The Agent-Orchestrated-ETL system requires a flexible configuration management approach that supports:
- Dynamic agent configuration updates
- Environment-specific settings (dev/staging/prod)
- Secret management for data source credentials
- Runtime configuration changes without system restarts
- Configuration validation and schema enforcement

## Options Considered

### Option 1: Environment Variables + YAML Files
- **Pros**: Simple, widely supported, version controllable
- **Cons**: Limited runtime updates, no schema validation, secret exposure risk
- **Agent Impact**: Requires restart for configuration changes

### Option 2: HashiCorp Consul + Vault
- **Pros**: Dynamic updates, secret management, service discovery
- **Cons**: Complex deployment, additional infrastructure, learning curve
- **Agent Impact**: Excellent for runtime configuration changes

### Option 3: Kubernetes ConfigMaps + Secrets
- **Pros**: Native K8s integration, RBAC, automatic mounting
- **Cons**: K8s dependency, limited to containerized deployments
- **Agent Impact**: Good for containerized agent deployments

### Option 4: AWS Systems Manager Parameter Store + Secrets Manager
- **Pros**: Managed service, encryption, fine-grained access control
- **Cons**: AWS vendor lock-in, API rate limits, cost
- **Agent Impact**: Requires AWS SDK integration

### Option 5: Hybrid Approach (YAML + Vault + Environment-specific)
- **Pros**: Best of multiple worlds, gradual migration path
- **Cons**: Complexity, multiple configuration sources
- **Agent Impact**: Flexible but requires abstraction layer

## Decision
We choose a **Hybrid Approach** combining:
1. **YAML files** for static configuration and defaults
2. **HashiCorp Vault** for secrets management
3. **Environment variables** for deployment-specific overrides
4. **Configuration abstraction layer** for runtime updates

## Configuration Hierarchy (highest to lowest priority)
1. Runtime agent updates (temporary)
2. Environment variables
3. Vault secrets
4. YAML configuration files
5. Built-in defaults

## Implementation Details

### Configuration Structure
```yaml
# config/base.yaml
agents:
  orchestrator:
    max_concurrent_pipelines: 10
    decision_timeout: 30s
  etl:
    batch_size: 1000
    retry_attempts: 3
  monitor:
    check_interval: 60s
    alert_thresholds:
      error_rate: 0.05
      latency_p95: 5000ms

data_sources:
  postgres:
    connection_pool_size: 10
    query_timeout: 300s
  s3:
    multipart_threshold: 64MB
    retry_config:
      max_attempts: 3
      backoff_mode: exponential

pipeline:
  default_timeout: 3600s
  max_retries: 2
  checkpoint_interval: 300s
```

### Secret Management
```yaml
# Vault paths
secrets:
  postgres_credentials: secret/data/etl/postgres
  s3_credentials: secret/data/etl/s3
  api_keys: secret/data/etl/external_apis
```

### Environment Overrides
```bash
# Environment-specific settings
ETL_AGENTS_ORCHESTRATOR_MAX_CONCURRENT_PIPELINES=20
ETL_VAULT_ADDRESS=https://vault.prod.company.com
ETL_LOG_LEVEL=INFO
```

## Configuration Abstraction Layer

### Python Implementation
```python
from typing import Any, Dict, Optional
import os
import yaml
import hvac  # HashiCorp Vault client

class ConfigManager:
    def __init__(self):
        self.config = self._load_config()
        self.vault_client = self._init_vault()
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value with hierarchy precedence"""
        # 1. Check environment variables
        env_key = f"ETL_{key.upper().replace('.', '_')}"
        if env_value := os.getenv(env_key):
            return self._parse_env_value(env_value)
        
        # 2. Check Vault for secrets
        if self._is_secret_key(key):
            if vault_value := self._get_vault_secret(key):
                return vault_value
        
        # 3. Check YAML configuration
        return self._get_nested_config(key, default)
    
    def update_runtime_config(self, key: str, value: Any) -> None:
        """Update configuration at runtime (agents can use this)"""
        self.runtime_overrides[key] = value
        self._notify_config_change(key, value)
```

## Consequences

### Positive
- Flexible configuration management supporting multiple use cases
- Secure secret management with Vault integration
- Runtime configuration updates for agent adaptability
- Environment-specific configuration support
- Configuration validation and schema enforcement capability
- Gradual migration path from simple to complex scenarios

### Negative
- Increased complexity compared to single-source configuration
- Multiple dependencies (Vault, YAML parser, etc.)
- Potential configuration drift between environments
- Learning curve for team members
- Additional operational overhead for Vault management

### Implementation Requirements
1. Configuration schema validation library
2. Vault integration and secret rotation handling
3. Configuration change notification system for agents
4. Environment-specific configuration templates
5. Configuration audit logging
6. Backup and disaster recovery for Vault

## Security Considerations
- Vault access control and authentication
- Configuration file encryption for sensitive data
- Audit logging for configuration changes
- Principle of least privilege for secret access
- Regular secret rotation procedures

## Migration Strategy
1. **Phase 1**: Implement YAML + environment variable support
2. **Phase 2**: Add Vault integration for secrets
3. **Phase 3**: Implement runtime configuration updates
4. **Phase 4**: Add configuration validation and schema enforcement
5. **Phase 5**: Implement configuration change notifications

## Related Decisions
- Builds on ADR-002 (Workflow Engine Choice) for Airflow configuration
- Influences ADR-005 (Security Architecture) for secret management
- Impacts agent coordination and communication strategies

## Review Date
This decision should be reviewed in Q3 2025 or if operational complexity becomes problematic.