# AWS Secrets Manager Integration Guide

This guide explains how to configure and use AWS Secrets Manager with the Agent-Orchestrated ETL system.

## Overview

The AWS Secrets Manager integration allows you to securely store and retrieve sensitive configuration values such as database passwords, API keys, and other credentials without hardcoding them in your application.

## Features

- **Secure credential storage**: Store secrets in AWS Secrets Manager instead of environment variables
- **Automatic caching**: Secrets are cached for 5 minutes by default to reduce API calls
- **JSON secret support**: Supports both simple string secrets and JSON key-value secrets
- **Error handling**: Comprehensive error handling with fallback to default values
- **Monitoring**: Cache statistics and monitoring capabilities

## Configuration

### Environment Variables

Configure AWS Secrets Manager integration using these environment variables:

```bash
# Enable AWS Secrets Manager as the secret provider
export AGENT_ETL_SECRET_PROVIDER=aws_secrets

# AWS region where secrets are stored (default: us-east-1)
export AGENT_ETL_AWS_SECRETS_REGION=us-west-2

# Prefix for secret names (default: agent-etl/)
export AGENT_ETL_AWS_SECRETS_PREFIX=myapp/prod/

# Cache TTL in seconds (default: 300)
export AGENT_ETL_AWS_SECRETS_CACHE_TTL=600
```

### AWS Credentials

Ensure AWS credentials are configured using one of these methods:

1. **Environment variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your-access-key
   export AWS_SECRET_ACCESS_KEY=your-secret-key
   ```

2. **IAM roles** (recommended for EC2/ECS/Lambda):
   - Attach appropriate IAM role to your compute instance

3. **AWS credentials file**:
   ```bash
   aws configure
   ```

### Required IAM Permissions

Your AWS credentials need the following permissions:

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "secretsmanager:GetSecretValue"
            ],
            "Resource": [
                "arn:aws:secretsmanager:REGION:ACCOUNT:secret:agent-etl/*"
            ]
        }
    ]
}
```

Replace `REGION` and `ACCOUNT` with your actual AWS region and account ID.

## Secret Management

### Creating Secrets

#### Simple String Secrets

Create a simple string secret:

```bash
aws secretsmanager create-secret \
    --name "agent-etl/db_password" \
    --secret-string "my-secure-password"
```

#### JSON Key-Value Secrets

Create a JSON secret with multiple values:

```bash
aws secretsmanager create-secret \
    --name "agent-etl/database" \
    --secret-string '{"username":"dbuser","password":"dbpass","host":"db.example.com"}'
```

### Secret Naming Convention

Secrets should follow this naming pattern:
```
{prefix}{key_name}
```

For example, with prefix `agent-etl/` and key `db_password`:
- Secret name: `agent-etl/db_password`
- Retrieved via: `secret_manager.get_secret("db_password")`

### Supported Secret Types

1. **String secrets**: Simple text values
2. **JSON secrets with specific keys**: Returns the value for the requested key
3. **JSON secrets with single key-value pair**: Returns the single value

## Usage Examples

### Basic Usage

```python
from agent_orchestrated_etl.config import get_config

# Load configuration with AWS Secrets Manager
config = get_config()

# Database credentials are automatically retrieved from AWS Secrets Manager
db_host = config.database.host
db_password = config.database.password
```

### Direct Secret Manager Usage

```python
from agent_orchestrated_etl.config import SecretManager, SecurityConfig

# Configure for AWS Secrets Manager
security_config = SecurityConfig(
    secret_provider="aws_secrets",
    aws_secrets_region="us-west-2",
    aws_secrets_prefix="myapp/"
)

secret_manager = SecretManager(
    provider="aws_secrets",
    security_config=security_config
)

# Retrieve secrets
db_password = secret_manager.get_secret("db_password")
api_key = secret_manager.get_secret("api_key", default="fallback-key")
```

### Cache Management

```python
# Check cache statistics
stats = secret_manager.get_cache_stats()
print(f"Cache entries: {stats['total_entries']}")
print(f"Valid entries: {stats['valid_entries']}")
print(f"Expired entries: {stats['expired_entries']}")

# Clear cache to force refresh
secret_manager.clear_cache()
```

## Migration from Environment Variables

To migrate from environment variables to AWS Secrets Manager:

1. **Create secrets in AWS**:
   ```bash
   # Instead of AGENT_ETL_DB_PASSWORD environment variable
   aws secretsmanager create-secret \
       --name "agent-etl/db_password" \
       --secret-string "your-password"
   ```

2. **Update configuration**:
   ```bash
   # Remove the old environment variable
   unset AGENT_ETL_DB_PASSWORD
   
   # Enable AWS Secrets Manager
   export AGENT_ETL_SECRET_PROVIDER=aws_secrets
   ```

3. **Test the integration**:
   ```python
   from agent_orchestrated_etl.config import get_config
   config = get_config()
   # Should now retrieve from AWS Secrets Manager
   ```

## Security Best Practices

1. **Use IAM roles**: Prefer IAM roles over access keys when possible
2. **Least privilege**: Grant only `secretsmanager:GetSecretValue` permission
3. **Secret rotation**: Enable automatic secret rotation in AWS Secrets Manager
4. **Audit access**: Monitor CloudTrail logs for secret access
5. **Resource-based policies**: Use resource-based policies to restrict access
6. **Encryption**: Secrets are encrypted at rest and in transit by default

## Troubleshooting

### Common Issues

1. **"AWS credentials not configured properly"**
   - Ensure AWS credentials are set via environment variables, IAM role, or credentials file
   - Verify credentials have the required permissions

2. **"Secret not found"**
   - Check the secret name matches the expected pattern: `{prefix}{key}`
   - Verify the secret exists in the correct AWS region

3. **"Access denied"**
   - Ensure IAM permissions include `secretsmanager:GetSecretValue`
   - Check resource ARNs match your secret names

4. **"Region not available"**
   - Verify the AWS region is correct and Secrets Manager is available
   - Check network connectivity to AWS

### Debug Mode

Enable debug logging to troubleshoot issues:

```bash
export AGENT_ETL_LOG_LEVEL=DEBUG
```

### Testing Connection

Test AWS Secrets Manager connectivity:

```python
from agent_orchestrated_etl.config import SecretManager

secret_manager = SecretManager(provider="aws_secrets")
try:
    value = secret_manager.get_secret("test_key", default="test-works")
    print(f"AWS Secrets Manager integration: OK")
except Exception as e:
    print(f"AWS Secrets Manager error: {e}")
```

## Performance Considerations

- **Caching**: Secrets are cached for 5 minutes by default to reduce API calls
- **Cold starts**: First secret retrieval may be slower due to AWS SDK initialization
- **Rate limits**: AWS Secrets Manager has rate limits; caching helps mitigate this
- **Cost optimization**: Caching reduces the number of API calls and associated costs

## Monitoring and Alerting

Monitor your AWS Secrets Manager integration:

1. **CloudWatch metrics**: Monitor API call volume and errors
2. **Application metrics**: Use `get_cache_stats()` for cache performance
3. **CloudTrail**: Audit secret access for security compliance
4. **Cost monitoring**: Track Secrets Manager usage costs

## Development vs Production

### Development Environment

```bash
# Use environment variables for development
export AGENT_ETL_SECRET_PROVIDER=env
export AGENT_ETL_DB_PASSWORD=dev-password
```

### Production Environment

```bash
# Use AWS Secrets Manager for production
export AGENT_ETL_SECRET_PROVIDER=aws_secrets
export AGENT_ETL_AWS_SECRETS_REGION=us-east-1
export AGENT_ETL_AWS_SECRETS_PREFIX=prod/
```

## Backup and Disaster Recovery

1. **Secret backup**: AWS Secrets Manager handles backup automatically
2. **Cross-region replication**: Consider replicating critical secrets across regions
3. **Fallback strategy**: Implement fallback to environment variables if needed:

```python
# Automatic fallback is built into the system
config = get_config()  # Will fall back to env vars if AWS fails
```