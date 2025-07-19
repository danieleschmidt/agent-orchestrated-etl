"""Configuration template generation for Agent-Orchestrated ETL."""

from __future__ import annotations

import json
import yaml
from pathlib import Path
from typing import Any, Dict, Optional

from .config import AppConfig


def generate_config_template(
    format_type: str = "yaml",
    include_comments: bool = True,
    include_examples: bool = True,
    environment: str = "development",
) -> str:
    """Generate a configuration template file.
    
    Args:
        format_type: Output format ("yaml" or "json")
        include_comments: Whether to include explanatory comments
        include_examples: Whether to include example values
        environment: Target environment for the template
        
    Returns:
        Configuration template as string
    """
    if format_type.lower() == "yaml":
        return _generate_yaml_template(include_comments, include_examples, environment)
    elif format_type.lower() == "json":
        return _generate_json_template(include_examples, environment)
    else:
        raise ValueError(f"Unsupported format: {format_type}")


def _generate_yaml_template(
    include_comments: bool,
    include_examples: bool,
    environment: str,
) -> str:
    """Generate YAML configuration template."""
    
    template_lines = []
    
    if include_comments:
        template_lines.extend([
            "# Agent-Orchestrated ETL Configuration",
            f"# Environment: {environment}",
            "# This file contains configuration settings for the ETL application.",
            "# Environment variables will override these settings.",
            "",
        ])
    
    # Core settings
    if include_comments:
        template_lines.extend([
            "# Core application settings",
        ])
    
    template_lines.extend([
        f"environment: {environment}",
        f"debug: {'true' if environment == 'development' else 'false'}",
        "",
    ])
    
    # Security settings
    if include_comments:
        template_lines.extend([
            "# Security configuration",
        ])
    
    template_lines.extend([
        "security:",
        f"  secret_provider: env  # Options: env, aws_secrets, vault",
        f"  secret_prefix: AGENT_ETL_",
        f"  max_dag_id_length: 200",
        f"  max_task_name_length: 250",
        f"  max_file_path_length: 4096",
        f"  max_pipeline_executions_per_minute: 60",
        f"  max_dag_generation_per_minute: 120",
        "",
    ])
    
    # Database settings
    if include_comments:
        template_lines.extend([
            "# Database configuration",
            "# Use environment variables for sensitive values:",
            "# AGENT_ETL_DB_HOST, AGENT_ETL_DB_USER, AGENT_ETL_DB_PASSWORD",
        ])
    
    if include_examples:
        host_example = "localhost" if environment == "development" else "prod-db.example.com"
        db_example = "agent_etl_dev" if environment == "development" else "agent_etl_prod"
    else:
        host_example = "null"
        db_example = "null"
    
    template_lines.extend([
        "database:",
        f"  host: {host_example}  # Override with AGENT_ETL_DB_HOST",
        f"  port: 5432",
        f"  database: {db_example}  # Override with AGENT_ETL_DB_NAME",
        f"  username: null  # Override with AGENT_ETL_DB_USER",
        f"  password: null  # Override with AGENT_ETL_DB_PASSWORD",
        f"  ssl_mode: require",
        f"  connection_timeout: 30",
        "",
    ])
    
    # S3 settings
    if include_comments:
        template_lines.extend([
            "# S3 configuration",
            "# Use environment variables for credentials:",
            "# AGENT_ETL_S3_ACCESS_KEY_ID, AGENT_ETL_S3_SECRET_ACCESS_KEY",
        ])
    
    template_lines.extend([
        "s3:",
        f"  region: us-east-1",
        f"  access_key_id: null  # Override with AGENT_ETL_S3_ACCESS_KEY_ID",
        f"  secret_access_key: null  # Override with AGENT_ETL_S3_SECRET_ACCESS_KEY",
        f"  session_token: null  # Override with AGENT_ETL_S3_SESSION_TOKEN",
        f"  endpoint_url: null  # For S3-compatible services",
        f"  use_ssl: true",
        "",
    ])
    
    # API settings
    if include_comments:
        template_lines.extend([
            "# External API configuration",
        ])
    
    if include_examples:
        api_url = "https://api.example.com" if environment == "production" else "https://api-dev.example.com"
    else:
        api_url = "null"
    
    template_lines.extend([
        "api:",
        f"  base_url: {api_url}",
        f"  api_key: null  # Override with AGENT_ETL_API_KEY",
        f"  timeout: 30",
        f"  max_retries: 3",
        f"  user_agent: agent-orchestrated-etl/1.0",
        "",
    ])
    
    # Logging settings
    if include_comments:
        template_lines.extend([
            "# Logging configuration",
        ])
    
    log_level = "INFO" if environment == "production" else "DEBUG"
    log_output = "file" if environment == "production" else "console"
    log_file = f"/var/log/agent-etl/app.log" if environment == "production" else "null"
    
    template_lines.extend([
        "logging:",
        f"  level: {log_level}  # DEBUG, INFO, WARNING, ERROR, CRITICAL",
        f"  format: json  # json or text",
        f"  output: {log_output}  # console, file, or both",
        f"  file_path: {log_file}",
        f"  max_file_size_mb: 100",
        f"  backup_count: 5",
        "",
    ])
    
    # Paths
    if include_comments:
        template_lines.extend([
            "# Directory paths",
        ])
    
    if environment == "production":
        temp_dir = "/tmp/agent_etl"
        data_dir = "/opt/agent_etl/data"
        output_dir = "/opt/agent_etl/output"
    else:
        temp_dir = "./tmp"
        data_dir = "./data"
        output_dir = "./output"
    
    template_lines.extend([
        "paths:",
        f"  temp_dir: {temp_dir}",
        f"  data_dir: {data_dir}",
        f"  output_dir: {output_dir}",
    ])
    
    return "\n".join(template_lines)


def _generate_json_template(include_examples: bool, environment: str) -> str:
    """Generate JSON configuration template."""
    
    # Create template structure
    template = {
        "environment": environment,
        "debug": environment == "development",
        "security": {
            "secret_provider": "env",
            "secret_prefix": "AGENT_ETL_",
            "max_dag_id_length": 200,
            "max_task_name_length": 250,
            "max_file_path_length": 4096,
            "max_pipeline_executions_per_minute": 60,
            "max_dag_generation_per_minute": 120,
        },
        "database": {
            "host": "localhost" if include_examples and environment == "development" else None,
            "port": 5432,
            "database": "agent_etl_dev" if include_examples and environment == "development" else None,
            "username": None,
            "password": None,
            "ssl_mode": "require",
            "connection_timeout": 30,
        },
        "s3": {
            "region": "us-east-1",
            "access_key_id": None,
            "secret_access_key": None,
            "session_token": None,
            "endpoint_url": None,
            "use_ssl": True,
        },
        "api": {
            "base_url": "https://api.example.com" if include_examples else None,
            "api_key": None,
            "timeout": 30,
            "max_retries": 3,
            "user_agent": "agent-orchestrated-etl/1.0",
        },
        "logging": {
            "level": "INFO" if environment == "production" else "DEBUG",
            "format": "json",
            "output": "file" if environment == "production" else "console",
            "file_path": "/var/log/agent-etl/app.log" if environment == "production" else None,
            "max_file_size_mb": 100,
            "backup_count": 5,
        },
    }
    
    return json.dumps(template, indent=2, ensure_ascii=False)


def generate_environment_configs() -> Dict[str, str]:
    """Generate configuration templates for all environments.
    
    Returns:
        Dictionary with environment names as keys and YAML configs as values
    """
    environments = ["development", "staging", "production"]
    configs = {}
    
    for env in environments:
        configs[env] = generate_config_template(
            format_type="yaml",
            include_comments=True,
            include_examples=True,
            environment=env,
        )
    
    return configs


def save_config_template(
    output_path: Path,
    format_type: str = "yaml",
    environment: str = "development",
    overwrite: bool = False,
) -> None:
    """Save a configuration template to file.
    
    Args:
        output_path: Path where to save the template
        format_type: Output format ("yaml" or "json")
        environment: Target environment
        overwrite: Whether to overwrite existing files
        
    Raises:
        FileExistsError: If file exists and overwrite is False
        ValueError: If format_type is not supported
    """
    if output_path.exists() and not overwrite:
        raise FileExistsError(f"File already exists: {output_path}")
    
    # Ensure parent directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Generate template
    template_content = generate_config_template(
        format_type=format_type,
        include_comments=True,
        include_examples=True,
        environment=environment,
    )
    
    # Write to file
    output_path.write_text(template_content, encoding="utf-8")


def generate_docker_compose_template(environment: str = "development") -> str:
    """Generate a Docker Compose template for the application.
    
    Args:
        environment: Target environment
        
    Returns:
        Docker Compose YAML content
    """
    
    template_lines = [
        "# Docker Compose configuration for Agent-Orchestrated ETL",
        f"# Environment: {environment}",
        "",
        "version: '3.8'",
        "",
        "services:",
        "  agent-etl:",
        "    build: .",
        "    container_name: agent-etl",
        "    environment:",
        f"      - AGENT_ETL_ENVIRONMENT={environment}",
        "      - AGENT_ETL_LOG_LEVEL=INFO",
        "      - AGENT_ETL_LOG_FORMAT=json",
        "      - AGENT_ETL_LOG_OUTPUT=console",
        "    volumes:",
        "      - ./data:/app/data",
        "      - ./output:/app/output",
        "      - ./config:/app/config",
        "    ports:",
        "      - \"8080:8080\"",
        "    depends_on:",
        "      - postgres",
        "      - redis",
        "",
        "  postgres:",
        "    image: postgres:15",
        "    container_name: agent-etl-postgres",
        "    environment:",
        "      - POSTGRES_DB=agent_etl",
        "      - POSTGRES_USER=agent_etl",
        "      - POSTGRES_PASSWORD=agent_etl_password",
        "    volumes:",
        "      - postgres_data:/var/lib/postgresql/data",
        "    ports:",
        "      - \"5432:5432\"",
        "",
        "  redis:",
        "    image: redis:7",
        "    container_name: agent-etl-redis",
        "    ports:",
        "      - \"6379:6379\"",
        "",
        "volumes:",
        "  postgres_data:",
    ]
    
    if environment == "production":
        # Add production-specific configurations
        template_lines.extend([
            "",
            "  # Production-specific configurations",
            "  # - Use external managed databases",
            "  # - Configure proper secrets management",
            "  # - Set up monitoring and logging",
        ])
    
    return "\n".join(template_lines)


def generate_env_template(environment: str = "development") -> str:
    """Generate environment variables template.
    
    Args:
        environment: Target environment
        
    Returns:
        Environment variables file content
    """
    
    template_lines = [
        f"# Environment variables for Agent-Orchestrated ETL",
        f"# Environment: {environment}",
        f"# Copy this file to .env and update the values",
        "",
        "# Core settings",
        f"AGENT_ETL_ENVIRONMENT={environment}",
        f"AGENT_ETL_DEBUG={'true' if environment == 'development' else 'false'}",
        "",
        "# Security settings",
        "AGENT_ETL_SECRET_PROVIDER=env",
        "AGENT_ETL_SECRET_PREFIX=AGENT_ETL_",
        "",
        "# Database connection",
        "AGENT_ETL_DB_HOST=localhost",
        "AGENT_ETL_DB_PORT=5432",
        f"AGENT_ETL_DB_NAME=agent_etl_{environment}",
        "AGENT_ETL_DB_USER=agent_etl",
        "AGENT_ETL_DB_PASSWORD=change_me_in_production",
        "AGENT_ETL_DB_SSL_MODE=require",
        "",
        "# S3 configuration",
        "AGENT_ETL_S3_REGION=us-east-1",
        "AGENT_ETL_S3_ACCESS_KEY_ID=your_access_key_here",
        "AGENT_ETL_S3_SECRET_ACCESS_KEY=your_secret_key_here",
        "# AGENT_ETL_S3_SESSION_TOKEN=optional_session_token",
        "# AGENT_ETL_S3_ENDPOINT_URL=https://s3-compatible-service.com",
        "",
        "# External API configuration",
        "AGENT_ETL_API_BASE_URL=https://api.example.com",
        "AGENT_ETL_API_KEY=your_api_key_here",
        "AGENT_ETL_API_TIMEOUT=30",
        "",
        "# Logging configuration",
        f"AGENT_ETL_LOG_LEVEL={'INFO' if environment == 'production' else 'DEBUG'}",
        "AGENT_ETL_LOG_FORMAT=json",
        f"AGENT_ETL_LOG_OUTPUT={'file' if environment == 'production' else 'console'}",
        "# AGENT_ETL_LOG_FILE=/var/log/agent-etl/app.log",
        "",
        "# Directory paths",
        "AGENT_ETL_TEMP_DIR=/tmp/agent_etl",
        f"AGENT_ETL_DATA_DIR={'./data' if environment == 'development' else '/opt/agent_etl/data'}",
        f"AGENT_ETL_OUTPUT_DIR={'./output' if environment == 'development' else '/opt/agent_etl/output'}",
    ]
    
    if environment == "production":
        template_lines.extend([
            "",
            "# Production-specific settings",
            "# Consider using a proper secret management system",
            "# and removing sensitive values from this file",
        ])
    
    return "\n".join(template_lines)


def create_config_package(
    output_dir: Path,
    environments: Optional[list[str]] = None,
    overwrite: bool = False,
) -> None:
    """Create a complete configuration package with templates for all environments.
    
    Args:
        output_dir: Directory to create the configuration package
        environments: List of environments to generate configs for
        overwrite: Whether to overwrite existing files
    """
    if environments is None:
        environments = ["development", "staging", "production"]
    
    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate configuration files for each environment
    for env in environments:
        # YAML config
        yaml_path = output_dir / f"config-{env}.yaml"
        save_config_template(yaml_path, "yaml", env, overwrite)
        
        # JSON config
        json_path = output_dir / f"config-{env}.json"
        save_config_template(json_path, "json", env, overwrite)
        
        # Environment variables
        env_path = output_dir / f".env.{env}"
        if not env_path.exists() or overwrite:
            env_path.write_text(generate_env_template(env), encoding="utf-8")
    
    # Generate Docker Compose files
    for env in environments:
        compose_path = output_dir / f"docker-compose.{env}.yml"
        if not compose_path.exists() or overwrite:
            compose_path.write_text(generate_docker_compose_template(env), encoding="utf-8")
    
    # Create README
    readme_path = output_dir / "README.md"
    if not readme_path.exists() or overwrite:
        readme_content = f"""# Agent-Orchestrated ETL Configuration

This directory contains configuration templates for different environments.

## Files

### Configuration Files
- `config-development.yaml` - Development environment config (YAML)
- `config-staging.yaml` - Staging environment config (YAML)  
- `config-production.yaml` - Production environment config (YAML)
- `config-*.json` - JSON versions of the above

### Environment Variables
- `.env.development` - Development environment variables
- `.env.staging` - Staging environment variables
- `.env.production` - Production environment variables

### Docker Compose
- `docker-compose.development.yml` - Development Docker setup
- `docker-compose.staging.yml` - Staging Docker setup
- `docker-compose.production.yml` - Production Docker setup

## Usage

1. Copy the appropriate config file for your environment
2. Update the values to match your setup
3. Set environment variables or use the .env file
4. Run the application with: `python -m agent_orchestrated_etl --config config-development.yaml`

## Security Notes

- Never commit real secrets to version control
- Use environment variables or a secret management system for sensitive values
- Review all configurations before deploying to production
"""
        readme_path.write_text(readme_content, encoding="utf-8")


# CLI interface for config generation
def main():
    """CLI interface for configuration template generation."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate configuration templates")
    parser.add_argument("--format", choices=["yaml", "json"], default="yaml",
                       help="Output format")
    parser.add_argument("--environment", default="development",
                       help="Target environment")
    parser.add_argument("--output", type=Path, default=Path("config.yaml"),
                       help="Output file path")
    parser.add_argument("--package", type=Path,
                       help="Create complete config package in directory")
    parser.add_argument("--overwrite", action="store_true",
                       help="Overwrite existing files")
    
    args = parser.parse_args()
    
    if args.package:
        create_config_package(args.package, overwrite=args.overwrite)
        print(f"Configuration package created in: {args.package}")
    else:
        save_config_template(args.output, args.format, args.environment, args.overwrite)
        print(f"Configuration template saved to: {args.output}")


if __name__ == "__main__":
    main()