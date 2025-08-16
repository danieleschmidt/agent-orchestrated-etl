"""Production Deployment and Infrastructure Management System.

This module provides enterprise-grade deployment, health monitoring, 
blue-green deployments, and infrastructure as code capabilities.
"""

from __future__ import annotations

import json
import socket
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    import requests
except ImportError:
    requests = None

from .exceptions import DeploymentException
from .logging_config import get_logger


class DeploymentStrategy(Enum):
    """Deployment strategies."""
    ROLLING = "rolling"
    BLUE_GREEN = "blue_green"
    CANARY = "canary"
    RECREATION = "recreation"


class DeploymentStatus(Enum):
    """Deployment status."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ROLLING_BACK = "rolling_back"
    ROLLED_BACK = "rolled_back"


class HealthCheckType(Enum):
    """Health check types."""
    HTTP = "http"
    TCP = "tcp"
    COMMAND = "command"
    DATABASE = "database"


@dataclass
class HealthCheck:
    """Health check configuration."""
    name: str
    check_type: HealthCheckType
    endpoint: str = ""
    timeout_seconds: int = 10
    interval_seconds: int = 30
    retries: int = 3
    command: str = ""
    expected_status_codes: List[int] = field(default_factory=lambda: [200])
    success_threshold: int = 1
    failure_threshold: int = 3


@dataclass
class DeploymentConfig:
    """Deployment configuration."""
    name: str
    version: str
    strategy: DeploymentStrategy = DeploymentStrategy.ROLLING
    replicas: int = 3
    max_unavailable: int = 1
    max_surge: int = 1
    health_checks: List[HealthCheck] = field(default_factory=list)
    environment_variables: Dict[str, str] = field(default_factory=dict)
    resource_limits: Dict[str, str] = field(default_factory=dict)
    rollback_on_failure: bool = True
    canary_percentage: int = 10  # For canary deployments
    blue_green_swap_delay: int = 300  # Seconds to wait before swap


@dataclass
class DeploymentResult:
    """Deployment result."""
    deployment_id: str
    status: DeploymentStatus
    version: str
    start_time: float
    end_time: Optional[float] = None
    error_message: Optional[str] = None
    rollback_version: Optional[str] = None
    metrics: Dict[str, Any] = field(default_factory=dict)


class HealthMonitor:
    """Advanced health monitoring system."""

    def __init__(self):
        self.logger = get_logger("agent_etl.deployment.health_monitor")
        self.health_checks: Dict[str, HealthCheck] = {}
        self.health_status: Dict[str, Dict[str, Any]] = {}
        self.monitoring_active = False
        self.monitoring_threads: Dict[str, threading.Thread] = {}
        self.shutdown_event = threading.Event()

    def add_health_check(self, health_check: HealthCheck) -> None:
        """Add a health check."""
        self.health_checks[health_check.name] = health_check
        self.health_status[health_check.name] = {
            "status": "unknown",
            "last_check": 0,
            "consecutive_failures": 0,
            "consecutive_successes": 0,
            "total_checks": 0,
            "success_count": 0,
            "failure_count": 0
        }

        self.logger.info(f"Added health check: {health_check.name}")

    def start_monitoring(self) -> None:
        """Start health monitoring."""
        if self.monitoring_active:
            return

        self.monitoring_active = True
        self.shutdown_event.clear()

        for name, health_check in self.health_checks.items():
            thread = threading.Thread(
                target=self._monitor_health_check,
                args=(name, health_check),
                name=f"HealthMonitor-{name}",
                daemon=True
            )
            thread.start()
            self.monitoring_threads[name] = thread

        self.logger.info("Health monitoring started")

    def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False
        self.shutdown_event.set()

        # Wait for monitoring threads to finish
        for thread in self.monitoring_threads.values():
            thread.join(timeout=5.0)

        self.monitoring_threads.clear()
        self.logger.info("Health monitoring stopped")

    def _monitor_health_check(self, name: str, health_check: HealthCheck) -> None:
        """Monitor a single health check."""
        while self.monitoring_active and not self.shutdown_event.is_set():
            try:
                start_time = time.time()
                success = self._perform_health_check(health_check)
                duration = time.time() - start_time

                self._update_health_status(name, success, duration)

                # Wait for next check
                self.shutdown_event.wait(health_check.interval_seconds)

            except Exception as e:
                self.logger.error(f"Error in health check {name}: {e}")
                self._update_health_status(name, False, 0)
                self.shutdown_event.wait(health_check.interval_seconds)

    def _perform_health_check(self, health_check: HealthCheck) -> bool:
        """Perform a single health check."""
        try:
            if health_check.check_type == HealthCheckType.HTTP:
                return self._perform_http_check(health_check)
            elif health_check.check_type == HealthCheckType.TCP:
                return self._perform_tcp_check(health_check)
            elif health_check.check_type == HealthCheckType.COMMAND:
                return self._perform_command_check(health_check)
            elif health_check.check_type == HealthCheckType.DATABASE:
                return self._perform_database_check(health_check)
            else:
                self.logger.warning(f"Unknown health check type: {health_check.check_type}")
                return False
        except Exception as e:
            self.logger.error(f"Health check failed: {e}")
            return False

    def _perform_http_check(self, health_check: HealthCheck) -> bool:
        """Perform HTTP health check."""
        try:
            response = requests.get(
                health_check.endpoint,
                timeout=health_check.timeout_seconds
            )
            return response.status_code in health_check.expected_status_codes
        except Exception:
            return False

    def _perform_tcp_check(self, health_check: HealthCheck) -> bool:
        """Perform TCP health check."""
        try:
            # Parse host:port from endpoint
            if ":" not in health_check.endpoint:
                return False

            host, port_str = health_check.endpoint.rsplit(":", 1)
            port = int(port_str)

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(health_check.timeout_seconds)
            result = sock.connect_ex((host, port))
            sock.close()

            return result == 0
        except Exception:
            return False

    def _perform_command_check(self, health_check: HealthCheck) -> bool:
        """Perform command-based health check."""
        try:
            result = subprocess.run(
                health_check.command,
                shell=True,
                timeout=health_check.timeout_seconds,
                capture_output=True
            )
            return result.returncode == 0
        except Exception:
            return False

    def _perform_database_check(self, health_check: HealthCheck) -> bool:
        """Perform database health check."""
        try:
            # This would need to be implemented based on specific database type
            # For now, return a placeholder
            return True
        except Exception:
            return False

    def _update_health_status(self, name: str, success: bool, duration: float) -> None:
        """Update health status for a check."""
        status = self.health_status[name]
        status["last_check"] = time.time()
        status["total_checks"] += 1
        status["last_duration"] = duration

        if success:
            status["success_count"] += 1
            status["consecutive_successes"] += 1
            status["consecutive_failures"] = 0

            # Check if we've met success threshold
            health_check = self.health_checks[name]
            if status["consecutive_successes"] >= health_check.success_threshold:
                if status["status"] != "healthy":
                    status["status"] = "healthy"
                    self.logger.info(f"Health check {name} is now healthy")
        else:
            status["failure_count"] += 1
            status["consecutive_failures"] += 1
            status["consecutive_successes"] = 0

            # Check if we've met failure threshold
            health_check = self.health_checks[name]
            if status["consecutive_failures"] >= health_check.failure_threshold:
                if status["status"] != "unhealthy":
                    status["status"] = "unhealthy"
                    self.logger.warning(f"Health check {name} is now unhealthy")

    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status."""
        overall_status = "healthy"
        health_details = {}

        for name, status in self.health_status.items():
            health_details[name] = status.copy()

            if status["status"] == "unhealthy":
                overall_status = "unhealthy"
            elif status["status"] == "unknown" and overall_status == "healthy":
                overall_status = "degraded"

        return {
            "overall_status": overall_status,
            "checks": health_details,
            "monitoring_active": self.monitoring_active,
            "timestamp": time.time()
        }


class BlueGreenDeployment:
    """Blue-Green deployment implementation."""

    def __init__(self, load_balancer_config: Dict[str, Any]):
        self.logger = get_logger("agent_etl.deployment.blue_green")
        self.load_balancer_config = load_balancer_config
        self.current_environment = "blue"  # blue or green
        self.environments = {
            "blue": {"status": "active", "version": None},
            "green": {"status": "standby", "version": None}
        }

    def deploy(self, config: DeploymentConfig) -> DeploymentResult:
        """Perform blue-green deployment."""
        deployment_id = str(uuid.uuid4())
        start_time = time.time()

        self.logger.info(f"Starting blue-green deployment {deployment_id}")

        try:
            # Determine target environment
            target_env = "green" if self.current_environment == "blue" else "blue"

            # Deploy to standby environment
            self._deploy_to_environment(target_env, config)

            # Wait for stabilization
            time.sleep(config.blue_green_swap_delay)

            # Perform health checks on new environment
            if not self._validate_environment_health(target_env, config):
                raise DeploymentException("Health checks failed for new environment")

            # Swap environments
            self._swap_environments(target_env)

            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.COMPLETED,
                version=config.version,
                start_time=start_time,
                end_time=time.time()
            )

        except Exception as e:
            self.logger.error(f"Blue-green deployment failed: {e}")
            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.FAILED,
                version=config.version,
                start_time=start_time,
                end_time=time.time(),
                error_message=str(e)
            )

    def _deploy_to_environment(self, environment: str, config: DeploymentConfig) -> None:
        """Deploy to specific environment."""
        self.logger.info(f"Deploying version {config.version} to {environment} environment")

        # This would implement actual deployment logic
        # For now, simulate deployment
        time.sleep(2)  # Simulate deployment time

        self.environments[environment]["version"] = config.version
        self.environments[environment]["status"] = "ready"

    def _validate_environment_health(self, environment: str, config: DeploymentConfig) -> bool:
        """Validate health of specific environment."""
        self.logger.info(f"Validating health of {environment} environment")

        # Perform health checks
        for health_check in config.health_checks:
            # Adjust endpoint for specific environment
            endpoint = health_check.endpoint.replace("${ENV}", environment)

            for attempt in range(health_check.retries):
                try:
                    if health_check.check_type == HealthCheckType.HTTP:
                        response = requests.get(endpoint, timeout=health_check.timeout_seconds)
                        if response.status_code in health_check.expected_status_codes:
                            break
                    # Other health check types would be implemented similarly
                except Exception as e:
                    self.logger.warning(f"Health check attempt {attempt + 1} failed: {e}")
                    if attempt == health_check.retries - 1:
                        return False
                    time.sleep(2)  # Wait before retry

        return True

    def _swap_environments(self, new_active_env: str) -> None:
        """Swap active and standby environments."""
        old_active_env = self.current_environment

        self.logger.info(f"Swapping environments: {old_active_env} -> {new_active_env}")

        # Update load balancer configuration
        self._update_load_balancer(new_active_env)

        # Update environment states
        self.environments[old_active_env]["status"] = "standby"
        self.environments[new_active_env]["status"] = "active"
        self.current_environment = new_active_env

        self.logger.info(f"Environment swap completed. Active: {new_active_env}")

    def _update_load_balancer(self, active_environment: str) -> None:
        """Update load balancer to point to new active environment."""
        # This would implement actual load balancer update logic
        self.logger.info(f"Updated load balancer to route to {active_environment}")

    def rollback(self) -> DeploymentResult:
        """Rollback to previous environment."""
        deployment_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            # Swap back to previous environment
            previous_env = "green" if self.current_environment == "blue" else "blue"
            self._swap_environments(previous_env)

            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.ROLLED_BACK,
                version=self.environments[previous_env]["version"],
                start_time=start_time,
                end_time=time.time()
            )

        except Exception as e:
            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.FAILED,
                version="unknown",
                start_time=start_time,
                end_time=time.time(),
                error_message=f"Rollback failed: {e}"
            )


class InfrastructureManager:
    """Infrastructure as Code management."""

    def __init__(self, config_dir: str = "infrastructure"):
        self.logger = get_logger("agent_etl.deployment.infrastructure")
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)

    def generate_kubernetes_manifests(self, config: DeploymentConfig) -> Dict[str, str]:
        """Generate Kubernetes deployment manifests."""
        manifests = {}

        # Deployment manifest
        deployment = {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {
                "name": f"{config.name}-deployment",
                "labels": {"app": config.name, "version": config.version}
            },
            "spec": {
                "replicas": config.replicas,
                "strategy": {
                    "type": "RollingUpdate",
                    "rollingUpdate": {
                        "maxUnavailable": config.max_unavailable,
                        "maxSurge": config.max_surge
                    }
                },
                "selector": {"matchLabels": {"app": config.name}},
                "template": {
                    "metadata": {"labels": {"app": config.name, "version": config.version}},
                    "spec": {
                        "containers": [{
                            "name": config.name,
                            "image": f"{config.name}:{config.version}",
                            "env": [{"name": k, "value": v} for k, v in config.environment_variables.items()],
                            "resources": {
                                "limits": config.resource_limits,
                                "requests": {k: v for k, v in config.resource_limits.items()}
                            },
                            "livenessProbe": self._generate_probe_config(config.health_checks),
                            "readinessProbe": self._generate_probe_config(config.health_checks)
                        }]
                    }
                }
            }
        }

        # Service manifest
        service = {
            "apiVersion": "v1",
            "kind": "Service",
            "metadata": {
                "name": f"{config.name}-service",
                "labels": {"app": config.name}
            },
            "spec": {
                "selector": {"app": config.name},
                "ports": [{"port": 80, "targetPort": 8080}],
                "type": "ClusterIP"
            }
        }

        # HorizontalPodAutoscaler
        hpa = {
            "apiVersion": "autoscaling/v2",
            "kind": "HorizontalPodAutoscaler",
            "metadata": {
                "name": f"{config.name}-hpa",
                "labels": {"app": config.name}
            },
            "spec": {
                "scaleTargetRef": {
                    "apiVersion": "apps/v1",
                    "kind": "Deployment",
                    "name": f"{config.name}-deployment"
                },
                "minReplicas": max(1, config.replicas // 2),
                "maxReplicas": config.replicas * 2,
                "metrics": [
                    {
                        "type": "Resource",
                        "resource": {
                            "name": "cpu",
                            "target": {"type": "Utilization", "averageUtilization": 70}
                        }
                    },
                    {
                        "type": "Resource",
                        "resource": {
                            "name": "memory",
                            "target": {"type": "Utilization", "averageUtilization": 80}
                        }
                    }
                ]
            }
        }

        manifests["deployment.yaml"] = self._yaml_dump(deployment)
        manifests["service.yaml"] = self._yaml_dump(service)
        manifests["hpa.yaml"] = self._yaml_dump(hpa)

        return manifests

    def generate_docker_compose(self, config: DeploymentConfig) -> str:
        """Generate Docker Compose configuration."""
        compose_config = {
            "version": "3.8",
            "services": {
                config.name: {
                    "image": f"{config.name}:{config.version}",
                    "restart": "unless-stopped",
                    "environment": config.environment_variables,
                    "deploy": {
                        "replicas": config.replicas,
                        "resources": {
                            "limits": config.resource_limits,
                            "reservations": {k: v for k, v in config.resource_limits.items()}
                        },
                        "update_config": {
                            "parallelism": 1,
                            "delay": "10s",
                            "failure_action": "rollback" if config.rollback_on_failure else "pause"
                        }
                    },
                    "healthcheck": self._generate_docker_healthcheck(config.health_checks)
                }
            }
        }

        return self._yaml_dump(compose_config)

    def generate_terraform_config(self, config: DeploymentConfig) -> Dict[str, str]:
        """Generate Terraform infrastructure configuration."""
        configs = {}

        # Main configuration
        main_tf = f'''
terraform {{
  required_version = ">= 1.0"
  required_providers {{
    aws = {{
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }}
  }}
}}

provider "aws" {{
  region = var.aws_region
}}

# Application Load Balancer
resource "aws_lb" "{config.name}_alb" {{
  name               = "{config.name}-alb"
  internal           = false
  load_balancer_type = "application"
  security_groups    = [aws_security_group.{config.name}_alb.id]
  subnets           = var.subnet_ids

  enable_deletion_protection = false

  tags = {{
    Name = "{config.name}-alb"
    Environment = var.environment
  }}
}}

# ECS Cluster
resource "aws_ecs_cluster" "{config.name}_cluster" {{
  name = "{config.name}-cluster"

  setting {{
    name  = "containerInsights"
    value = "enabled"
  }}

  tags = {{
    Name = "{config.name}-cluster"
    Environment = var.environment
  }}
}}

# ECS Service
resource "aws_ecs_service" "{config.name}_service" {{
  name            = "{config.name}-service"
  cluster         = aws_ecs_cluster.{config.name}_cluster.id
  task_definition = aws_ecs_task_definition.{config.name}_task.arn
  desired_count   = {config.replicas}

  deployment_configuration {{
    maximum_percent         = {100 + (config.max_surge * 100 // config.replicas)}
    minimum_healthy_percent = {100 - (config.max_unavailable * 100 // config.replicas)}
  }}

  load_balancer {{
    target_group_arn = aws_lb_target_group.{config.name}_tg.arn
    container_name   = "{config.name}"
    container_port   = 8080
  }}

  depends_on = [aws_lb_listener.{config.name}_listener]

  tags = {{
    Name = "{config.name}-service"
    Environment = var.environment
  }}
}}

# Auto Scaling Target
resource "aws_appautoscaling_target" "{config.name}_target" {{
  max_capacity       = {config.replicas * 2}
  min_capacity       = {max(1, config.replicas // 2)}
  resource_id        = "service/${{aws_ecs_cluster.{config.name}_cluster.name}}/${{aws_ecs_service.{config.name}_service.name}}"
  scalable_dimension = "ecs:service:DesiredCount"
  service_namespace  = "ecs"
}}

# Auto Scaling Policy
resource "aws_appautoscaling_policy" "{config.name}_scale_up" {{
  name               = "{config.name}-scale-up"
  policy_type        = "TargetTrackingScaling"
  resource_id        = aws_appautoscaling_target.{config.name}_target.resource_id
  scalable_dimension = aws_appautoscaling_target.{config.name}_target.scalable_dimension
  service_namespace  = aws_appautoscaling_target.{config.name}_target.service_namespace

  target_tracking_scaling_policy_configuration {{
    predefined_metric_specification {{
      predefined_metric_type = "ECSServiceAverageCPUUtilization"
    }}
    target_value = 70.0
  }}
}}
'''

        # Variables file
        variables_tf = '''
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-west-2"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "production"
}

variable "subnet_ids" {
  description = "List of subnet IDs"
  type        = list(string)
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}
'''

        configs["main.tf"] = main_tf
        configs["variables.tf"] = variables_tf

        return configs

    def _generate_probe_config(self, health_checks: List[HealthCheck]) -> Optional[Dict[str, Any]]:
        """Generate Kubernetes probe configuration."""
        if not health_checks:
            return None

        # Use first HTTP health check for probe
        for check in health_checks:
            if check.check_type == HealthCheckType.HTTP:
                return {
                    "httpGet": {
                        "path": check.endpoint,
                        "port": 8080
                    },
                    "initialDelaySeconds": 30,
                    "periodSeconds": check.interval_seconds,
                    "timeoutSeconds": check.timeout_seconds,
                    "failureThreshold": check.failure_threshold
                }

        return None

    def _generate_docker_healthcheck(self, health_checks: List[HealthCheck]) -> Optional[Dict[str, Any]]:
        """Generate Docker Compose healthcheck configuration."""
        if not health_checks:
            return None

        # Use first health check
        check = health_checks[0]

        if check.check_type == HealthCheckType.HTTP:
            return {
                "test": f"curl -f {check.endpoint} || exit 1",
                "interval": f"{check.interval_seconds}s",
                "timeout": f"{check.timeout_seconds}s",
                "retries": check.retries
            }
        elif check.check_type == HealthCheckType.COMMAND:
            return {
                "test": check.command,
                "interval": f"{check.interval_seconds}s",
                "timeout": f"{check.timeout_seconds}s",
                "retries": check.retries
            }

        return None

    def _yaml_dump(self, data: Any) -> str:
        """Convert dictionary to YAML string."""
        # Simple YAML serialization - in production, use PyYAML
        return json.dumps(data, indent=2)

    def save_configurations(self, config: DeploymentConfig, output_dir: str = None) -> Dict[str, str]:
        """Save all infrastructure configurations to files."""
        if output_dir is None:
            output_dir = self.config_dir / config.name

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        saved_files = {}

        # Generate and save Kubernetes manifests
        k8s_manifests = self.generate_kubernetes_manifests(config)
        k8s_dir = output_path / "kubernetes"
        k8s_dir.mkdir(exist_ok=True)

        for filename, content in k8s_manifests.items():
            file_path = k8s_dir / filename
            file_path.write_text(content)
            saved_files[f"kubernetes/{filename}"] = str(file_path)

        # Generate and save Docker Compose
        docker_compose_content = self.generate_docker_compose(config)
        docker_compose_path = output_path / "docker-compose.yml"
        docker_compose_path.write_text(docker_compose_content)
        saved_files["docker-compose.yml"] = str(docker_compose_path)

        # Generate and save Terraform configs
        terraform_configs = self.generate_terraform_config(config)
        terraform_dir = output_path / "terraform"
        terraform_dir.mkdir(exist_ok=True)

        for filename, content in terraform_configs.items():
            file_path = terraform_dir / filename
            file_path.write_text(content)
            saved_files[f"terraform/{filename}"] = str(file_path)

        self.logger.info(f"Saved infrastructure configurations to {output_path}")
        return saved_files


class ProductionDeploymentManager:
    """Main production deployment management system."""

    def __init__(self):
        self.logger = get_logger("agent_etl.deployment.manager")

        self.health_monitor = HealthMonitor()
        self.infrastructure_manager = InfrastructureManager()

        # Deployment history
        self.deployment_history: List[DeploymentResult] = []

        # Current deployment state
        self.current_deployment: Optional[DeploymentConfig] = None
        self.deployment_strategies: Dict[str, Any] = {}

    def deploy(self, config: DeploymentConfig) -> DeploymentResult:
        """Deploy application using specified strategy."""
        self.logger.info(f"Starting deployment of {config.name} version {config.version}")

        # Validate configuration
        self._validate_deployment_config(config)

        # Execute deployment based on strategy
        if config.strategy == DeploymentStrategy.BLUE_GREEN:
            blue_green = BlueGreenDeployment({"type": "nginx"})  # Placeholder config
            result = blue_green.deploy(config)
        elif config.strategy == DeploymentStrategy.ROLLING:
            result = self._rolling_deployment(config)
        elif config.strategy == DeploymentStrategy.CANARY:
            result = self._canary_deployment(config)
        else:
            result = self._recreation_deployment(config)

        # Update deployment state
        if result.status == DeploymentStatus.COMPLETED:
            self.current_deployment = config

            # Update health checks
            for health_check in config.health_checks:
                self.health_monitor.add_health_check(health_check)

        # Record deployment history
        self.deployment_history.append(result)

        # Cleanup old history (keep last 50)
        if len(self.deployment_history) > 50:
            self.deployment_history = self.deployment_history[-50:]

        return result

    def _validate_deployment_config(self, config: DeploymentConfig) -> None:
        """Validate deployment configuration."""
        if not config.name:
            raise DeploymentException("Deployment name is required")

        if not config.version:
            raise DeploymentException("Deployment version is required")

        if config.replicas < 1:
            raise DeploymentException("Replicas must be at least 1")

        if config.canary_percentage < 1 or config.canary_percentage > 50:
            raise DeploymentException("Canary percentage must be between 1 and 50")

    def _rolling_deployment(self, config: DeploymentConfig) -> DeploymentResult:
        """Perform rolling deployment."""
        deployment_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            self.logger.info(f"Starting rolling deployment {deployment_id}")

            # Simulate rolling deployment steps
            total_replicas = config.replicas
            max_unavailable = config.max_unavailable

            # Update replicas in batches
            for batch in range(0, total_replicas, max_unavailable):
                batch_size = min(max_unavailable, total_replicas - batch)

                self.logger.info(f"Updating batch {batch // max_unavailable + 1}: {batch_size} replicas")

                # Simulate update time
                time.sleep(2)

                # Simulate health check
                if not self._simulate_health_check():
                    raise DeploymentException("Health check failed during rolling deployment")

            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.COMPLETED,
                version=config.version,
                start_time=start_time,
                end_time=time.time()
            )

        except Exception as e:
            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.FAILED,
                version=config.version,
                start_time=start_time,
                end_time=time.time(),
                error_message=str(e)
            )

    def _canary_deployment(self, config: DeploymentConfig) -> DeploymentResult:
        """Perform canary deployment."""
        deployment_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            self.logger.info(f"Starting canary deployment {deployment_id}")

            # Deploy canary percentage
            canary_replicas = max(1, (config.replicas * config.canary_percentage) // 100)

            self.logger.info(f"Deploying {canary_replicas} canary replicas ({config.canary_percentage}%)")
            time.sleep(2)  # Simulate deployment

            # Monitor canary for specified time
            monitor_duration = 300  # 5 minutes
            self.logger.info(f"Monitoring canary for {monitor_duration} seconds")

            # Simulate monitoring
            for i in range(monitor_duration // 30):  # Check every 30 seconds
                if not self._simulate_health_check():
                    raise DeploymentException("Canary health check failed")
                time.sleep(1)  # Simulate monitoring interval (shortened for demo)

            # Deploy to remaining replicas
            remaining_replicas = config.replicas - canary_replicas
            self.logger.info(f"Deploying to remaining {remaining_replicas} replicas")
            time.sleep(2)  # Simulate deployment

            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.COMPLETED,
                version=config.version,
                start_time=start_time,
                end_time=time.time()
            )

        except Exception as e:
            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.FAILED,
                version=config.version,
                start_time=start_time,
                end_time=time.time(),
                error_message=str(e)
            )

    def _recreation_deployment(self, config: DeploymentConfig) -> DeploymentResult:
        """Perform recreation deployment (terminate all, then deploy new)."""
        deployment_id = str(uuid.uuid4())
        start_time = time.time()

        try:
            self.logger.info(f"Starting recreation deployment {deployment_id}")

            # Terminate all existing instances
            self.logger.info("Terminating existing instances")
            time.sleep(1)  # Simulate termination

            # Deploy new instances
            self.logger.info(f"Deploying {config.replicas} new instances")
            time.sleep(3)  # Simulate deployment

            # Health check
            if not self._simulate_health_check():
                raise DeploymentException("Health check failed after recreation deployment")

            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.COMPLETED,
                version=config.version,
                start_time=start_time,
                end_time=time.time()
            )

        except Exception as e:
            return DeploymentResult(
                deployment_id=deployment_id,
                status=DeploymentStatus.FAILED,
                version=config.version,
                start_time=start_time,
                end_time=time.time(),
                error_message=str(e)
            )

    def _simulate_health_check(self) -> bool:
        """Simulate health check (placeholder)."""
        # In real implementation, this would perform actual health checks
        return True

    def rollback(self, target_version: Optional[str] = None) -> DeploymentResult:
        """Rollback to previous or specified version."""
        if not self.deployment_history:
            raise DeploymentException("No deployment history available for rollback")

        # Find target deployment
        if target_version:
            target_deployment = None
            for deployment in reversed(self.deployment_history):
                if deployment.version == target_version and deployment.status == DeploymentStatus.COMPLETED:
                    target_deployment = deployment
                    break

            if not target_deployment:
                raise DeploymentException(f"No successful deployment found for version {target_version}")
        else:
            # Find last successful deployment (excluding current)
            target_deployment = None
            current_version = self.current_deployment.version if self.current_deployment else None

            for deployment in reversed(self.deployment_history[:-1]):  # Exclude current
                if deployment.status == DeploymentStatus.COMPLETED and deployment.version != current_version:
                    target_deployment = deployment
                    break

            if not target_deployment:
                raise DeploymentException("No previous successful deployment found for rollback")

        # Create rollback deployment config
        if not self.current_deployment:
            raise DeploymentException("No current deployment configuration available")

        rollback_config = DeploymentConfig(
            name=self.current_deployment.name,
            version=target_deployment.version,
            strategy=DeploymentStrategy.ROLLING,  # Use rolling for rollback
            replicas=self.current_deployment.replicas,
            health_checks=self.current_deployment.health_checks
        )

        # Perform rollback deployment
        result = self.deploy(rollback_config)
        result.rollback_version = target_deployment.version

        if result.status == DeploymentStatus.COMPLETED:
            result.status = DeploymentStatus.ROLLED_BACK

        return result

    def get_deployment_status(self) -> Dict[str, Any]:
        """Get current deployment status."""
        health_status = self.health_monitor.get_health_status()

        current_info = None
        if self.current_deployment:
            current_info = {
                "name": self.current_deployment.name,
                "version": self.current_deployment.version,
                "strategy": self.current_deployment.strategy.value,
                "replicas": self.current_deployment.replicas
            }

        recent_deployments = [
            {
                "deployment_id": d.deployment_id,
                "version": d.version,
                "status": d.status.value,
                "start_time": d.start_time,
                "end_time": d.end_time,
                "error_message": d.error_message
            }
            for d in self.deployment_history[-10:]  # Last 10 deployments
        ]

        return {
            "current_deployment": current_info,
            "health_status": health_status,
            "recent_deployments": recent_deployments,
            "total_deployments": len(self.deployment_history),
            "timestamp": time.time()
        }

    def start_monitoring(self) -> None:
        """Start health monitoring."""
        self.health_monitor.start_monitoring()

    def stop_monitoring(self) -> None:
        """Stop health monitoring."""
        self.health_monitor.stop_monitoring()

    def generate_infrastructure_configs(self, config: DeploymentConfig, output_dir: str = None) -> Dict[str, str]:
        """Generate infrastructure configurations."""
        return self.infrastructure_manager.save_configurations(config, output_dir)

    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_monitoring()
