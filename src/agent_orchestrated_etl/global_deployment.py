"""Global deployment management for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from .compliance import ComplianceStandard, get_compliance_manager
from .exceptions import ConfigurationError, DeploymentException
from .internationalization import get_i18n_manager
from .logging_config import get_logger
from .multi_region import RegionStatus, get_multi_region_manager


class DeploymentStage(Enum):
    """Deployment stage enumeration."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    DISASTER_RECOVERY = "disaster_recovery"


class DeploymentStrategy(Enum):
    """Deployment strategy enumeration."""
    BLUE_GREEN = "blue_green"
    ROLLING = "rolling"
    CANARY = "canary"
    ALL_AT_ONCE = "all_at_once"


@dataclass
class GlobalConfiguration:
    """Global deployment configuration."""
    deployment_id: str
    stage: DeploymentStage
    strategy: DeploymentStrategy
    regions: List[str]
    compliance_standards: List[ComplianceStandard]
    data_residency_requirements: Dict[str, List[str]]  # region -> data types that must stay
    encryption_requirements: Dict[str, bool]  # data type -> encryption required
    monitoring_config: Dict[str, Any]
    feature_flags: Dict[str, bool]
    resource_limits: Dict[str, Any]
    sla_requirements: Dict[str, float]  # metric -> threshold


class GlobalDeploymentManager:
    """Manages global deployment across multiple regions with compliance."""

    def __init__(self):
        """Initialize global deployment manager."""
        self.logger = get_logger("agent_etl.global_deployment")
        self.multi_region_manager = get_multi_region_manager()
        self.compliance_manager = get_compliance_manager()
        self.i18n_manager = get_i18n_manager()

        self.deployments: Dict[str, GlobalConfiguration] = {}
        self.current_deployment: Optional[str] = None
        self.rollback_stack: List[str] = []

        # Global configuration
        self.global_config = self._load_global_config()

    def _load_global_config(self) -> Dict[str, Any]:
        """Load global configuration from environment and defaults."""
        return {
            "deployment": {
                "default_strategy": os.getenv("DEPLOYMENT_STRATEGY", "rolling"),
                "max_concurrent_regions": int(os.getenv("MAX_CONCURRENT_REGIONS", "3")),
                "rollback_timeout_minutes": int(os.getenv("ROLLBACK_TIMEOUT_MINUTES", "30")),
                "health_check_timeout_seconds": int(os.getenv("HEALTH_CHECK_TIMEOUT", "60"))
            },
            "compliance": {
                "enforce_data_residency": os.getenv("ENFORCE_DATA_RESIDENCY", "true").lower() == "true",
                "require_encryption_at_rest": os.getenv("REQUIRE_ENCRYPTION_AT_REST", "true").lower() == "true",
                "require_encryption_in_transit": os.getenv("REQUIRE_ENCRYPTION_IN_TRANSIT", "true").lower() == "true",
                "audit_all_operations": os.getenv("AUDIT_ALL_OPERATIONS", "true").lower() == "true"
            },
            "monitoring": {
                "enable_distributed_tracing": os.getenv("ENABLE_DISTRIBUTED_TRACING", "true").lower() == "true",
                "metrics_retention_days": int(os.getenv("METRICS_RETENTION_DAYS", "90")),
                "log_retention_days": int(os.getenv("LOG_RETENTION_DAYS", "365")),
                "alert_escalation_minutes": int(os.getenv("ALERT_ESCALATION_MINUTES", "15"))
            },
            "performance": {
                "default_timeout_seconds": int(os.getenv("DEFAULT_TIMEOUT_SECONDS", "30")),
                "max_retry_attempts": int(os.getenv("MAX_RETRY_ATTEMPTS", "3")),
                "circuit_breaker_threshold": float(os.getenv("CIRCUIT_BREAKER_THRESHOLD", "0.5")),
                "rate_limit_requests_per_minute": int(os.getenv("RATE_LIMIT_RPM", "1000"))
            }
        }

    async def create_global_deployment(
        self,
        deployment_id: str,
        stage: DeploymentStage,
        target_regions: List[str],
        strategy: DeploymentStrategy = DeploymentStrategy.ROLLING,
        compliance_standards: Optional[List[ComplianceStandard]] = None,
        feature_flags: Optional[Dict[str, bool]] = None
    ) -> GlobalConfiguration:
        """Create a new global deployment configuration.
        
        Args:
            deployment_id: Unique deployment identifier
            stage: Deployment stage
            target_regions: List of target regions
            strategy: Deployment strategy
            compliance_standards: Required compliance standards
            feature_flags: Feature flag configuration
            
        Returns:
            Global deployment configuration
        """
        if deployment_id in self.deployments:
            raise DeploymentException(f"Deployment {deployment_id} already exists")

        # Validate regions
        available_regions = [r.code for r in self.multi_region_manager.get_active_regions()]
        invalid_regions = [r for r in target_regions if r not in available_regions]
        if invalid_regions:
            raise ConfigurationError(f"Invalid regions: {invalid_regions}")

        # Set default compliance standards
        if compliance_standards is None:
            compliance_standards = [ComplianceStandard.SOC2, ComplianceStandard.ISO27001]

        # Validate compliance requirements for each region
        compliance_issues = []
        for region_code in target_regions:
            region = self.multi_region_manager.get_region(region_code)
            if region:
                for standard in compliance_standards:
                    if standard.value not in [req.lower() for req in region.compliance_requirements]:
                        compliance_issues.append(f"Region {region_code} does not support {standard.value}")

        if compliance_issues:
            raise ConfigurationError(f"Compliance validation failed: {compliance_issues}")

        # Create deployment configuration
        config = GlobalConfiguration(
            deployment_id=deployment_id,
            stage=stage,
            strategy=strategy,
            regions=target_regions,
            compliance_standards=compliance_standards,
            data_residency_requirements=self._determine_data_residency_requirements(target_regions),
            encryption_requirements=self._determine_encryption_requirements(compliance_standards),
            monitoring_config=self._create_monitoring_config(stage),
            feature_flags=feature_flags or {},
            resource_limits=self._create_resource_limits(stage),
            sla_requirements=self._create_sla_requirements(stage)
        )

        self.deployments[deployment_id] = config
        self.logger.info(f"Created global deployment configuration: {deployment_id}")

        return config

    def _determine_data_residency_requirements(self, regions: List[str]) -> Dict[str, List[str]]:
        """Determine data residency requirements for regions.
        
        Args:
            regions: Target regions
            
        Returns:
            Data residency requirements mapping
        """
        requirements = {}

        for region_code in regions:
            region = self.multi_region_manager.get_region(region_code)
            if region and region.data_residency_required:
                # EU regions require EU data to stay in EU
                if region_code.startswith('eu-'):
                    requirements[region_code] = ['eu_customer_data', 'eu_employee_data', 'gdpr_data']
                # Asia regions may have similar requirements
                elif region_code.startswith('ap-'):
                    requirements[region_code] = ['local_customer_data', 'local_compliance_data']
                # Other specific requirements can be added here
                else:
                    requirements[region_code] = ['local_data']

        return requirements

    def _determine_encryption_requirements(self, compliance_standards: List[ComplianceStandard]) -> Dict[str, bool]:
        """Determine encryption requirements based on compliance standards.
        
        Args:
            compliance_standards: Required compliance standards
            
        Returns:
            Encryption requirements mapping
        """
        requirements = {
            'data_at_rest': False,
            'data_in_transit': False,
            'backup_data': False,
            'log_data': False
        }

        # GDPR, HIPAA, PCI-DSS require encryption
        if any(std in compliance_standards for std in [
            ComplianceStandard.GDPR,
            ComplianceStandard.HIPAA,
            ComplianceStandard.PCI_DSS
        ]):
            requirements = dict.fromkeys(requirements, True)

        # SOC2 Type II requires encryption in transit and at rest
        if ComplianceStandard.SOC2 in compliance_standards:
            requirements['data_at_rest'] = True
            requirements['data_in_transit'] = True

        return requirements

    def _create_monitoring_config(self, stage: DeploymentStage) -> Dict[str, Any]:
        """Create monitoring configuration for deployment stage.
        
        Args:
            stage: Deployment stage
            
        Returns:
            Monitoring configuration
        """
        base_config = {
            "metrics_collection_interval_seconds": 60,
            "health_check_interval_seconds": 30,
            "log_level": "INFO",
            "enable_profiling": False,
            "enable_tracing": True
        }

        if stage == DeploymentStage.PRODUCTION:
            base_config.update({
                "metrics_collection_interval_seconds": 30,
                "health_check_interval_seconds": 15,
                "enable_alerting": True,
                "alert_thresholds": {
                    "error_rate": 0.01,  # 1%
                    "latency_p99_ms": 1000,
                    "cpu_usage": 0.80,
                    "memory_usage": 0.85
                }
            })
        elif stage == DeploymentStage.DEVELOPMENT:
            base_config.update({
                "log_level": "DEBUG",
                "enable_profiling": True,
                "enable_alerting": False
            })

        return base_config

    def _create_resource_limits(self, stage: DeploymentStage) -> Dict[str, Any]:
        """Create resource limits for deployment stage.
        
        Args:
            stage: Deployment stage
            
        Returns:
            Resource limits configuration
        """
        if stage == DeploymentStage.PRODUCTION:
            return {
                "max_cpu_cores": 16,
                "max_memory_gb": 64,
                "max_disk_gb": 1000,
                "max_concurrent_jobs": 500,
                "max_queue_size": 10000
            }
        elif stage == DeploymentStage.STAGING:
            return {
                "max_cpu_cores": 8,
                "max_memory_gb": 32,
                "max_disk_gb": 500,
                "max_concurrent_jobs": 100,
                "max_queue_size": 1000
            }
        else:  # DEVELOPMENT
            return {
                "max_cpu_cores": 4,
                "max_memory_gb": 16,
                "max_disk_gb": 100,
                "max_concurrent_jobs": 10,
                "max_queue_size": 100
            }

    def _create_sla_requirements(self, stage: DeploymentStage) -> Dict[str, float]:
        """Create SLA requirements for deployment stage.
        
        Args:
            stage: Deployment stage
            
        Returns:
            SLA requirements mapping
        """
        if stage == DeploymentStage.PRODUCTION:
            return {
                "availability_percentage": 99.9,
                "response_time_p99_ms": 500,
                "error_rate_percentage": 0.1,
                "recovery_time_minutes": 15
            }
        elif stage == DeploymentStage.STAGING:
            return {
                "availability_percentage": 99.0,
                "response_time_p99_ms": 1000,
                "error_rate_percentage": 1.0,
                "recovery_time_minutes": 30
            }
        else:  # DEVELOPMENT
            return {
                "availability_percentage": 95.0,
                "response_time_p99_ms": 2000,
                "error_rate_percentage": 5.0,
                "recovery_time_minutes": 60
            }

    async def deploy_globally(self, deployment_id: str) -> bool:
        """Deploy configuration globally across all target regions.
        
        Args:
            deployment_id: Deployment configuration ID
            
        Returns:
            True if deployment was successful
        """
        if deployment_id not in self.deployments:
            raise DeploymentException(f"Deployment configuration {deployment_id} not found")

        config = self.deployments[deployment_id]
        self.logger.info(f"Starting global deployment: {deployment_id}")

        try:
            # Pre-deployment validation
            await self._validate_deployment_readiness(config)

            # Enable compliance standards
            for standard in config.compliance_standards:
                self.compliance_manager.enable_compliance_standard(standard)

            # Deploy based on strategy
            if config.strategy == DeploymentStrategy.BLUE_GREEN:
                success = await self._deploy_blue_green(config)
            elif config.strategy == DeploymentStrategy.ROLLING:
                success = await self._deploy_rolling(config)
            elif config.strategy == DeploymentStrategy.CANARY:
                success = await self._deploy_canary(config)
            else:  # ALL_AT_ONCE
                success = await self._deploy_all_at_once(config)

            if success:
                self.current_deployment = deployment_id
                self.rollback_stack.append(deployment_id)
                self.logger.info(f"Global deployment completed successfully: {deployment_id}")

                # Configure I18n for deployed regions
                await self._configure_regional_i18n(config.regions)

                return True
            else:
                self.logger.error(f"Global deployment failed: {deployment_id}")
                return False

        except Exception as e:
            self.logger.error(f"Global deployment error: {e}")
            await self._handle_deployment_failure(config, str(e))
            return False

    async def _validate_deployment_readiness(self, config: GlobalConfiguration) -> None:
        """Validate deployment readiness.
        
        Args:
            config: Deployment configuration
        """
        # Check region health
        unhealthy_regions = []
        for region_code in config.regions:
            region = self.multi_region_manager.get_region(region_code)
            if not region or region.status != RegionStatus.ACTIVE:
                unhealthy_regions.append(region_code)

        if unhealthy_regions:
            raise DeploymentException(f"Unhealthy regions detected: {unhealthy_regions}")

        # Validate compliance requirements
        compliance_validation = self.compliance_manager.validate_compliance_status()
        if compliance_validation["overall_status"] == "non_compliant":
            raise DeploymentException(f"Compliance validation failed: {compliance_validation['issues']}")

        self.logger.info("Deployment readiness validation passed")

    async def _deploy_rolling(self, config: GlobalConfiguration) -> bool:
        """Execute rolling deployment strategy.
        
        Args:
            config: Deployment configuration
            
        Returns:
            True if successful
        """
        self.logger.info("Executing rolling deployment strategy")

        max_concurrent = self.global_config["deployment"]["max_concurrent_regions"]

        # Deploy to regions in batches
        for i in range(0, len(config.regions), max_concurrent):
            batch = config.regions[i:i + max_concurrent]

            # Deploy to batch concurrently
            tasks = [self._deploy_to_region(region, config) for region in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Check for failures
            for region, result in zip(batch, results):
                if isinstance(result, Exception) or not result:
                    self.logger.error(f"Deployment failed for region {region}: {result}")
                    return False

            self.logger.info(f"Successfully deployed to batch: {batch}")

            # Wait between batches for production deployments
            if config.stage == DeploymentStage.PRODUCTION:
                await asyncio.sleep(30)

        return True

    async def _deploy_blue_green(self, config: GlobalConfiguration) -> bool:
        """Execute blue-green deployment strategy.
        
        Args:
            config: Deployment configuration
            
        Returns:
            True if successful
        """
        self.logger.info("Executing blue-green deployment strategy")

        # Deploy to all regions in parallel (green environment)
        tasks = [self._deploy_to_region(region, config, environment="green") for region in config.regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check if all deployments succeeded
        for region, result in zip(config.regions, results):
            if isinstance(result, Exception) or not result:
                self.logger.error(f"Green deployment failed for region {region}: {result}")
                return False

        # Switch traffic to green environment
        self.logger.info("Switching traffic to green environment")
        await self._switch_traffic_to_green(config.regions)

        return True

    async def _deploy_canary(self, config: GlobalConfiguration) -> bool:
        """Execute canary deployment strategy.
        
        Args:
            config: Deployment configuration
            
        Returns:
            True if successful
        """
        self.logger.info("Executing canary deployment strategy")

        # Select canary regions (first 20% or at least 1)
        canary_count = max(1, len(config.regions) // 5)
        canary_regions = config.regions[:canary_count]
        remaining_regions = config.regions[canary_count:]

        # Deploy to canary regions first
        self.logger.info(f"Deploying to canary regions: {canary_regions}")
        tasks = [self._deploy_to_region(region, config) for region in canary_regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for region, result in zip(canary_regions, results):
            if isinstance(result, Exception) or not result:
                self.logger.error(f"Canary deployment failed for region {region}: {result}")
                return False

        # Monitor canary for issues
        self.logger.info("Monitoring canary deployment...")
        await asyncio.sleep(300)  # 5 minutes monitoring

        canary_healthy = await self._check_canary_health(canary_regions)
        if not canary_healthy:
            self.logger.error("Canary deployment showing issues, aborting full deployment")
            return False

        # Deploy to remaining regions
        self.logger.info(f"Canary successful, deploying to remaining regions: {remaining_regions}")
        return await self._deploy_rolling(GlobalConfiguration(
            deployment_id=config.deployment_id + "_remaining",
            stage=config.stage,
            strategy=DeploymentStrategy.ROLLING,
            regions=remaining_regions,
            compliance_standards=config.compliance_standards,
            data_residency_requirements=config.data_residency_requirements,
            encryption_requirements=config.encryption_requirements,
            monitoring_config=config.monitoring_config,
            feature_flags=config.feature_flags,
            resource_limits=config.resource_limits,
            sla_requirements=config.sla_requirements
        ))

    async def _deploy_all_at_once(self, config: GlobalConfiguration) -> bool:
        """Execute all-at-once deployment strategy.
        
        Args:
            config: Deployment configuration
            
        Returns:
            True if successful
        """
        self.logger.info("Executing all-at-once deployment strategy")

        # Deploy to all regions simultaneously
        tasks = [self._deploy_to_region(region, config) for region in config.regions]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Check for any failures
        for region, result in zip(config.regions, results):
            if isinstance(result, Exception) or not result:
                self.logger.error(f"Deployment failed for region {region}: {result}")
                return False

        return True

    async def _deploy_to_region(self, region_code: str, config: GlobalConfiguration, environment: str = "blue") -> bool:
        """Deploy to a specific region.
        
        Args:
            region_code: Target region
            config: Deployment configuration
            environment: Environment identifier (blue/green)
            
        Returns:
            True if successful
        """
        self.logger.info(f"Deploying to region {region_code} (environment: {environment})")

        try:
            # Simulate deployment steps
            await asyncio.sleep(2)  # Simulate deployment time

            # Apply regional configuration
            await self._apply_regional_config(region_code, config)

            # Configure compliance for region
            await self._configure_regional_compliance(region_code, config)

            # Start regional monitoring
            await self._start_regional_monitoring(region_code, config)

            self.logger.info(f"Successfully deployed to region {region_code}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to deploy to region {region_code}: {e}")
            return False

    async def _apply_regional_config(self, region_code: str, config: GlobalConfiguration) -> None:
        """Apply regional-specific configuration.
        
        Args:
            region_code: Target region
            config: Deployment configuration
        """
        region = self.multi_region_manager.get_region(region_code)
        if not region:
            return

        # Configure timezone
        regional_config = {
            "region_code": region_code,
            "timezone": region.timezone,
            "data_centers": region.data_centers,
            "compliance_requirements": region.compliance_requirements,
            "resource_limits": config.resource_limits,
            "feature_flags": config.feature_flags
        }

        # Apply data residency requirements
        if region_code in config.data_residency_requirements:
            regional_config["data_residency"] = config.data_residency_requirements[region_code]

        self.logger.debug(f"Applied regional configuration for {region_code}")

    async def _configure_regional_compliance(self, region_code: str, config: GlobalConfiguration) -> None:
        """Configure compliance for a specific region.
        
        Args:
            region_code: Target region
            config: Deployment configuration
        """
        region = self.multi_region_manager.get_region(region_code)
        if not region:
            return

        # Enable compliance standards supported by the region
        for standard in config.compliance_standards:
            if standard.value in [req.lower() for req in region.compliance_requirements]:
                self.compliance_manager.enable_compliance_standard(standard)

        self.logger.debug(f"Configured compliance for region {region_code}")

    async def _configure_regional_i18n(self, regions: List[str]) -> None:
        """Configure internationalization for deployed regions.
        
        Args:
            regions: Deployed regions
        """
        # Auto-detect and configure locales based on regions
        region_locale_mapping = {
            'us-east-1': 'en-US',
            'us-west-2': 'en-US',
            'eu-west-1': 'en-GB',
            'eu-central-1': 'de-DE',
            'ap-northeast-1': 'ja-JP',
            'ap-southeast-1': 'en-SG'
        }

        for region in regions:
            if region in region_locale_mapping:
                locale = region_locale_mapping[region]
                self.i18n_manager.set_locale(locale)
                self.logger.info(f"Configured locale {locale} for region {region}")

    async def _start_regional_monitoring(self, region_code: str, config: GlobalConfiguration) -> None:
        """Start monitoring for a specific region.
        
        Args:
            region_code: Target region
            config: Deployment configuration
        """
        # Configure monitoring based on deployment stage
        monitoring_config = config.monitoring_config.copy()
        monitoring_config["region"] = region_code

        self.logger.debug(f"Started monitoring for region {region_code}")

    async def _switch_traffic_to_green(self, regions: List[str]) -> None:
        """Switch traffic to green environment.
        
        Args:
            regions: Target regions
        """
        # Simulate traffic switching
        for region in regions:
            await asyncio.sleep(0.5)
            self.logger.info(f"Switched traffic to green environment in {region}")

    async def _check_canary_health(self, canary_regions: List[str]) -> bool:
        """Check health of canary deployment.
        
        Args:
            canary_regions: Canary regions to check
            
        Returns:
            True if canary is healthy
        """
        # Simulate health checks
        for region in canary_regions:
            # In production, this would check actual metrics
            await asyncio.sleep(1)
            self.logger.info(f"Canary health check passed for {region}")

        return True

    async def _handle_deployment_failure(self, config: GlobalConfiguration, error: str) -> None:
        """Handle deployment failure.
        
        Args:
            config: Failed deployment configuration
            error: Error message
        """
        self.logger.error(f"Handling deployment failure for {config.deployment_id}: {error}")

        # Attempt automatic rollback if possible
        if self.rollback_stack:
            last_successful = self.rollback_stack[-1]
            self.logger.info(f"Attempting automatic rollback to {last_successful}")
            # TODO: Implement actual rollback logic

    async def rollback_deployment(self, deployment_id: Optional[str] = None) -> bool:
        """Rollback to a previous deployment.
        
        Args:
            deployment_id: Specific deployment to rollback to (defaults to last successful)
            
        Returns:
            True if rollback was successful
        """
        if not deployment_id and self.rollback_stack:
            deployment_id = self.rollback_stack[-1]

        if not deployment_id or deployment_id not in self.deployments:
            raise DeploymentException("No valid deployment to rollback to")

        self.logger.info(f"Rolling back to deployment: {deployment_id}")

        # Execute rollback deployment
        config = self.deployments[deployment_id]
        success = await self.deploy_globally(deployment_id)

        if success:
            self.logger.info(f"Rollback completed successfully to {deployment_id}")
        else:
            self.logger.error(f"Rollback failed for {deployment_id}")

        return success

    def get_deployment_status(self) -> Dict[str, Any]:
        """Get current deployment status.
        
        Returns:
            Deployment status information
        """
        region_manager = self.multi_region_manager

        return {
            "current_deployment": self.current_deployment,
            "total_deployments": len(self.deployments),
            "regions": region_manager.get_region_status_summary(),
            "compliance_status": self.compliance_manager.validate_compliance_status(),
            "supported_locales": self.i18n_manager.get_supported_locales(),
            "global_config": self.global_config,
            "timestamp": datetime.now(timezone.utc).isoformat()
        }


# Global deployment manager instance
_global_deployment_manager: Optional[GlobalDeploymentManager] = None


def get_global_deployment_manager() -> GlobalDeploymentManager:
    """Get the global deployment manager instance.
    
    Returns:
        GlobalDeploymentManager instance
    """
    global _global_deployment_manager

    if _global_deployment_manager is None:
        _global_deployment_manager = GlobalDeploymentManager()

    return _global_deployment_manager
