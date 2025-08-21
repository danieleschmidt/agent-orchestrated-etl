"""Multi-region support for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from .exceptions import ConfigurationError
from .logging_config import get_logger


class RegionStatus(Enum):
    """Region status enumeration."""
    ACTIVE = "active"
    INACTIVE = "inactive"
    DEGRADED = "degraded"
    MAINTENANCE = "maintenance"
    FAILOVER = "failover"


@dataclass
class RegionConfig:
    """Configuration for a specific region."""
    name: str
    code: str
    endpoint: str
    data_centers: List[str]
    timezone: str
    backup_regions: List[str]
    compliance_requirements: List[str]
    data_residency_required: bool = False
    encryption_required: bool = True
    status: RegionStatus = RegionStatus.ACTIVE
    priority: int = 1  # Lower number = higher priority
    capacity_limits: Optional[Dict[str, int]] = None


@dataclass
class RegionMetrics:
    """Metrics for a specific region."""
    region_code: str
    latency_ms: float
    throughput_rps: float
    error_rate: float
    cpu_usage: float
    memory_usage: float
    disk_usage: float
    active_connections: int
    last_updated: float


class MultiRegionManager:
    """Manages multi-region deployment and operations."""

    def __init__(self):
        """Initialize multi-region manager."""
        self.logger = get_logger("agent_etl.multi_region")
        self.regions: Dict[str, RegionConfig] = {}
        self.region_metrics: Dict[str, RegionMetrics] = {}
        self.primary_region: Optional[str] = None
        self.failover_regions: List[str] = []
        self._health_check_interval = 30  # seconds
        self._monitoring_task: Optional[asyncio.Task] = None

        # Initialize default regions
        self._initialize_default_regions()

    def _initialize_default_regions(self) -> None:
        """Initialize default region configurations."""
        default_regions = [
            RegionConfig(
                name="US East (N. Virginia)",
                code="us-east-1",
                endpoint="https://us-east-1.agent-etl.com",
                data_centers=["use1-az1", "use1-az2", "use1-az3"],
                timezone="America/New_York",
                backup_regions=["us-west-2", "eu-west-1"],
                compliance_requirements=["SOC2", "HIPAA", "PCI-DSS"],
                data_residency_required=False,
                priority=1,
                capacity_limits={"max_pipelines": 1000, "max_concurrent_jobs": 500}
            ),
            RegionConfig(
                name="US West (Oregon)",
                code="us-west-2",
                endpoint="https://us-west-2.agent-etl.com",
                data_centers=["usw2-az1", "usw2-az2", "usw2-az3"],
                timezone="America/Los_Angeles",
                backup_regions=["us-east-1", "ap-northeast-1"],
                compliance_requirements=["SOC2", "CCPA"],
                data_residency_required=False,
                priority=2,
                capacity_limits={"max_pipelines": 800, "max_concurrent_jobs": 400}
            ),
            RegionConfig(
                name="Europe (Ireland)",
                code="eu-west-1",
                endpoint="https://eu-west-1.agent-etl.com",
                data_centers=["euw1-az1", "euw1-az2", "euw1-az3"],
                timezone="Europe/Dublin",
                backup_regions=["eu-central-1", "us-east-1"],
                compliance_requirements=["GDPR", "ISO27001", "SOC2"],
                data_residency_required=True,
                priority=1,
                capacity_limits={"max_pipelines": 600, "max_concurrent_jobs": 300}
            ),
            RegionConfig(
                name="Europe (Frankfurt)",
                code="eu-central-1",
                endpoint="https://eu-central-1.agent-etl.com",
                data_centers=["euc1-az1", "euc1-az2", "euc1-az3"],
                timezone="Europe/Berlin",
                backup_regions=["eu-west-1", "ap-southeast-1"],
                compliance_requirements=["GDPR", "ISO27001"],
                data_residency_required=True,
                priority=2,
                capacity_limits={"max_pipelines": 500, "max_concurrent_jobs": 250}
            ),
            RegionConfig(
                name="Asia Pacific (Tokyo)",
                code="ap-northeast-1",
                endpoint="https://ap-northeast-1.agent-etl.com",
                data_centers=["apne1-az1", "apne1-az2", "apne1-az3"],
                timezone="Asia/Tokyo",
                backup_regions=["ap-southeast-1", "us-west-2"],
                compliance_requirements=["ISO27001", "SOC2"],
                data_residency_required=True,
                priority=1,
                capacity_limits={"max_pipelines": 400, "max_concurrent_jobs": 200}
            ),
            RegionConfig(
                name="Asia Pacific (Singapore)",
                code="ap-southeast-1",
                endpoint="https://ap-southeast-1.agent-etl.com",
                data_centers=["apse1-az1", "apse1-az2", "apse1-az3"],
                timezone="Asia/Singapore",
                backup_regions=["ap-northeast-1", "eu-central-1"],
                compliance_requirements=["ISO27001", "PDPA"],
                data_residency_required=True,
                priority=2,
                capacity_limits={"max_pipelines": 300, "max_concurrent_jobs": 150}
            )
        ]

        for region in default_regions:
            self.regions[region.code] = region

        # Set primary region
        self.primary_region = "us-east-1"
        self.failover_regions = ["us-west-2", "eu-west-1"]

        self.logger.info(f"Initialized {len(default_regions)} default regions")

    def add_region(self, region_config: RegionConfig) -> None:
        """Add a new region configuration.
        
        Args:
            region_config: Region configuration to add
        """
        if region_config.code in self.regions:
            self.logger.warning(f"Region {region_config.code} already exists, updating configuration")

        self.regions[region_config.code] = region_config
        self.logger.info(f"Added region: {region_config.name} ({region_config.code})")

    def remove_region(self, region_code: str) -> bool:
        """Remove a region configuration.
        
        Args:
            region_code: Region code to remove
            
        Returns:
            True if region was removed, False if not found
        """
        if region_code not in self.regions:
            return False

        # Don't allow removing primary region
        if region_code == self.primary_region:
            raise ConfigurationError("Cannot remove primary region")

        del self.regions[region_code]

        # Remove from failover regions if present
        if region_code in self.failover_regions:
            self.failover_regions.remove(region_code)

        self.logger.info(f"Removed region: {region_code}")
        return True

    def set_primary_region(self, region_code: str) -> None:
        """Set the primary region.
        
        Args:
            region_code: Region code to set as primary
            
        Raises:
            ConfigurationError: If region doesn't exist or is not active
        """
        if region_code not in self.regions:
            raise ConfigurationError(f"Region {region_code} not found")

        region = self.regions[region_code]
        if region.status != RegionStatus.ACTIVE:
            raise ConfigurationError(f"Cannot set non-active region {region_code} as primary")

        old_primary = self.primary_region
        self.primary_region = region_code

        self.logger.info(f"Primary region changed from {old_primary} to {region_code}")

    def get_region(self, region_code: str) -> Optional[RegionConfig]:
        """Get region configuration.
        
        Args:
            region_code: Region code to get
            
        Returns:
            Region configuration or None if not found
        """
        return self.regions.get(region_code)

    def get_active_regions(self) -> List[RegionConfig]:
        """Get all active regions.
        
        Returns:
            List of active region configurations
        """
        return [
            region for region in self.regions.values()
            if region.status == RegionStatus.ACTIVE
        ]

    def get_regions_by_compliance(self, requirement: str) -> List[RegionConfig]:
        """Get regions that meet a specific compliance requirement.
        
        Args:
            requirement: Compliance requirement (e.g., 'GDPR', 'HIPAA')
            
        Returns:
            List of compliant region configurations
        """
        return [
            region for region in self.regions.values()
            if requirement in region.compliance_requirements
            and region.status == RegionStatus.ACTIVE
        ]

    def get_regions_with_data_residency(self) -> List[RegionConfig]:
        """Get regions that require data residency.
        
        Returns:
            List of regions with data residency requirements
        """
        return [
            region for region in self.regions.values()
            if region.data_residency_required
            and region.status == RegionStatus.ACTIVE
        ]

    def select_optimal_region(
        self,
        user_location: Optional[str] = None,
        compliance_requirements: Optional[List[str]] = None,
        data_residency: bool = False
    ) -> Optional[str]:
        """Select the optimal region based on criteria.
        
        Args:
            user_location: User location/timezone preference
            compliance_requirements: Required compliance standards
            data_residency: Whether data residency is required
            
        Returns:
            Optimal region code or None if no suitable region found
        """
        candidates = self.get_active_regions()

        # Filter by compliance requirements
        if compliance_requirements:
            candidates = [
                region for region in candidates
                if all(req in region.compliance_requirements for req in compliance_requirements)
            ]

        # Filter by data residency requirement
        if data_residency:
            candidates = [region for region in candidates if region.data_residency_required]

        if not candidates:
            self.logger.warning("No regions meet the specified criteria")
            return None

        # Sort by priority and select best
        candidates.sort(key=lambda r: (r.priority, r.code))

        # Consider latency if we have metrics
        if self.region_metrics and user_location:
            # Simple heuristic: prefer regions with lower latency
            candidates_with_metrics = [
                (region, self.region_metrics.get(region.code))
                for region in candidates
            ]
            candidates_with_metrics = [
                (region, metrics) for region, metrics in candidates_with_metrics
                if metrics is not None
            ]

            if candidates_with_metrics:
                candidates_with_metrics.sort(key=lambda x: x[1].latency_ms)
                return candidates_with_metrics[0][0].code

        return candidates[0].code

    async def initiate_failover(self, failed_region: str, reason: str) -> bool:
        """Initiate failover from a failed region.
        
        Args:
            failed_region: Region that failed
            reason: Reason for failover
            
        Returns:
            True if failover was successful
        """
        self.logger.warning(f"Initiating failover from {failed_region}: {reason}")

        if failed_region not in self.regions:
            self.logger.error(f"Cannot failover from unknown region: {failed_region}")
            return False

        # Mark the failed region
        self.regions[failed_region].status = RegionStatus.FAILOVER

        # Select new primary if needed
        if failed_region == self.primary_region:
            # Find best failover candidate
            candidates = [
                region for region_code, region in self.regions.items()
                if (region.status == RegionStatus.ACTIVE and
                    region_code != failed_region and
                    region_code in self.failover_regions)
            ]

            if not candidates:
                # Fallback to any active region
                candidates = [
                    region for region_code, region in self.regions.items()
                    if region.status == RegionStatus.ACTIVE and region_code != failed_region
                ]

            if candidates:
                # Select highest priority (lowest number) candidate
                candidates.sort(key=lambda r: r.priority)
                new_primary = candidates[0].code

                self.logger.info(f"Failing over primary region from {failed_region} to {new_primary}")
                self.primary_region = new_primary

                # TODO: Implement actual failover logic (DNS updates, load balancer changes, etc.)
                await self._perform_region_failover(failed_region, new_primary)

                return True
            else:
                self.logger.critical("No available regions for failover!")
                return False

        return True

    async def _perform_region_failover(self, from_region: str, to_region: str) -> None:
        """Perform the actual failover operations.
        
        Args:
            from_region: Source region
            to_region: Target region
        """
        # Simulate failover operations
        self.logger.info(f"Performing failover operations from {from_region} to {to_region}")

        # Simulate DNS update
        await asyncio.sleep(1)
        self.logger.info("DNS records updated")

        # Simulate load balancer reconfiguration
        await asyncio.sleep(0.5)
        self.logger.info("Load balancer reconfigured")

        # Simulate data replication verification
        await asyncio.sleep(2)
        self.logger.info("Data replication verified")

        self.logger.info(f"Failover completed: {from_region} -> {to_region}")

    def update_region_metrics(self, region_code: str, metrics: RegionMetrics) -> None:
        """Update metrics for a region.
        
        Args:
            region_code: Region code
            metrics: Updated metrics
        """
        self.region_metrics[region_code] = metrics

        # Check for automatic actions based on metrics
        self._evaluate_region_health(region_code, metrics)

    def _evaluate_region_health(self, region_code: str, metrics: RegionMetrics) -> None:
        """Evaluate region health and trigger actions if needed.
        
        Args:
            region_code: Region code
            metrics: Current metrics
        """
        region = self.regions.get(region_code)
        if not region:
            return

        # Define health thresholds
        error_threshold = 0.05  # 5% error rate
        latency_threshold = 1000  # 1 second
        cpu_threshold = 0.90  # 90% CPU usage
        memory_threshold = 0.85  # 85% memory usage

        issues = []

        if metrics.error_rate > error_threshold:
            issues.append(f"High error rate: {metrics.error_rate:.2%}")

        if metrics.latency_ms > latency_threshold:
            issues.append(f"High latency: {metrics.latency_ms}ms")

        if metrics.cpu_usage > cpu_threshold:
            issues.append(f"High CPU usage: {metrics.cpu_usage:.1%}")

        if metrics.memory_usage > memory_threshold:
            issues.append(f"High memory usage: {metrics.memory_usage:.1%}")

        if issues:
            if len(issues) >= 2 or metrics.error_rate > 0.10:
                # Mark as degraded
                if region.status == RegionStatus.ACTIVE:
                    region.status = RegionStatus.DEGRADED
                    self.logger.warning(f"Region {region_code} marked as degraded: {', '.join(issues)}")
            else:
                self.logger.warning(f"Region {region_code} health issues: {', '.join(issues)}")
        else:
            # Region is healthy, restore if it was degraded
            if region.status == RegionStatus.DEGRADED:
                region.status = RegionStatus.ACTIVE
                self.logger.info(f"Region {region_code} restored to active status")

    async def start_monitoring(self) -> None:
        """Start region health monitoring."""
        if self._monitoring_task and not self._monitoring_task.done():
            self.logger.warning("Region monitoring already running")
            return

        self._monitoring_task = asyncio.create_task(self._monitoring_loop())
        self.logger.info("Started region health monitoring")

    async def stop_monitoring(self) -> None:
        """Stop region health monitoring."""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            self._monitoring_task = None
            self.logger.info("Stopped region health monitoring")

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while True:
            try:
                await self._check_all_regions()
                await asyncio.sleep(self._health_check_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(5)  # Brief pause before retrying

    async def _check_all_regions(self) -> None:
        """Check health of all regions."""
        for region_code in self.regions:
            try:
                await self._check_region_health(region_code)
            except Exception as e:
                self.logger.error(f"Health check failed for region {region_code}: {e}")

    async def _check_region_health(self, region_code: str) -> None:
        """Check health of a specific region.
        
        Args:
            region_code: Region to check
        """
        # Simulate health check (in production, this would make actual HTTP requests)
        await asyncio.sleep(0.1)

        # Simulate metrics
        import random
        metrics = RegionMetrics(
            region_code=region_code,
            latency_ms=random.uniform(50, 200),
            throughput_rps=random.uniform(100, 1000),
            error_rate=random.uniform(0, 0.02),
            cpu_usage=random.uniform(0.3, 0.8),
            memory_usage=random.uniform(0.4, 0.7),
            disk_usage=random.uniform(0.2, 0.6),
            active_connections=random.randint(50, 500),
            last_updated=time.time()
        )

        self.update_region_metrics(region_code, metrics)

    def get_region_status_summary(self) -> Dict[str, Any]:
        """Get a summary of all region statuses.
        
        Returns:
            Region status summary
        """
        active_count = sum(1 for r in self.regions.values() if r.status == RegionStatus.ACTIVE)
        degraded_count = sum(1 for r in self.regions.values() if r.status == RegionStatus.DEGRADED)
        inactive_count = sum(1 for r in self.regions.values() if r.status == RegionStatus.INACTIVE)

        return {
            "total_regions": len(self.regions),
            "active_regions": active_count,
            "degraded_regions": degraded_count,
            "inactive_regions": inactive_count,
            "primary_region": self.primary_region,
            "failover_regions": self.failover_regions,
            "regions": {
                code: {
                    "name": region.name,
                    "status": region.status.value,
                    "priority": region.priority,
                    "data_residency_required": region.data_residency_required,
                    "compliance": region.compliance_requirements,
                    "metrics": self.region_metrics.get(code).__dict__ if code in self.region_metrics else None
                }
                for code, region in self.regions.items()
            }
        }


# Global multi-region manager instance
_multi_region_manager: Optional[MultiRegionManager] = None


def get_multi_region_manager() -> MultiRegionManager:
    """Get the global multi-region manager instance.
    
    Returns:
        MultiRegionManager instance
    """
    global _multi_region_manager

    if _multi_region_manager is None:
        _multi_region_manager = MultiRegionManager()

    return _multi_region_manager
