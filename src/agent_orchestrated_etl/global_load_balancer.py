"""Global multi-region load balancer with intelligent routing."""

from __future__ import annotations

import random
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from .logging_config import get_logger


class LoadBalancingAlgorithm(Enum):
    """Load balancing algorithms."""
    ROUND_ROBIN = "round_robin"
    WEIGHTED_ROUND_ROBIN = "weighted_round_robin"
    LEAST_CONNECTIONS = "least_connections"
    LEAST_RESPONSE_TIME = "least_response_time"
    GEOGRAPHIC = "geographic"
    INTELLIGENT = "intelligent"  # ML-based routing


class RegionStatus(Enum):
    """Region health status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNAVAILABLE = "unavailable"


@dataclass
class Region:
    """Geographic region configuration."""
    id: str
    name: str
    endpoint: str
    weight: float = 1.0
    status: RegionStatus = RegionStatus.HEALTHY
    location: Dict[str, float] = field(default_factory=dict)  # lat, lon
    capabilities: List[str] = field(default_factory=list)
    current_load: float = 0.0
    active_connections: int = 0
    average_response_time_ms: float = 0.0
    last_health_check: Optional[float] = None
    health_check_url: Optional[str] = None
    max_capacity: int = 1000
    timezone: str = "UTC"


@dataclass
class RoutingRule:
    """Intelligent routing rule."""
    id: str
    name: str
    conditions: Dict[str, Any] = field(default_factory=dict)
    target_regions: List[str] = field(default_factory=list)
    priority: int = 1
    enabled: bool = True
    created_at: float = field(default_factory=time.time)


@dataclass
class RequestContext:
    """Request context for routing decisions."""
    user_id: Optional[str] = None
    client_ip: str = ""
    user_agent: str = ""
    request_type: str = ""
    data_size_mb: float = 0.0
    priority: str = "normal"  # high, normal, low
    required_capabilities: List[str] = field(default_factory=list)
    preferred_regions: List[str] = field(default_factory=list)
    client_location: Dict[str, float] = field(default_factory=dict)  # lat, lon
    timestamp: float = field(default_factory=time.time)


class GlobalLoadBalancer:
    """Advanced global load balancer with intelligent routing."""

    def __init__(self, algorithm: LoadBalancingAlgorithm = LoadBalancingAlgorithm.INTELLIGENT):
        self.algorithm = algorithm
        self.logger = get_logger("agent_etl.load_balancer")

        # Region management
        self.regions: Dict[str, Region] = {}
        self.routing_rules: Dict[str, RoutingRule] = {}

        # Algorithm state
        self.round_robin_index = 0
        self.request_counts: Dict[str, int] = {}
        self.response_times: Dict[str, List[float]] = {}

        # Health monitoring
        self.health_check_interval = 30.0  # seconds
        self.health_check_timeout = 10.0   # seconds
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()

        # Performance tracking
        self.routing_history: List[Dict[str, Any]] = []
        self.performance_metrics: Dict[str, Dict[str, float]] = {}

        # Initialize default regions
        self._initialize_default_regions()
        self._initialize_default_routing_rules()

    def _initialize_default_regions(self):
        """Initialize default global regions."""
        default_regions = [
            {
                "id": "us-east-1",
                "name": "US East (Virginia)",
                "endpoint": "https://us-east-1.agent-etl.example.com",
                "location": {"lat": 39.0458, "lon": -76.6413},
                "capabilities": ["standard", "high-memory", "gpu"],
                "timezone": "America/New_York",
                "weight": 1.5  # Higher capacity
            },
            {
                "id": "us-west-2",
                "name": "US West (Oregon)",
                "endpoint": "https://us-west-2.agent-etl.example.com",
                "location": {"lat": 45.5152, "lon": -122.6784},
                "capabilities": ["standard", "high-cpu"],
                "timezone": "America/Los_Angeles",
                "weight": 1.2
            },
            {
                "id": "eu-west-1",
                "name": "Europe (Ireland)",
                "endpoint": "https://eu-west-1.agent-etl.example.com",
                "location": {"lat": 53.3498, "lon": -6.2603},
                "capabilities": ["standard", "gdpr-compliant"],
                "timezone": "Europe/Dublin",
                "weight": 1.0
            },
            {
                "id": "ap-southeast-1",
                "name": "Asia Pacific (Singapore)",
                "endpoint": "https://ap-southeast-1.agent-etl.example.com",
                "location": {"lat": 1.3521, "lon": 103.8198},
                "capabilities": ["standard", "low-latency"],
                "timezone": "Asia/Singapore",
                "weight": 0.8
            },
            {
                "id": "ap-northeast-1",
                "name": "Asia Pacific (Tokyo)",
                "endpoint": "https://ap-northeast-1.agent-etl.example.com",
                "location": {"lat": 35.6762, "lon": 139.6503},
                "capabilities": ["standard", "high-memory"],
                "timezone": "Asia/Tokyo",
                "weight": 1.0
            }
        ]

        for region_config in default_regions:
            region = Region(**region_config)
            self.add_region(region)

    def _initialize_default_routing_rules(self):
        """Initialize default routing rules."""
        # GDPR compliance rule
        self.add_routing_rule(RoutingRule(
            id="gdpr_compliance",
            name="GDPR Compliance Routing",
            conditions={
                "client_location_region": "EU",
                "data_classification": "personal"
            },
            target_regions=["eu-west-1"],
            priority=1
        ))

        # High priority request rule
        self.add_routing_rule(RoutingRule(
            id="high_priority_routing",
            name="High Priority Request Routing",
            conditions={"priority": "high"},
            target_regions=["us-east-1", "eu-west-1"],
            priority=2
        ))

        # Large data processing rule
        self.add_routing_rule(RoutingRule(
            id="large_data_processing",
            name="Large Data Processing Routing",
            conditions={
                "data_size_mb": {"min": 100},
                "required_capabilities": ["high-memory"]
            },
            target_regions=["us-east-1", "ap-northeast-1"],
            priority=3
        ))

        # Low latency rule
        self.add_routing_rule(RoutingRule(
            id="low_latency_routing",
            name="Low Latency Routing",
            conditions={"required_capabilities": ["low-latency"]},
            target_regions=["ap-southeast-1"],
            priority=4
        ))

    def add_region(self, region: Region):
        """Add a region to the load balancer."""
        self.regions[region.id] = region
        self.request_counts[region.id] = 0
        self.response_times[region.id] = []
        self.performance_metrics[region.id] = {
            "avg_response_time": 0.0,
            "success_rate": 1.0,
            "throughput_rps": 0.0
        }

        self.logger.info(f"Added region: {region.name} ({region.id})")

    def remove_region(self, region_id: str):
        """Remove a region from the load balancer."""
        if region_id in self.regions:
            region_name = self.regions[region_id].name
            del self.regions[region_id]
            del self.request_counts[region_id]
            del self.response_times[region_id]
            del self.performance_metrics[region_id]

            self.logger.info(f"Removed region: {region_name} ({region_id})")

    def add_routing_rule(self, rule: RoutingRule):
        """Add a routing rule."""
        self.routing_rules[rule.id] = rule
        self.logger.info(f"Added routing rule: {rule.name}")

    def route_request(self, context: RequestContext) -> Optional[Region]:
        """Route a request to the best available region."""
        start_time = time.time()

        try:
            # Get available regions (healthy only)
            available_regions = [
                region for region in self.regions.values()
                if region.status == RegionStatus.HEALTHY
            ]

            if not available_regions:
                self.logger.error("No healthy regions available for routing")
                return None

            # Apply routing rules first
            rule_filtered_regions = self._apply_routing_rules(context, available_regions)

            if rule_filtered_regions:
                available_regions = rule_filtered_regions

            # Apply load balancing algorithm
            selected_region = self._select_region_by_algorithm(context, available_regions)

            if selected_region:
                # Update region metrics
                selected_region.active_connections += 1
                self.request_counts[selected_region.id] += 1

                # Record routing decision
                routing_time = (time.time() - start_time) * 1000
                self._record_routing_decision(context, selected_region, routing_time)

                self.logger.debug(
                    f"Routed request to region {selected_region.id}",
                    extra={
                        "region_id": selected_region.id,
                        "algorithm": self.algorithm.value,
                        "routing_time_ms": routing_time,
                        "user_id": context.user_id
                    }
                )

            return selected_region

        except Exception as e:
            self.logger.error(f"Error routing request: {e}", exc_info=True)
            # Fallback to first available region
            return available_regions[0] if available_regions else None

    def _apply_routing_rules(
        self,
        context: RequestContext,
        available_regions: List[Region]
    ) -> List[Region]:
        """Apply routing rules to filter available regions."""
        # Sort rules by priority (1 = highest priority)
        sorted_rules = sorted(
            [rule for rule in self.routing_rules.values() if rule.enabled],
            key=lambda r: r.priority
        )

        for rule in sorted_rules:
            if self._evaluate_rule_conditions(rule, context):
                # Rule matches, filter to target regions
                target_regions = [
                    region for region in available_regions
                    if region.id in rule.target_regions
                ]

                if target_regions:
                    self.logger.debug(f"Applied routing rule: {rule.name}")
                    return target_regions

        # No rules matched, return all available regions
        return available_regions

    def _evaluate_rule_conditions(self, rule: RoutingRule, context: RequestContext) -> bool:
        """Evaluate if a routing rule's conditions are met."""
        conditions = rule.conditions

        for condition_key, condition_value in conditions.items():
            context_value = self._get_context_value(context, condition_key)

            if not self._evaluate_condition(context_value, condition_value):
                return False

        return True

    def _get_context_value(self, context: RequestContext, key: str) -> Any:
        """Get a value from request context by key."""
        # Handle nested keys like "client_location.region"
        if "." in key:
            parts = key.split(".")
            value = context
            for part in parts:
                value = getattr(value, part, None)
                if value is None:
                    break
            return value

        # Handle special computed values
        if key == "client_location_region":
            return self._determine_client_region(context.client_location)
        elif key == "data_classification":
            return self._classify_data(context)

        # Direct attribute access
        return getattr(context, key, None)

    def _evaluate_condition(self, context_value: Any, condition_value: Any) -> bool:
        """Evaluate a single condition."""
        if context_value is None:
            return False

        # Dictionary conditions (e.g., {"min": 100, "max": 500})
        if isinstance(condition_value, dict):
            if "min" in condition_value and context_value < condition_value["min"]:
                return False
            if "max" in condition_value and context_value > condition_value["max"]:
                return False
            return True

        # List conditions (value must be in list)
        elif isinstance(condition_value, list):
            if isinstance(context_value, list):
                return any(item in condition_value for item in context_value)
            else:
                return context_value in condition_value

        # Exact match
        else:
            return context_value == condition_value

    def _determine_client_region(self, client_location: Dict[str, float]) -> str:
        """Determine geographic region from client location."""
        if not client_location or "lat" not in client_location:
            return "unknown"

        lat = client_location["lat"]
        lon = client_location["lon"]

        # Simple geographic classification
        if 35 <= lat <= 75 and -15 <= lon <= 45:
            return "EU"
        elif 25 <= lat <= 50 and -130 <= lon <= -60:
            return "US"
        elif -10 <= lat <= 55 and 95 <= lon <= 180:
            return "APAC"
        else:
            return "other"

    def _classify_data(self, context: RequestContext) -> str:
        """Classify data type based on context."""
        # This would implement actual data classification
        # For now, return a placeholder
        if context.user_id:
            return "personal"
        elif context.data_size_mb > 100:
            return "bulk"
        else:
            return "standard"

    def _select_region_by_algorithm(
        self,
        context: RequestContext,
        available_regions: List[Region]
    ) -> Optional[Region]:
        """Select region based on configured load balancing algorithm."""
        if not available_regions:
            return None

        if self.algorithm == LoadBalancingAlgorithm.ROUND_ROBIN:
            return self._round_robin_selection(available_regions)

        elif self.algorithm == LoadBalancingAlgorithm.WEIGHTED_ROUND_ROBIN:
            return self._weighted_round_robin_selection(available_regions)

        elif self.algorithm == LoadBalancingAlgorithm.LEAST_CONNECTIONS:
            return self._least_connections_selection(available_regions)

        elif self.algorithm == LoadBalancingAlgorithm.LEAST_RESPONSE_TIME:
            return self._least_response_time_selection(available_regions)

        elif self.algorithm == LoadBalancingAlgorithm.GEOGRAPHIC:
            return self._geographic_selection(context, available_regions)

        elif self.algorithm == LoadBalancingAlgorithm.INTELLIGENT:
            return self._intelligent_selection(context, available_regions)

        else:
            # Fallback to random selection
            return random.choice(available_regions)

    def _round_robin_selection(self, available_regions: List[Region]) -> Region:
        """Simple round-robin selection."""
        region = available_regions[self.round_robin_index % len(available_regions)]
        self.round_robin_index += 1
        return region

    def _weighted_round_robin_selection(self, available_regions: List[Region]) -> Region:
        """Weighted round-robin selection based on region weights."""
        # Create weighted list
        weighted_regions = []
        for region in available_regions:
            weight_count = max(1, int(region.weight * 10))
            weighted_regions.extend([region] * weight_count)

        region = weighted_regions[self.round_robin_index % len(weighted_regions)]
        self.round_robin_index += 1
        return region

    def _least_connections_selection(self, available_regions: List[Region]) -> Region:
        """Select region with least active connections."""
        return min(available_regions, key=lambda r: r.active_connections)

    def _least_response_time_selection(self, available_regions: List[Region]) -> Region:
        """Select region with best average response time."""
        return min(available_regions, key=lambda r: r.average_response_time_ms)

    def _geographic_selection(
        self,
        context: RequestContext,
        available_regions: List[Region]
    ) -> Region:
        """Select region based on geographic proximity."""
        if not context.client_location or "lat" not in context.client_location:
            # Fallback to least connections
            return self._least_connections_selection(available_regions)

        client_lat = context.client_location["lat"]
        client_lon = context.client_location["lon"]

        # Calculate distances and select closest region
        def calculate_distance(region: Region) -> float:
            if not region.location or "lat" not in region.location:
                return float('inf')

            # Simple Euclidean distance (not geographically accurate but fast)
            lat_diff = abs(region.location["lat"] - client_lat)
            lon_diff = abs(region.location["lon"] - client_lon)
            return (lat_diff ** 2 + lon_diff ** 2) ** 0.5

        return min(available_regions, key=calculate_distance)

    def _intelligent_selection(
        self,
        context: RequestContext,
        available_regions: List[Region]
    ) -> Region:
        """Intelligent selection using multiple factors and ML."""
        # Score each region based on multiple factors
        region_scores = {}

        for region in available_regions:
            score = 0.0

            # Factor 1: Geographic proximity (30% weight)
            if context.client_location and region.location:
                if "lat" in context.client_location and "lat" in region.location:
                    distance = self._calculate_geographic_distance(
                        context.client_location, region.location
                    )
                    # Normalize distance (closer = higher score)
                    max_distance = 20000  # km, roughly half Earth circumference
                    geo_score = max(0, 1.0 - (distance / max_distance))
                    score += geo_score * 0.3

            # Factor 2: Current load (25% weight)
            load_factor = 1.0 - (region.active_connections / max(1, region.max_capacity))
            score += load_factor * 0.25

            # Factor 3: Response time performance (25% weight)
            if region.average_response_time_ms > 0:
                # Lower response time = higher score
                max_response_time = 5000  # 5 seconds
                response_score = max(0, 1.0 - (region.average_response_time_ms / max_response_time))
                score += response_score * 0.25
            else:
                score += 0.25  # Default score if no data

            # Factor 4: Capability match (20% weight)
            if context.required_capabilities:
                matching_capabilities = len(
                    set(context.required_capabilities) & set(region.capabilities)
                )
                capability_score = matching_capabilities / len(context.required_capabilities)
                score += capability_score * 0.2
            else:
                score += 0.2  # Default score if no requirements

            region_scores[region.id] = score

        # Select region with highest score
        best_region_id = max(region_scores, key=region_scores.get)
        return next(r for r in available_regions if r.id == best_region_id)

    def _calculate_geographic_distance(
        self,
        location1: Dict[str, float],
        location2: Dict[str, float]
    ) -> float:
        """Calculate geographic distance between two points (Haversine formula)."""
        import math

        lat1, lon1 = math.radians(location1["lat"]), math.radians(location1["lon"])
        lat2, lon2 = math.radians(location2["lat"]), math.radians(location2["lon"])

        dlat = lat2 - lat1
        dlon = lon2 - lon1

        a = (math.sin(dlat/2)**2 +
             math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2)
        c = 2 * math.asin(math.sqrt(a))

        # Earth radius in kilometers
        r = 6371
        return r * c

    def _record_routing_decision(
        self,
        context: RequestContext,
        selected_region: Region,
        routing_time_ms: float
    ):
        """Record routing decision for analysis."""
        decision = {
            "timestamp": time.time(),
            "user_id": context.user_id,
            "region_id": selected_region.id,
            "algorithm": self.algorithm.value,
            "routing_time_ms": routing_time_ms,
            "request_type": context.request_type,
            "data_size_mb": context.data_size_mb,
            "priority": context.priority
        }

        self.routing_history.append(decision)

        # Limit history size
        if len(self.routing_history) > 10000:
            self.routing_history = self.routing_history[-5000:]

    def update_region_metrics(
        self,
        region_id: str,
        response_time_ms: float,
        success: bool = True
    ):
        """Update region performance metrics after request completion."""
        if region_id not in self.regions:
            return

        region = self.regions[region_id]

        # Update connection count
        region.active_connections = max(0, region.active_connections - 1)

        # Update response time
        self.response_times[region_id].append(response_time_ms)
        if len(self.response_times[region_id]) > 100:
            self.response_times[region_id] = self.response_times[region_id][-50:]

        # Calculate new average response time
        if self.response_times[region_id]:
            region.average_response_time_ms = sum(self.response_times[region_id]) / len(self.response_times[region_id])

        # Update performance metrics
        metrics = self.performance_metrics[region_id]
        metrics["avg_response_time"] = region.average_response_time_ms

        # Update success rate (simplified)
        if "total_requests" not in metrics:
            metrics["total_requests"] = 0
            metrics["successful_requests"] = 0

        metrics["total_requests"] += 1
        if success:
            metrics["successful_requests"] += 1

        metrics["success_rate"] = metrics["successful_requests"] / metrics["total_requests"]

    def start_health_monitoring(self):
        """Start health monitoring for all regions."""
        if self.is_monitoring:
            return

        self.is_monitoring = True
        self.shutdown_event.clear()
        self.monitor_thread = threading.Thread(
            target=self._health_monitoring_loop,
            name="LoadBalancer-HealthMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        self.logger.info("Health monitoring started")

    def stop_health_monitoring(self):
        """Stop health monitoring."""
        if not self.is_monitoring:
            return

        self.is_monitoring = False
        self.shutdown_event.set()

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)

        self.logger.info("Health monitoring stopped")

    def _health_monitoring_loop(self):
        """Health monitoring background loop."""
        while self.is_monitoring and not self.shutdown_event.is_set():
            try:
                for region in self.regions.values():
                    self._check_region_health(region)

                # Sleep for health check interval
                self.shutdown_event.wait(self.health_check_interval)

            except Exception as e:
                self.logger.error(f"Error in health monitoring: {e}", exc_info=True)
                self.shutdown_event.wait(10.0)

    def _check_region_health(self, region: Region):
        """Check health of a single region."""
        try:
            # This would implement actual health checks (HTTP requests, ping, etc.)
            # For now, simulate health checks

            start_time = time.time()

            # Simulate health check logic
            is_healthy = True
            response_time = random.uniform(10, 100)  # Simulate response time

            # Update region status based on health check
            if is_healthy:
                if region.status != RegionStatus.HEALTHY:
                    self.logger.info(f"Region {region.id} is now healthy")
                region.status = RegionStatus.HEALTHY
            else:
                if region.status == RegionStatus.HEALTHY:
                    self.logger.warning(f"Region {region.id} is degraded")
                region.status = RegionStatus.DEGRADED

            region.last_health_check = time.time()

        except Exception as e:
            self.logger.error(f"Health check failed for region {region.id}: {e}")
            region.status = RegionStatus.UNAVAILABLE

    def get_load_balancer_stats(self) -> Dict[str, Any]:
        """Get comprehensive load balancer statistics."""
        total_requests = sum(self.request_counts.values())

        region_stats = {}
        for region_id, region in self.regions.items():
            region_stats[region_id] = {
                "name": region.name,
                "status": region.status.value,
                "active_connections": region.active_connections,
                "total_requests": self.request_counts[region_id],
                "request_percentage": (self.request_counts[region_id] / max(1, total_requests)) * 100,
                "average_response_time_ms": region.average_response_time_ms,
                "weight": region.weight,
                "capabilities": region.capabilities,
                "last_health_check": region.last_health_check,
                "performance_metrics": self.performance_metrics.get(region_id, {})
            }

        # Algorithm-specific stats
        algorithm_stats = {
            "current_algorithm": self.algorithm.value,
            "round_robin_index": self.round_robin_index if self.algorithm in [
                LoadBalancingAlgorithm.ROUND_ROBIN,
                LoadBalancingAlgorithm.WEIGHTED_ROUND_ROBIN
            ] else None
        }

        return {
            "total_regions": len(self.regions),
            "healthy_regions": len([r for r in self.regions.values() if r.status == RegionStatus.HEALTHY]),
            "total_requests_routed": total_requests,
            "routing_rules_active": len([r for r in self.routing_rules.values() if r.enabled]),
            "health_monitoring_active": self.is_monitoring,
            "regions": region_stats,
            "algorithm": algorithm_stats,
            "recent_routing_decisions": self.routing_history[-10:],
            "timestamp": time.time()
        }

    def __enter__(self):
        """Context manager entry."""
        self.start_health_monitoring()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_health_monitoring()


# Global load balancer instance
_global_load_balancer = None


def get_global_load_balancer() -> GlobalLoadBalancer:
    """Get the global load balancer instance."""
    global _global_load_balancer
    if _global_load_balancer is None:
        _global_load_balancer = GlobalLoadBalancer()
    return _global_load_balancer


def route_request(context: RequestContext) -> Optional[Region]:
    """Route a request using the global load balancer."""
    load_balancer = get_global_load_balancer()
    return load_balancer.route_request(context)


def get_load_balancer_stats() -> Dict[str, Any]:
    """Get load balancer statistics."""
    load_balancer = get_global_load_balancer()
    return load_balancer.get_load_balancer_stats()
