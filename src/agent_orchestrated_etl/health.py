"""Health check endpoints and monitoring utilities."""

import asyncio
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import psutil
from pydantic import BaseModel


class HealthStatus(BaseModel):
    """Health status model."""

    status: str  # "healthy", "degraded", "unhealthy"
    timestamp: datetime
    version: str
    uptime_seconds: float
    checks: Dict[str, Any]


class HealthCheck:
    """Health check utility class."""

    def __init__(self, version: str = "0.0.1"):
        self.version = version
        self.start_time = time.time()
        self._checks: Dict[str, callable] = {}

    def register_check(self, name: str, check_func: callable) -> None:
        """Register a health check function."""
        self._checks[name] = check_func

    async def get_health_status(self) -> HealthStatus:
        """Get comprehensive health status."""
        checks = {}
        overall_status = "healthy"

        # Run all registered checks
        for name, check_func in self._checks.items():
            try:
                if asyncio.iscoroutinefunction(check_func):
                    result = await check_func()
                else:
                    result = check_func()
                checks[name] = result

                # Update overall status based on check results
                if isinstance(result, dict) and result.get("status") == "unhealthy":
                    overall_status = "unhealthy"
                elif isinstance(result, dict) and result.get("status") == "degraded":
                    if overall_status == "healthy":
                        overall_status = "degraded"

            except Exception as e:
                checks[name] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                overall_status = "unhealthy"

        # Add system metrics
        checks["system"] = self._get_system_metrics()

        return HealthStatus(
            status=overall_status,
            timestamp=datetime.now(timezone.utc),
            version=self.version,
            uptime_seconds=time.time() - self.start_time,
            checks=checks
        )

    def _get_system_metrics(self) -> Dict[str, Any]:
        """Get system performance metrics."""
        return {
            "status": "healthy",
            "cpu_percent": psutil.cpu_percent(interval=1),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent,
            "load_average": psutil.getloadavg() if hasattr(psutil, 'getloadavg') else None,
            "process_count": len(psutil.pids())
        }


# Default health checker instance
health_checker = HealthCheck()


def check_database_connection() -> Dict[str, Any]:
    """Check database connection health."""
    try:
        # Import here to avoid circular imports
        import sqlalchemy

        from agent_orchestrated_etl.config import get_database_url

        engine = sqlalchemy.create_engine(get_database_url())
        with engine.connect() as conn:
            conn.execute(sqlalchemy.text("SELECT 1"))

        return {
            "status": "healthy",
            "message": "Database connection successful"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Database connection failed"
        }


def check_redis_connection() -> Dict[str, Any]:
    """Check Redis connection health."""
    try:
        import redis

        from agent_orchestrated_etl.config import get_redis_url

        r = redis.from_url(get_redis_url())
        r.ping()

        return {
            "status": "healthy",
            "message": "Redis connection successful"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Redis connection failed"
        }


def check_disk_space(threshold_percent: float = 90.0) -> Dict[str, Any]:
    """Check disk space usage."""
    try:
        disk_usage = psutil.disk_usage('/')
        used_percent = (disk_usage.used / disk_usage.total) * 100

        if used_percent >= threshold_percent:
            status = "unhealthy"
            message = f"Disk usage critical: {used_percent:.1f}%"
        elif used_percent >= threshold_percent * 0.8:
            status = "degraded"
            message = f"Disk usage high: {used_percent:.1f}%"
        else:
            status = "healthy"
            message = f"Disk usage normal: {used_percent:.1f}%"

        return {
            "status": status,
            "message": message,
            "used_percent": used_percent,
            "free_bytes": disk_usage.free,
            "total_bytes": disk_usage.total
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Failed to check disk space"
        }


def check_memory_usage(threshold_percent: float = 90.0) -> Dict[str, Any]:
    """Check memory usage."""
    try:
        memory = psutil.virtual_memory()
        used_percent = memory.percent

        if used_percent >= threshold_percent:
            status = "unhealthy"
            message = f"Memory usage critical: {used_percent:.1f}%"
        elif used_percent >= threshold_percent * 0.8:
            status = "degraded"
            message = f"Memory usage high: {used_percent:.1f}%"
        else:
            status = "healthy"
            message = f"Memory usage normal: {used_percent:.1f}%"

        return {
            "status": status,
            "message": message,
            "used_percent": used_percent,
            "available_bytes": memory.available,
            "total_bytes": memory.total
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Failed to check memory usage"
        }


async def check_agent_health() -> Dict[str, Any]:
    """Check agent system health."""
    try:
        from agent_orchestrated_etl.orchestrator import DataOrchestrator

        # Try to create an orchestrator instance
        orchestrator = DataOrchestrator()

        # Verify basic functionality
        test_pipeline = orchestrator.create_pipeline(source="mock://health-check")

        return {
            "status": "healthy",
            "message": "Agent system operational",
            "orchestrator_available": True
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "error": str(e),
            "message": "Agent system unavailable",
            "orchestrator_available": False
        }


# Register default health checks
health_checker.register_check("database", check_database_connection)
health_checker.register_check("redis", check_redis_connection)
health_checker.register_check("disk_space", check_disk_space)
health_checker.register_check("memory_usage", check_memory_usage)
health_checker.register_check("agent_system", check_agent_health)


class MetricsCollector:
    """Metrics collection utility."""

    def __init__(self):
        self.metrics: Dict[str, List[Dict[str, Any]]] = {}

    def record_metric(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a metric value."""
        metric_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "value": value,
            "labels": labels or {}
        }

        if name not in self.metrics:
            self.metrics[name] = []

        self.metrics[name].append(metric_data)

        # Keep only last 1000 metrics per name
        if len(self.metrics[name]) > 1000:
            self.metrics[name] = self.metrics[name][-1000:]

    def get_metrics(self, name: Optional[str] = None) -> Dict[str, List[Dict[str, Any]]]:
        """Get collected metrics."""
        if name:
            return {name: self.metrics.get(name, [])}
        return self.metrics.copy()

    def get_prometheus_format(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []

        for metric_name, values in self.metrics.items():
            if not values:
                continue

            # Use the latest value for each unique label combination
            latest_by_labels = {}
            for value_data in values:
                labels_key = tuple(sorted(value_data["labels"].items()))
                latest_by_labels[labels_key] = value_data

            for value_data in latest_by_labels.values():
                labels_str = ""
                if value_data["labels"]:
                    label_pairs = [f'{k}="{v}"' for k, v in value_data["labels"].items()]
                    labels_str = "{" + ",".join(label_pairs) + "}"

                lines.append(f"{metric_name}{labels_str} {value_data['value']}")

        return "\n".join(lines)


# Global metrics collector
metrics_collector = MetricsCollector()


def record_pipeline_execution(pipeline_id: str, duration_seconds: float, status: str) -> None:
    """Record pipeline execution metrics."""
    metrics_collector.record_metric(
        "pipeline_execution_duration_seconds",
        duration_seconds,
        {"pipeline_id": pipeline_id, "status": status}
    )

    metrics_collector.record_metric(
        "pipeline_executions_total",
        1,
        {"pipeline_id": pipeline_id, "status": status}
    )


def record_agent_operation(agent_type: str, operation: str, duration_seconds: float) -> None:
    """Record agent operation metrics."""
    metrics_collector.record_metric(
        "agent_operation_duration_seconds",
        duration_seconds,
        {"agent_type": agent_type, "operation": operation}
    )


def record_data_processed(source_type: str, records_count: int, bytes_processed: int) -> None:
    """Record data processing metrics."""
    metrics_collector.record_metric(
        "data_records_processed_total",
        records_count,
        {"source_type": source_type}
    )

    metrics_collector.record_metric(
        "data_bytes_processed_total",
        bytes_processed,
        {"source_type": source_type}
    )
