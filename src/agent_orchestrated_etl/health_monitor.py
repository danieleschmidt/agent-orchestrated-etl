"""Advanced health monitoring and metrics collection for Agent-Orchestrated ETL."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .logging_config import get_logger


class HealthStatus(Enum):
    """Health status levels."""
    HEALTHY = "healthy"
    WARNING = "warning"
    CRITICAL = "critical"
    DOWN = "down"


@dataclass
class HealthCheck:
    """Individual health check configuration."""
    name: str
    check_function: Callable[[], Dict[str, Any]]
    interval_seconds: float = 60.0
    timeout_seconds: float = 10.0
    failure_threshold: int = 3
    recovery_threshold: int = 2
    enabled: bool = True
    last_run: Optional[float] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0
    status: HealthStatus = HealthStatus.HEALTHY
    last_error: Optional[str] = None


@dataclass
class SystemMetrics:
    """System performance metrics."""
    timestamp: float = field(default_factory=time.time)
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0
    memory_usage_percent: float = 0.0
    disk_usage_percent: float = 0.0
    active_connections: int = 0
    pipeline_count: int = 0
    tasks_processed: int = 0
    error_rate: float = 0.0
    average_response_time_ms: float = 0.0


class AdvancedHealthMonitor:
    """Advanced health monitoring system with comprehensive checks."""

    def __init__(self):
        self.logger = get_logger("agent_etl.health_monitor")
        self.health_checks: Dict[str, HealthCheck] = {}
        self.metrics_history: List[SystemMetrics] = []
        self.is_running = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.shutdown_event = threading.Event()
        self.max_history_size = 1000

        # Initialize default health checks
        self._initialize_default_checks()

    def _initialize_default_checks(self):
        """Initialize default health checks."""
        # System health checks
        self.register_health_check(
            "system_resources",
            self._check_system_resources,
            interval_seconds=30.0
        )

        self.register_health_check(
            "database_connectivity",
            self._check_database_connectivity,
            interval_seconds=60.0
        )

        self.register_health_check(
            "circuit_breakers",
            self._check_circuit_breakers,
            interval_seconds=45.0
        )

        self.register_health_check(
            "memory_leaks",
            self._check_memory_leaks,
            interval_seconds=120.0
        )

        self.register_health_check(
            "disk_space",
            self._check_disk_space,
            interval_seconds=300.0
        )

    def register_health_check(
        self,
        name: str,
        check_function: Callable[[], Dict[str, Any]],
        interval_seconds: float = 60.0,
        timeout_seconds: float = 10.0,
        failure_threshold: int = 3,
        recovery_threshold: int = 2
    ):
        """Register a new health check."""
        self.health_checks[name] = HealthCheck(
            name=name,
            check_function=check_function,
            interval_seconds=interval_seconds,
            timeout_seconds=timeout_seconds,
            failure_threshold=failure_threshold,
            recovery_threshold=recovery_threshold
        )
        self.logger.info(f"Registered health check: {name}")

    def start_monitoring(self):
        """Start the health monitoring background thread."""
        if self.is_running:
            return

        self.is_running = True
        self.shutdown_event.clear()
        self.monitor_thread = threading.Thread(
            target=self._monitoring_loop,
            name="HealthMonitor",
            daemon=True
        )
        self.monitor_thread.start()
        self.logger.info("Health monitoring started")

    def stop_monitoring(self):
        """Stop the health monitoring background thread."""
        if not self.is_running:
            return

        self.is_running = False
        self.shutdown_event.set()

        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=5.0)

        self.logger.info("Health monitoring stopped")

    def _monitoring_loop(self):
        """Main monitoring loop running in background thread."""
        while self.is_running and not self.shutdown_event.is_set():
            try:
                current_time = time.time()

                # Run health checks that are due
                for check in self.health_checks.values():
                    if not check.enabled:
                        continue

                    if (check.last_run is None or
                        current_time - check.last_run >= check.interval_seconds):
                        self._run_health_check(check)

                # Collect system metrics
                self._collect_system_metrics()

                # Clean up old history
                self._cleanup_metrics_history()

                # Sleep for a short interval
                self.shutdown_event.wait(1.0)

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}", exc_info=True)
                self.shutdown_event.wait(5.0)

    def _run_health_check(self, check: HealthCheck):
        """Run a single health check with timeout and error handling."""
        start_time = time.time()
        check.last_run = start_time

        try:
            # Run check with timeout
            result = self._run_with_timeout(
                check.check_function,
                check.timeout_seconds
            )

            # Process successful result
            check.consecutive_failures = 0
            check.consecutive_successes += 1
            check.last_error = None

            # Determine status based on result
            if result.get("status") == "healthy":
                if check.consecutive_successes >= check.recovery_threshold:
                    check.status = HealthStatus.HEALTHY
            elif result.get("status") == "warning":
                check.status = HealthStatus.WARNING
            elif result.get("status") == "critical":
                check.status = HealthStatus.CRITICAL

            self.logger.debug(
                f"Health check '{check.name}' completed",
                extra={
                    "check_name": check.name,
                    "status": check.status.value,
                    "duration_ms": (time.time() - start_time) * 1000,
                    "result": result
                }
            )

        except Exception as e:
            # Process failed result
            check.consecutive_successes = 0
            check.consecutive_failures += 1
            check.last_error = str(e)

            # Update status based on failure count
            if check.consecutive_failures >= check.failure_threshold:
                check.status = HealthStatus.CRITICAL
            elif check.consecutive_failures > 1:
                check.status = HealthStatus.WARNING

            self.logger.warning(
                f"Health check '{check.name}' failed",
                extra={
                    "check_name": check.name,
                    "error": str(e),
                    "consecutive_failures": check.consecutive_failures,
                    "status": check.status.value
                }
            )

    def _run_with_timeout(self, func: Callable, timeout_seconds: float) -> Dict[str, Any]:
        """Run a function with timeout using threading."""
        result = {}
        exception = None

        def target():
            nonlocal result, exception
            try:
                result = func()
            except Exception as e:
                exception = e

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(timeout=timeout_seconds)

        if thread.is_alive():
            # Thread is still running, consider it timed out
            raise TimeoutError(f"Health check timed out after {timeout_seconds} seconds")

        if exception:
            raise exception

        return result

    def _collect_system_metrics(self):
        """Collect current system metrics."""
        try:
            import psutil

            # CPU and memory metrics
            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Network connections
            connections = len(psutil.net_connections())

            metrics = SystemMetrics(
                cpu_usage_percent=cpu_percent,
                memory_usage_mb=memory.used / (1024 * 1024),
                memory_usage_percent=memory.percent,
                disk_usage_percent=disk.percent,
                active_connections=connections,
                # Additional metrics would be populated by other components
            )

            self.metrics_history.append(metrics)

            # Log metrics if they indicate potential issues
            if (cpu_percent > 80 or memory.percent > 85 or disk.percent > 90):
                self.logger.warning(
                    "High resource usage detected",
                    extra={
                        "cpu_percent": cpu_percent,
                        "memory_percent": memory.percent,
                        "disk_percent": disk.percent
                    }
                )

        except ImportError:
            # psutil not available, collect basic metrics
            metrics = SystemMetrics()
            self.metrics_history.append(metrics)
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")

    def _cleanup_metrics_history(self):
        """Remove old metrics to prevent memory growth."""
        if len(self.metrics_history) > self.max_history_size:
            self.metrics_history = self.metrics_history[-self.max_history_size:]

    def _check_system_resources(self) -> Dict[str, Any]:
        """Check system resource usage."""
        try:
            import psutil

            cpu_percent = psutil.cpu_percent()
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')

            # Determine status based on thresholds
            if cpu_percent > 90 or memory.percent > 95 or disk.percent > 95:
                status = "critical"
            elif cpu_percent > 80 or memory.percent > 85 or disk.percent > 90:
                status = "warning"
            else:
                status = "healthy"

            return {
                "status": status,
                "cpu_percent": cpu_percent,
                "memory_percent": memory.percent,
                "disk_percent": disk.percent,
                "available_memory_mb": memory.available / (1024 * 1024)
            }

        except ImportError:
            return {"status": "warning", "message": "psutil not available for resource monitoring"}
        except Exception as e:
            return {"status": "critical", "error": str(e)}

    def _check_database_connectivity(self) -> Dict[str, Any]:
        """Check database connectivity."""
        try:
            # This would be implemented with actual database connections
            # For now, return a simple check
            return {
                "status": "healthy",
                "message": "Database connectivity check not implemented"
            }
        except Exception as e:
            return {"status": "critical", "error": str(e)}

    def _check_circuit_breakers(self) -> Dict[str, Any]:
        """Check circuit breaker status."""
        try:
            from .circuit_breaker import circuit_breaker_registry

            stats = circuit_breaker_registry.get_all_stats()
            open_breakers = [
                name for name, stat in stats.items()
                if stat.get("state") == "open"
            ]

            if open_breakers:
                return {
                    "status": "warning",
                    "open_breakers": open_breakers,
                    "total_breakers": len(stats)
                }
            else:
                return {
                    "status": "healthy",
                    "total_breakers": len(stats)
                }

        except Exception as e:
            return {"status": "warning", "error": str(e)}

    def _check_memory_leaks(self) -> Dict[str, Any]:
        """Check for potential memory leaks."""
        try:
            if len(self.metrics_history) < 10:
                return {"status": "healthy", "message": "Insufficient data for leak detection"}

            # Simple leak detection: check if memory usage is consistently increasing
            recent_metrics = self.metrics_history[-10:]
            memory_trend = [m.memory_usage_mb for m in recent_metrics]

            # Calculate trend - if consistently increasing, potential leak
            increasing_count = 0
            for i in range(1, len(memory_trend)):
                if memory_trend[i] > memory_trend[i-1]:
                    increasing_count += 1

            leak_ratio = increasing_count / (len(memory_trend) - 1)

            if leak_ratio > 0.8:  # 80% of checks show increasing memory
                return {
                    "status": "warning",
                    "message": "Potential memory leak detected",
                    "memory_trend": memory_trend[-5:],  # Last 5 readings
                    "leak_ratio": leak_ratio
                }
            else:
                return {"status": "healthy", "leak_ratio": leak_ratio}

        except Exception as e:
            return {"status": "warning", "error": str(e)}

    def _check_disk_space(self) -> Dict[str, Any]:
        """Check available disk space."""
        try:
            import psutil

            disk = psutil.disk_usage('/')
            free_gb = disk.free / (1024 ** 3)

            if free_gb < 1.0:  # Less than 1GB free
                status = "critical"
            elif free_gb < 5.0:  # Less than 5GB free
                status = "warning"
            else:
                status = "healthy"

            return {
                "status": status,
                "free_space_gb": free_gb,
                "total_space_gb": disk.total / (1024 ** 3),
                "usage_percent": disk.percent
            }

        except ImportError:
            return {"status": "warning", "message": "psutil not available for disk monitoring"}
        except Exception as e:
            return {"status": "critical", "error": str(e)}

    def get_health_status(self) -> Dict[str, Any]:
        """Get overall health status and all check results."""
        overall_status = HealthStatus.HEALTHY
        checks_summary = {}

        for name, check in self.health_checks.items():
            checks_summary[name] = {
                "status": check.status.value,
                "last_run": check.last_run,
                "consecutive_failures": check.consecutive_failures,
                "last_error": check.last_error,
                "enabled": check.enabled
            }

            # Determine overall status (worst case)
            if check.status == HealthStatus.CRITICAL:
                overall_status = HealthStatus.CRITICAL
            elif check.status == HealthStatus.WARNING and overall_status != HealthStatus.CRITICAL:
                overall_status = HealthStatus.WARNING
            elif check.status == HealthStatus.DOWN:
                overall_status = HealthStatus.DOWN

        return {
            "overall_status": overall_status.value,
            "timestamp": time.time(),
            "checks": checks_summary,
            "metrics_collected": len(self.metrics_history),
            "monitoring_active": self.is_running
        }

    def get_metrics_summary(self, last_minutes: int = 60) -> Dict[str, Any]:
        """Get metrics summary for the last N minutes."""
        cutoff_time = time.time() - (last_minutes * 60)
        recent_metrics = [
            m for m in self.metrics_history
            if m.timestamp > cutoff_time
        ]

        if not recent_metrics:
            return {"message": "No metrics available for the specified period"}

        # Calculate averages and maximums
        cpu_values = [m.cpu_usage_percent for m in recent_metrics]
        memory_values = [m.memory_usage_percent for m in recent_metrics]

        return {
            "period_minutes": last_minutes,
            "sample_count": len(recent_metrics),
            "cpu": {
                "average": sum(cpu_values) / len(cpu_values),
                "maximum": max(cpu_values),
                "current": cpu_values[-1] if cpu_values else 0
            },
            "memory": {
                "average": sum(memory_values) / len(memory_values),
                "maximum": max(memory_values),
                "current": memory_values[-1] if memory_values else 0
            },
            "timestamp": time.time()
        }

    def __enter__(self):
        """Context manager entry."""
        self.start_monitoring()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.stop_monitoring()


# Global health monitor instance
_health_monitor = None


def get_health_monitor() -> AdvancedHealthMonitor:
    """Get the global health monitor instance."""
    global _health_monitor
    if _health_monitor is None:
        _health_monitor = AdvancedHealthMonitor()
    return _health_monitor


def start_health_monitoring():
    """Start global health monitoring."""
    monitor = get_health_monitor()
    monitor.start_monitoring()


def stop_health_monitoring():
    """Stop global health monitoring."""
    monitor = get_health_monitor()
    monitor.stop_monitoring()


def get_system_health() -> Dict[str, Any]:
    """Get current system health status."""
    monitor = get_health_monitor()
    return monitor.get_health_status()


def get_system_metrics(last_minutes: int = 60) -> Dict[str, Any]:
    """Get system metrics summary."""
    monitor = get_health_monitor()
    return monitor.get_metrics_summary(last_minutes)
