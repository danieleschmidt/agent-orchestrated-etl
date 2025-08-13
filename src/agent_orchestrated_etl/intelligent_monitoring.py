"""Intelligent Monitoring System for Pipeline Auto-Detection."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

from .logging_config import get_logger


class MonitoringLevel(str, Enum):
    """Monitoring level enumeration."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class AlertType(str, Enum):
    """Alert type enumeration."""
    PERFORMANCE_DEGRADATION = "performance_degradation"
    ERROR_THRESHOLD_EXCEEDED = "error_threshold_exceeded"
    RESOURCE_EXHAUSTION = "resource_exhaustion"
    DATA_QUALITY_ISSUE = "data_quality_issue"
    PIPELINE_FAILURE = "pipeline_failure"
    SECURITY_INCIDENT = "security_incident"


@dataclass
class MetricPoint:
    """A single metric measurement."""
    timestamp: float
    metric_name: str
    value: float
    tags: Dict[str, str] = None

    def __post_init__(self):
        if self.tags is None:
            self.tags = {}


@dataclass
class Alert:
    """Alert information."""
    alert_id: str
    alert_type: AlertType
    severity: MonitoringLevel
    message: str
    timestamp: float
    metadata: Dict[str, Any] = None
    resolved: bool = False

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


@dataclass
class HealthCheck:
    """Health check result."""
    component: str
    status: str
    timestamp: float
    response_time_ms: float
    error_message: Optional[str] = None
    metadata: Dict[str, Any] = None

    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}


class IntelligentMonitor:
    """Intelligent monitoring system with adaptive thresholds and alerting."""

    def __init__(self, alert_callback: Optional[Callable[[Alert], None]] = None):
        """
        Initialize the intelligent monitoring system.
        
        Args:
            alert_callback: Optional callback function for alert notifications
        """
        self.logger = get_logger("intelligent_monitor")
        self.alert_callback = alert_callback

        # Metrics storage (in production, use a proper time series DB)
        self.metrics: Dict[str, List[MetricPoint]] = {}
        self.alerts: List[Alert] = []
        self.health_checks: Dict[str, HealthCheck] = {}

        # Configuration
        self.max_metrics_per_series = 1000  # Keep last N measurements
        self.alert_thresholds = self._initialize_alert_thresholds()
        self.health_check_components = [
            "pipeline_detector",
            "data_analyzer",
            "transformation_engine",
            "monitoring_system"
        ]

        # Monitoring state
        self._monitoring_active = False
        self._monitoring_thread = None
        self._executor = ThreadPoolExecutor(max_workers=3, thread_name_prefix="monitor")
        self._lock = threading.RLock()

        self.logger.info("Intelligent monitoring system initialized")

    def start_monitoring(self) -> None:
        """Start the monitoring system."""
        if self._monitoring_active:
            self.logger.warning("Monitoring already active")
            return

        self._monitoring_active = True
        self._monitoring_thread = threading.Thread(
            target=self._monitoring_loop,
            daemon=True,
            name="intelligent_monitor"
        )
        self._monitoring_thread.start()

        self.logger.info("Intelligent monitoring started")

    def stop_monitoring(self) -> None:
        """Stop the monitoring system."""
        if not self._monitoring_active:
            return

        self._monitoring_active = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5.0)

        self._executor.shutdown(wait=True)
        self.logger.info("Intelligent monitoring stopped")

    def record_metric(self, metric_name: str, value: float, tags: Optional[Dict[str, str]] = None) -> None:
        """
        Record a metric measurement.
        
        Args:
            metric_name: Name of the metric
            value: Metric value
            tags: Optional tags for the metric
        """
        with self._lock:
            if metric_name not in self.metrics:
                self.metrics[metric_name] = []

            metric_point = MetricPoint(
                timestamp=time.time(),
                metric_name=metric_name,
                value=value,
                tags=tags or {}
            )

            self.metrics[metric_name].append(metric_point)

            # Keep only recent measurements
            if len(self.metrics[metric_name]) > self.max_metrics_per_series:
                self.metrics[metric_name] = self.metrics[metric_name][-self.max_metrics_per_series:]

        # Check for threshold violations
        self._check_thresholds(metric_name, value)

    def record_health_check(self, component: str, status: str, response_time_ms: float,
                          error_message: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a health check result.
        
        Args:
            component: Component name
            status: Health status (healthy, degraded, unhealthy)
            response_time_ms: Response time in milliseconds
            error_message: Optional error message
            metadata: Optional additional metadata
        """
        health_check = HealthCheck(
            component=component,
            status=status,
            timestamp=time.time(),
            response_time_ms=response_time_ms,
            error_message=error_message,
            metadata=metadata
        )

        with self._lock:
            self.health_checks[component] = health_check

        # Generate alert if unhealthy
        if status == "unhealthy":
            self._generate_alert(
                AlertType.PIPELINE_FAILURE,
                MonitoringLevel.ERROR,
                f"Component {component} is unhealthy: {error_message or 'Unknown error'}",
                {"component": component, "response_time_ms": response_time_ms}
            )

    def get_metrics(self, metric_name: str, time_window_seconds: Optional[int] = None) -> List[MetricPoint]:
        """
        Get metric measurements.
        
        Args:
            metric_name: Name of the metric
            time_window_seconds: Optional time window to filter measurements
        
        Returns:
            List of metric measurements
        """
        with self._lock:
            if metric_name not in self.metrics:
                return []

            metrics = self.metrics[metric_name].copy()

        if time_window_seconds:
            cutoff_time = time.time() - time_window_seconds
            metrics = [m for m in metrics if m.timestamp >= cutoff_time]

        return metrics

    def get_metric_summary(self, metric_name: str, time_window_seconds: int = 3600) -> Dict[str, float]:
        """
        Get metric summary statistics.
        
        Args:
            metric_name: Name of the metric
            time_window_seconds: Time window for analysis (default 1 hour)
        
        Returns:
            Summary statistics
        """
        metrics = self.get_metrics(metric_name, time_window_seconds)

        if not metrics:
            return {
                "count": 0,
                "min": 0.0,
                "max": 0.0,
                "avg": 0.0,
                "latest": 0.0
            }

        values = [m.value for m in metrics]

        return {
            "count": len(values),
            "min": min(values),
            "max": max(values),
            "avg": sum(values) / len(values),
            "latest": values[-1] if values else 0.0,
            "trend": self._calculate_trend(values)
        }

    def get_system_health(self) -> Dict[str, Any]:
        """Get overall system health status."""
        with self._lock:
            health_data = {
                "overall_status": "healthy",
                "components": {},
                "alerts_count": len([a for a in self.alerts if not a.resolved]),
                "last_update": time.time()
            }

            unhealthy_count = 0
            for component, check in self.health_checks.items():
                health_data["components"][component] = {
                    "status": check.status,
                    "response_time_ms": check.response_time_ms,
                    "last_check": check.timestamp,
                    "error": check.error_message
                }

                if check.status == "unhealthy":
                    unhealthy_count += 1

            # Determine overall status
            if unhealthy_count > 0:
                if unhealthy_count >= len(self.health_check_components) // 2:
                    health_data["overall_status"] = "unhealthy"
                else:
                    health_data["overall_status"] = "degraded"

        return health_data

    def get_alerts(self, resolved: Optional[bool] = None, limit: Optional[int] = None) -> List[Alert]:
        """
        Get alerts.
        
        Args:
            resolved: Filter by resolution status (None for all)
            limit: Maximum number of alerts to return
        
        Returns:
            List of alerts
        """
        with self._lock:
            alerts = self.alerts.copy()

        if resolved is not None:
            alerts = [a for a in alerts if a.resolved == resolved]

        # Sort by timestamp (newest first)
        alerts.sort(key=lambda a: a.timestamp, reverse=True)

        if limit:
            alerts = alerts[:limit]

        return alerts

    def resolve_alert(self, alert_id: str) -> bool:
        """
        Resolve an alert.
        
        Args:
            alert_id: Alert identifier
        
        Returns:
            True if alert was found and resolved
        """
        with self._lock:
            for alert in self.alerts:
                if alert.alert_id == alert_id:
                    alert.resolved = True
                    self.logger.info(f"Alert {alert_id} resolved")
                    return True

        return False

    def _monitoring_loop(self) -> None:
        """Main monitoring loop."""
        while self._monitoring_active:
            try:
                # Run health checks
                self._run_health_checks()

                # Analyze metrics for anomalies
                self._analyze_metric_anomalies()

                # Clean up old data
                self._cleanup_old_data()

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                time.sleep(10)  # Back off on error

    def _run_health_checks(self) -> None:
        """Run health checks on all components."""
        for component in self.health_check_components:
            self._executor.submit(self._perform_health_check, component)

    def _perform_health_check(self, component: str) -> None:
        """Perform health check for a specific component."""
        start_time = time.time()

        try:
            if component == "pipeline_detector":
                self._check_pipeline_detector_health()
            elif component == "data_analyzer":
                self._check_data_analyzer_health()
            elif component == "transformation_engine":
                self._check_transformation_engine_health()
            elif component == "monitoring_system":
                self._check_monitoring_system_health()

            response_time_ms = (time.time() - start_time) * 1000
            self.record_health_check(component, "healthy", response_time_ms)

        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            self.record_health_check(
                component,
                "unhealthy",
                response_time_ms,
                str(e)
            )

    def _check_pipeline_detector_health(self) -> None:
        """Check pipeline detector component health."""
        # Import here to avoid circular imports
        from .intelligent_pipeline_detector import IntelligentPipelineDetector

        detector = IntelligentPipelineDetector()

        # Test basic functionality
        source_type = detector.detect_data_source_type("s3://test-bucket/test.csv")
        if not source_type:
            raise Exception("Pipeline detector unable to detect source types")

    def _check_data_analyzer_health(self) -> None:
        """Check data analyzer component health."""
        # Test basic data analysis functionality
        test_metadata = {
            "fields": ["id", "name", "timestamp"],
            "analysis_metadata": {
                "total_files": 1,
                "file_formats": {"csv": 1},
                "total_size_mb": 1
            }
        }

        # Import here to avoid circular imports
        from .intelligent_pipeline_detector import IntelligentPipelineDetector

        detector = IntelligentPipelineDetector()
        patterns = detector.analyze_data_patterns("test://source", test_metadata)

        if not patterns:
            raise Exception("Data analyzer unable to analyze patterns")

    def _check_transformation_engine_health(self) -> None:
        """Check transformation engine component health."""
        # Test basic transformation functionality
        from .core import transform_data

        test_data = [{"id": 1, "value": 100}]
        result = transform_data(test_data)

        if not result:
            raise Exception("Transformation engine unable to process data")

    def _check_monitoring_system_health(self) -> None:
        """Check monitoring system health."""
        # Check if monitoring system can record metrics
        test_metric = f"health_check_test_{time.time()}"
        self.record_metric(test_metric, 1.0)

        metrics = self.get_metrics(test_metric)
        if not metrics:
            raise Exception("Monitoring system unable to record metrics")

    def _check_thresholds(self, metric_name: str, value: float) -> None:
        """Check if metric value violates any thresholds."""
        thresholds = self.alert_thresholds.get(metric_name, {})

        for threshold_type, config in thresholds.items():
            threshold_value = config["value"]
            operator = config["operator"]  # "gt", "lt", "eq"
            alert_type = config["alert_type"]
            severity = config["severity"]

            should_alert = False

            if operator == "gt" and value > threshold_value:
                should_alert = True
            elif operator == "lt" and value < threshold_value:
                should_alert = True
            elif operator == "eq" and abs(value - threshold_value) < 0.001:
                should_alert = True

            if should_alert:
                self._generate_alert(
                    alert_type,
                    severity,
                    f"Metric {metric_name} = {value} violates threshold {threshold_type} ({threshold_value})",
                    {"metric_name": metric_name, "metric_value": value, "threshold": threshold_value}
                )

    def _analyze_metric_anomalies(self) -> None:
        """Analyze metrics for anomalies using simple statistical methods."""
        for metric_name in self.metrics:
            try:
                # Get recent measurements
                recent_metrics = self.get_metrics(metric_name, time_window_seconds=3600)  # Last hour
                if len(recent_metrics) < 10:  # Need enough data points
                    continue

                values = [m.value for m in recent_metrics]

                # Simple anomaly detection using z-score
                mean_value = sum(values) / len(values)
                variance = sum((x - mean_value) ** 2 for x in values) / len(values)
                std_dev = variance ** 0.5

                if std_dev == 0:  # No variance
                    continue

                latest_value = values[-1]
                z_score = abs(latest_value - mean_value) / std_dev

                # Alert if z-score > 3 (unusual value)
                if z_score > 3:
                    self._generate_alert(
                        AlertType.PERFORMANCE_DEGRADATION,
                        MonitoringLevel.WARNING,
                        f"Anomalous value detected for {metric_name}: {latest_value} (z-score: {z_score:.2f})",
                        {
                            "metric_name": metric_name,
                            "value": latest_value,
                            "z_score": z_score,
                            "mean": mean_value,
                            "std_dev": std_dev
                        }
                    )

            except Exception as e:
                self.logger.error(f"Error analyzing anomalies for {metric_name}: {e}")

    def _generate_alert(self, alert_type: AlertType, severity: MonitoringLevel,
                       message: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Generate a new alert."""
        alert = Alert(
            alert_id=f"{alert_type.value}_{int(time.time() * 1000)}",
            alert_type=alert_type,
            severity=severity,
            message=message,
            timestamp=time.time(),
            metadata=metadata or {}
        )

        with self._lock:
            self.alerts.append(alert)
            # Keep only recent alerts
            if len(self.alerts) > 1000:
                # Remove oldest resolved alerts first
                resolved_alerts = [a for a in self.alerts if a.resolved]
                if resolved_alerts:
                    resolved_alerts.sort(key=lambda a: a.timestamp)
                    alerts_to_remove = resolved_alerts[:100]  # Remove 100 oldest
                    for alert_to_remove in alerts_to_remove:
                        self.alerts.remove(alert_to_remove)

        self.logger.log(
            getattr(self.logger, severity.value.upper()),
            f"Alert generated: {message}"
        )

        # Call alert callback if configured
        if self.alert_callback:
            try:
                self.alert_callback(alert)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")

    def _cleanup_old_data(self) -> None:
        """Clean up old metrics and alerts."""
        cutoff_time = time.time() - (24 * 3600)  # 24 hours ago

        with self._lock:
            # Clean old metrics
            for metric_name in list(self.metrics.keys()):
                self.metrics[metric_name] = [
                    m for m in self.metrics[metric_name]
                    if m.timestamp >= cutoff_time
                ]

                # Remove empty metric series
                if not self.metrics[metric_name]:
                    del self.metrics[metric_name]

            # Clean old resolved alerts
            self.alerts = [
                a for a in self.alerts
                if not a.resolved or a.timestamp >= cutoff_time
            ]

    def _calculate_trend(self, values: List[float]) -> str:
        """Calculate trend direction for a series of values."""
        if len(values) < 2:
            return "stable"

        # Simple linear trend calculation
        n = len(values)
        x_vals = list(range(n))

        # Calculate slope using simple linear regression
        x_mean = sum(x_vals) / n
        y_mean = sum(values) / n

        numerator = sum((x_vals[i] - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((x_vals[i] - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return "stable"

        slope = numerator / denominator

        if slope > 0.1:
            return "increasing"
        elif slope < -0.1:
            return "decreasing"
        else:
            return "stable"

    def _initialize_alert_thresholds(self) -> Dict[str, Dict[str, Dict[str, Any]]]:
        """Initialize default alert thresholds."""
        return {
            "pipeline_execution_time_seconds": {
                "high": {
                    "value": 3600,  # 1 hour
                    "operator": "gt",
                    "alert_type": AlertType.PERFORMANCE_DEGRADATION,
                    "severity": MonitoringLevel.WARNING
                },
                "critical": {
                    "value": 7200,  # 2 hours
                    "operator": "gt",
                    "alert_type": AlertType.PERFORMANCE_DEGRADATION,
                    "severity": MonitoringLevel.ERROR
                }
            },
            "error_rate_percent": {
                "warning": {
                    "value": 5.0,
                    "operator": "gt",
                    "alert_type": AlertType.ERROR_THRESHOLD_EXCEEDED,
                    "severity": MonitoringLevel.WARNING
                },
                "critical": {
                    "value": 20.0,
                    "operator": "gt",
                    "alert_type": AlertType.ERROR_THRESHOLD_EXCEEDED,
                    "severity": MonitoringLevel.ERROR
                }
            },
            "memory_usage_percent": {
                "warning": {
                    "value": 80.0,
                    "operator": "gt",
                    "alert_type": AlertType.RESOURCE_EXHAUSTION,
                    "severity": MonitoringLevel.WARNING
                },
                "critical": {
                    "value": 95.0,
                    "operator": "gt",
                    "alert_type": AlertType.RESOURCE_EXHAUSTION,
                    "severity": MonitoringLevel.ERROR
                }
            },
            "cpu_usage_percent": {
                "warning": {
                    "value": 85.0,
                    "operator": "gt",
                    "alert_type": AlertType.RESOURCE_EXHAUSTION,
                    "severity": MonitoringLevel.WARNING
                },
                "critical": {
                    "value": 95.0,
                    "operator": "gt",
                    "alert_type": AlertType.RESOURCE_EXHAUSTION,
                    "severity": MonitoringLevel.ERROR
                }
            },
            "data_quality_score": {
                "warning": {
                    "value": 0.8,
                    "operator": "lt",
                    "alert_type": AlertType.DATA_QUALITY_ISSUE,
                    "severity": MonitoringLevel.WARNING
                },
                "critical": {
                    "value": 0.6,
                    "operator": "lt",
                    "alert_type": AlertType.DATA_QUALITY_ISSUE,
                    "severity": MonitoringLevel.ERROR
                }
            }
        }


class MetricsCollector:
    """Utility class for collecting system metrics."""

    def __init__(self, monitor: IntelligentMonitor):
        self.monitor = monitor
        self.logger = get_logger("metrics_collector")

    def collect_system_metrics(self) -> None:
        """Collect basic system metrics."""
        try:
            import psutil

            # CPU metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            self.monitor.record_metric("cpu_usage_percent", cpu_percent)

            # Memory metrics
            memory = psutil.virtual_memory()
            self.monitor.record_metric("memory_usage_percent", memory.percent)
            self.monitor.record_metric("memory_used_bytes", memory.used)
            self.monitor.record_metric("memory_available_bytes", memory.available)

            # Disk metrics
            disk = psutil.disk_usage('/')
            disk_percent = (disk.used / disk.total) * 100
            self.monitor.record_metric("disk_usage_percent", disk_percent)

        except ImportError:
            self.logger.warning("psutil not available - system metrics collection disabled")
        except Exception as e:
            self.logger.error(f"Error collecting system metrics: {e}")

    def collect_pipeline_metrics(self, pipeline_id: str, execution_time_seconds: float,
                                records_processed: int, errors_count: int) -> None:
        """
        Collect pipeline execution metrics.
        
        Args:
            pipeline_id: Pipeline identifier
            execution_time_seconds: Pipeline execution time
            records_processed: Number of records processed
            errors_count: Number of errors encountered
        """
        tags = {"pipeline_id": pipeline_id}

        self.monitor.record_metric("pipeline_execution_time_seconds", execution_time_seconds, tags)
        self.monitor.record_metric("pipeline_records_processed", records_processed, tags)
        self.monitor.record_metric("pipeline_errors_count", errors_count, tags)

        # Calculate derived metrics
        if execution_time_seconds > 0:
            throughput = records_processed / execution_time_seconds
            self.monitor.record_metric("pipeline_throughput_records_per_second", throughput, tags)

        if records_processed > 0:
            error_rate = (errors_count / records_processed) * 100
            self.monitor.record_metric("error_rate_percent", error_rate, tags)


# Context manager for monitoring pipeline execution
class MonitoredExecution:
    """Context manager for monitoring pipeline execution."""

    def __init__(self, monitor: IntelligentMonitor, pipeline_id: str, operation_name: str):
        self.monitor = monitor
        self.pipeline_id = pipeline_id
        self.operation_name = operation_name
        self.start_time = None
        self.records_processed = 0
        self.errors_count = 0

    def __enter__(self):
        self.start_time = time.time()
        self.monitor.record_metric(
            f"{self.operation_name}_started",
            1.0,
            {"pipeline_id": self.pipeline_id}
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        execution_time = time.time() - self.start_time

        # Record execution metrics
        tags = {"pipeline_id": self.pipeline_id, "operation": self.operation_name}
        self.monitor.record_metric(f"{self.operation_name}_execution_time_seconds", execution_time, tags)
        self.monitor.record_metric(f"{self.operation_name}_records_processed", self.records_processed, tags)
        self.monitor.record_metric(f"{self.operation_name}_errors_count", self.errors_count, tags)

        # Record success/failure
        success = exc_type is None
        self.monitor.record_metric(
            f"{self.operation_name}_success",
            1.0 if success else 0.0,
            tags
        )

        if not success:
            self.monitor.record_metric(
                f"{self.operation_name}_failure",
                1.0,
                {**tags, "error_type": exc_type.__name__ if exc_type else "unknown"}
            )

    def add_processed_records(self, count: int) -> None:
        """Add to processed records count."""
        self.records_processed += count

    def add_error(self, error: Exception = None) -> None:
        """Add to error count."""
        self.errors_count += 1
