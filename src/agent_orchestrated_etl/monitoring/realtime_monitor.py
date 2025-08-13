"""Real-time monitoring system with WebSocket support and metrics collection."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import time
from collections import defaultdict, deque
from typing import Any, Callable, Dict, List, Optional, Set

import psutil

from ..exceptions import MonitoringException
from ..logging_config import get_logger


class MetricsCollector:
    """Collects and stores performance metrics with time series support."""

    def __init__(self, db_path: Optional[str] = None):
        self.logger = get_logger("monitoring.metrics_collector")
        self.db_path = db_path or ":memory:"
        self.metrics_buffer: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.metric_definitions: Dict[str, Dict[str, Any]] = {}
        self.collection_intervals: Dict[str, float] = {}
        self.active_collectors: Dict[str, asyncio.Task] = {}
        self._initialize_database()

    def _initialize_database(self) -> None:
        """Initialize metrics database schema."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS metrics (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        metric_name TEXT NOT NULL,
                        metric_value REAL NOT NULL,
                        timestamp REAL NOT NULL,
                        pipeline_id TEXT,
                        task_id TEXT,
                        metadata TEXT
                    )
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_metrics_name_time 
                    ON metrics(metric_name, timestamp)
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_metrics_pipeline_time 
                    ON metrics(pipeline_id, timestamp)
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS alert_history (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        alert_type TEXT NOT NULL,
                        severity TEXT NOT NULL,
                        message TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        resolved BOOLEAN DEFAULT FALSE,
                        pipeline_id TEXT,
                        metadata TEXT
                    )
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_type_time 
                    ON alert_history(alert_type, timestamp)
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_alerts_pipeline_time 
                    ON alert_history(pipeline_id, timestamp)
                """)

                conn.execute("""
                    CREATE TABLE IF NOT EXISTS sla_compliance (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        pipeline_id TEXT NOT NULL,
                        sla_name TEXT NOT NULL,
                        target_value REAL NOT NULL,
                        actual_value REAL NOT NULL,
                        compliant BOOLEAN NOT NULL,
                        timestamp REAL NOT NULL
                    )
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sla_pipeline_time 
                    ON sla_compliance(pipeline_id, timestamp)
                """)

                conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_sla_name_time 
                    ON sla_compliance(sla_name, timestamp)
                """)

                conn.commit()
                self.logger.info("Metrics database initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize metrics database: {e}")
            raise MonitoringException(
                f"Database initialization failed: {e}",
                monitoring_target="database",
                monitoring_type="initialization"
            )

    def register_metric(self, name: str, description: str, unit: str, metric_type: str = "gauge") -> None:
        """Register a new metric definition."""
        self.metric_definitions[name] = {
            "description": description,
            "unit": unit,
            "type": metric_type,
            "registered_at": time.time()
        }
        self.logger.info(f"Registered metric: {name} ({metric_type})")

    def collect_metric(self, name: str, value: float, pipeline_id: Optional[str] = None,
                      task_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Collect a metric value."""
        timestamp = time.time()

        # Store in memory buffer
        metric_entry = {
            "value": value,
            "timestamp": timestamp,
            "pipeline_id": pipeline_id,
            "task_id": task_id,
            "metadata": metadata or {}
        }
        self.metrics_buffer[name].append(metric_entry)

        # Store in database
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO metrics (metric_name, metric_value, timestamp, pipeline_id, task_id, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (name, value, timestamp, pipeline_id, task_id, json.dumps(metadata or {})))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to store metric {name}: {e}")

    def get_metric_history(self, name: str, hours: int = 24) -> List[Dict[str, Any]]:
        """Get metric history for specified time range."""
        cutoff_time = time.time() - (hours * 3600)

        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.execute("""
                    SELECT metric_value, timestamp, pipeline_id, task_id, metadata
                    FROM metrics 
                    WHERE metric_name = ? AND timestamp >= ?
                    ORDER BY timestamp DESC
                """, (name, cutoff_time))

                return [
                    {
                        "value": row[0],
                        "timestamp": row[1],
                        "pipeline_id": row[2],
                        "task_id": row[3],
                        "metadata": json.loads(row[4] or "{}")
                    }
                    for row in cursor.fetchall()
                ]
        except Exception as e:
            self.logger.error(f"Failed to retrieve metric history for {name}: {e}")
            return []

    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metric values and statistics."""
        current_metrics = {}

        for metric_name, entries in self.metrics_buffer.items():
            if not entries:
                continue

            values = [entry["value"] for entry in entries]
            latest_entry = entries[-1]

            current_metrics[metric_name] = {
                "current_value": latest_entry["value"],
                "timestamp": latest_entry["timestamp"],
                "statistics": {
                    "count": len(values),
                    "average": sum(values) / len(values),
                    "minimum": min(values),
                    "maximum": max(values),
                    "latest": latest_entry["value"]
                },
                "definition": self.metric_definitions.get(metric_name, {})
            }

        return current_metrics


class AlertManager:
    """Manages alerts and notifications with escalation rules."""

    def __init__(self, metrics_collector: MetricsCollector):
        self.logger = get_logger("monitoring.alert_manager")
        self.metrics_collector = metrics_collector
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self.notification_channels: List[Callable] = []
        self.alert_history: List[Dict[str, Any]] = []
        self.active_alerts: Dict[str, Dict[str, Any]] = {}
        self.suppression_rules: List[Dict[str, Any]] = []

        # Default alert thresholds
        self._register_default_alert_rules()

    def _register_default_alert_rules(self) -> None:
        """Register default alert rules for common metrics."""
        default_rules = [
            {
                "name": "high_cpu_usage",
                "metric": "system.cpu_percent",
                "condition": "greater_than",
                "threshold": 90.0,
                "severity": "critical",
                "duration": 300  # 5 minutes
            },
            {
                "name": "high_memory_usage",
                "metric": "system.memory_percent",
                "condition": "greater_than",
                "threshold": 85.0,
                "severity": "warning",
                "duration": 180  # 3 minutes
            },
            {
                "name": "pipeline_failure",
                "metric": "pipeline.task_failure_rate",
                "condition": "greater_than",
                "threshold": 10.0,
                "severity": "critical",
                "duration": 0  # Immediate
            },
            {
                "name": "slow_pipeline_execution",
                "metric": "pipeline.execution_time",
                "condition": "greater_than",
                "threshold": 3600.0,  # 1 hour
                "severity": "warning",
                "duration": 0
            }
        ]

        for rule in default_rules:
            self.register_alert_rule(**rule)

    def register_alert_rule(self, name: str, metric: str, condition: str, threshold: float,
                           severity: str = "warning", duration: int = 0,
                           description: Optional[str] = None) -> None:
        """Register a new alert rule."""
        self.alert_rules[name] = {
            "metric": metric,
            "condition": condition,
            "threshold": threshold,
            "severity": severity,
            "duration": duration,
            "description": description or f"Alert when {metric} {condition} {threshold}",
            "registered_at": time.time(),
            "triggered_count": 0,
            "last_triggered": None
        }
        self.logger.info(f"Registered alert rule: {name}")

    def add_notification_channel(self, channel_func: Callable) -> None:
        """Add a notification channel function."""
        self.notification_channels.append(channel_func)
        self.logger.info(f"Added notification channel: {channel_func.__name__}")

    async def check_alerts(self) -> List[Dict[str, Any]]:
        """Check all alert rules and trigger alerts if conditions are met."""
        triggered_alerts = []
        current_time = time.time()

        for rule_name, rule in self.alert_rules.items():
            try:
                metric_name = rule["metric"]
                condition = rule["condition"]
                threshold = rule["threshold"]
                duration = rule["duration"]

                # Get recent metric values
                recent_metrics = self.metrics_collector.get_metric_history(metric_name, hours=1)

                if not recent_metrics:
                    continue

                # Check if condition is met
                latest_value = recent_metrics[0]["value"]
                condition_met = self._evaluate_condition(latest_value, condition, threshold)

                if condition_met:
                    # Check duration requirement
                    if duration > 0:
                        # Check if condition has been met for required duration
                        duration_met = self._check_duration_condition(
                            recent_metrics, condition, threshold, duration
                        )
                        if not duration_met:
                            continue

                    # Check if this alert is already active
                    alert_key = f"{rule_name}_{metric_name}"
                    if alert_key in self.active_alerts:
                        # Update existing alert
                        self.active_alerts[alert_key]["last_triggered"] = current_time
                        self.active_alerts[alert_key]["trigger_count"] += 1
                        continue

                    # Trigger new alert
                    alert = await self._trigger_alert(rule_name, rule, latest_value, current_time)
                    triggered_alerts.append(alert)

                else:
                    # Condition not met - resolve active alert if exists
                    alert_key = f"{rule_name}_{metric_name}"
                    if alert_key in self.active_alerts:
                        await self._resolve_alert(alert_key, current_time)

            except Exception as e:
                self.logger.error(f"Error checking alert rule {rule_name}: {e}")

        return triggered_alerts

    def _evaluate_condition(self, value: float, condition: str, threshold: float) -> bool:
        """Evaluate if alert condition is met."""
        if condition == "greater_than":
            return value > threshold
        elif condition == "less_than":
            return value < threshold
        elif condition == "equals":
            return abs(value - threshold) < 0.01
        elif condition == "not_equals":
            return abs(value - threshold) >= 0.01
        else:
            self.logger.warning(f"Unknown condition: {condition}")
            return False

    def _check_duration_condition(self, metrics: List[Dict[str, Any]], condition: str,
                                 threshold: float, duration: int) -> bool:
        """Check if condition has been met for required duration."""
        cutoff_time = time.time() - duration

        for metric in metrics:
            if metric["timestamp"] < cutoff_time:
                break
            if not self._evaluate_condition(metric["value"], condition, threshold):
                return False

        return True

    async def _trigger_alert(self, rule_name: str, rule: Dict[str, Any],
                           value: float, timestamp: float) -> Dict[str, Any]:
        """Trigger an alert and send notifications."""
        alert = {
            "id": f"alert_{int(timestamp * 1000)}",
            "rule_name": rule_name,
            "metric": rule["metric"],
            "severity": rule["severity"],
            "threshold": rule["threshold"],
            "actual_value": value,
            "message": f"{rule['description']} - Current value: {value}",
            "timestamp": timestamp,
            "resolved": False,
            "trigger_count": 1
        }

        # Store in active alerts
        alert_key = f"{rule_name}_{rule['metric']}"
        self.active_alerts[alert_key] = alert

        # Store in history
        self.alert_history.append(alert.copy())

        # Store in database
        try:
            with sqlite3.connect(self.metrics_collector.db_path) as conn:
                conn.execute("""
                    INSERT INTO alert_history (alert_type, severity, message, timestamp, resolved, metadata)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (rule_name, rule["severity"], alert["message"], timestamp, False, json.dumps(alert)))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to store alert in database: {e}")

        # Update rule statistics
        rule["triggered_count"] += 1
        rule["last_triggered"] = timestamp

        # Send notifications
        await self._send_notifications(alert)

        self.logger.warning(f"Alert triggered: {alert['message']}")
        return alert

    async def _resolve_alert(self, alert_key: str, timestamp: float) -> None:
        """Resolve an active alert."""
        if alert_key not in self.active_alerts:
            return

        alert = self.active_alerts[alert_key]
        alert["resolved"] = True
        alert["resolved_at"] = timestamp

        # Update in database
        try:
            with sqlite3.connect(self.metrics_collector.db_path) as conn:
                conn.execute("""
                    UPDATE alert_history 
                    SET resolved = TRUE 
                    WHERE alert_type = ? AND timestamp = ?
                """, (alert["rule_name"], alert["timestamp"]))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to update alert resolution in database: {e}")

        # Remove from active alerts
        del self.active_alerts[alert_key]

        self.logger.info(f"Alert resolved: {alert['rule_name']}")

    async def _send_notifications(self, alert: Dict[str, Any]) -> None:
        """Send alert notifications through configured channels."""
        for channel in self.notification_channels:
            try:
                await channel(alert)
            except Exception as e:
                self.logger.error(f"Failed to send notification via {channel.__name__}: {e}")


class SLAMonitor:
    """Monitors Service Level Agreement compliance."""

    def __init__(self, metrics_collector: MetricsCollector):
        self.logger = get_logger("monitoring.sla_monitor")
        self.metrics_collector = metrics_collector
        self.sla_definitions: Dict[str, Dict[str, Any]] = {}
        self.compliance_history: List[Dict[str, Any]] = []

        # Register default SLAs
        self._register_default_slas()

    def _register_default_slas(self) -> None:
        """Register default SLA definitions."""
        default_slas = [
            {
                "name": "pipeline_availability",
                "description": "Pipeline should be available 99.9% of the time",
                "target_value": 99.9,
                "metric": "pipeline.availability_percent",
                "measurement_period": "daily"
            },
            {
                "name": "pipeline_completion_time",
                "description": "Pipelines should complete within 30 minutes",
                "target_value": 1800.0,  # 30 minutes in seconds
                "metric": "pipeline.execution_time",
                "measurement_period": "per_execution"
            },
            {
                "name": "data_processing_accuracy",
                "description": "Data processing should maintain 99.5% accuracy",
                "target_value": 99.5,
                "metric": "pipeline.data_accuracy_percent",
                "measurement_period": "daily"
            }
        ]

        for sla in default_slas:
            self.register_sla(**sla)

    def register_sla(self, name: str, description: str, target_value: float,
                    metric: str, measurement_period: str = "daily") -> None:
        """Register a new SLA definition."""
        self.sla_definitions[name] = {
            "description": description,
            "target_value": target_value,
            "metric": metric,
            "measurement_period": measurement_period,
            "registered_at": time.time()
        }
        self.logger.info(f"Registered SLA: {name}")

    async def check_sla_compliance(self, pipeline_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Check SLA compliance for all registered SLAs."""
        compliance_results = []
        current_time = time.time()

        for sla_name, sla_def in self.sla_definitions.items():
            try:
                compliance_result = await self._check_single_sla(
                    sla_name, sla_def, pipeline_id, current_time
                )
                compliance_results.append(compliance_result)

                # Store compliance record
                await self._store_compliance_record(compliance_result)

            except Exception as e:
                self.logger.error(f"Error checking SLA {sla_name}: {e}")
                compliance_results.append({
                    "sla_name": sla_name,
                    "status": "error",
                    "error_message": str(e),
                    "timestamp": current_time
                })

        return compliance_results

    async def _check_single_sla(self, sla_name: str, sla_def: Dict[str, Any],
                               pipeline_id: Optional[str], timestamp: float) -> Dict[str, Any]:
        """Check compliance for a single SLA."""
        metric_name = sla_def["metric"]
        target_value = sla_def["target_value"]
        measurement_period = sla_def["measurement_period"]

        # Determine time range based on measurement period
        if measurement_period == "daily":
            hours = 24
        elif measurement_period == "hourly":
            hours = 1
        elif measurement_period == "weekly":
            hours = 168  # 24 * 7
        else:  # per_execution or other
            hours = 1

        # Get metric history
        metric_history = self.metrics_collector.get_metric_history(metric_name, hours)

        if not metric_history:
            return {
                "sla_name": sla_name,
                "status": "no_data",
                "message": f"No data available for metric {metric_name}",
                "timestamp": timestamp
            }

        # Calculate compliance based on SLA type
        if measurement_period == "per_execution":
            # Check each execution individually
            compliant_executions = 0
            total_executions = len(metric_history)

            for metric_entry in metric_history:
                if pipeline_id and metric_entry.get("pipeline_id") != pipeline_id:
                    continue

                if self._meets_sla_target(metric_entry["value"], target_value, sla_name):
                    compliant_executions += 1

            compliance_percentage = (compliant_executions / total_executions) * 100 if total_executions > 0 else 0
            is_compliant = compliance_percentage >= 95.0  # 95% of executions should meet SLA

            return {
                "sla_name": sla_name,
                "status": "compliant" if is_compliant else "non_compliant",
                "target_value": target_value,
                "actual_value": compliance_percentage,
                "compliant_executions": compliant_executions,
                "total_executions": total_executions,
                "measurement_period": measurement_period,
                "pipeline_id": pipeline_id,
                "timestamp": timestamp
            }

        else:
            # Calculate average for time-based periods
            values = [entry["value"] for entry in metric_history
                     if not pipeline_id or entry.get("pipeline_id") == pipeline_id]

            if not values:
                return {
                    "sla_name": sla_name,
                    "status": "no_data",
                    "message": f"No data for pipeline {pipeline_id}",
                    "timestamp": timestamp
                }

            average_value = sum(values) / len(values)
            is_compliant = self._meets_sla_target(average_value, target_value, sla_name)

            return {
                "sla_name": sla_name,
                "status": "compliant" if is_compliant else "non_compliant",
                "target_value": target_value,
                "actual_value": average_value,
                "data_points": len(values),
                "measurement_period": measurement_period,
                "pipeline_id": pipeline_id,
                "timestamp": timestamp
            }

    def _meets_sla_target(self, actual_value: float, target_value: float, sla_name: str) -> bool:
        """Determine if actual value meets SLA target."""
        # For completion time, lower is better
        if "time" in sla_name.lower() or "duration" in sla_name.lower():
            return actual_value <= target_value

        # For availability, accuracy, success rate - higher is better
        elif any(term in sla_name.lower() for term in ["availability", "accuracy", "success"]):
            return actual_value >= target_value

        # For error rates, failure rates - lower is better
        elif any(term in sla_name.lower() for term in ["error", "failure"]):
            return actual_value <= target_value

        # Default: assume higher is better
        else:
            return actual_value >= target_value

    async def _store_compliance_record(self, compliance_result: Dict[str, Any]) -> None:
        """Store SLA compliance record in database."""
        try:
            with sqlite3.connect(self.metrics_collector.db_path) as conn:
                conn.execute("""
                    INSERT INTO sla_compliance 
                    (pipeline_id, sla_name, target_value, actual_value, compliant, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    compliance_result.get("pipeline_id"),
                    compliance_result["sla_name"],
                    compliance_result.get("target_value", 0),
                    compliance_result.get("actual_value", 0),
                    compliance_result["status"] == "compliant",
                    compliance_result["timestamp"]
                ))
                conn.commit()
        except Exception as e:
            self.logger.error(f"Failed to store SLA compliance record: {e}")


class RealtimeMonitor:
    """Main real-time monitoring system that coordinates all monitoring components."""

    def __init__(self, db_path: Optional[str] = None):
        self.logger = get_logger("monitoring.realtime_monitor")
        self.metrics_collector = MetricsCollector(db_path)
        self.alert_manager = AlertManager(self.metrics_collector)
        self.sla_monitor = SLAMonitor(self.metrics_collector)

        # Real-time monitoring state
        self.monitoring_active = False
        self.monitoring_tasks: List[asyncio.Task] = []
        self.websocket_clients: Set[Any] = set()  # WebSocket connections for real-time updates

        # System resource monitoring
        self.system_monitoring_interval = 10.0  # seconds
        self.pipeline_monitoring_interval = 5.0  # seconds

        # Initialize built-in metrics
        self._register_builtin_metrics()

        # Set up notification channels
        self._setup_notification_channels()

    def _register_builtin_metrics(self) -> None:
        """Register built-in system and pipeline metrics."""
        builtin_metrics = [
            ("system.cpu_percent", "CPU usage percentage", "%", "gauge"),
            ("system.memory_percent", "Memory usage percentage", "%", "gauge"),
            ("system.disk_usage_percent", "Disk usage percentage", "%", "gauge"),
            ("system.network_bytes_sent", "Network bytes sent", "bytes", "counter"),
            ("system.network_bytes_recv", "Network bytes received", "bytes", "counter"),
            ("pipeline.execution_time", "Pipeline execution time", "seconds", "histogram"),
            ("pipeline.task_count", "Number of tasks in pipeline", "count", "gauge"),
            ("pipeline.task_failure_rate", "Task failure rate", "%", "gauge"),
            ("pipeline.data_accuracy_percent", "Data processing accuracy", "%", "gauge"),
            ("pipeline.availability_percent", "Pipeline availability", "%", "gauge"),
            ("pipeline.throughput_records_per_second", "Data processing throughput", "records/sec", "gauge")
        ]

        for name, description, unit, metric_type in builtin_metrics:
            self.metrics_collector.register_metric(name, description, unit, metric_type)

    def _setup_notification_channels(self) -> None:
        """Set up default notification channels."""
        # Console notification channel
        async def console_notification(alert: Dict[str, Any]) -> None:
            severity_emoji = {
                "critical": "🚨",
                "warning": "⚠️",
                "info": "ℹ️"
            }
            emoji = severity_emoji.get(alert["severity"], "📢")
            print(f"{emoji} ALERT [{alert['severity'].upper()}]: {alert['message']}")

        self.alert_manager.add_notification_channel(console_notification)

        # WebSocket notification channel (if clients are connected)
        async def websocket_notification(alert: Dict[str, Any]) -> None:
            if self.websocket_clients:
                message = {
                    "type": "alert",
                    "data": alert
                }
                await self._broadcast_to_websockets(message)

        self.alert_manager.add_notification_channel(websocket_notification)

    async def start_monitoring(self) -> None:
        """Start real-time monitoring."""
        if self.monitoring_active:
            self.logger.warning("Monitoring is already active")
            return

        self.monitoring_active = True
        self.logger.info("Starting real-time monitoring system")

        # Start monitoring tasks
        self.monitoring_tasks = [
            asyncio.create_task(self._system_monitoring_loop()),
            asyncio.create_task(self._alert_checking_loop()),
            asyncio.create_task(self._sla_monitoring_loop()),
            asyncio.create_task(self._metrics_cleanup_loop())
        ]

        self.logger.info(f"Started {len(self.monitoring_tasks)} monitoring tasks")

    async def stop_monitoring(self) -> None:
        """Stop real-time monitoring."""
        if not self.monitoring_active:
            return

        self.monitoring_active = False
        self.logger.info("Stopping real-time monitoring system")

        # Cancel all monitoring tasks
        for task in self.monitoring_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        self.monitoring_tasks.clear()
        self.logger.info("Real-time monitoring stopped")

    async def _system_monitoring_loop(self) -> None:
        """Continuously monitor system resources."""
        self.logger.info("Started system monitoring loop")

        while self.monitoring_active:
            try:
                # Collect system metrics
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                network = psutil.net_io_counters()

                # Collect metrics
                self.metrics_collector.collect_metric("system.cpu_percent", cpu_percent)
                self.metrics_collector.collect_metric("system.memory_percent", memory.percent)
                self.metrics_collector.collect_metric("system.disk_usage_percent", (disk.used / disk.total) * 100)
                self.metrics_collector.collect_metric("system.network_bytes_sent", network.bytes_sent)
                self.metrics_collector.collect_metric("system.network_bytes_recv", network.bytes_recv)

                # Broadcast metrics to WebSocket clients
                if self.websocket_clients:
                    metrics_update = {
                        "type": "metrics_update",
                        "data": {
                            "system": {
                                "cpu_percent": cpu_percent,
                                "memory_percent": memory.percent,
                                "disk_usage_percent": (disk.used / disk.total) * 100,
                                "timestamp": time.time()
                            }
                        }
                    }
                    await self._broadcast_to_websockets(metrics_update)

                await asyncio.sleep(self.system_monitoring_interval)

            except Exception as e:
                self.logger.error(f"Error in system monitoring loop: {e}")
                await asyncio.sleep(self.system_monitoring_interval)

    async def _alert_checking_loop(self) -> None:
        """Continuously check for alert conditions."""
        self.logger.info("Started alert checking loop")

        while self.monitoring_active:
            try:
                triggered_alerts = await self.alert_manager.check_alerts()

                if triggered_alerts and self.websocket_clients:
                    alerts_update = {
                        "type": "alerts_update",
                        "data": {
                            "new_alerts": triggered_alerts,
                            "active_alerts": list(self.alert_manager.active_alerts.values()),
                            "timestamp": time.time()
                        }
                    }
                    await self._broadcast_to_websockets(alerts_update)

                await asyncio.sleep(30)  # Check alerts every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in alert checking loop: {e}")
                await asyncio.sleep(30)

    async def _sla_monitoring_loop(self) -> None:
        """Continuously monitor SLA compliance."""
        self.logger.info("Started SLA monitoring loop")

        while self.monitoring_active:
            try:
                compliance_results = await self.sla_monitor.check_sla_compliance()

                if self.websocket_clients:
                    sla_update = {
                        "type": "sla_update",
                        "data": {
                            "compliance_results": compliance_results,
                            "timestamp": time.time()
                        }
                    }
                    await self._broadcast_to_websockets(sla_update)

                await asyncio.sleep(300)  # Check SLA compliance every 5 minutes

            except Exception as e:
                self.logger.error(f"Error in SLA monitoring loop: {e}")
                await asyncio.sleep(300)

    async def _metrics_cleanup_loop(self) -> None:
        """Periodically clean up old metrics data."""
        self.logger.info("Started metrics cleanup loop")

        while self.monitoring_active:
            try:
                # Clean up metrics older than 7 days
                cutoff_time = time.time() - (7 * 24 * 3600)

                with sqlite3.connect(self.metrics_collector.db_path) as conn:
                    # Clean up old metrics
                    cursor = conn.execute("DELETE FROM metrics WHERE timestamp < ?", (cutoff_time,))
                    metrics_deleted = cursor.rowcount

                    # Clean up old alerts
                    cursor = conn.execute("DELETE FROM alert_history WHERE timestamp < ?", (cutoff_time,))
                    alerts_deleted = cursor.rowcount

                    # Clean up old SLA records
                    cursor = conn.execute("DELETE FROM sla_compliance WHERE timestamp < ?", (cutoff_time,))
                    sla_records_deleted = cursor.rowcount

                    conn.commit()

                if metrics_deleted > 0 or alerts_deleted > 0 or sla_records_deleted > 0:
                    self.logger.info(f"Cleaned up old data: {metrics_deleted} metrics, {alerts_deleted} alerts, {sla_records_deleted} SLA records")

                # Sleep for 1 hour
                await asyncio.sleep(3600)

            except Exception as e:
                self.logger.error(f"Error in metrics cleanup loop: {e}")
                await asyncio.sleep(3600)

    async def _broadcast_to_websockets(self, message: Dict[str, Any]) -> None:
        """Broadcast message to all connected WebSocket clients."""
        if not self.websocket_clients:
            return

        message_json = json.dumps(message)
        disconnected_clients = set()

        for client in self.websocket_clients:
            try:
                await client.send(message_json)
            except Exception as e:
                self.logger.warning(f"Failed to send message to WebSocket client: {e}")
                disconnected_clients.add(client)

        # Remove disconnected clients
        self.websocket_clients -= disconnected_clients

    def add_websocket_client(self, websocket) -> None:
        """Add a WebSocket client for real-time updates."""
        self.websocket_clients.add(websocket)
        self.logger.info(f"Added WebSocket client. Total: {len(self.websocket_clients)}")

    def remove_websocket_client(self, websocket) -> None:
        """Remove a WebSocket client."""
        self.websocket_clients.discard(websocket)
        self.logger.info(f"Removed WebSocket client. Total: {len(self.websocket_clients)}")

    def collect_pipeline_metric(self, metric_name: str, value: float, pipeline_id: str,
                               task_id: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Collect a pipeline-specific metric."""
        self.metrics_collector.collect_metric(metric_name, value, pipeline_id, task_id, metadata)

    def get_monitoring_dashboard_data(self) -> Dict[str, Any]:
        """Get comprehensive dashboard data for monitoring UI."""
        current_metrics = self.metrics_collector.get_current_metrics()
        active_alerts = list(self.alert_manager.active_alerts.values())
        recent_alerts = [alert for alert in self.alert_manager.alert_history
                        if alert.get("timestamp", 0) > time.time() - 3600]

        return {
            "system_status": {
                "monitoring_active": self.monitoring_active,
                "connected_clients": len(self.websocket_clients),
                "total_metrics": len(current_metrics),
                "active_alerts": len(active_alerts),
                "timestamp": time.time()
            },
            "current_metrics": current_metrics,
            "active_alerts": active_alerts,
            "recent_alerts": recent_alerts,
            "alert_rules": list(self.alert_manager.alert_rules.keys()),
            "sla_definitions": list(self.sla_monitor.sla_definitions.keys())
        }
