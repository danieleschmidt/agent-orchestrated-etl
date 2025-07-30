#!/usr/bin/env python3
"""
Intelligent Observability Platform
Advanced monitoring, alerting, and observability with ML-driven insights
"""

import asyncio
import json
import time
import statistics
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Callable, Any, Union
from enum import Enum
import logging
import hashlib
from pathlib import Path
import aiohttp
import psutil
from collections import defaultdict, deque

class MetricType(Enum):
    """Types of metrics for monitoring."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    SUMMARY = "summary"

class AlertSeverity(Enum):
    """Alert severity levels."""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"
    DEBUG = "debug"

class AlertStatus(Enum):
    """Alert status tracking."""
    ACTIVE = "active"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

@dataclass
class Metric:
    """Structured metric data."""
    name: str
    value: Union[float, int]
    metric_type: MetricType
    timestamp: float
    labels: Dict[str, str]
    metadata: Optional[Dict[str, Any]] = None

@dataclass
class Alert:
    """Alert definition and state."""
    alert_id: str
    rule_name: str
    severity: AlertSeverity
    status: AlertStatus
    message: str
    timestamp: float
    labels: Dict[str, str]
    annotations: Dict[str, str]
    resolved_at: Optional[float] = None

@dataclass
class ObservabilityConfig:
    """Observability platform configuration."""
    metric_retention_days: int
    alert_evaluation_interval: int
    anomaly_detection_enabled: bool
    ml_insights_enabled: bool
    export_endpoints: List[str]
    custom_dashboards: List[str]

class IntelligentObservabilityPlatform:
    """Advanced observability platform with ML-driven insights."""
    
    def __init__(self, config: Optional[ObservabilityConfig] = None):
        self.config = config or self._create_default_config()
        self.metrics_store: Dict[str, deque] = defaultdict(lambda: deque(maxlen=10000))
        self.alerts: Dict[str, Alert] = {}
        self.alert_rules: List[Dict[str, Any]] = []
        self.anomaly_baselines: Dict[str, Dict[str, float]] = {}
        self.logger = self._setup_logging()
        self.is_monitoring = False
        self.dashboard_data: Dict[str, Any] = {}
        
    def _setup_logging(self) -> logging.Logger:
        """Setup comprehensive observability logging."""
        logger = logging.getLogger("observability_platform")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Structured JSON logging for observability
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger
    
    def _create_default_config(self) -> ObservabilityConfig:
        """Create default observability configuration."""
        return ObservabilityConfig(
            metric_retention_days=30,
            alert_evaluation_interval=30,
            anomaly_detection_enabled=True,
            ml_insights_enabled=True,
            export_endpoints=["prometheus", "grafana"],
            custom_dashboards=["system", "application", "business"]
        )
    
    def register_metric(self, metric: Metric):
        """Register a new metric in the observability platform."""
        metric_key = f"{metric.name}:{':'.join([f'{k}={v}' for k, v in metric.labels.items()])}"
        self.metrics_store[metric_key].append(metric)
        
        # Update anomaly detection baselines
        self._update_anomaly_baseline(metric_key, metric.value)
        
        self.logger.debug(f"Registered metric: {metric.name} = {metric.value}")
    
    def _update_anomaly_baseline(self, metric_key: str, value: Union[float, int]):
        """Update anomaly detection baseline for a metric."""
        if metric_key not in self.anomaly_baselines:
            self.anomaly_baselines[metric_key] = {
                "mean": float(value),
                "std": 0.0,
                "count": 1,
                "min": float(value),
                "max": float(value)
            }
        else:
            baseline = self.anomaly_baselines[metric_key]
            baseline["count"] += 1
            
            # Update running statistics
            old_mean = baseline["mean"]
            baseline["mean"] = old_mean + (float(value) - old_mean) / baseline["count"]
            
            if baseline["count"] > 1:
                baseline["std"] = ((baseline["count"] - 2) * baseline["std"]**2 + 
                                 (float(value) - old_mean) * (float(value) - baseline["mean"])) / (baseline["count"] - 1)
                baseline["std"] = baseline["std"] ** 0.5
            
            baseline["min"] = min(baseline["min"], float(value))
            baseline["max"] = max(baseline["max"], float(value))
    
    def detect_anomalies(self, metric_key: str, current_value: Union[float, int]) -> Dict[str, Any]:
        """Detect anomalies using statistical analysis."""
        if metric_key not in self.anomaly_baselines:
            return {"is_anomaly": False, "confidence": 0.0}
        
        baseline = self.anomaly_baselines[metric_key]
        
        if baseline["count"] < 10 or baseline["std"] == 0:
            return {"is_anomaly": False, "confidence": 0.0, "reason": "insufficient_data"}
        
        # Z-score based anomaly detection
        z_score = abs(float(current_value) - baseline["mean"]) / baseline["std"]
        
        # Consider values beyond 3 standard deviations as anomalous
        is_anomaly = z_score > 3.0
        confidence = min(1.0, z_score / 3.0) if is_anomaly else 0.0
        
        return {
            "is_anomaly": is_anomaly,
            "confidence": confidence,
            "z_score": z_score,
            "baseline_mean": baseline["mean"],
            "baseline_std": baseline["std"],
            "deviation": float(current_value) - baseline["mean"]
        }
    
    def add_alert_rule(self, rule: Dict[str, Any]):
        """Add a new alert rule for monitoring."""
        required_fields = ["name", "condition", "severity", "message"]
        if not all(field in rule for field in required_fields):
            raise ValueError(f"Alert rule must contain: {required_fields}")
        
        rule["id"] = hashlib.md5(rule["name"].encode()).hexdigest()[:8]
        self.alert_rules.append(rule)
        self.logger.info(f"Added alert rule: {rule['name']}")
    
    def evaluate_alert_rules(self) -> List[Alert]:
        """Evaluate all alert rules and generate alerts."""
        new_alerts = []
        current_time = time.time()
        
        for rule in self.alert_rules:
            try:
                alert_triggered = self._evaluate_single_rule(rule)
                
                if alert_triggered:
                    alert_id = f"{rule['id']}:{int(current_time)}"
                    
                    alert = Alert(
                        alert_id=alert_id,
                        rule_name=rule["name"],
                        severity=AlertSeverity(rule["severity"]),
                        status=AlertStatus.ACTIVE,
                        message=rule["message"],
                        timestamp=current_time,
                        labels=rule.get("labels", {}),
                        annotations=rule.get("annotations", {})
                    )
                    
                    self.alerts[alert_id] = alert
                    new_alerts.append(alert)
                    
                    self.logger.warning(f"Alert triggered: {rule['name']} - {rule['message']}")
            
            except Exception as e:
                self.logger.error(f"Error evaluating alert rule {rule['name']}: {e}")
        
        return new_alerts
    
    def _evaluate_single_rule(self, rule: Dict[str, Any]) -> bool:
        """Evaluate a single alert rule condition."""
        condition = rule["condition"]
        
        # Simple condition evaluation (in production, use more sophisticated parser)
        if "metric" in condition and "threshold" in condition:
            metric_name = condition["metric"]
            threshold = condition["threshold"]
            operator = condition.get("operator", ">")
            
            # Get latest metric value
            metric_keys = [k for k in self.metrics_store.keys() if k.startswith(f"{metric_name}:")]
            if not metric_keys:
                return False
            
            latest_metric = self.metrics_store[metric_keys[0]][-1] if self.metrics_store[metric_keys[0]] else None
            if not latest_metric:
                return False
            
            # Evaluate condition
            if operator == ">":
                return latest_metric.value > threshold
            elif operator == "<":
                return latest_metric.value < threshold
            elif operator == ">=":
                return latest_metric.value >= threshold
            elif operator == "<=":
                return latest_metric.value <= threshold
            elif operator == "==":
                return latest_metric.value == threshold
        
        # Anomaly-based alerting
        if "anomaly_detection" in condition:
            metric_name = condition["metric"]
            confidence_threshold = condition.get("confidence_threshold", 0.8)
            
            metric_keys = [k for k in self.metrics_store.keys() if k.startswith(f"{metric_name}:")]
            if not metric_keys:
                return False
            
            latest_metric = self.metrics_store[metric_keys[0]][-1] if self.metrics_store[metric_keys[0]] else None
            if not latest_metric:
                return False
            
            anomaly_result = self.detect_anomalies(metric_keys[0], latest_metric.value)
            return anomaly_result["is_anomaly"] and anomaly_result["confidence"] >= confidence_threshold
        
        return False
    
    async def collect_system_metrics(self):
        """Collect comprehensive system metrics."""
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        self.register_metric(Metric(
            name="system_cpu_percent",
            value=cpu_percent,
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system"}
        ))
        
        # Memory metrics
        memory = psutil.virtual_memory()
        self.register_metric(Metric(
            name="system_memory_percent",
            value=memory.percent,
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system"}
        ))
        
        self.register_metric(Metric(
            name="system_memory_available_bytes",
            value=memory.available,
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system"}
        ))
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        self.register_metric(Metric(
            name="system_disk_usage_percent",
            value=(disk.used / disk.total) * 100,
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system", "mountpoint": "/"}
        ))
        
        # Network metrics
        network = psutil.net_io_counters()
        self.register_metric(Metric(
            name="system_network_bytes_sent",
            value=network.bytes_sent,
            metric_type=MetricType.COUNTER,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system", "direction": "sent"}
        ))
        
        self.register_metric(Metric(
            name="system_network_bytes_received",
            value=network.bytes_recv,
            metric_type=MetricType.COUNTER,
            timestamp=time.time(),
            labels={"host": "localhost", "component": "system", "direction": "received"}
        ))
    
    async def collect_application_metrics(self):
        """Collect application-specific metrics."""
        # Simulate application metrics
        import random
        
        # Request rate
        self.register_metric(Metric(
            name="app_requests_per_second",
            value=100 + random.randint(-20, 50),
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"app": "agent-orchestrated-etl", "endpoint": "api"}
        ))
        
        # Response time
        self.register_metric(Metric(
            name="app_response_time_ms",
            value=150 + random.randint(-50, 100),
            metric_type=MetricType.HISTOGRAM,
            timestamp=time.time(),
            labels={"app": "agent-orchestrated-etl", "endpoint": "api"}
        ))
        
        # Error rate
        self.register_metric(Metric(
            name="app_error_rate",
            value=0.02 + random.random() * 0.03,
            metric_type=MetricType.GAUGE,
            timestamp=time.time(),
            labels={"app": "agent-orchestrated-etl", "component": "api"}
        ))
        
        # Business metrics
        self.register_metric(Metric(
            name="etl_pipelines_processed",
            value=random.randint(10, 50),
            metric_type=MetricType.COUNTER,
            timestamp=time.time(),
            labels={"app": "agent-orchestrated-etl", "component": "pipeline"}
        ))
    
    def setup_default_alert_rules(self):
        """Setup default alert rules for common issues."""
        default_rules = [
            {
                "name": "high_cpu_usage",
                "condition": {
                    "metric": "system_cpu_percent",
                    "operator": ">",
                    "threshold": 80.0
                },
                "severity": "warning",
                "message": "CPU usage is above 80%",
                "labels": {"team": "platform", "component": "system"},
                "annotations": {"runbook": "https://docs.company.com/runbooks/high-cpu"}
            },
            {
                "name": "high_memory_usage",
                "condition": {
                    "metric": "system_memory_percent",
                    "operator": ">",
                    "threshold": 85.0
                },
                "severity": "critical",
                "message": "Memory usage is critically high",
                "labels": {"team": "platform", "component": "system"}
            },
            {
                "name": "high_error_rate",
                "condition": {
                    "metric": "app_error_rate",
                    "operator": ">",
                    "threshold": 0.05
                },
                "severity": "critical",
                "message": "Application error rate exceeds 5%",
                "labels": {"team": "engineering", "component": "application"}
            },
            {
                "name": "response_time_anomaly",
                "condition": {
                    "metric": "app_response_time_ms",
                    "anomaly_detection": True,
                    "confidence_threshold": 0.8
                },
                "severity": "warning",
                "message": "Response time shows anomalous behavior",
                "labels": {"team": "engineering", "component": "performance"}
            }
        ]
        
        for rule in default_rules:
            self.add_alert_rule(rule)
    
    def generate_dashboard_data(self) -> Dict[str, Any]:
        """Generate comprehensive dashboard data."""
        current_time = time.time()
        dashboard = {
            "timestamp": current_time,
            "system_overview": self._generate_system_overview(),
            "application_metrics": self._generate_application_overview(),
            "alerts_summary": self._generate_alerts_summary(),
            "anomaly_insights": self._generate_anomaly_insights(),
            "performance_trends": self._generate_performance_trends(),
            "resource_utilization": self._generate_resource_utilization()
        }
        
        self.dashboard_data = dashboard
        return dashboard
    
    def _generate_system_overview(self) -> Dict[str, Any]:
        """Generate system metrics overview."""
        system_metrics = {}
        
        for metric_key, metric_deque in self.metrics_store.items():
            if "system_" in metric_key and metric_deque:
                latest_metric = metric_deque[-1]
                metric_name = latest_metric.name.replace("system_", "")
                system_metrics[metric_name] = {
                    "current": latest_metric.value,
                    "timestamp": latest_metric.timestamp
                }
        
        return system_metrics
    
    def _generate_application_overview(self) -> Dict[str, Any]:
        """Generate application metrics overview."""
        app_metrics = {}
        
        for metric_key, metric_deque in self.metrics_store.items():
            if "app_" in metric_key and metric_deque:
                latest_metric = metric_deque[-1]
                metric_name = latest_metric.name.replace("app_", "")
                app_metrics[metric_name] = {
                    "current": latest_metric.value,
                    "timestamp": latest_metric.timestamp
                }
        
        return app_metrics
    
    def _generate_alerts_summary(self) -> Dict[str, Any]:
        """Generate alerts summary for dashboard."""
        active_alerts = [alert for alert in self.alerts.values() if alert.status == AlertStatus.ACTIVE]
        
        severity_counts = defaultdict(int)
        for alert in active_alerts:
            severity_counts[alert.severity.value] += 1
        
        return {
            "total_active": len(active_alerts),
            "by_severity": dict(severity_counts),
            "recent_alerts": [asdict(alert) for alert in sorted(active_alerts, key=lambda a: a.timestamp, reverse=True)[:5]]
        }
    
    def _generate_anomaly_insights(self) -> Dict[str, Any]:
        """Generate ML-driven anomaly insights."""
        insights = {
            "anomalies_detected": 0,
            "metrics_analyzed": len(self.anomaly_baselines),
            "top_anomalies": []
        }
        
        # Check recent metrics for anomalies
        anomaly_results = []
        for metric_key, metric_deque in self.metrics_store.items():
            if metric_deque:
                latest_metric = metric_deque[-1]
                anomaly_result = self.detect_anomalies(metric_key, latest_metric.value)
                
                if anomaly_result["is_anomaly"]:
                    anomaly_results.append({
                        "metric": latest_metric.name,
                        "confidence": anomaly_result["confidence"],
                        "z_score": anomaly_result["z_score"],
                        "current_value": latest_metric.value,
                        "baseline_mean": anomaly_result["baseline_mean"]
                    })
        
        insights["anomalies_detected"] = len(anomaly_results)
        insights["top_anomalies"] = sorted(anomaly_results, key=lambda a: a["confidence"], reverse=True)[:5]
        
        return insights
    
    def _generate_performance_trends(self) -> Dict[str, Any]:
        """Generate performance trend analysis."""
        trends = {}
        
        # Analyze trends for key performance metrics
        performance_metrics = ["app_response_time_ms", "app_requests_per_second", "system_cpu_percent"]
        
        for metric_name in performance_metrics:
            metric_keys = [k for k in self.metrics_store.keys() if metric_name in k]
            
            if metric_keys and self.metrics_store[metric_keys[0]]:
                values = [m.value for m in list(self.metrics_store[metric_keys[0]])[-20:]]  # Last 20 values
                
                if len(values) >= 2:
                    # Simple trend calculation
                    recent_avg = statistics.mean(values[-5:]) if len(values) >= 5 else values[-1]
                    older_avg = statistics.mean(values[:5]) if len(values) >= 10 else statistics.mean(values[:-5]) if len(values) > 5 else values[0]
                    
                    trend_direction = "up" if recent_avg > older_avg else "down" if recent_avg < older_avg else "stable"
                    trend_magnitude = abs(recent_avg - older_avg) / older_avg * 100 if older_avg != 0 else 0
                    
                    trends[metric_name] = {
                        "direction": trend_direction,
                        "magnitude": trend_magnitude,
                        "current": recent_avg,
                        "previous": older_avg
                    }
        
        return trends
    
    def _generate_resource_utilization(self) -> Dict[str, Any]:
        """Generate resource utilization analysis."""
        utilization = {
            "cpu": {},
            "memory": {},
            "disk": {},
            "network": {}
        }
        
        # Get latest system metrics
        for metric_key, metric_deque in self.metrics_store.items():
            if metric_deque:
                latest_metric = metric_deque[-1]
                
                if "cpu_percent" in metric_key:
                    utilization["cpu"]["current"] = latest_metric.value
                    utilization["cpu"]["status"] = "high" if latest_metric.value > 80 else "normal"
                elif "memory_percent" in metric_key:
                    utilization["memory"]["current"] = latest_metric.value
                    utilization["memory"]["status"] = "high" if latest_metric.value > 85 else "normal"
                elif "disk_usage_percent" in metric_key:
                    utilization["disk"]["current"] = latest_metric.value
                    utilization["disk"]["status"] = "high" if latest_metric.value > 90 else "normal"
        
        return utilization
    
    async def start_monitoring(self):
        """Start comprehensive monitoring and observability."""
        self.is_monitoring = True
        self.logger.info("Starting intelligent observability monitoring...")
        
        # Setup default alert rules
        self.setup_default_alert_rules()
        
        while self.is_monitoring:
            try:
                # Collect metrics
                await self.collect_system_metrics()
                await self.collect_application_metrics()
                
                # Evaluate alerts
                new_alerts = self.evaluate_alert_rules()
                
                # Generate dashboard data
                dashboard_data = self.generate_dashboard_data()
                
                # Log monitoring status
                active_alerts = len([a for a in self.alerts.values() if a.status == AlertStatus.ACTIVE])
                self.logger.info(f"Monitoring cycle complete - {len(self.metrics_store)} metrics tracked, {active_alerts} active alerts")
                
                await asyncio.sleep(self.config.alert_evaluation_interval)
                
            except Exception as e:
                self.logger.error(f"Error in monitoring loop: {e}")
                await asyncio.sleep(self.config.alert_evaluation_interval)
    
    def stop_monitoring(self):
        """Stop observability monitoring."""
        self.is_monitoring = False
        self.logger.info("Observability monitoring stopped")
    
    def export_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        prometheus_output = []
        
        for metric_key, metric_deque in self.metrics_store.items():
            if metric_deque:
                latest_metric = metric_deque[-1]
                
                # Format labels
                labels_str = ",".join([f'{k}="{v}"' for k, v in latest_metric.labels.items()])
                labels_part = f"{{{labels_str}}}" if labels_str else ""
                
                # Add metric line
                prometheus_output.append(f"{latest_metric.name}{labels_part} {latest_metric.value} {int(latest_metric.timestamp * 1000)}")
        
        return "\n".join(prometheus_output)
    
    def export_observability_report(self, filename: str = "observability-report.json") -> Dict[str, Any]:
        """Export comprehensive observability analysis report."""
        report = {
            "timestamp": time.time(),
            "config": asdict(self.config),
            "metrics_summary": {
                "total_metrics": len(self.metrics_store),
                "metrics_with_data": len([k for k, v in self.metrics_store.items() if v]),
                "anomaly_baselines": len(self.anomaly_baselines)
            },
            "alerts_summary": {
                "total_rules": len(self.alert_rules),
                "total_alerts": len(self.alerts),
                "active_alerts": len([a for a in self.alerts.values() if a.status == AlertStatus.ACTIVE])
            },
            "dashboard_data": self.dashboard_data,
            "alert_rules": self.alert_rules,
            "recent_alerts": [asdict(alert) for alert in sorted(self.alerts.values(), key=lambda a: a.timestamp, reverse=True)[:10]],
            "anomaly_baselines": self.anomaly_baselines,
            "recommendations": self._generate_observability_recommendations()
        }
        
        with open(filename, 'w') as f:
            json.dump(report, f, indent=2)
        
        return report
    
    def _generate_observability_recommendations(self) -> List[str]:
        """Generate actionable observability recommendations."""
        recommendations = []
        
        active_alerts = len([a for a in self.alerts.values() if a.status == AlertStatus.ACTIVE])
        if active_alerts > 5:
            recommendations.append("High number of active alerts suggests need for alert rule tuning")
        
        metrics_count = len(self.metrics_store)
        if metrics_count < 10:
            recommendations.append("Consider adding more application-specific metrics for better observability")
        
        anomaly_enabled_metrics = len(self.anomaly_baselines)
        if anomaly_enabled_metrics > 0:
            recommendations.append(f"Anomaly detection active for {anomaly_enabled_metrics} metrics - providing ML-driven insights")
        
        if self.config.ml_insights_enabled:
            recommendations.append("ML insights enabled - leveraging advanced analytics for predictive monitoring")
        
        return recommendations

# CLI usage example
async def main():
    """Example usage of intelligent observability platform."""
    platform = IntelligentObservabilityPlatform()
    
    # Start monitoring for a demonstration period
    monitoring_task = asyncio.create_task(platform.start_monitoring())
    
    # Let it run for 2 minutes to collect data
    await asyncio.sleep(120)
    
    # Stop monitoring and generate report
    platform.stop_monitoring()
    await monitoring_task
    
    # Export reports
    report = platform.export_observability_report()
    prometheus_metrics = platform.export_prometheus_metrics()
    
    print("Observability platform demonstration complete!")
    print(f"Tracked {report['metrics_summary']['total_metrics']} metrics")
    print(f"Generated {report['alerts_summary']['total_alerts']} alerts")
    print(f"Detected {report['dashboard_data']['anomaly_insights']['anomalies_detected']} anomalies")
    
    # Save Prometheus metrics
    with open("metrics.prom", "w") as f:
        f.write(prometheus_metrics)
    
    print("Reports generated: observability-report.json, metrics.prom")

if __name__ == "__main__":
    asyncio.run(main())