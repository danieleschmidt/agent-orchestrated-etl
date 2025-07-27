"""Metrics collection and export for monitoring."""

import time
from typing import Any, Dict, Optional
from collections import defaultdict, Counter
from threading import Lock

from pydantic import BaseModel


class MetricValue(BaseModel):
    """Metric value with metadata."""
    
    name: str
    value: float
    timestamp: float
    labels: Dict[str, str] = {}
    help_text: str = ""


class MetricsCollector:
    """Thread-safe metrics collector."""
    
    def __init__(self) -> None:
        self._metrics: Dict[str, MetricValue] = {}
        self._counters: Counter = Counter()
        self._histograms: Dict[str, list] = defaultdict(list)
        self._lock = Lock()
    
    def counter(self, name: str, labels: Optional[Dict[str, str]] = None, 
                help_text: str = "") -> None:
        """Increment a counter metric."""
        with self._lock:
            key = self._make_key(name, labels or {})
            self._counters[key] += 1
            
            self._metrics[key] = MetricValue(
                name=name,
                value=self._counters[key],
                timestamp=time.time(),
                labels=labels or {},
                help_text=help_text,
            )
    
    def gauge(self, name: str, value: float, 
              labels: Optional[Dict[str, str]] = None,
              help_text: str = "") -> None:
        """Set a gauge metric."""
        with self._lock:
            key = self._make_key(name, labels or {})
            
            self._metrics[key] = MetricValue(
                name=name,
                value=value,
                timestamp=time.time(),
                labels=labels or {},
                help_text=help_text,
            )
    
    def histogram(self, name: str, value: float,
                  labels: Optional[Dict[str, str]] = None,
                  help_text: str = "") -> None:
        """Record a histogram metric."""
        with self._lock:
            key = self._make_key(name, labels or {})
            self._histograms[key].append(value)
            
            # Calculate basic statistics
            values = self._histograms[key]
            mean_value = sum(values) / len(values)
            
            self._metrics[f"{key}_sum"] = MetricValue(
                name=f"{name}_sum",
                value=sum(values),
                timestamp=time.time(),
                labels=labels or {},
                help_text=f"{help_text} (sum)",
            )
            
            self._metrics[f"{key}_count"] = MetricValue(
                name=f"{name}_count",
                value=len(values),
                timestamp=time.time(),
                labels=labels or {},
                help_text=f"{help_text} (count)",
            )
            
            self._metrics[f"{key}_avg"] = MetricValue(
                name=f"{name}_avg",
                value=mean_value,
                timestamp=time.time(),
                labels=labels or {},
                help_text=f"{help_text} (average)",
            )
    
    def timing(self, name: str, duration: float,
               labels: Optional[Dict[str, str]] = None,
               help_text: str = "") -> None:
        """Record a timing metric."""
        self.histogram(
            name=f"{name}_duration_seconds",
            value=duration,
            labels=labels,
            help_text=help_text or f"Duration of {name} in seconds",
        )
    
    def get_metrics(self) -> Dict[str, MetricValue]:
        """Get all metrics."""
        with self._lock:
            return dict(self._metrics)
    
    def get_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        
        with self._lock:
            # Group metrics by name
            grouped = defaultdict(list)
            for metric in self._metrics.values():
                grouped[metric.name].append(metric)
            
            for name, metrics in grouped.items():
                # Add help text (use first metric's help text)
                if metrics[0].help_text:
                    lines.append(f"# HELP {name} {metrics[0].help_text}")
                
                # Determine metric type
                if name.endswith(('_total', '_count')):
                    lines.append(f"# TYPE {name} counter")
                elif name.endswith(('_sum', '_avg')):
                    lines.append(f"# TYPE {name} gauge")
                else:
                    lines.append(f"# TYPE {name} gauge")
                
                # Add metric values
                for metric in metrics:
                    labels_str = ""
                    if metric.labels:
                        label_pairs = [f'{k}="{v}"' for k, v in metric.labels.items()]
                        labels_str = "{" + ",".join(label_pairs) + "}"
                    
                    lines.append(f"{name}{labels_str} {metric.value}")
        
        return "\n".join(lines)
    
    def reset(self) -> None:
        """Reset all metrics."""
        with self._lock:
            self._metrics.clear()
            self._counters.clear()
            self._histograms.clear()
    
    @staticmethod
    def _make_key(name: str, labels: Dict[str, str]) -> str:
        """Create a unique key for a metric."""
        if not labels:
            return name
        
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}[{label_str}]"


# Global metrics collector
metrics = MetricsCollector()


class MetricsMiddleware:
    """Metrics middleware for collecting HTTP metrics."""
    
    def __init__(self, app: Any) -> None:
        self.app = app
    
    async def __call__(self, scope: Dict[str, Any], receive: Any, send: Any) -> None:
        """ASGI middleware for metrics collection."""
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        
        start_time = time.time()
        method = scope["method"]
        path = scope["path"]
        
        # Increment request counter
        metrics.counter(
            "http_requests_total",
            labels={"method": method, "path": path},
            help_text="Total HTTP requests"
        )
        
        async def send_wrapper(message: Dict[str, Any]) -> None:
            if message["type"] == "http.response.start":
                status_code = message["status"]
                duration = time.time() - start_time
                
                # Record timing
                metrics.timing(
                    "http_request",
                    duration,
                    labels={"method": method, "path": path, "status": str(status_code)},
                    help_text="HTTP request duration"
                )
                
                # Record status code
                metrics.counter(
                    "http_responses_total",
                    labels={"method": method, "path": path, "status": str(status_code)},
                    help_text="Total HTTP responses by status code"
                )
            
            await send(message)
        
        await self.app(scope, receive, send_wrapper)


def record_pipeline_metric(pipeline_id: str, status: str, duration: float) -> None:
    """Record pipeline execution metrics."""
    metrics.counter(
        "pipeline_executions_total",
        labels={"pipeline_id": pipeline_id, "status": status},
        help_text="Total pipeline executions"
    )
    
    metrics.timing(
        "pipeline_execution",
        duration,
        labels={"pipeline_id": pipeline_id, "status": status},
        help_text="Pipeline execution duration"
    )


def record_agent_metric(agent_type: str, action: str, success: bool) -> None:
    """Record agent action metrics."""
    status = "success" if success else "failure"
    
    metrics.counter(
        "agent_actions_total",
        labels={"agent_type": agent_type, "action": action, "status": status},
        help_text="Total agent actions"
    )


def record_data_processing_metric(source_type: str, records_count: int, 
                                 processing_time: float) -> None:
    """Record data processing metrics."""
    metrics.counter(
        "data_records_processed_total",
        labels={"source_type": source_type},
        help_text="Total data records processed"
    )
    
    metrics.gauge(
        "data_processing_records",
        records_count,
        labels={"source_type": source_type},
        help_text="Number of records in latest processing batch"
    )
    
    metrics.timing(
        "data_processing",
        processing_time,
        labels={"source_type": source_type},
        help_text="Data processing duration"
    )