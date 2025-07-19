"""Monitor agent for tracking ETL operations and system health."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, List, Optional, Set

from langchain_core.language_models.base import BaseLanguageModel

from .base_agent import BaseAgent, AgentConfig, AgentRole, AgentTask, AgentCapability
from .communication import AgentCommunicationHub
from .memory import AgentMemory, MemoryType, MemoryImportance
from .tools import AgentToolRegistry, get_tool_registry
from ..exceptions import AgentException, MonitoringException
from ..logging_config import LogContext


class MonitorAgent(BaseAgent):
    """Monitor agent for tracking ETL operations and system health."""
    
    def __init__(
        self,
        config: Optional[AgentConfig] = None,
        llm: Optional[BaseLanguageModel] = None,
        communication_hub: Optional[AgentCommunicationHub] = None,
        tool_registry: Optional[AgentToolRegistry] = None,
        monitoring_scope: str = "system",
    ):
        # Set default config for monitor agent
        if config is None:
            config = AgentConfig(
                name=f"MonitorAgent_{monitoring_scope}",
                role=AgentRole.MONITOR,
                max_concurrent_tasks=10,  # Monitors can handle many concurrent tasks
                task_timeout_seconds=300.0,
            )
        elif config.role != AgentRole.MONITOR:
            config.role = AgentRole.MONITOR
        
        # Monitor-specific attributes (set before super() call as _initialize_agent uses them)
        self.monitoring_scope = monitoring_scope
        
        super().__init__(config, llm, communication_hub)
        
        # Monitor-specific components
        self.tool_registry = tool_registry or get_tool_registry()
        self.memory = AgentMemory(
            agent_id=self.config.agent_id,
            max_entries=100000,  # Monitors need extensive memory for metrics
            working_memory_size=500,
        )
        
        # Monitoring configuration
        self.monitoring_scope = monitoring_scope
        self.monitoring_targets: Set[str] = set()
        self.alert_thresholds: Dict[str, Dict[str, Any]] = {}
        self.monitoring_intervals: Dict[str, float] = {}
        
        # Monitoring state
        self.active_monitors: Dict[str, Dict[str, Any]] = {}
        self.alert_history: List[Dict[str, Any]] = []
        self.performance_metrics: Dict[str, List[Dict[str, Any]]] = {}
        self.health_status: Dict[str, str] = {}
        
        # Background monitoring tasks
        self.monitoring_tasks: Dict[str, asyncio.Task] = {}
        self.monitoring_enabled = False
        
        # Alert management
        self.alert_escalation_rules: List[Dict[str, Any]] = []
        self.notification_channels: List[str] = []
    
    def _initialize_agent(self) -> None:
        """Initialize monitor agent-specific components."""
        self.logger.info(f"Initializing Monitor Agent with scope: {self.monitoring_scope}")
        
        # Set default alert thresholds
        self._set_default_thresholds()
        
        # Set default monitoring intervals
        self._set_default_intervals()
    
    def _set_default_thresholds(self) -> None:
        """Set default alert thresholds for various metrics."""
        self.alert_thresholds = {
            "cpu_usage": {"warning": 70.0, "critical": 90.0},
            "memory_usage": {"warning": 80.0, "critical": 95.0},
            "disk_usage": {"warning": 85.0, "critical": 95.0},
            "error_rate": {"warning": 5.0, "critical": 10.0},
            "response_time": {"warning": 5000.0, "critical": 10000.0},  # milliseconds
            "queue_depth": {"warning": 100, "critical": 500},
            "pipeline_duration": {"warning": 3600.0, "critical": 7200.0},  # seconds
        }
    
    def _set_default_intervals(self) -> None:
        """Set default monitoring intervals for different types of checks."""
        self.monitoring_intervals = {
            "system_health": 30.0,      # 30 seconds
            "pipeline_status": 60.0,    # 1 minute
            "performance_metrics": 120.0, # 2 minutes
            "agent_health": 60.0,       # 1 minute
            "data_quality": 300.0,      # 5 minutes
        }
    
    async def start(self) -> None:
        """Start the monitor agent and begin monitoring."""
        await super().start()
        
        # Start background monitoring
        await self._start_monitoring()
    
    async def stop(self) -> None:
        """Stop the monitor agent and all monitoring tasks."""
        # Stop background monitoring
        await self._stop_monitoring()
        
        await super().stop()
    
    async def _process_task(self, task: AgentTask) -> Dict[str, Any]:
        """Process monitor-specific tasks."""
        task_type = task.task_type.lower()
        
        with LogContext(task_type=task_type, monitoring_scope=self.monitoring_scope):
            if task_type == "start_monitoring":
                return await self._start_monitoring_task(task)
            elif task_type == "stop_monitoring":
                return await self._stop_monitoring_task(task)
            elif task_type == "check_health":
                return await self._check_health(task)
            elif task_type == "monitor_pipeline":
                return await self._monitor_pipeline_task(task)
            elif task_type == "generate_report":
                return await self._generate_report(task)
            elif task_type == "configure_alerts":
                return await self._configure_alerts(task)
            elif task_type == "analyze_performance":
                return await self._analyze_performance(task)
            elif task_type == "check_data_quality":
                return await self._check_data_quality(task)
            else:
                return await self._handle_generic_monitoring_task(task)
    
    def get_system_prompt(self) -> str:
        """Get the system prompt for the monitor agent."""
        return f"""You are an intelligent Monitor Agent responsible for tracking ETL operations and system health.

Your monitoring scope is: {self.monitoring_scope}
Current monitoring targets: {len(self.monitoring_targets)}

Your primary responsibilities include:
1. Continuously monitoring system health and performance
2. Tracking ETL pipeline execution and progress
3. Detecting anomalies and potential issues
4. Generating alerts and notifications
5. Collecting and analyzing performance metrics
6. Providing health status reports and recommendations

Monitoring capabilities:
- System resource monitoring (CPU, memory, disk, network)
- Pipeline execution monitoring and progress tracking
- Data quality monitoring and validation
- Performance metrics collection and analysis
- Error detection and alert generation
- Trend analysis and predictive monitoring

When monitoring systems, consider:
- Performance baselines and normal operating ranges
- Seasonal patterns and expected variations
- Critical vs. non-critical alerts
- Root cause analysis for detected issues
- Proactive monitoring and early warning signs
- Resource utilization optimization opportunities

Always provide:
- Clear status assessments (healthy, warning, critical)
- Specific metrics and measurements
- Actionable recommendations for issues
- Trend analysis and historical context
- Alert prioritization and escalation guidelines

Respond with structured monitoring data including timestamps, metrics, and status indicators."""
    
    async def _start_monitoring_task(self, task: AgentTask) -> Dict[str, Any]:
        """Start monitoring for specified targets."""
        targets = task.inputs.get("targets", [])
        monitoring_types = task.inputs.get("monitoring_types", ["system_health"])
        
        results = []
        for target in targets:
            try:
                await self._add_monitoring_target(target, monitoring_types)
                results.append({
                    "target": target,
                    "status": "monitoring_started",
                    "monitoring_types": monitoring_types,
                })
            except Exception as e:
                results.append({
                    "target": target,
                    "status": "failed_to_start",
                    "error": str(e),
                })
        
        return {
            "action": "start_monitoring",
            "results": results,
            "total_targets": len(self.monitoring_targets),
            "monitoring_enabled": self.monitoring_enabled,
        }
    
    async def _stop_monitoring_task(self, task: AgentTask) -> Dict[str, Any]:
        """Stop monitoring for specified targets."""
        targets = task.inputs.get("targets", [])
        
        if not targets:
            # Stop all monitoring
            await self._stop_monitoring()
            return {
                "action": "stop_all_monitoring",
                "status": "stopped",
                "monitoring_enabled": self.monitoring_enabled,
            }
        
        results = []
        for target in targets:
            try:
                await self._remove_monitoring_target(target)
                results.append({
                    "target": target,
                    "status": "monitoring_stopped",
                })
            except Exception as e:
                results.append({
                    "target": target,
                    "status": "failed_to_stop",
                    "error": str(e),
                })
        
        return {
            "action": "stop_monitoring",
            "results": results,
            "remaining_targets": len(self.monitoring_targets),
        }
    
    async def _check_health(self, task: AgentTask) -> Dict[str, Any]:
        """Check health status of monitored systems."""
        targets = task.inputs.get("targets", list(self.monitoring_targets))
        detailed = task.inputs.get("detailed", False)
        
        health_results = {}
        overall_status = "healthy"
        
        for target in targets:
            try:
                health_info = await self._get_target_health(target, detailed)
                health_results[target] = health_info
                
                # Update overall status
                if health_info["status"] == "critical":
                    overall_status = "critical"
                elif health_info["status"] == "warning" and overall_status != "critical":
                    overall_status = "warning"
                    
            except Exception as e:
                health_results[target] = {
                    "status": "unknown",
                    "error": str(e),
                    "timestamp": time.time(),
                }
                if overall_status == "healthy":
                    overall_status = "warning"
        
        return {
            "overall_status": overall_status,
            "health_results": health_results,
            "check_timestamp": time.time(),
            "targets_checked": len(targets),
        }
    
    async def _monitor_pipeline_task(self, task: AgentTask) -> Dict[str, Any]:
        """Monitor specific pipeline execution."""
        pipeline_id = task.inputs.get("pipeline_id")
        
        if not pipeline_id:
            raise AgentException("pipeline_id is required for pipeline monitoring")
        
        # Use pipeline monitoring tool
        result = await self._use_tool("monitor_pipeline", {
            "pipeline_id": pipeline_id,
            "include_logs": task.inputs.get("include_logs", False),
            "include_metrics": task.inputs.get("include_metrics", True),
        })
        
        # Store monitoring result in memory
        await self._store_monitoring_memory("pipeline_monitoring", {
            "pipeline_id": pipeline_id,
            "result": result,
            "monitoring_timestamp": time.time(),
        })
        
        return result
    
    async def _generate_report(self, task: AgentTask) -> Dict[str, Any]:
        """Generate monitoring and performance reports."""
        report_type = task.inputs.get("report_type", "summary")
        time_range = task.inputs.get("time_range", "24h")
        targets = task.inputs.get("targets", list(self.monitoring_targets))
        
        self.logger.info(f"Generating {report_type} report for {time_range}")
        
        try:
            if report_type == "summary":
                report = await self._generate_summary_report(targets, time_range)
            elif report_type == "performance":
                report = await self._generate_performance_report(targets, time_range)
            elif report_type == "health":
                report = await self._generate_health_report(targets, time_range)
            elif report_type == "alerts":
                report = await self._generate_alerts_report(time_range)
            else:
                report = await self._generate_custom_report(report_type, targets, time_range)
            
            # Store report in memory
            await self._store_monitoring_memory("report_generation", {
                "report_type": report_type,
                "time_range": time_range,
                "targets": targets,
                "report": report,
            })
            
            return report
            
        except Exception as e:
            self.logger.error(f"Report generation failed: {e}", exc_info=e)
            raise MonitoringException(f"Report generation failed: {e}") from e
    
    async def _configure_alerts(self, task: AgentTask) -> Dict[str, Any]:
        """Configure alert thresholds and rules."""
        alert_config = task.inputs.get("alert_config", {})
        
        results = []
        
        # Update thresholds
        if "thresholds" in alert_config:
            for metric, thresholds in alert_config["thresholds"].items():
                self.alert_thresholds[metric] = thresholds
                results.append({
                    "action": "threshold_updated",
                    "metric": metric,
                    "thresholds": thresholds,
                })
        
        # Update escalation rules
        if "escalation_rules" in alert_config:
            self.alert_escalation_rules = alert_config["escalation_rules"]
            results.append({
                "action": "escalation_rules_updated",
                "rules_count": len(self.alert_escalation_rules),
            })
        
        # Update notification channels
        if "notification_channels" in alert_config:
            self.notification_channels = alert_config["notification_channels"]
            results.append({
                "action": "notification_channels_updated",
                "channels": self.notification_channels,
            })
        
        return {
            "alert_configuration": "updated",
            "changes": results,
            "current_thresholds": self.alert_thresholds,
            "notification_channels": self.notification_channels,
        }
    
    async def _analyze_performance(self, task: AgentTask) -> Dict[str, Any]:
        """Analyze performance metrics and trends."""
        targets = task.inputs.get("targets", list(self.monitoring_targets))
        metrics = task.inputs.get("metrics", ["response_time", "throughput", "error_rate"])
        time_range = task.inputs.get("time_range", "24h")
        
        analysis_results = {}
        
        for target in targets:
            target_metrics = self.performance_metrics.get(target, [])
            
            if not target_metrics:
                analysis_results[target] = {
                    "status": "no_data",
                    "message": "No performance data available",
                }
                continue
            
            # Analyze metrics (simplified implementation)
            analysis = await self._analyze_target_metrics(target, target_metrics, metrics, time_range)
            analysis_results[target] = analysis
        
        return {
            "analysis_results": analysis_results,
            "analysis_timestamp": time.time(),
            "time_range": time_range,
            "metrics_analyzed": metrics,
        }
    
    async def _check_data_quality(self, task: AgentTask) -> Dict[str, Any]:
        """Check data quality metrics."""
        data_sources = task.inputs.get("data_sources", [])
        quality_checks = task.inputs.get("quality_checks", ["completeness", "accuracy", "consistency"])
        
        quality_results = {}
        
        for data_source in data_sources:
            try:
                # Use data quality validation tool
                quality_result = await self._use_tool("validate_data_quality", {
                    "data_source": data_source,
                    "validation_rules": self._generate_quality_rules(quality_checks),
                })
                
                quality_results[data_source] = quality_result
                
            except Exception as e:
                quality_results[data_source] = {
                    "status": "error",
                    "error": str(e),
                    "timestamp": time.time(),
                }
        
        return {
            "data_quality_results": quality_results,
            "quality_checks": quality_checks,
            "check_timestamp": time.time(),
        }
    
    async def _handle_generic_monitoring_task(self, task: AgentTask) -> Dict[str, Any]:
        """Handle generic monitoring tasks using LLM reasoning."""
        task_description = task.description
        task_inputs = task.inputs
        
        # Use LLM to process the task
        monitoring_prompt = f"""You are a Monitor Agent responsible for system monitoring and health checks. Process this monitoring task:

Task Description: {task_description}
Task Inputs: {json.dumps(task_inputs, indent=2)}
Monitoring Scope: {self.monitoring_scope}
Current Targets: {list(self.monitoring_targets)}
Available Tools: {', '.join(self.tool_registry.list_tool_names())}

Analyze the task and provide:
1. Monitoring approach and methodology
2. Relevant metrics to collect
3. Alert criteria and thresholds
4. Expected monitoring outcomes
5. Recommendations for optimization

If this is a monitoring task you can complete, provide specific monitoring results and status information.
Otherwise, explain what additional information or capabilities would be needed."""
        
        llm_response = await self.query_llm(monitoring_prompt)
        
        return {
            "task_type": task.task_type,
            "monitoring_scope": self.monitoring_scope,
            "analysis": llm_response,
            "processed_at": time.time(),
        }
    
    async def _start_monitoring(self) -> None:
        """Start background monitoring tasks."""
        if self.monitoring_enabled:
            return
        
        self.monitoring_enabled = True
        self.logger.info("Starting background monitoring")
        
        # Start monitoring tasks for different types
        monitoring_types = [
            "system_health",
            "pipeline_status", 
            "performance_metrics",
            "agent_health",
        ]
        
        for monitoring_type in monitoring_types:
            interval = self.monitoring_intervals.get(monitoring_type, 60.0)
            task = asyncio.create_task(self._monitoring_loop(monitoring_type, interval))
            self.monitoring_tasks[monitoring_type] = task
        
        self.logger.info(f"Started {len(self.monitoring_tasks)} monitoring tasks")
    
    async def _stop_monitoring(self) -> None:
        """Stop background monitoring tasks."""
        if not self.monitoring_enabled:
            return
        
        self.monitoring_enabled = False
        self.logger.info("Stopping background monitoring")
        
        # Cancel all monitoring tasks
        for task_name, task in self.monitoring_tasks.items():
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        self.monitoring_tasks.clear()
        self.logger.info("Background monitoring stopped")
    
    async def _monitoring_loop(self, monitoring_type: str, interval: float) -> None:
        """Background monitoring loop for a specific type."""
        self.logger.info(f"Starting {monitoring_type} monitoring loop (interval: {interval}s)")
        
        while self.monitoring_enabled:
            try:
                await self._perform_monitoring_check(monitoring_type)
                await asyncio.sleep(interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in {monitoring_type} monitoring: {e}", exc_info=e)
                await asyncio.sleep(interval)  # Continue despite errors
        
        self.logger.info(f"Stopped {monitoring_type} monitoring loop")
    
    async def _perform_monitoring_check(self, monitoring_type: str) -> None:
        """Perform a specific type of monitoring check."""
        if monitoring_type == "system_health":
            await self._check_system_health()
        elif monitoring_type == "pipeline_status":
            await self._check_pipeline_status()
        elif monitoring_type == "performance_metrics":
            await self._collect_performance_metrics()
        elif monitoring_type == "agent_health":
            await self._check_agent_health()
    
    async def _check_system_health(self) -> None:
        """Check overall system health."""
        for target in self.monitoring_targets:
            try:
                health_info = await self._get_target_health(target, detailed=False)
                self.health_status[target] = health_info["status"]
                
                # Check for alerts
                await self._check_alert_conditions(target, health_info)
                
            except Exception as e:
                self.logger.error(f"Health check failed for {target}: {e}")
                self.health_status[target] = "error"
    
    async def _check_pipeline_status(self) -> None:
        """Check status of active pipelines."""
        # This would integrate with the orchestrator to check pipeline status
        # For now, it's a placeholder
        pass
    
    async def _collect_performance_metrics(self) -> None:
        """Collect performance metrics from monitored targets."""
        for target in self.monitoring_targets:
            try:
                metrics = await self._collect_target_metrics(target)
                
                if target not in self.performance_metrics:
                    self.performance_metrics[target] = []
                
                # Add timestamp to metrics
                metrics["timestamp"] = time.time()
                self.performance_metrics[target].append(metrics)
                
                # Keep only recent metrics (last 24 hours)
                cutoff_time = time.time() - (24 * 60 * 60)
                self.performance_metrics[target] = [
                    m for m in self.performance_metrics[target]
                    if m.get("timestamp", 0) > cutoff_time
                ]
                
            except Exception as e:
                self.logger.error(f"Metrics collection failed for {target}: {e}")
    
    async def _check_agent_health(self) -> None:
        """Check health of other agents in the system."""
        if self.communication_hub:
            # This would check the health of registered agents
            # For now, it's a placeholder
            pass
    
    async def _add_monitoring_target(self, target: str, monitoring_types: List[str]) -> None:
        """Add a new monitoring target."""
        self.monitoring_targets.add(target)
        
        # Initialize monitoring state for target
        self.active_monitors[target] = {
            "target": target,
            "monitoring_types": monitoring_types,
            "added_at": time.time(),
            "status": "active",
        }
        
        self.logger.info(f"Added monitoring target: {target}")
    
    async def _remove_monitoring_target(self, target: str) -> None:
        """Remove a monitoring target."""
        self.monitoring_targets.discard(target)
        
        if target in self.active_monitors:
            del self.active_monitors[target]
        
        if target in self.health_status:
            del self.health_status[target]
        
        if target in self.performance_metrics:
            del self.performance_metrics[target]
        
        self.logger.info(f"Removed monitoring target: {target}")
    
    async def _get_target_health(self, target: str, detailed: bool = False) -> Dict[str, Any]:
        """Get health information for a specific target."""
        # Simplified health check implementation
        health_info = {
            "target": target,
            "status": "healthy",  # This would be determined by actual checks
            "timestamp": time.time(),
            "checks_performed": [
                {"check": "connectivity", "status": "passed"},
                {"check": "resource_usage", "status": "passed"},
                {"check": "response_time", "status": "passed"},
            ],
        }
        
        if detailed:
            health_info["detailed_metrics"] = {
                "cpu_usage": 45.2,
                "memory_usage": 62.8,
                "disk_usage": 78.1,
                "response_time": 150.5,
            }
        
        return health_info
    
    async def _collect_target_metrics(self, target: str) -> Dict[str, Any]:
        """Collect performance metrics for a specific target."""
        # Simplified metrics collection
        return {
            "target": target,
            "cpu_usage": 45.2,
            "memory_usage": 62.8,
            "disk_usage": 78.1,
            "response_time": 150.5,
            "throughput": 1250.0,
            "error_rate": 0.05,
        }
    
    async def _check_alert_conditions(self, target: str, health_info: Dict[str, Any]) -> None:
        """Check if alert conditions are met."""
        detailed_metrics = health_info.get("detailed_metrics", {})
        
        for metric, value in detailed_metrics.items():
            if metric in self.alert_thresholds:
                thresholds = self.alert_thresholds[metric]
                
                alert_level = None
                if value >= thresholds.get("critical", float('inf')):
                    alert_level = "critical"
                elif value >= thresholds.get("warning", float('inf')):
                    alert_level = "warning"
                
                if alert_level:
                    await self._generate_alert(target, metric, value, alert_level, thresholds)
    
    async def _generate_alert(self, target: str, metric: str, value: float, level: str, thresholds: Dict[str, Any]) -> None:
        """Generate an alert for threshold violation."""
        alert = {
            "alert_id": f"alert_{int(time.time() * 1000)}",
            "target": target,
            "metric": metric,
            "value": value,
            "level": level,
            "threshold": thresholds.get(level),
            "timestamp": time.time(),
            "message": f"{metric} on {target} is {value} (threshold: {thresholds.get(level)})",
        }
        
        self.alert_history.append(alert)
        
        # Keep only recent alerts (last 7 days)
        cutoff_time = time.time() - (7 * 24 * 60 * 60)
        self.alert_history = [
            a for a in self.alert_history
            if a.get("timestamp", 0) > cutoff_time
        ]
        
        self.logger.warning(f"Alert generated: {alert['message']}")
        
        # Store alert in memory
        await self._store_monitoring_memory("alert", alert)
    
    async def _generate_summary_report(self, targets: List[str], time_range: str) -> Dict[str, Any]:
        """Generate a summary monitoring report."""
        return {
            "report_type": "summary",
            "time_range": time_range,
            "targets": targets,
            "overall_health": "healthy",
            "total_alerts": len(self.alert_history),
            "critical_alerts": len([a for a in self.alert_history if a.get("level") == "critical"]),
            "monitored_targets": len(self.monitoring_targets),
            "uptime_percentage": 99.5,
            "generated_at": time.time(),
        }
    
    async def _generate_performance_report(self, targets: List[str], time_range: str) -> Dict[str, Any]:
        """Generate a performance monitoring report."""
        performance_summary = {}
        
        for target in targets:
            metrics = self.performance_metrics.get(target, [])
            if metrics:
                # Calculate averages
                avg_cpu = sum(m.get("cpu_usage", 0) for m in metrics) / len(metrics)
                avg_memory = sum(m.get("memory_usage", 0) for m in metrics) / len(metrics)
                avg_response_time = sum(m.get("response_time", 0) for m in metrics) / len(metrics)
                
                performance_summary[target] = {
                    "average_cpu_usage": avg_cpu,
                    "average_memory_usage": avg_memory,
                    "average_response_time": avg_response_time,
                    "data_points": len(metrics),
                }
        
        return {
            "report_type": "performance",
            "time_range": time_range,
            "performance_summary": performance_summary,
            "generated_at": time.time(),
        }
    
    async def _generate_health_report(self, targets: List[str], time_range: str) -> Dict[str, Any]:
        """Generate a health monitoring report."""
        health_summary = {}
        
        for target in targets:
            status = self.health_status.get(target, "unknown")
            health_summary[target] = {
                "current_status": status,
                "last_check": time.time(),
            }
        
        return {
            "report_type": "health",
            "time_range": time_range,
            "health_summary": health_summary,
            "overall_status": "healthy",
            "generated_at": time.time(),
        }
    
    async def _generate_alerts_report(self, time_range: str) -> Dict[str, Any]:
        """Generate an alerts report."""
        cutoff_time = time.time() - self._parse_time_range(time_range)
        recent_alerts = [
            a for a in self.alert_history
            if a.get("timestamp", 0) > cutoff_time
        ]
        
        alert_summary = {
            "total_alerts": len(recent_alerts),
            "critical_alerts": len([a for a in recent_alerts if a.get("level") == "critical"]),
            "warning_alerts": len([a for a in recent_alerts if a.get("level") == "warning"]),
            "alerts_by_target": {},
        }
        
        # Group alerts by target
        for alert in recent_alerts:
            target = alert.get("target", "unknown")
            if target not in alert_summary["alerts_by_target"]:
                alert_summary["alerts_by_target"][target] = []
            alert_summary["alerts_by_target"][target].append(alert)
        
        return {
            "report_type": "alerts",
            "time_range": time_range,
            "alert_summary": alert_summary,
            "recent_alerts": recent_alerts,
            "generated_at": time.time(),
        }
    
    async def _generate_custom_report(self, report_type: str, targets: List[str], time_range: str) -> Dict[str, Any]:
        """Generate a custom report type."""
        return {
            "report_type": report_type,
            "time_range": time_range,
            "targets": targets,
            "message": f"Custom report type '{report_type}' not implemented",
            "generated_at": time.time(),
        }
    
    async def _analyze_target_metrics(self, target: str, metrics: List[Dict[str, Any]], metric_names: List[str], time_range: str) -> Dict[str, Any]:
        """Analyze metrics for a specific target."""
        cutoff_time = time.time() - self._parse_time_range(time_range)
        recent_metrics = [
            m for m in metrics
            if m.get("timestamp", 0) > cutoff_time
        ]
        
        if not recent_metrics:
            return {
                "status": "no_data",
                "message": "No recent metrics available",
            }
        
        analysis = {
            "target": target,
            "time_range": time_range,
            "data_points": len(recent_metrics),
            "metrics_analysis": {},
        }
        
        for metric_name in metric_names:
            values = [m.get(metric_name, 0) for m in recent_metrics if metric_name in m]
            
            if values:
                analysis["metrics_analysis"][metric_name] = {
                    "average": sum(values) / len(values),
                    "minimum": min(values),
                    "maximum": max(values),
                    "trend": "stable",  # Simplified trend analysis
                    "data_points": len(values),
                }
        
        return analysis
    
    def _parse_time_range(self, time_range: str) -> float:
        """Parse time range string to seconds."""
        time_range = time_range.lower()
        
        if time_range.endswith('h'):
            return float(time_range[:-1]) * 3600
        elif time_range.endswith('d'):
            return float(time_range[:-1]) * 24 * 3600
        elif time_range.endswith('m'):
            return float(time_range[:-1]) * 60
        else:
            return 3600  # Default to 1 hour
    
    def _generate_quality_rules(self, quality_checks: List[str]) -> List[Dict[str, Any]]:
        """Generate data quality validation rules."""
        rules = []
        
        for check in quality_checks:
            if check == "completeness":
                rules.append({
                    "type": "completeness",
                    "threshold": 0.95,
                    "description": "Check for missing values",
                })
            elif check == "accuracy":
                rules.append({
                    "type": "accuracy",
                    "threshold": 0.98,
                    "description": "Check for data accuracy",
                })
            elif check == "consistency":
                rules.append({
                    "type": "consistency",
                    "threshold": 0.92,
                    "description": "Check for data consistency",
                })
        
        return rules
    
    async def _use_tool(self, tool_name: str, tool_inputs: Dict[str, Any]) -> Any:
        """Use a tool from the registry."""
        tool = self.tool_registry.get_tool(tool_name)
        if not tool:
            raise AgentException(f"Tool {tool_name} not found in registry")
        
        try:
            result = await tool._arun(**tool_inputs)
            
            # Try to parse JSON result
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return {"raw_result": result}
                
        except Exception as e:
            self.logger.error(f"Tool {tool_name} execution failed: {e}")
            raise AgentException(f"Tool execution failed: {e}") from e
    
    async def _store_monitoring_memory(self, operation_type: str, operation_info: Dict[str, Any]) -> None:
        """Store monitoring operation information in memory."""
        self.memory.store_memory(
            content={
                "operation_type": operation_type,
                "operation_info": operation_info,
                "monitoring_scope": self.monitoring_scope,
                "storage_timestamp": time.time(),
            },
            memory_type=MemoryType.EPISODIC,
            importance=MemoryImportance.MEDIUM,
            tags={operation_type, "monitoring", self.monitoring_scope},
        )
    
    async def _register_default_capabilities(self) -> None:
        """Register monitor-specific capabilities."""
        await super()._register_default_capabilities()
        
        monitor_capabilities = [
            AgentCapability(
                name="system_monitoring",
                description="Monitor system health and performance",
                input_types=["system_targets", "monitoring_config"],
                output_types=["health_status", "performance_metrics"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="pipeline_monitoring",
                description="Monitor ETL pipeline execution and progress",
                input_types=["pipeline_id", "monitoring_params"],
                output_types=["pipeline_status", "execution_metrics"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="alert_management",
                description="Generate and manage alerts for threshold violations",
                input_types=["alert_config", "threshold_rules"],
                output_types=["alert_notifications", "escalation_actions"],
                confidence_level=0.85,
            ),
            AgentCapability(
                name="performance_analysis",
                description="Analyze performance metrics and trends",
                input_types=["performance_data", "analysis_config"],
                output_types=["performance_analysis", "optimization_recommendations"],
                confidence_level=0.8,
            ),
            AgentCapability(
                name="report_generation",
                description="Generate monitoring and performance reports",
                input_types=["report_config", "time_range"],
                output_types=["monitoring_reports", "summary_dashboards"],
                confidence_level=0.85,
            ),
        ]
        
        for capability in monitor_capabilities:
            self.add_capability(capability)
    
    def get_monitor_status(self) -> Dict[str, Any]:
        """Get detailed monitor agent status."""
        base_status = self.get_status()
        
        monitor_status = {
            **base_status,
            "monitoring_scope": self.monitoring_scope,
            "monitoring_enabled": self.monitoring_enabled,
            "monitoring_targets": len(self.monitoring_targets),
            "active_monitoring_tasks": len(self.monitoring_tasks),
            "total_alerts": len(self.alert_history),
            "recent_alerts": len([a for a in self.alert_history if a.get("timestamp", 0) > time.time() - 3600]),
            "health_status_summary": {
                "healthy": len([s for s in self.health_status.values() if s == "healthy"]),
                "warning": len([s for s in self.health_status.values() if s == "warning"]),
                "critical": len([s for s in self.health_status.values() if s == "critical"]),
                "error": len([s for s in self.health_status.values() if s == "error"]),
            },
            "memory_entries": self.memory.get_memory_summary()["total_memories"],
        }
        
        return monitor_status