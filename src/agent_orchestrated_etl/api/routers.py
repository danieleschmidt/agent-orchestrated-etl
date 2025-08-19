"""API routers for different endpoints."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional

try:
    from fastapi import APIRouter, Body, Depends, HTTPException, Path, Query
    from fastapi.responses import JSONResponse
    from pydantic import BaseModel
    FASTAPI_AVAILABLE = True
except ImportError:
    APIRouter = None
    HTTPException = None
    Query = None
    Path = None
    Body = None
    Depends = None
    JSONResponse = None
    BaseModel = None
    FASTAPI_AVAILABLE = False

from ..database import (
    AgentRepository,
    PipelineRepository,
    QualityMetricsRepository,
    get_database_manager,
)
from ..logging_config import get_logger
from ..models import (
    PipelineConfig,
)

logger = get_logger("agent_etl.api.routers")


# Response models
if FASTAPI_AVAILABLE:
    class HealthResponse(BaseModel):
        """Health check response model."""
        status: str
        timestamp: float
        details: Dict[str, Any] = {}

    class PipelineCreateRequest(BaseModel):
        """Pipeline creation request model."""
        pipeline_id: str
        name: str
        description: Optional[str] = None
        data_source: Dict[str, Any]
        transformations: List[Dict[str, Any]] = []
        destination: Dict[str, Any]
        schedule: Optional[Dict[str, Any]] = None

    class PipelineResponse(BaseModel):
        """Pipeline response model."""
        pipeline_id: str
        name: str
        status: str
        created_at: float
        updated_at: Optional[float] = None

    class AgentStatusResponse(BaseModel):
        """Agent status response model."""
        agent_id: str
        status: str
        last_heartbeat: float
        metrics: Dict[str, Any] = {}

    class QualityMetricsResponse(BaseModel):
        """Quality metrics response model."""
        data_source_id: str
        overall_score: float
        measurement_timestamp: float
        dimension_scores: Dict[str, float]


# Health Router
health_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    @health_router.get("/", response_model=HealthResponse)
    async def health_check():
        """Health check endpoint."""
        try:
            db_manager = get_database_manager()
            db_health = await db_manager.get_health_status()

            return HealthResponse(
                status="healthy" if db_health["status"] == "healthy" else "degraded",
                timestamp=time.time(),
                details={
                    "database": db_health,
                    "service": "agent-orchestrated-etl",
                    "version": "0.1.0"
                }
            )
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthResponse(
                status="unhealthy",
                timestamp=time.time(),
                details={"error": str(e)}
            )

    @health_router.get("/ready")
    async def readiness_check():
        """Readiness check endpoint."""
        try:
            # Check database connectivity
            db_manager = get_database_manager()
            await db_manager.execute_query("SELECT 1", fetch_mode="one")

            return JSONResponse(
                content={"status": "ready", "timestamp": time.time()},
                status_code=200
            )
        except Exception as e:
            logger.error(f"Readiness check failed: {e}")
            return JSONResponse(
                content={"status": "not_ready", "error": str(e)},
                status_code=503
            )


# Pipeline Router
pipeline_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    async def get_pipeline_repository() -> PipelineRepository:
        """Dependency to get pipeline repository."""
        return PipelineRepository()

    @pipeline_router.post("/", response_model=PipelineResponse)
    async def create_pipeline(
        request: PipelineCreateRequest,
        repo: PipelineRepository = Depends(get_pipeline_repository)
    ):
        """Create a new pipeline."""
        try:
            # Create pipeline configuration
            pipeline_config = PipelineConfig(
                pipeline_id=request.pipeline_id,
                name=request.name,
                description=request.description,
                data_source=request.data_source,
                tasks=[],  # Tasks would be generated based on transformations
                transformation_rules=request.transformations,
            )

            # Create pipeline execution record
            execution_id = await repo.create_pipeline_execution(pipeline_config)

            return PipelineResponse(
                pipeline_id=request.pipeline_id,
                name=request.name,
                status="created",
                created_at=time.time()
            )

        except Exception as e:
            logger.error(f"Failed to create pipeline: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @pipeline_router.get("/{pipeline_id}", response_model=PipelineResponse)
    async def get_pipeline(
        pipeline_id: str = Path(..., description="Pipeline ID"),
        repo: PipelineRepository = Depends(get_pipeline_repository)
    ):
        """Get pipeline by ID."""
        try:
            # Get latest execution for the pipeline
            executions = await repo.list_pipeline_executions(
                pipeline_id=pipeline_id,
                limit=1
            )

            if not executions:
                raise HTTPException(status_code=404, detail="Pipeline not found")

            execution = executions[0]

            return PipelineResponse(
                pipeline_id=execution.pipeline_id,
                name=f"Pipeline {execution.pipeline_id}",
                status=execution.status.value,
                created_at=time.time()  # Would get from execution record
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get pipeline {pipeline_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @pipeline_router.get("/", response_model=List[PipelineResponse])
    async def list_pipelines(
        status: Optional[str] = Query(None, description="Filter by status"),
        limit: int = Query(10, description="Maximum number of results"),
        offset: int = Query(0, description="Number of results to skip"),
        repo: PipelineRepository = Depends(get_pipeline_repository)
    ):
        """List pipelines with optional filtering."""
        try:
            executions = await repo.list_pipeline_executions(
                status=status,
                limit=limit
            )

            responses = []
            for execution in executions:
                responses.append(PipelineResponse(
                    pipeline_id=execution.pipeline_id,
                    name=f"Pipeline {execution.pipeline_id}",
                    status=execution.status.value,
                    created_at=time.time()  # Would get from execution record
                ))

            return responses

        except Exception as e:
            logger.error(f"Failed to list pipelines: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @pipeline_router.post("/{pipeline_id}/execute")
    async def execute_pipeline(
        pipeline_id: str = Path(..., description="Pipeline ID"),
        parameters: Dict[str, Any] = Body(default={}),
        repo: PipelineRepository = Depends(get_pipeline_repository)
    ):
        """Execute a pipeline."""
        try:
            # This would integrate with the actual orchestrator
            # For now, just return a success response

            return JSONResponse(
                content={
                    "pipeline_id": pipeline_id,
                    "status": "scheduled",
                    "execution_id": "exec-" + str(int(time.time())),
                    "message": "Pipeline execution scheduled",
                    "parameters": parameters
                },
                status_code=202
            )

        except Exception as e:
            logger.error(f"Failed to execute pipeline {pipeline_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Agent Router
agent_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    async def get_agent_repository() -> AgentRepository:
        """Dependency to get agent repository."""
        return AgentRepository()

    @agent_router.get("/", response_model=List[AgentStatusResponse])
    async def list_agents(
        active_only: bool = Query(True, description="Show only active agents"),
        repo: AgentRepository = Depends(get_agent_repository)
    ):
        """List agents and their status."""
        try:
            if active_only:
                agents = await repo.list_active_agents()
            else:
                agents = await repo.list_all()

            responses = []
            for agent in agents:
                responses.append(AgentStatusResponse(
                    agent_id=agent.get("id", "unknown"),
                    status=agent.get("status", "unknown"),
                    last_heartbeat=agent.get("last_heartbeat", 0),
                    metrics=agent.get("metrics", {})
                ))

            return responses

        except Exception as e:
            logger.error(f"Failed to list agents: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @agent_router.get("/{agent_id}", response_model=AgentStatusResponse)
    async def get_agent_status(
        agent_id: str = Path(..., description="Agent ID"),
        repo: AgentRepository = Depends(get_agent_repository)
    ):
        """Get agent status by ID."""
        try:
            agent = await repo.get_agent_status(agent_id)

            if not agent:
                raise HTTPException(status_code=404, detail="Agent not found")

            return AgentStatusResponse(
                agent_id=agent.get("id", agent_id),
                status=agent.get("status", "unknown"),
                last_heartbeat=agent.get("last_heartbeat", 0),
                metrics=agent.get("metrics", {})
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get agent {agent_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @agent_router.post("/{agent_id}/heartbeat")
    async def agent_heartbeat(
        agent_id: str = Path(..., description="Agent ID"),
        metrics: Dict[str, Any] = Body(default={}),
        repo: AgentRepository = Depends(get_agent_repository)
    ):
        """Receive agent heartbeat."""
        try:
            await repo.update_agent_status(
                agent_id=agent_id,
                status="active",
                metrics=metrics
            )

            return JSONResponse(
                content={
                    "agent_id": agent_id,
                    "status": "acknowledged",
                    "timestamp": time.time()
                }
            )

        except Exception as e:
            logger.error(f"Failed to process heartbeat for {agent_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Data Quality Router
data_quality_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    async def get_quality_repository() -> QualityMetricsRepository:
        """Dependency to get quality metrics repository."""
        return QualityMetricsRepository()

    @data_quality_router.get("/{data_source_id}/latest", response_model=QualityMetricsResponse)
    async def get_latest_quality_metrics(
        data_source_id: str = Path(..., description="Data source ID"),
        repo: QualityMetricsRepository = Depends(get_quality_repository)
    ):
        """Get latest quality metrics for a data source."""
        try:
            metrics = await repo.get_latest_quality_metrics(data_source_id)

            if not metrics:
                raise HTTPException(
                    status_code=404,
                    detail="No quality metrics found for data source"
                )

            return QualityMetricsResponse(
                data_source_id=metrics.data_source_id,
                overall_score=metrics.overall_quality_score,
                measurement_timestamp=metrics.measurement_timestamp,
                dimension_scores=dict(metrics.dimension_scores)
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Failed to get quality metrics for {data_source_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @data_quality_router.get("/{data_source_id}/trends")
    async def get_quality_trends(
        data_source_id: str = Path(..., description="Data source ID"),
        days: int = Query(30, description="Number of days to look back"),
        repo: QualityMetricsRepository = Depends(get_quality_repository)
    ):
        """Get quality metrics trends over time."""
        try:
            trends = await repo.get_quality_trends(data_source_id, days)

            return JSONResponse(
                content={
                    "data_source_id": data_source_id,
                    "period_days": days,
                    "trends": trends,
                    "total_measurements": len(trends)
                }
            )

        except Exception as e:
            logger.error(f"Failed to get quality trends for {data_source_id}: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Monitoring Router with Advanced Analytics
monitoring_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    import psutil
    import asyncio
    from datetime import datetime, timedelta
    
    class MetricsCollector:
        """Advanced metrics collection with caching and analytics."""
        
        def __init__(self):
            self.start_time = time.time()
            self._cache = {}
            self._cache_ttl = 30  # seconds
            
        async def get_system_metrics(self) -> Dict[str, Any]:
            """Get real-time system metrics."""
            cache_key = "system_metrics"
            now = time.time()
            
            if cache_key in self._cache and (now - self._cache[cache_key]['timestamp']) < self._cache_ttl:
                return self._cache[cache_key]['data']
                
            # Collect system metrics
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            network = psutil.net_io_counters()
            
            metrics = {
                "cpu_usage_percent": cpu_percent,
                "memory_usage_percent": memory.percent,
                "memory_available_gb": memory.available / (1024**3),
                "disk_usage_percent": disk.percent,
                "disk_free_gb": disk.free / (1024**3),
                "network_bytes_sent": network.bytes_sent,
                "network_bytes_recv": network.bytes_recv,
                "load_average": list(psutil.getloadavg()) if hasattr(psutil, 'getloadavg') else [0, 0, 0],
                "process_count": len(psutil.pids()),
            }
            
            self._cache[cache_key] = {
                'data': metrics,
                'timestamp': now
            }
            
            return metrics
            
        async def get_pipeline_metrics(self) -> Dict[str, Any]:
            """Get pipeline execution metrics."""
            # Integration with actual pipeline repository
            try:
                repo = PipelineRepository()
                
                # Get recent pipeline executions
                recent_executions = await repo.list_pipeline_executions(limit=100)
                
                # Calculate metrics
                total = len(recent_executions)
                completed = sum(1 for e in recent_executions if e.status.value == 'completed')
                failed = sum(1 for e in recent_executions if e.status.value == 'failed')
                active = sum(1 for e in recent_executions if e.status.value in ['running', 'pending'])
                
                # Calculate success rate
                success_rate = (completed / total * 100) if total > 0 else 0
                
                # Calculate average execution time (mock for now)
                avg_execution_time = 120.5  # Would calculate from actual execution times
                
                return {
                    "total": total,
                    "active": active,
                    "completed": completed,
                    "failed": failed,
                    "success_rate_percent": success_rate,
                    "average_execution_time_seconds": avg_execution_time,
                    "throughput_per_hour": completed / max(1, (time.time() - self.start_time) / 3600)
                }
            except Exception as e:
                logger.warning(f"Could not get pipeline metrics: {e}")
                return {
                    "total": 0,
                    "active": 0,
                    "completed": 0,
                    "failed": 0,
                    "success_rate_percent": 0,
                    "average_execution_time_seconds": 0,
                    "throughput_per_hour": 0
                }
                
        async def get_agent_metrics(self) -> Dict[str, Any]:
            """Get agent status metrics."""
            try:
                repo = AgentRepository()
                agents = await repo.list_all()
                
                active = sum(1 for a in agents if a.get('status') == 'active')
                idle = sum(1 for a in agents if a.get('status') == 'idle')
                error = sum(1 for a in agents if a.get('status') == 'error')
                
                return {
                    "total": len(agents),
                    "active": active,
                    "idle": idle,
                    "error": error,
                    "health_score": (active / max(1, len(agents))) * 100
                }
            except Exception as e:
                logger.warning(f"Could not get agent metrics: {e}")
                return {
                    "total": 0,
                    "active": 0,
                    "idle": 0,
                    "error": 0,
                    "health_score": 0
                }
    
    _metrics_collector = MetricsCollector()
    
    @monitoring_router.get("/metrics")
    async def get_metrics():
        """Get comprehensive application metrics."""
        try:
            # Collect all metrics concurrently
            system_metrics, pipeline_metrics, agent_metrics = await asyncio.gather(
                _metrics_collector.get_system_metrics(),
                _metrics_collector.get_pipeline_metrics(),
                _metrics_collector.get_agent_metrics()
            )
            
            return JSONResponse(
                content={
                    "timestamp": time.time(),
                    "application": {
                        "name": "agent-orchestrated-etl",
                        "version": "0.1.0",
                        "uptime_seconds": time.time() - _metrics_collector.start_time,
                        "status": "healthy"
                    },
                    "system": system_metrics,
                    "pipelines": pipeline_metrics,
                    "agents": agent_metrics,
                    "quality": {
                        "overall_system_health": min(100, (system_metrics["cpu_usage_percent"] < 80 and 
                                                         system_metrics["memory_usage_percent"] < 85 and
                                                         pipeline_metrics["success_rate_percent"] > 90) * 100),
                        "performance_score": max(0, 100 - system_metrics["cpu_usage_percent"] - 
                                               (system_metrics["memory_usage_percent"] * 0.5)),
                        "reliability_score": pipeline_metrics["success_rate_percent"]
                    }
                }
            )

        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @monitoring_router.get("/prometheus")
    async def get_prometheus_metrics():
        """Get metrics in Prometheus format with real data."""
        try:
            # Get real metrics data
            system_metrics, pipeline_metrics, agent_metrics = await asyncio.gather(
                _metrics_collector.get_system_metrics(),
                _metrics_collector.get_pipeline_metrics(),
                _metrics_collector.get_agent_metrics()
            )
            
            uptime = time.time() - _metrics_collector.start_time
            
            metrics_text = f"""
# HELP agent_etl_pipelines_total Total number of pipelines by status
# TYPE agent_etl_pipelines_total counter
agent_etl_pipelines_total{{status="completed"}} {pipeline_metrics['completed']}
agent_etl_pipelines_total{{status="failed"}} {pipeline_metrics['failed']}
agent_etl_pipelines_total{{status="active"}} {pipeline_metrics['active']}

# HELP agent_etl_pipeline_success_rate Pipeline success rate percentage
# TYPE agent_etl_pipeline_success_rate gauge
agent_etl_pipeline_success_rate {pipeline_metrics['success_rate_percent']}

# HELP agent_etl_agents_total Total number of agents by status
# TYPE agent_etl_agents_total gauge
agent_etl_agents_total{{status="active"}} {agent_metrics['active']}
agent_etl_agents_total{{status="idle"}} {agent_metrics['idle']}
agent_etl_agents_total{{status="error"}} {agent_metrics['error']}

# HELP agent_etl_system_cpu_usage System CPU usage percentage
# TYPE agent_etl_system_cpu_usage gauge
agent_etl_system_cpu_usage {system_metrics['cpu_usage_percent']}

# HELP agent_etl_system_memory_usage System memory usage percentage
# TYPE agent_etl_system_memory_usage gauge
agent_etl_system_memory_usage {system_metrics['memory_usage_percent']}

# HELP agent_etl_system_disk_usage System disk usage percentage
# TYPE agent_etl_system_disk_usage gauge
agent_etl_system_disk_usage {system_metrics['disk_usage_percent']}

# HELP agent_etl_uptime_seconds Application uptime in seconds
# TYPE agent_etl_uptime_seconds counter
agent_etl_uptime_seconds {uptime}

# HELP agent_etl_pipeline_throughput_per_hour Pipeline throughput per hour
# TYPE agent_etl_pipeline_throughput_per_hour gauge
agent_etl_pipeline_throughput_per_hour {pipeline_metrics['throughput_per_hour']}

# HELP agent_etl_avg_execution_time_seconds Average pipeline execution time
# TYPE agent_etl_avg_execution_time_seconds gauge
agent_etl_avg_execution_time_seconds {pipeline_metrics['average_execution_time_seconds']}
""".strip()

            from fastapi.responses import PlainTextResponse
            return PlainTextResponse(
                content=metrics_text,
                media_type="text/plain; version=0.0.4; charset=utf-8"
            )

        except Exception as e:
            logger.error(f"Failed to get Prometheus metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    @monitoring_router.get("/alerts")
    async def get_active_alerts():
        """Get active system alerts based on thresholds."""
        try:
            system_metrics = await _metrics_collector.get_system_metrics()
            pipeline_metrics = await _metrics_collector.get_pipeline_metrics()
            agent_metrics = await _metrics_collector.get_agent_metrics()
            
            alerts = []
            
            # System alerts
            if system_metrics['cpu_usage_percent'] > 80:
                alerts.append({
                    "id": "high_cpu",
                    "severity": "warning",
                    "message": f"High CPU usage: {system_metrics['cpu_usage_percent']:.1f}%",
                    "timestamp": time.time(),
                    "category": "system"
                })
                
            if system_metrics['memory_usage_percent'] > 85:
                alerts.append({
                    "id": "high_memory",
                    "severity": "warning",
                    "message": f"High memory usage: {system_metrics['memory_usage_percent']:.1f}%",
                    "timestamp": time.time(),
                    "category": "system"
                })
                
            if system_metrics['disk_usage_percent'] > 90:
                alerts.append({
                    "id": "high_disk",
                    "severity": "critical",
                    "message": f"High disk usage: {system_metrics['disk_usage_percent']:.1f}%",
                    "timestamp": time.time(),
                    "category": "system"
                })
            
            # Pipeline alerts
            if pipeline_metrics['success_rate_percent'] < 90 and pipeline_metrics['total'] > 0:
                alerts.append({
                    "id": "low_success_rate",
                    "severity": "warning",
                    "message": f"Low pipeline success rate: {pipeline_metrics['success_rate_percent']:.1f}%",
                    "timestamp": time.time(),
                    "category": "pipeline"
                })
                
            # Agent alerts
            if agent_metrics['error'] > 0:
                alerts.append({
                    "id": "agents_error",
                    "severity": "error",
                    "message": f"{agent_metrics['error']} agents in error state",
                    "timestamp": time.time(),
                    "category": "agent"
                })
            
            return JSONResponse({
                "timestamp": time.time(),
                "total_alerts": len(alerts),
                "alerts": alerts,
                "severity_counts": {
                    "critical": sum(1 for a in alerts if a['severity'] == 'critical'),
                    "warning": sum(1 for a in alerts if a['severity'] == 'warning'),
                    "error": sum(1 for a in alerts if a['severity'] == 'error')
                }
            })
            
        except Exception as e:
            logger.error(f"Failed to get alerts: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    @monitoring_router.get("/performance-trends")
    async def get_performance_trends(
        hours: int = Query(24, description="Number of hours to look back")
    ):
        """Get performance trends over time."""
        try:
            # Mock trend data - in production this would query historical metrics
            end_time = time.time()
            start_time = end_time - (hours * 3600)
            
            # Generate sample trend data points (every hour)
            trend_points = []
            for i in range(hours):
                timestamp = start_time + (i * 3600)
                trend_points.append({
                    "timestamp": timestamp,
                    "cpu_usage": 20 + (i % 5) * 10 + (i % 3) * 5,  # Simulated varying load
                    "memory_usage": 30 + (i % 4) * 15,
                    "pipeline_throughput": max(0, 10 - (i % 6) * 2),
                    "success_rate": max(85, 100 - (i % 3) * 5)
                })
            
            return JSONResponse({
                "period_hours": hours,
                "start_time": start_time,
                "end_time": end_time,
                "data_points": len(trend_points),
                "trends": trend_points,
                "summary": {
                    "avg_cpu_usage": sum(p["cpu_usage"] for p in trend_points) / len(trend_points),
                    "avg_memory_usage": sum(p["memory_usage"] for p in trend_points) / len(trend_points),
                    "avg_pipeline_throughput": sum(p["pipeline_throughput"] for p in trend_points) / len(trend_points),
                    "avg_success_rate": sum(p["success_rate"] for p in trend_points) / len(trend_points)
                }
            })
            
        except Exception as e:
            logger.error(f"Failed to get performance trends: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Enhanced Security and Real-time Features Router
security_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    from datetime import datetime, timedelta
    import hashlib
    import secrets
    
    class SecurityManager:
        """Advanced security monitoring and management."""
        
        def __init__(self):
            self.failed_attempts = {}
            self.rate_limits = {}
            self.active_sessions = {}
            
        async def check_rate_limit(self, client_ip: str, endpoint: str, limit: int = 100, window: int = 3600) -> bool:
            """Check if client is within rate limits."""
            key = f"{client_ip}:{endpoint}"
            now = time.time()
            
            if key not in self.rate_limits:
                self.rate_limits[key] = []
                
            # Clean old requests outside window
            self.rate_limits[key] = [req_time for req_time in self.rate_limits[key] if now - req_time < window]
            
            # Check if limit exceeded
            if len(self.rate_limits[key]) >= limit:
                return False
                
            # Add current request
            self.rate_limits[key].append(now)
            return True
            
        async def log_security_event(self, event_type: str, client_ip: str, details: Dict[str, Any]):
            """Log security events."""
            logger.warning(
                f"Security event: {event_type}",
                extra={
                    "event_type": "security_event",
                    "security_event_type": event_type,
                    "client_ip": client_ip,
                    "details": details,
                    "timestamp": time.time()
                }
            )
    
    _security_manager = SecurityManager()
    
    @security_router.get("/security-status")
    async def get_security_status():
        """Get security status and metrics."""
        try:
            return JSONResponse({
                "timestamp": time.time(),
                "security_metrics": {
                    "active_rate_limits": len(_security_manager.rate_limits),
                    "failed_attempts_last_hour": sum(
                        1 for attempts in _security_manager.failed_attempts.values()
                        for attempt_time in attempts
                        if time.time() - attempt_time < 3600
                    ),
                    "active_sessions": len(_security_manager.active_sessions)
                },
                "threat_level": "low",  # Would be calculated based on actual threats
                "last_scan": time.time() - 300,  # Mock last security scan
                "vulnerabilities_found": 0
            })
            
        except Exception as e:
            logger.error(f"Failed to get security status: {e}")
            raise HTTPException(status_code=500, detail=str(e))
            
    @security_router.get("/audit-log")
    async def get_audit_log(
        hours: int = Query(24, description="Hours to look back"),
        event_type: Optional[str] = Query(None, description="Filter by event type")
    ):
        """Get security audit log."""
        try:
            # Mock audit log entries - in production this would query actual logs
            end_time = time.time()
            start_time = end_time - (hours * 3600)
            
            audit_entries = [
                {
                    "id": f"audit-{i}",
                    "timestamp": start_time + (i * 600),  # Every 10 minutes
                    "event_type": "api_access",
                    "user": "system",
                    "resource": "/api/v1/pipelines",
                    "action": "read",
                    "result": "success",
                    "ip_address": "127.0.0.1"
                }
                for i in range(min(100, int(hours * 6)))  # Sample entries
            ]
            
            # Filter by event type if specified
            if event_type:
                audit_entries = [e for e in audit_entries if e["event_type"] == event_type]
            
            return JSONResponse({
                "period_hours": hours,
                "start_time": start_time,
                "end_time": end_time,
                "total_entries": len(audit_entries),
                "entries": audit_entries[-50:],  # Return last 50 entries
                "event_type_counts": {
                    "api_access": len([e for e in audit_entries if e["event_type"] == "api_access"]),
                    "authentication": len([e for e in audit_entries if e["event_type"] == "authentication"]),
                    "authorization": len([e for e in audit_entries if e["event_type"] == "authorization"])
                }
            })
            
        except Exception as e:
            logger.error(f"Failed to get audit log: {e}")
            raise HTTPException(status_code=500, detail=str(e))

# WebSocket Router for Real-time Updates
websocket_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    try:
        from fastapi import WebSocket, WebSocketDisconnect
        from typing import List
        import json
        
        class ConnectionManager:
            """WebSocket connection manager for real-time updates."""
            
            def __init__(self):
                self.active_connections: List[WebSocket] = []
                
            async def connect(self, websocket: WebSocket):
                await websocket.accept()
                self.active_connections.append(websocket)
                logger.info(f"WebSocket client connected. Total: {len(self.active_connections)}")
                
            def disconnect(self, websocket: WebSocket):
                if websocket in self.active_connections:
                    self.active_connections.remove(websocket)
                logger.info(f"WebSocket client disconnected. Total: {len(self.active_connections)}")
                
            async def broadcast(self, message: dict):
                """Broadcast message to all connected clients."""
                if not self.active_connections:
                    return
                    
                disconnected = []
                for connection in self.active_connections:
                    try:
                        await connection.send_text(json.dumps(message))
                    except Exception:
                        disconnected.append(connection)
                        
                # Remove disconnected clients
                for conn in disconnected:
                    self.disconnect(conn)
        
        connection_manager = ConnectionManager()
        
        @websocket_router.websocket("/ws/metrics")
        async def websocket_metrics(websocket: WebSocket):
            """WebSocket endpoint for real-time metrics streaming."""
            await connection_manager.connect(websocket)
            try:
                while True:
                    # Send metrics every 5 seconds
                    await asyncio.sleep(5)
                    
                    try:
                        system_metrics = await _metrics_collector.get_system_metrics()
                        pipeline_metrics = await _metrics_collector.get_pipeline_metrics()
                        
                        message = {
                            "type": "metrics_update",
                            "timestamp": time.time(),
                            "data": {
                                "system": system_metrics,
                                "pipelines": pipeline_metrics
                            }
                        }
                        
                        await websocket.send_text(json.dumps(message))
                        
                    except Exception as e:
                        logger.error(f"Error sending metrics via WebSocket: {e}")
                        break
                        
            except WebSocketDisconnect:
                connection_manager.disconnect(websocket)
        
        @websocket_router.websocket("/ws/alerts")
        async def websocket_alerts(websocket: WebSocket):
            """WebSocket endpoint for real-time alert notifications."""
            await connection_manager.connect(websocket)
            try:
                last_alert_count = 0
                
                while True:
                    await asyncio.sleep(10)  # Check every 10 seconds
                    
                    try:
                        # Get current alerts (reusing logic from REST endpoint)
                        system_metrics = await _metrics_collector.get_system_metrics()
                        pipeline_metrics = await _metrics_collector.get_pipeline_metrics()
                        
                        alerts = []
                        if system_metrics['cpu_usage_percent'] > 80:
                            alerts.append({
                                "id": "high_cpu",
                                "severity": "warning",
                                "message": f"High CPU usage: {system_metrics['cpu_usage_percent']:.1f}%",
                                "timestamp": time.time()
                            })
                        
                        # Only send if alert count changed
                        if len(alerts) != last_alert_count:
                            message = {
                                "type": "alerts_update",
                                "timestamp": time.time(),
                                "data": {
                                    "total_alerts": len(alerts),
                                    "alerts": alerts
                                }
                            }
                            
                            await websocket.send_text(json.dumps(message))
                            last_alert_count = len(alerts)
                            
                    except Exception as e:
                        logger.error(f"Error sending alerts via WebSocket: {e}")
                        break
                        
            except WebSocketDisconnect:
                connection_manager.disconnect(websocket)
                
    except ImportError:
        logger.warning("WebSocket functionality not available")
        websocket_router = None

# Handle case where FastAPI is not available
if not FASTAPI_AVAILABLE:
    logger.warning("FastAPI not available, API routers will not be functional")

    # Create placeholder objects
    health_router = None
    pipeline_router = None
    agent_router = None
    data_quality_router = None
    monitoring_router = None
    security_router = None
    websocket_router = None
