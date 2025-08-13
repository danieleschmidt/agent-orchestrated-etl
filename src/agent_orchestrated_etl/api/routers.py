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


# Monitoring Router
monitoring_router = APIRouter() if FASTAPI_AVAILABLE else None

if FASTAPI_AVAILABLE:
    @monitoring_router.get("/metrics")
    async def get_metrics():
        """Get application metrics."""
        try:
            # This would integrate with actual metrics collection
            return JSONResponse(
                content={
                    "timestamp": time.time(),
                    "application": {
                        "name": "agent-orchestrated-etl",
                        "version": "0.1.0",
                        "uptime_seconds": 0  # Would track actual uptime
                    },
                    "system": {
                        "cpu_usage": 0.0,
                        "memory_usage": 0.0,
                        "disk_usage": 0.0
                    },
                    "pipelines": {
                        "active": 0,
                        "completed": 0,
                        "failed": 0
                    },
                    "agents": {
                        "active": 0,
                        "idle": 0,
                        "error": 0
                    }
                }
            )

        except Exception as e:
            logger.error(f"Failed to get metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))

    @monitoring_router.get("/prometheus")
    async def get_prometheus_metrics():
        """Get metrics in Prometheus format."""
        try:
            # This would generate actual Prometheus metrics
            metrics_text = """
# HELP agent_etl_pipelines_total Total number of pipelines
# TYPE agent_etl_pipelines_total counter
agent_etl_pipelines_total{status="completed"} 0
agent_etl_pipelines_total{status="failed"} 0
agent_etl_pipelines_total{status="active"} 0

# HELP agent_etl_agents_total Total number of agents
# TYPE agent_etl_agents_total gauge
agent_etl_agents_total{status="active"} 0
agent_etl_agents_total{status="idle"} 0
agent_etl_agents_total{status="error"} 0

# HELP agent_etl_uptime_seconds Application uptime in seconds
# TYPE agent_etl_uptime_seconds counter
agent_etl_uptime_seconds 0
""".strip()

            from fastapi.responses import PlainTextResponse
            return PlainTextResponse(
                content=metrics_text,
                media_type="text/plain; version=0.0.4; charset=utf-8"
            )

        except Exception as e:
            logger.error(f"Failed to get Prometheus metrics: {e}")
            raise HTTPException(status_code=500, detail=str(e))


# Handle case where FastAPI is not available
if not FASTAPI_AVAILABLE:
    logger.warning("FastAPI not available, API routers will not be functional")

    # Create placeholder objects
    health_router = None
    pipeline_router = None
    agent_router = None
    data_quality_router = None
    monitoring_router = None
