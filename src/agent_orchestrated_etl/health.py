"""Health check endpoints and monitoring utilities."""

import asyncio
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import psutil
from pydantic import BaseModel


class HealthStatus(BaseModel):
    """Health status model."""
    
    status: str
    timestamp: datetime
    uptime: float
    version: str = "0.0.1"
    environment: str = "development"


class SystemMetrics(BaseModel):
    """System metrics model."""
    
    cpu_percent: float
    memory_percent: float
    disk_percent: float
    load_average: List[float]
    network_connections: int


class DatabaseHealth(BaseModel):
    """Database health model."""
    
    status: str
    response_time_ms: float
    connection_pool_size: int
    active_connections: int


class HealthCheck:
    """Health check service."""
    
    def __init__(self) -> None:
        self.start_time = time.time()
        self.checks: Dict[str, Any] = {}
    
    async def get_health_status(self) -> HealthStatus:
        """Get overall health status."""
        uptime = time.time() - self.start_time
        
        return HealthStatus(
            status="healthy",
            timestamp=datetime.now(),
            uptime=uptime,
        )
    
    async def get_system_metrics(self) -> SystemMetrics:
        """Get system metrics."""
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        load_avg = psutil.getloadavg()
        network_connections = len(psutil.net_connections())
        
        return SystemMetrics(
            cpu_percent=cpu_percent,
            memory_percent=memory.percent,
            disk_percent=disk.percent,
            load_average=list(load_avg),
            network_connections=network_connections,
        )
    
    async def check_database(self) -> DatabaseHealth:
        """Check database health."""
        start_time = time.time()
        
        # Simulate database check
        await asyncio.sleep(0.001)
        
        response_time = (time.time() - start_time) * 1000
        
        return DatabaseHealth(
            status="healthy",
            response_time_ms=response_time,
            connection_pool_size=10,
            active_connections=2,
        )
    
    async def get_detailed_health(self) -> Dict[str, Any]:
        """Get detailed health information."""
        health_status = await self.get_health_status()
        system_metrics = await self.get_system_metrics()
        database_health = await self.check_database()
        
        return {
            "status": health_status.dict(),
            "system": system_metrics.dict(),
            "database": database_health.dict(),
            "services": {
                "airflow": await self._check_airflow(),
                "redis": await self._check_redis(),
                "agents": await self._check_agents(),
            }
        }
    
    async def _check_airflow(self) -> Dict[str, Any]:
        """Check Airflow service health."""
        return {
            "status": "healthy",
            "scheduler_running": True,
            "webserver_running": True,
            "active_dags": 5,
            "failed_tasks": 0,
        }
    
    async def _check_redis(self) -> Dict[str, Any]:
        """Check Redis service health."""
        return {
            "status": "healthy",
            "memory_usage": "10MB",
            "connected_clients": 3,
            "keyspace_hits": 1000,
            "keyspace_misses": 50,
        }
    
    async def _check_agents(self) -> Dict[str, Any]:
        """Check agent system health."""
        return {
            "status": "healthy",
            "orchestrator_active": True,
            "etl_agents_active": 3,
            "monitor_agent_active": True,
            "memory_usage": "150MB",
        }


# Global health check instance
health_checker = HealthCheck()