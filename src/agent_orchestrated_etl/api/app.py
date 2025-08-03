"""FastAPI application setup and configuration."""

from __future__ import annotations

import os
import time
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Dict

try:
    from fastapi import FastAPI, Request, Response
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.middleware.gzip import GZipMiddleware
    from fastapi.responses import JSONResponse
    FASTAPI_AVAILABLE = True
except ImportError:
    FastAPI = None
    Request = None
    Response = None
    JSONResponse = None
    CORSMiddleware = None
    GZipMiddleware = None
    FASTAPI_AVAILABLE = False

from ..database import initialize_database, close_database
from ..logging_config import get_logger
from ..exceptions import AgentException, DatabaseException, ValidationError

if FASTAPI_AVAILABLE:
    from .routers import (
        health_router,
        pipeline_router,
        agent_router,
        data_quality_router,
        monitoring_router,
    )


logger = get_logger("agent_etl.api")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Application lifespan context manager."""
    logger.info("Starting Agent-Orchestrated-ETL API...")
    
    try:
        # Initialize database connections
        await initialize_database()
        logger.info("Database initialized")
        
        # Additional startup tasks could go here
        # - Initialize Redis connections
        # - Start background tasks
        # - Register with service discovery
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down Agent-Orchestrated-ETL API...")
        await close_database()
        logger.info("Database connections closed")


def create_app() -> FastAPI:
    """Create and configure the FastAPI application.
    
    Returns:
        Configured FastAPI application instance
    """
    if not FASTAPI_AVAILABLE:
        raise ImportError("FastAPI is not available. Install with 'pip install fastapi'")
    
    # Application metadata
    app = FastAPI(
        title="Agent-Orchestrated-ETL API",
        description="Intelligent ETL pipeline orchestration with AI agents",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )
    
    # Add middleware
    setup_middleware(app)
    
    # Add exception handlers
    setup_exception_handlers(app)
    
    # Add request/response middleware
    setup_request_middleware(app)
    
    # Include routers
    setup_routers(app)
    
    return app


def setup_middleware(app: FastAPI) -> None:
    """Setup application middleware."""
    
    # CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=_get_cors_origins(),
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH"],
        allow_headers=["*"],
    )
    
    # Compression middleware
    app.add_middleware(GZipMiddleware, minimum_size=1000)


def setup_exception_handlers(app: FastAPI) -> None:
    """Setup custom exception handlers."""
    
    @app.exception_handler(ValidationError)
    async def validation_error_handler(request: Request, exc: ValidationError) -> JSONResponse:
        """Handle validation errors."""
        logger.warning(f"Validation error: {exc}")
        return JSONResponse(
            status_code=400,
            content={
                "error": "validation_error",
                "message": str(exc),
                "type": "ValidationError",
            }
        )
    
    @app.exception_handler(DatabaseException)
    async def database_error_handler(request: Request, exc: DatabaseException) -> JSONResponse:
        """Handle database errors."""
        logger.error(f"Database error: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "database_error",
                "message": "Internal database error occurred",
                "type": "DatabaseException",
            }
        )
    
    @app.exception_handler(AgentException)
    async def agent_error_handler(request: Request, exc: AgentException) -> JSONResponse:
        """Handle agent errors."""
        logger.error(f"Agent error: {exc}")
        return JSONResponse(
            status_code=500,
            content={
                "error": "agent_error",
                "message": str(exc),
                "type": "AgentException",
            }
        )
    
    @app.exception_handler(Exception)
    async def general_error_handler(request: Request, exc: Exception) -> JSONResponse:
        """Handle general errors."""
        logger.error(f"Unhandled error: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={
                "error": "internal_server_error",
                "message": "An internal server error occurred",
                "type": "InternalServerError",
            }
        )


def setup_request_middleware(app: FastAPI) -> None:
    """Setup request/response middleware."""
    
    @app.middleware("http")
    async def add_process_time_header(request: Request, call_next) -> Response:
        """Add processing time header to responses."""
        start_time = time.time()
        response = await call_next(request)
        process_time = time.time() - start_time
        response.headers["X-Process-Time"] = str(process_time)
        return response
    
    @app.middleware("http") 
    async def log_requests(request: Request, call_next) -> Response:
        """Log all requests."""
        start_time = time.time()
        
        # Log request
        logger.info(
            f"Request: {request.method} {request.url.path}",
            extra={
                "method": request.method,
                "path": request.url.path,
                "query_params": str(request.query_params),
                "client_ip": request.client.host if request.client else None,
            }
        )
        
        # Process request
        response = await call_next(request)
        
        # Log response
        process_time = time.time() - start_time
        logger.info(
            f"Response: {response.status_code} in {process_time:.3f}s",
            extra={
                "status_code": response.status_code,
                "process_time": process_time,
                "method": request.method,
                "path": request.url.path,
            }
        )
        
        return response


def setup_routers(app: FastAPI) -> None:
    """Setup API routers."""
    
    # Health and monitoring endpoints
    app.include_router(
        health_router,
        prefix="/health",
        tags=["health"]
    )
    
    app.include_router(
        monitoring_router,
        prefix="/monitoring",
        tags=["monitoring"]
    )
    
    # Core API endpoints
    app.include_router(
        pipeline_router,
        prefix="/api/v1/pipelines",
        tags=["pipelines"]
    )
    
    app.include_router(
        agent_router,
        prefix="/api/v1/agents",
        tags=["agents"]
    )
    
    app.include_router(
        data_quality_router,
        prefix="/api/v1/data-quality",
        tags=["data-quality"]
    )


def _get_cors_origins() -> list[str]:
    """Get CORS origins from environment."""
    cors_origins = os.getenv("CORS_ORIGINS", "")
    
    if cors_origins:
        return [origin.strip() for origin in cors_origins.split(",")]
    
    # Default CORS origins for development
    return [
        "http://localhost:3000",
        "http://localhost:8080",
        "http://localhost:8793",
        "http://127.0.0.1:3000",
        "http://127.0.0.1:8080",
        "http://127.0.0.1:8793",
    ]


# Create application instance for import
if FASTAPI_AVAILABLE:
    app = create_app()
else:
    app = None
    logger.warning("FastAPI not available, API endpoints will not be available")