"""API endpoints for monitoring and observability."""

import asyncio

from aiohttp import web
from aiohttp.web import Request, Response, json_response

from .health import health_checker, metrics_collector


async def health_endpoint(request: Request) -> Response:
    """Health check endpoint."""
    try:
        health_status = await health_checker.get_health_status()
        status_code = 200 if health_status.status == "healthy" else 503
        return json_response(health_status.dict(), status=status_code)
    except Exception as e:
        return json_response(
            {"status": "unhealthy", "error": str(e)},
            status=503
        )


async def ready_endpoint(request: Request) -> Response:
    """Readiness check endpoint."""
    try:
        # Check if critical dependencies are available
        health_status = await health_checker.get_health_status()

        # Consider ready if not unhealthy
        is_ready = health_status.status in ["healthy", "degraded"]

        if is_ready:
            return json_response({"status": "ready", "message": "Service is ready"})
        else:
            return json_response(
                {"status": "not_ready", "message": "Service is not ready"},
                status=503
            )
    except Exception as e:
        return json_response(
            {"status": "not_ready", "error": str(e)},
            status=503
        )


async def metrics_endpoint(request: Request) -> Response:
    """Prometheus metrics endpoint."""
    try:
        metrics_format = request.query.get("format", "prometheus")

        if metrics_format == "prometheus":
            metrics_text = metrics_collector.get_prometheus_format()
            return Response(
                text=metrics_text,
                content_type="text/plain; version=0.0.4; charset=utf-8"
            )
        elif metrics_format == "json":
            metrics_data = metrics_collector.get_metrics()
            return json_response(metrics_data)
        else:
            return json_response(
                {"error": "Unsupported format. Use 'prometheus' or 'json'"},
                status=400
            )
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


async def version_endpoint(request: Request) -> Response:
    """Version information endpoint."""
    try:
        from . import __version__
        return json_response({
            "version": __version__,
            "service": "agent-orchestrated-etl",
            "build_date": "2025-07-28"  # This would be injected during build
        })
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


async def pipeline_status_endpoint(request: Request) -> Response:
    """Pipeline status endpoint."""
    try:
        # This would integrate with the actual pipeline management system
        pipeline_id = request.query.get("pipeline_id")

        if pipeline_id:
            # Return specific pipeline status
            return json_response({
                "pipeline_id": pipeline_id,
                "status": "running",  # This would be actual status
                "message": "Pipeline status retrieved"
            })
        else:
            # Return all pipelines status
            return json_response({
                "active_pipelines": 0,
                "queued_pipelines": 0,
                "failed_pipelines": 0,
                "completed_pipelines": 0
            })
    except Exception as e:
        return json_response({"error": str(e)}, status=500)


def setup_monitoring_routes(app: web.Application) -> None:
    """Setup monitoring and observability routes."""
    app.router.add_get("/health", health_endpoint)
    app.router.add_get("/ready", ready_endpoint)
    app.router.add_get("/metrics", metrics_endpoint)
    app.router.add_get("/version", version_endpoint)
    app.router.add_get("/pipeline/status", pipeline_status_endpoint)


async def create_monitoring_app() -> web.Application:
    """Create monitoring web application."""
    app = web.Application()
    setup_monitoring_routes(app)
    return app


if __name__ == "__main__":
    async def main():
        app = await create_monitoring_app()
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8793)
        await site.start()
        print("Monitoring server started on http://0.0.0.0:8793")

        # Keep the server running
        try:
            await asyncio.Event().wait()
        except KeyboardInterrupt:
            pass
        finally:
            await runner.cleanup()

    asyncio.run(main())
