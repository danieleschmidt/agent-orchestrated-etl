"""WebSocket server for real-time monitoring updates."""

from __future__ import annotations

import asyncio
import json
import time
from typing import Any, Dict, Optional, Set

import websockets
from websockets.server import WebSocketServerProtocol

from ..logging_config import get_logger
from .realtime_monitor import RealtimeMonitor


class MonitoringWebSocketServer:
    """WebSocket server for real-time monitoring data streaming."""

    def __init__(self, monitor: RealtimeMonitor, host: str = "localhost", port: int = 8765):
        self.logger = get_logger("monitoring.websocket_server")
        self.monitor = monitor
        self.host = host
        self.port = port
        self.server: Optional[Any] = None
        self.clients: Set[WebSocketServerProtocol] = set()
        self.running = False

        # Message handlers
        self.message_handlers = {
            "subscribe": self._handle_subscribe,
            "unsubscribe": self._handle_unsubscribe,
            "get_dashboard_data": self._handle_get_dashboard_data,
            "get_metric_history": self._handle_get_metric_history,
            "get_alert_history": self._handle_get_alert_history,
            "configure_alert": self._handle_configure_alert,
            "acknowledge_alert": self._handle_acknowledge_alert
        }

        # Subscription management
        self.client_subscriptions: Dict[WebSocketServerProtocol, Set[str]] = {}

    async def start_server(self) -> None:
        """Start the WebSocket server."""
        if self.running:
            self.logger.warning("WebSocket server is already running")
            return

        try:
            self.server = await websockets.serve(
                self._handle_client,
                self.host,
                self.port,
                ping_interval=30,
                ping_timeout=10
            )
            self.running = True
            self.logger.info(f"WebSocket monitoring server started on ws://{self.host}:{self.port}")

            # Start the server heartbeat
            asyncio.create_task(self._heartbeat_loop())

        except Exception as e:
            self.logger.error(f"Failed to start WebSocket server: {e}")
            raise

    async def stop_server(self) -> None:
        """Stop the WebSocket server."""
        if not self.running:
            return

        self.running = False

        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("WebSocket monitoring server stopped")

        # Disconnect all clients
        for client in self.clients.copy():
            await client.close()

        self.clients.clear()
        self.client_subscriptions.clear()

    async def _handle_client(self, websocket: WebSocketServerProtocol, path: str) -> None:
        """Handle a new WebSocket client connection."""
        client_address = websocket.remote_address
        self.logger.info(f"New WebSocket client connected: {client_address}")

        # Add client to our sets
        self.clients.add(websocket)
        self.client_subscriptions[websocket] = set()
        self.monitor.add_websocket_client(websocket)

        try:
            # Send welcome message with server capabilities
            welcome_message = {
                "type": "welcome",
                "data": {
                    "server_version": "1.0.0",
                    "available_subscriptions": [
                        "metrics_updates",
                        "alerts",
                        "sla_compliance",
                        "pipeline_status",
                        "system_health"
                    ],
                    "available_commands": list(self.message_handlers.keys()),
                    "timestamp": time.time()
                }
            }
            await websocket.send(json.dumps(welcome_message))

            # Handle incoming messages
            async for message in websocket:
                try:
                    await self._process_message(websocket, message)
                except json.JSONDecodeError:
                    await self._send_error(websocket, "Invalid JSON format")
                except Exception as e:
                    self.logger.error(f"Error processing message from {client_address}: {e}")
                    await self._send_error(websocket, f"Message processing error: {e}")

        except websockets.exceptions.ConnectionClosed:
            self.logger.info(f"WebSocket client disconnected: {client_address}")
        except Exception as e:
            self.logger.error(f"Error handling WebSocket client {client_address}: {e}")
        finally:
            # Clean up client
            self.clients.discard(websocket)
            self.client_subscriptions.pop(websocket, None)
            self.monitor.remove_websocket_client(websocket)

    async def _process_message(self, websocket: WebSocketServerProtocol, message: str) -> None:
        """Process incoming WebSocket message."""
        try:
            data = json.loads(message)
            message_type = data.get("type")

            if message_type not in self.message_handlers:
                await self._send_error(websocket, f"Unknown message type: {message_type}")
                return

            handler = self.message_handlers[message_type]
            await handler(websocket, data.get("data", {}))

        except Exception as e:
            await self._send_error(websocket, f"Error processing message: {e}")

    async def _handle_subscribe(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle subscription requests."""
        subscription_type = data.get("subscription_type")

        if not subscription_type:
            await self._send_error(websocket, "subscription_type is required")
            return

        self.client_subscriptions[websocket].add(subscription_type)

        response = {
            "type": "subscription_confirmed",
            "data": {
                "subscription_type": subscription_type,
                "active_subscriptions": list(self.client_subscriptions[websocket]),
                "timestamp": time.time()
            }
        }
        await websocket.send(json.dumps(response))

        self.logger.info(f"Client subscribed to: {subscription_type}")

    async def _handle_unsubscribe(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle unsubscription requests."""
        subscription_type = data.get("subscription_type")

        if subscription_type:
            self.client_subscriptions[websocket].discard(subscription_type)
        else:
            # Unsubscribe from all
            self.client_subscriptions[websocket].clear()

        response = {
            "type": "unsubscription_confirmed",
            "data": {
                "subscription_type": subscription_type,
                "active_subscriptions": list(self.client_subscriptions[websocket]),
                "timestamp": time.time()
            }
        }
        await websocket.send(json.dumps(response))

    async def _handle_get_dashboard_data(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle request for dashboard data."""
        try:
            dashboard_data = self.monitor.get_monitoring_dashboard_data()

            response = {
                "type": "dashboard_data",
                "data": dashboard_data
            }
            await websocket.send(json.dumps(response))

        except Exception as e:
            await self._send_error(websocket, f"Failed to get dashboard data: {e}")

    async def _handle_get_metric_history(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle request for metric history."""
        metric_name = data.get("metric_name")
        hours = data.get("hours", 24)

        if not metric_name:
            await self._send_error(websocket, "metric_name is required")
            return

        try:
            history = self.monitor.metrics_collector.get_metric_history(metric_name, hours)

            response = {
                "type": "metric_history",
                "data": {
                    "metric_name": metric_name,
                    "hours": hours,
                    "history": history,
                    "timestamp": time.time()
                }
            }
            await websocket.send(json.dumps(response))

        except Exception as e:
            await self._send_error(websocket, f"Failed to get metric history: {e}")

    async def _handle_get_alert_history(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle request for alert history."""
        hours = data.get("hours", 24)

        try:
            cutoff_time = time.time() - (hours * 3600)
            recent_alerts = [
                alert for alert in self.monitor.alert_manager.alert_history
                if alert.get("timestamp", 0) > cutoff_time
            ]

            response = {
                "type": "alert_history",
                "data": {
                    "hours": hours,
                    "alerts": recent_alerts,
                    "active_alerts": list(self.monitor.alert_manager.active_alerts.values()),
                    "timestamp": time.time()
                }
            }
            await websocket.send(json.dumps(response))

        except Exception as e:
            await self._send_error(websocket, f"Failed to get alert history: {e}")

    async def _handle_configure_alert(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle alert configuration requests."""
        required_fields = ["name", "metric", "condition", "threshold"]

        if not all(field in data for field in required_fields):
            await self._send_error(websocket, f"Required fields: {required_fields}")
            return

        try:
            self.monitor.alert_manager.register_alert_rule(
                name=data["name"],
                metric=data["metric"],
                condition=data["condition"],
                threshold=data["threshold"],
                severity=data.get("severity", "warning"),
                duration=data.get("duration", 0),
                description=data.get("description")
            )

            response = {
                "type": "alert_configured",
                "data": {
                    "name": data["name"],
                    "status": "success",
                    "timestamp": time.time()
                }
            }
            await websocket.send(json.dumps(response))

        except Exception as e:
            await self._send_error(websocket, f"Failed to configure alert: {e}")

    async def _handle_acknowledge_alert(self, websocket: WebSocketServerProtocol, data: Dict[str, Any]) -> None:
        """Handle alert acknowledgment."""
        alert_id = data.get("alert_id")

        if not alert_id:
            await self._send_error(websocket, "alert_id is required")
            return

        # Find and acknowledge the alert
        acknowledged = False
        for alert_key, alert in self.monitor.alert_manager.active_alerts.items():
            if alert.get("id") == alert_id:
                alert["acknowledged"] = True
                alert["acknowledged_at"] = time.time()
                alert["acknowledged_by"] = data.get("acknowledged_by", "unknown")
                acknowledged = True
                break

        response = {
            "type": "alert_acknowledged",
            "data": {
                "alert_id": alert_id,
                "acknowledged": acknowledged,
                "timestamp": time.time()
            }
        }
        await websocket.send(json.dumps(response))

    async def _send_error(self, websocket: WebSocketServerProtocol, error_message: str) -> None:
        """Send error message to client."""
        error_response = {
            "type": "error",
            "data": {
                "message": error_message,
                "timestamp": time.time()
            }
        }
        try:
            await websocket.send(json.dumps(error_response))
        except Exception as e:
            self.logger.error(f"Failed to send error message: {e}")

    async def broadcast_to_subscribers(self, message_type: str, data: Dict[str, Any]) -> None:
        """Broadcast message to subscribed clients."""
        message = {
            "type": message_type,
            "data": data
        }
        message_json = json.dumps(message)

        disconnected_clients = set()

        for client, subscriptions in self.client_subscriptions.items():
            if message_type in subscriptions:
                try:
                    await client.send(message_json)
                except Exception as e:
                    self.logger.warning(f"Failed to send message to client: {e}")
                    disconnected_clients.add(client)

        # Clean up disconnected clients
        for client in disconnected_clients:
            self.clients.discard(client)
            self.client_subscriptions.pop(client, None)
            self.monitor.remove_websocket_client(client)

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat to all connected clients."""
        while self.running:
            try:
                heartbeat_message = {
                    "type": "heartbeat",
                    "data": {
                        "server_time": time.time(),
                        "connected_clients": len(self.clients),
                        "monitoring_active": self.monitor.monitoring_active
                    }
                }

                await self.broadcast_to_subscribers("heartbeat", heartbeat_message["data"])
                await asyncio.sleep(30)  # Send heartbeat every 30 seconds

            except Exception as e:
                self.logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(30)

    def get_server_stats(self) -> Dict[str, Any]:
        """Get server statistics."""
        subscription_counts = {}
        for subscriptions in self.client_subscriptions.values():
            for sub in subscriptions:
                subscription_counts[sub] = subscription_counts.get(sub, 0) + 1

        return {
            "server_running": self.running,
            "host": self.host,
            "port": self.port,
            "connected_clients": len(self.clients),
            "subscription_counts": subscription_counts,
            "total_subscriptions": sum(len(subs) for subs in self.client_subscriptions.values())
        }


async def start_monitoring_server(monitor: RealtimeMonitor, host: str = "localhost", port: int = 8765) -> MonitoringWebSocketServer:
    """Start the monitoring WebSocket server."""
    server = MonitoringWebSocketServer(monitor, host, port)
    await server.start_server()
    return server
