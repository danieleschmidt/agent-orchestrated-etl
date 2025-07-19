"""Agent communication protocols and message handling."""

from __future__ import annotations

import asyncio
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, TYPE_CHECKING

if TYPE_CHECKING:
    from .base_agent import BaseAgent


from ..exceptions import CommunicationException
from ..logging_config import get_logger


class MessageType(Enum):
    """Types of messages that can be exchanged between agents."""
    
    TASK_REQUEST = "task_request"
    TASK_RESPONSE = "task_response"
    STATUS_UPDATE = "status_update"
    ERROR_NOTIFICATION = "error_notification"
    COORDINATION = "coordination"
    QUERY = "query"
    RESPONSE = "response"
    HEARTBEAT = "heartbeat"
    SHUTDOWN = "shutdown"


class MessagePriority(Enum):
    """Message priority levels."""
    
    LOW = 1
    NORMAL = 5
    HIGH = 8
    URGENT = 10


@dataclass
class Message:
    """Represents a message between agents."""
    
    message_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sender_id: str = ""
    recipient_id: str = ""
    message_type: MessageType = MessageType.QUERY
    priority: MessagePriority = MessagePriority.NORMAL
    content: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    created_at: float = field(default_factory=time.time)
    expires_at: Optional[float] = None
    correlation_id: Optional[str] = None  # For request-response correlation
    reply_to: Optional[str] = None  # Channel for replies
    
    def is_expired(self) -> bool:
        """Check if the message has expired."""
        return self.expires_at is not None and time.time() > self.expires_at
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary representation."""
        return {
            "message_id": self.message_id,
            "sender_id": self.sender_id,
            "recipient_id": self.recipient_id,
            "message_type": self.message_type.value,
            "priority": self.priority.value,
            "content": self.content,
            "metadata": self.metadata,
            "created_at": self.created_at,
            "expires_at": self.expires_at,
            "correlation_id": self.correlation_id,
            "reply_to": self.reply_to,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create message from dictionary representation."""
        return cls(
            message_id=data["message_id"],
            sender_id=data["sender_id"],
            recipient_id=data["recipient_id"],
            message_type=MessageType(data["message_type"]),
            priority=MessagePriority(data["priority"]),
            content=data["content"],
            metadata=data.get("metadata", {}),
            created_at=data["created_at"],
            expires_at=data.get("expires_at"),
            correlation_id=data.get("correlation_id"),
            reply_to=data.get("reply_to"),
        )


class CommunicationChannel:
    """Represents a communication channel between agents."""
    
    def __init__(self, channel_id: str, max_queue_size: int = 1000):
        self.channel_id = channel_id
        self.max_queue_size = max_queue_size
        self.message_queue: asyncio.Queue[Message] = asyncio.Queue(maxsize=max_queue_size)
        self.subscribers: Set[str] = set()
        self.message_handlers: Dict[MessageType, List[Callable]] = {}
        self.stats = {
            "messages_sent": 0,
            "messages_received": 0,
            "messages_dropped": 0,
            "last_activity": time.time(),
        }
        self.logger = get_logger(f"communication.channel.{channel_id}")
    
    async def send_message(self, message: Message) -> bool:
        """Send a message through this channel.
        
        Args:
            message: The message to send
            
        Returns:
            True if message was queued successfully, False otherwise
        """
        try:
            # Check if message has expired
            if message.is_expired():
                self.logger.warning(f"Dropping expired message: {message.message_id}")
                self.stats["messages_dropped"] += 1
                return False
            
            # Try to put message in queue (non-blocking)
            self.message_queue.put_nowait(message)
            self.stats["messages_sent"] += 1
            self.stats["last_activity"] = time.time()
            
            self.logger.debug(
                f"Message queued: {message.message_id}",
                extra={
                    "message_type": message.message_type.value,
                    "sender": message.sender_id,
                    "recipient": message.recipient_id,
                }
            )
            return True
            
        except asyncio.QueueFull:
            self.logger.warning(f"Channel queue full, dropping message: {message.message_id}")
            self.stats["messages_dropped"] += 1
            return False
        except Exception as e:
            self.logger.error(f"Error sending message: {e}", exc_info=e)
            self.stats["messages_dropped"] += 1
            return False
    
    async def receive_message(self, timeout: Optional[float] = None) -> Optional[Message]:
        """Receive a message from this channel.
        
        Args:
            timeout: Maximum time to wait for a message
            
        Returns:
            Message if available, None if timeout or no message
        """
        try:
            if timeout:
                message = await asyncio.wait_for(self.message_queue.get(), timeout=timeout)
            else:
                message = await self.message_queue.get()
            
            self.stats["messages_received"] += 1
            self.stats["last_activity"] = time.time()
            
            self.logger.debug(
                f"Message received: {message.message_id}",
                extra={
                    "message_type": message.message_type.value,
                    "sender": message.sender_id,
                    "recipient": message.recipient_id,
                }
            )
            return message
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            self.logger.error(f"Error receiving message: {e}", exc_info=e)
            return None
    
    def add_subscriber(self, agent_id: str) -> None:
        """Add an agent as a subscriber to this channel."""
        self.subscribers.add(agent_id)
        self.logger.info(f"Agent {agent_id} subscribed to channel {self.channel_id}")
    
    def remove_subscriber(self, agent_id: str) -> None:
        """Remove an agent from this channel's subscribers."""
        self.subscribers.discard(agent_id)
        self.logger.info(f"Agent {agent_id} unsubscribed from channel {self.channel_id}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get channel statistics."""
        return {
            "channel_id": self.channel_id,
            "queue_size": self.message_queue.qsize(),
            "max_queue_size": self.max_queue_size,
            "subscribers": len(self.subscribers),
            "stats": self.stats.copy(),
        }


class AgentCommunicationHub:
    """Central communication hub for agent message exchange."""
    
    def __init__(self, max_agents: int = 100):
        self.max_agents = max_agents
        self.agents: Dict[str, 'BaseAgent'] = {}
        self.channels: Dict[str, CommunicationChannel] = {}
        self.message_routes: Dict[str, Set[str]] = {}  # agent_id -> set of channel_ids
        self.pending_responses: Dict[str, asyncio.Event] = {}  # correlation_id -> event
        self.response_data: Dict[str, Any] = {}  # correlation_id -> response_data
        
        self.logger = get_logger("communication.hub")
        self._running = False
        self._message_processor_task: Optional[asyncio.Task] = None
        
        # Performance metrics
        self.metrics = {
            "total_messages": 0,
            "messages_routed": 0,
            "routing_errors": 0,
            "active_agents": 0,
            "active_channels": 0,
        }
    
    async def start(self) -> None:
        """Start the communication hub."""
        if self._running:
            return
        
        self.logger.info("Starting communication hub")
        self._running = True
        
        # Start background message processor
        self._message_processor_task = asyncio.create_task(self._process_messages())
        
        # Create default channels
        await self.create_channel("broadcast")
        await self.create_channel("orchestrator")
        await self.create_channel("monitoring")
        
        self.logger.info("Communication hub started")
    
    async def stop(self) -> None:
        """Stop the communication hub."""
        if not self._running:
            return
        
        self.logger.info("Stopping communication hub")
        self._running = False
        
        # Cancel message processor
        if self._message_processor_task:
            self._message_processor_task.cancel()
            try:
                await self._message_processor_task
            except asyncio.CancelledError:
                pass
        
        # Clear all channels and agents
        self.channels.clear()
        self.agents.clear()
        self.message_routes.clear()
        
        self.logger.info("Communication hub stopped")
    
    async def register_agent(self, agent: 'BaseAgent') -> None:
        """Register an agent with the communication hub.
        
        Args:
            agent: The agent to register
            
        Raises:
            CommunicationException: If registration fails
        """
        if len(self.agents) >= self.max_agents:
            raise CommunicationException("Maximum number of agents reached")
        
        agent_id = agent.config.agent_id
        
        if agent_id in self.agents:
            raise CommunicationException(f"Agent {agent_id} already registered")
        
        try:
            self.agents[agent_id] = agent
            self.message_routes[agent_id] = set()
            
            # Auto-subscribe to role-based channels
            await self._auto_subscribe_agent(agent)
            
            self.metrics["active_agents"] = len(self.agents)
            
            self.logger.info(
                f"Agent registered: {agent_id}",
                extra={
                    "agent_name": agent.config.name,
                    "agent_role": agent.config.role.value,
                }
            )
            
        except Exception as e:
            raise CommunicationException(f"Failed to register agent {agent_id}: {e}") from e
    
    async def unregister_agent(self, agent_id: str) -> None:
        """Unregister an agent from the communication hub."""
        if agent_id not in self.agents:
            self.logger.warning(f"Attempted to unregister unknown agent: {agent_id}")
            return
        
        try:
            # Remove from all channels
            if agent_id in self.message_routes:
                for channel_id in self.message_routes[agent_id].copy():
                    await self.unsubscribe_agent(agent_id, channel_id)
                del self.message_routes[agent_id]
            
            # Remove agent
            del self.agents[agent_id]
            self.metrics["active_agents"] = len(self.agents)
            
            self.logger.info(f"Agent unregistered: {agent_id}")
            
        except Exception as e:
            self.logger.error(f"Error unregistering agent {agent_id}: {e}", exc_info=e)
    
    async def create_channel(self, channel_id: str, max_queue_size: int = 1000) -> CommunicationChannel:
        """Create a new communication channel.
        
        Args:
            channel_id: Unique identifier for the channel
            max_queue_size: Maximum queue size for the channel
            
        Returns:
            The created channel
            
        Raises:
            CommunicationException: If channel already exists
        """
        if channel_id in self.channels:
            raise CommunicationException(f"Channel {channel_id} already exists")
        
        channel = CommunicationChannel(channel_id, max_queue_size)
        self.channels[channel_id] = channel
        self.metrics["active_channels"] = len(self.channels)
        
        self.logger.info(f"Channel created: {channel_id}")
        return channel
    
    async def delete_channel(self, channel_id: str) -> None:
        """Delete a communication channel."""
        if channel_id not in self.channels:
            self.logger.warning(f"Attempted to delete unknown channel: {channel_id}")
            return
        
        # Remove all subscribers
        channel = self.channels[channel_id]
        for agent_id in channel.subscribers.copy():
            await self.unsubscribe_agent(agent_id, channel_id)
        
        del self.channels[channel_id]
        self.metrics["active_channels"] = len(self.channels)
        
        self.logger.info(f"Channel deleted: {channel_id}")
    
    async def subscribe_agent(self, agent_id: str, channel_id: str) -> bool:
        """Subscribe an agent to a channel.
        
        Args:
            agent_id: ID of the agent to subscribe
            channel_id: ID of the channel to subscribe to
            
        Returns:
            True if subscription successful, False otherwise
        """
        if agent_id not in self.agents:
            self.logger.error(f"Cannot subscribe unknown agent: {agent_id}")
            return False
        
        if channel_id not in self.channels:
            # Auto-create channel if it doesn't exist
            await self.create_channel(channel_id)
        
        channel = self.channels[channel_id]
        channel.add_subscriber(agent_id)
        self.message_routes[agent_id].add(channel_id)
        
        return True
    
    async def unsubscribe_agent(self, agent_id: str, channel_id: str) -> None:
        """Unsubscribe an agent from a channel."""
        if channel_id in self.channels:
            self.channels[channel_id].remove_subscriber(agent_id)
        
        if agent_id in self.message_routes:
            self.message_routes[agent_id].discard(channel_id)
    
    async def send_message(self, message: Message) -> bool:
        """Send a message through the communication hub.
        
        Args:
            message: The message to send
            
        Returns:
            True if message was sent successfully, False otherwise
        """
        try:
            self.metrics["total_messages"] += 1
            
            # Route message to appropriate channel(s)
            routed = await self._route_message(message)
            
            if routed:
                self.metrics["messages_routed"] += 1
                return True
            else:
                self.metrics["routing_errors"] += 1
                return False
                
        except Exception as e:
            self.logger.error(f"Error sending message: {e}", exc_info=e)
            self.metrics["routing_errors"] += 1
            return False
    
    async def send_request(
        self,
        sender_id: str,
        recipient_id: str,
        request_content: Dict[str, Any],
        timeout: float = 30.0,
    ) -> Optional[Dict[str, Any]]:
        """Send a request and wait for response.
        
        Args:
            sender_id: ID of the sender agent
            recipient_id: ID of the recipient agent
            request_content: Content of the request
            timeout: Maximum time to wait for response
            
        Returns:
            Response content if received, None if timeout
        """
        correlation_id = str(uuid.uuid4())
        
        # Create response event
        response_event = asyncio.Event()
        self.pending_responses[correlation_id] = response_event
        
        try:
            # Send request message
            request_message = Message(
                sender_id=sender_id,
                recipient_id=recipient_id,
                message_type=MessageType.QUERY,
                content=request_content,
                correlation_id=correlation_id,
                expires_at=time.time() + timeout,
            )
            
            success = await self.send_message(request_message)
            if not success:
                return None
            
            # Wait for response
            try:
                await asyncio.wait_for(response_event.wait(), timeout=timeout)
                return self.response_data.get(correlation_id)
            except asyncio.TimeoutError:
                self.logger.warning(f"Request timeout: {correlation_id}")
                return None
            
        finally:
            # Cleanup
            self.pending_responses.pop(correlation_id, None)
            self.response_data.pop(correlation_id, None)
    
    async def _route_message(self, message: Message) -> bool:
        """Route a message to the appropriate channel(s)."""
        recipient_id = message.recipient_id
        
        # Handle broadcast messages
        if recipient_id == "broadcast":
            broadcast_channel = self.channels.get("broadcast")
            if broadcast_channel:
                return await broadcast_channel.send_message(message)
            return False
        
        # Handle direct agent messages
        if recipient_id in self.agents:
            # Route to agent's channels
            agent_channels = self.message_routes.get(recipient_id, set())
            if not agent_channels:
                self.logger.warning(f"Agent {recipient_id} has no channels")
                return False
            
            # Send to first available channel (in practice, you might want more sophisticated routing)
            for channel_id in agent_channels:
                channel = self.channels.get(channel_id)
                if channel:
                    return await channel.send_message(message)
            
            return False
        
        # Handle channel-specific messages
        if recipient_id in self.channels:
            channel = self.channels[recipient_id]
            return await channel.send_message(message)
        
        # Fallback: Route unknown recipients to broadcast channel
        # This ensures message delivery even when specific recipients aren't registered
        fallback_channel = self.channels.get("broadcast")
        if fallback_channel:
            self.logger.info(f"Routing message for unknown recipient '{recipient_id}' to broadcast channel")
            return await fallback_channel.send_message(message)
        
        self.logger.warning(f"No route found for recipient: {recipient_id} and no fallback channel available")
        return False
    
    async def _process_messages(self) -> None:
        """Background task to process messages from all channels."""
        self.logger.info("Message processor started")
        
        while self._running:
            try:
                # Process messages from all channels
                for channel in self.channels.values():
                    message = await channel.receive_message(timeout=0.1)
                    if message:
                        await self._handle_message(message)
                
                # Small delay to prevent busy waiting
                await asyncio.sleep(0.01)
                
            except Exception as e:
                self.logger.error(f"Error in message processor: {e}", exc_info=e)
                await asyncio.sleep(1.0)  # Longer delay on error
        
        self.logger.info("Message processor stopped")
    
    async def _handle_message(self, message: Message) -> None:
        """Handle a received message."""
        try:
            # Check for response messages
            if message.correlation_id and message.correlation_id in self.pending_responses:
                self.response_data[message.correlation_id] = message.content
                self.pending_responses[message.correlation_id].set()
                return
            
            # Route to recipient agent
            recipient_id = message.recipient_id
            if recipient_id in self.agents:
                agent = self.agents[recipient_id]
                # Here you would call agent-specific message handlers
                # For now, just log the message
                self.logger.debug(
                    f"Message delivered to agent {recipient_id}",
                    extra={
                        "message_id": message.message_id,
                        "message_type": message.message_type.value,
                        "sender": message.sender_id,
                    }
                )
            
        except Exception as e:
            self.logger.error(f"Error handling message {message.message_id}: {e}", exc_info=e)
    
    async def _auto_subscribe_agent(self, agent: 'BaseAgent') -> None:
        """Automatically subscribe agent to appropriate channels based on role."""
        agent_id = agent.config.agent_id
        role = agent.config.role
        
        # Subscribe to broadcast channel
        await self.subscribe_agent(agent_id, "broadcast")
        
        # Subscribe to role-specific channels
        if role.value == "orchestrator":
            await self.subscribe_agent(agent_id, "orchestrator")
        elif role.value == "monitor":
            await self.subscribe_agent(agent_id, "monitoring")
        
        # Create agent-specific channel
        await self.create_channel(agent_id)
        await self.subscribe_agent(agent_id, agent_id)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get communication hub statistics."""
        channel_stats = {cid: channel.get_stats() for cid, channel in self.channels.items()}
        
        return {
            "hub_metrics": self.metrics.copy(),
            "agents": {aid: agent.get_status() for aid, agent in self.agents.items()},
            "channels": channel_stats,
            "running": self._running,
        }