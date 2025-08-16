"""Distributed Consensus for Multi-Agent Coordination using Byzantine Fault Tolerant algorithms."""

from __future__ import annotations

import asyncio
import json
import time
import hashlib
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from enum import Enum
from abc import ABC, abstractmethod
import uuid
import random

from .logging_config import get_logger
from .exceptions import DataProcessingException


class MessageType(Enum):
    """Types of consensus messages."""
    PROPOSE = "propose"
    PROMISE = "promise"
    ACCEPT = "accept"
    ACCEPTED = "accepted"
    PREPARE = "prepare"
    COMMIT = "commit"
    ABORT = "abort"
    HEARTBEAT = "heartbeat"
    LEADER_ELECTION = "leader_election"
    VIEW_CHANGE = "view_change"


class NodeState(Enum):
    """States of consensus nodes."""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    FAILED = "failed"
    RECOVERING = "recovering"


@dataclass
class ConsensusMessage:
    """Message for consensus protocol."""
    message_id: str
    message_type: MessageType
    sender_id: str
    receiver_id: Optional[str]
    view_number: int
    proposal_number: int
    value: Optional[Any]
    timestamp: datetime
    signature: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary."""
        return {
            "message_id": self.message_id,
            "message_type": self.message_type.value,
            "sender_id": self.sender_id,
            "receiver_id": self.receiver_id,
            "view_number": self.view_number,
            "proposal_number": self.proposal_number,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "signature": self.signature
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> ConsensusMessage:
        """Create message from dictionary."""
        return cls(
            message_id=data["message_id"],
            message_type=MessageType(data["message_type"]),
            sender_id=data["sender_id"],
            receiver_id=data.get("receiver_id"),
            view_number=data["view_number"],
            proposal_number=data["proposal_number"],
            value=data.get("value"),
            timestamp=datetime.fromisoformat(data["timestamp"]),
            signature=data.get("signature")
        )


@dataclass
class ConsensusDecision:
    """Result of consensus process."""
    decision_id: str
    value: Any
    view_number: int
    proposal_number: int
    committed_by: List[str]
    timestamp: datetime
    confidence: float


class NetworkTransport(ABC):
    """Abstract network transport for consensus messages."""
    
    @abstractmethod
    async def send_message(self, message: ConsensusMessage, target_node: str) -> bool:
        """Send message to target node."""
        pass
    
    @abstractmethod
    async def broadcast_message(self, message: ConsensusMessage, exclude: Set[str] = None) -> int:
        """Broadcast message to all nodes."""
        pass
    
    @abstractmethod
    async def receive_message(self, timeout: float = 1.0) -> Optional[ConsensusMessage]:
        """Receive next message."""
        pass


class InMemoryTransport(NetworkTransport):
    """In-memory transport for testing and simulation."""
    
    def __init__(self, node_id: str):
        self.node_id = node_id
        self.message_queues: Dict[str, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.network_delay = 0.01  # Simulate network delay
        self.message_loss_rate = 0.0  # Simulate message loss
        self.logger = get_logger(f"transport.{node_id}")
    
    async def send_message(self, message: ConsensusMessage, target_node: str) -> bool:
        """Send message to target node."""
        try:
            # Simulate network delay
            await asyncio.sleep(self.network_delay)
            
            # Simulate message loss
            if random.random() < self.message_loss_rate:
                self.logger.debug(f"Message lost: {message.message_id}")
                return False
            
            # Deliver message
            await self.message_queues[target_node].put(message)
            self.logger.debug(f"Sent {message.message_type.value} to {target_node}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")
            return False
    
    async def broadcast_message(self, message: ConsensusMessage, exclude: Set[str] = None) -> int:
        """Broadcast message to all nodes."""
        exclude = exclude or set()
        exclude.add(self.node_id)  # Don't send to self
        
        sent_count = 0
        for node_id in list(self.message_queues.keys()):
            if node_id not in exclude:
                if await self.send_message(message, node_id):
                    sent_count += 1
        
        return sent_count
    
    async def receive_message(self, timeout: float = 1.0) -> Optional[ConsensusMessage]:
        """Receive next message."""
        try:
            message = await asyncio.wait_for(
                self.message_queues[self.node_id].get(),
                timeout=timeout
            )
            self.logger.debug(f"Received {message.message_type.value} from {message.sender_id}")
            return message
            
        except asyncio.TimeoutError:
            return None
        except Exception as e:
            self.logger.error(f"Failed to receive message: {e}")
            return None


class PaxosConsensusNode:
    """Implementation of Paxos consensus algorithm for Byzantine fault tolerance."""
    
    def __init__(
        self,
        node_id: str,
        transport: NetworkTransport,
        cluster_nodes: List[str],
        heartbeat_interval: float = 5.0
    ):
        self.node_id = node_id
        self.transport = transport
        self.cluster_nodes = set(cluster_nodes)
        self.heartbeat_interval = heartbeat_interval
        
        # Consensus state
        self.state = NodeState.FOLLOWER
        self.current_view = 0
        self.highest_proposal = 0
        self.accepted_proposal = None
        self.accepted_value = None
        self.promised_proposal = 0
        
        # Leader election state
        self.current_leader = None
        self.last_heartbeat = {}
        self.election_timeout = 10.0
        self.votes_received = set()
        
        # Message handling
        self.pending_decisions = {}
        self.committed_decisions = {}
        
        # Monitoring
        self.logger = get_logger(f"consensus.{node_id}")
        self.metrics = {
            "messages_sent": 0,
            "messages_received": 0,
            "consensus_rounds": 0,
            "leadership_changes": 0,
            "failed_proposals": 0
        }
        
        # Tasks
        self.running_tasks = []
        
    async def start(self) -> None:
        """Start the consensus node."""
        self.logger.info(f"Starting consensus node {self.node_id}")
        
        # Start background tasks
        self.running_tasks = [
            asyncio.create_task(self._message_handler()),
            asyncio.create_task(self._heartbeat_sender()),
            asyncio.create_task(self._failure_detector()),
            asyncio.create_task(self._leader_election_monitor())
        ]
        
        # Initial leader election
        await self._start_leader_election()
    
    async def stop(self) -> None:
        """Stop the consensus node."""
        self.logger.info(f"Stopping consensus node {self.node_id}")
        
        for task in self.running_tasks:
            task.cancel()
        
        await asyncio.gather(*self.running_tasks, return_exceptions=True)
    
    async def propose_value(self, value: Any, decision_id: str = None) -> ConsensusDecision:
        """Propose a value for consensus."""
        decision_id = decision_id or str(uuid.uuid4())
        
        if self.state != NodeState.LEADER:
            raise DataProcessingException("Only leader can propose values")
        
        self.logger.info(f"Proposing value for decision {decision_id}")
        
        try:
            # Phase 1: Prepare
            proposal_number = self._generate_proposal_number()
            prepare_responses = await self._send_prepare(proposal_number)
            
            if len(prepare_responses) < self._quorum_size():
                self.metrics["failed_proposals"] += 1
                raise DataProcessingException("Failed to get majority in prepare phase")
            
            # Phase 2: Accept
            accept_responses = await self._send_accept(proposal_number, value)
            
            if len(accept_responses) < self._quorum_size():
                self.metrics["failed_proposals"] += 1
                raise DataProcessingException("Failed to get majority in accept phase")
            
            # Phase 3: Commit
            decision = ConsensusDecision(
                decision_id=decision_id,
                value=value,
                view_number=self.current_view,
                proposal_number=proposal_number,
                committed_by=[resp.sender_id for resp in accept_responses],
                timestamp=datetime.now(),
                confidence=len(accept_responses) / len(self.cluster_nodes)
            )
            
            await self._send_commit(decision)
            
            self.committed_decisions[decision_id] = decision
            self.metrics["consensus_rounds"] += 1
            
            self.logger.info(f"Consensus reached for decision {decision_id}")
            return decision
            
        except Exception as e:
            self.logger.error(f"Consensus proposal failed: {e}")
            self.metrics["failed_proposals"] += 1
            raise
    
    async def _message_handler(self) -> None:
        """Handle incoming consensus messages."""
        while True:
            try:
                message = await self.transport.receive_message(timeout=0.1)
                
                if message:
                    self.metrics["messages_received"] += 1
                    await self._process_message(message)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Message handler error: {e}")
                await asyncio.sleep(0.1)
    
    async def _process_message(self, message: ConsensusMessage) -> None:
        """Process incoming consensus message."""
        try:
            if message.message_type == MessageType.HEARTBEAT:
                await self._handle_heartbeat(message)
            
            elif message.message_type == MessageType.LEADER_ELECTION:
                await self._handle_leader_election(message)
            
            elif message.message_type == MessageType.PREPARE:
                await self._handle_prepare(message)
            
            elif message.message_type == MessageType.PROMISE:
                await self._handle_promise(message)
            
            elif message.message_type == MessageType.ACCEPT:
                await self._handle_accept(message)
            
            elif message.message_type == MessageType.ACCEPTED:
                await self._handle_accepted(message)
            
            elif message.message_type == MessageType.COMMIT:
                await self._handle_commit(message)
            
            elif message.message_type == MessageType.VIEW_CHANGE:
                await self._handle_view_change(message)
            
        except Exception as e:
            self.logger.error(f"Failed to process message {message.message_type}: {e}")
    
    async def _handle_heartbeat(self, message: ConsensusMessage) -> None:
        """Handle heartbeat message."""
        self.last_heartbeat[message.sender_id] = datetime.now()
        
        if message.sender_id == self.current_leader:
            # Reset election timeout
            self.election_timeout = time.time() + 10.0
    
    async def _handle_leader_election(self, message: ConsensusMessage) -> None:
        """Handle leader election message."""
        if message.value.get("action") == "request_vote":
            await self._handle_vote_request(message)
        elif message.value.get("action") == "vote":
            await self._handle_vote(message)
        elif message.value.get("action") == "leader_announcement":
            await self._handle_leader_announcement(message)
    
    async def _handle_vote_request(self, message: ConsensusMessage) -> None:
        """Handle vote request in leader election."""
        candidate_view = message.view_number
        
        # Vote for candidate if they have higher view number and we haven't voted
        if (candidate_view > self.current_view and 
            message.sender_id not in self.votes_received):
            
            vote_message = ConsensusMessage(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.LEADER_ELECTION,
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                view_number=self.current_view,
                proposal_number=0,
                value={"action": "vote", "candidate": message.sender_id},
                timestamp=datetime.now()
            )
            
            await self.transport.send_message(vote_message, message.sender_id)
            self.logger.debug(f"Voted for {message.sender_id}")
    
    async def _handle_vote(self, message: ConsensusMessage) -> None:
        """Handle vote in leader election."""
        if self.state == NodeState.CANDIDATE:
            self.votes_received.add(message.sender_id)
            
            # Check if we have majority
            if len(self.votes_received) >= self._quorum_size():
                await self._become_leader()
    
    async def _handle_leader_announcement(self, message: ConsensusMessage) -> None:
        """Handle leader announcement."""
        if message.view_number >= self.current_view:
            self.current_leader = message.sender_id
            self.current_view = message.view_number
            self.state = NodeState.FOLLOWER
            self.metrics["leadership_changes"] += 1
            
            self.logger.info(f"New leader: {self.current_leader} (view {self.current_view})")
    
    async def _handle_prepare(self, message: ConsensusMessage) -> None:
        """Handle prepare message in Paxos."""
        if message.proposal_number > self.promised_proposal:
            self.promised_proposal = message.proposal_number
            
            promise_message = ConsensusMessage(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.PROMISE,
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                view_number=self.current_view,
                proposal_number=message.proposal_number,
                value={
                    "accepted_proposal": self.accepted_proposal,
                    "accepted_value": self.accepted_value
                },
                timestamp=datetime.now()
            )
            
            await self.transport.send_message(promise_message, message.sender_id)
    
    async def _handle_promise(self, message: ConsensusMessage) -> None:
        """Handle promise message in Paxos."""
        # Store promise for proposal processing
        proposal_key = f"proposal_{message.proposal_number}"
        
        if proposal_key not in self.pending_decisions:
            self.pending_decisions[proposal_key] = []
        
        self.pending_decisions[proposal_key].append(message)
    
    async def _handle_accept(self, message: ConsensusMessage) -> None:
        """Handle accept message in Paxos."""
        if message.proposal_number >= self.promised_proposal:
            self.accepted_proposal = message.proposal_number
            self.accepted_value = message.value
            
            accepted_message = ConsensusMessage(
                message_id=str(uuid.uuid4()),
                message_type=MessageType.ACCEPTED,
                sender_id=self.node_id,
                receiver_id=message.sender_id,
                view_number=self.current_view,
                proposal_number=message.proposal_number,
                value=message.value,
                timestamp=datetime.now()
            )
            
            await self.transport.send_message(accepted_message, message.sender_id)
    
    async def _handle_accepted(self, message: ConsensusMessage) -> None:
        """Handle accepted message in Paxos."""
        # Store accepted response for proposal processing
        proposal_key = f"accept_{message.proposal_number}"
        
        if proposal_key not in self.pending_decisions:
            self.pending_decisions[proposal_key] = []
        
        self.pending_decisions[proposal_key].append(message)
    
    async def _handle_commit(self, message: ConsensusMessage) -> None:
        """Handle commit message."""
        decision = ConsensusDecision(
            decision_id=message.value["decision_id"],
            value=message.value["value"],
            view_number=message.view_number,
            proposal_number=message.proposal_number,
            committed_by=message.value["committed_by"],
            timestamp=datetime.fromisoformat(message.value["timestamp"]),
            confidence=message.value["confidence"]
        )
        
        self.committed_decisions[decision.decision_id] = decision
        self.logger.info(f"Committed decision {decision.decision_id}")
    
    async def _handle_view_change(self, message: ConsensusMessage) -> None:
        """Handle view change message."""
        if message.view_number > self.current_view:
            self.current_view = message.view_number
            self.state = NodeState.FOLLOWER
            self.current_leader = None
            await self._start_leader_election()
    
    async def _heartbeat_sender(self) -> None:
        """Send periodic heartbeats if leader."""
        while True:
            try:
                if self.state == NodeState.LEADER:
                    heartbeat = ConsensusMessage(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.HEARTBEAT,
                        sender_id=self.node_id,
                        receiver_id=None,
                        view_number=self.current_view,
                        proposal_number=0,
                        value={"leader_id": self.node_id},
                        timestamp=datetime.now()
                    )
                    
                    await self.transport.broadcast_message(heartbeat)
                    self.metrics["messages_sent"] += 1
                
                await asyncio.sleep(self.heartbeat_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Heartbeat sender error: {e}")
                await asyncio.sleep(self.heartbeat_interval)
    
    async def _failure_detector(self) -> None:
        """Detect failed nodes and trigger recovery."""
        while True:
            try:
                now = datetime.now()
                failed_nodes = []
                
                for node_id in self.cluster_nodes:
                    if node_id == self.node_id:
                        continue
                    
                    last_seen = self.last_heartbeat.get(node_id)
                    if last_seen and (now - last_seen).total_seconds() > 15.0:
                        failed_nodes.append(node_id)
                
                # Handle leader failure
                if self.current_leader in failed_nodes:
                    self.logger.warning(f"Leader {self.current_leader} failed")
                    self.current_leader = None
                    await self._start_leader_election()
                
                await asyncio.sleep(5.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Failure detector error: {e}")
                await asyncio.sleep(5.0)
    
    async def _leader_election_monitor(self) -> None:
        """Monitor for leader election timeout."""
        while True:
            try:
                if (self.state == NodeState.FOLLOWER and 
                    self.current_leader is None and 
                    time.time() > getattr(self, 'election_timeout', 0)):
                    
                    await self._start_leader_election()
                
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Election monitor error: {e}")
                await asyncio.sleep(1.0)
    
    async def _start_leader_election(self) -> None:
        """Start leader election process."""
        self.state = NodeState.CANDIDATE
        self.current_view += 1
        self.votes_received = {self.node_id}  # Vote for self
        
        self.logger.info(f"Starting leader election for view {self.current_view}")
        
        vote_request = ConsensusMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.LEADER_ELECTION,
            sender_id=self.node_id,
            receiver_id=None,
            view_number=self.current_view,
            proposal_number=0,
            value={"action": "request_vote"},
            timestamp=datetime.now()
        )
        
        await self.transport.broadcast_message(vote_request)
        self.metrics["messages_sent"] += 1
        
        # Wait for votes with timeout
        await asyncio.sleep(5.0)
        
        if len(self.votes_received) >= self._quorum_size():
            await self._become_leader()
        else:
            self.state = NodeState.FOLLOWER
            self.election_timeout = time.time() + random.uniform(10.0, 20.0)
    
    async def _become_leader(self) -> None:
        """Become the leader."""
        self.state = NodeState.LEADER
        self.current_leader = self.node_id
        
        self.logger.info(f"Became leader for view {self.current_view}")
        
        # Announce leadership
        announcement = ConsensusMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.LEADER_ELECTION,
            sender_id=self.node_id,
            receiver_id=None,
            view_number=self.current_view,
            proposal_number=0,
            value={"action": "leader_announcement"},
            timestamp=datetime.now()
        )
        
        await self.transport.broadcast_message(announcement)
        self.metrics["messages_sent"] += 1
        self.metrics["leadership_changes"] += 1
    
    async def _send_prepare(self, proposal_number: int) -> List[ConsensusMessage]:
        """Send prepare messages for Paxos phase 1."""
        prepare_message = ConsensusMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.PREPARE,
            sender_id=self.node_id,
            receiver_id=None,
            view_number=self.current_view,
            proposal_number=proposal_number,
            value=None,
            timestamp=datetime.now()
        )
        
        await self.transport.broadcast_message(prepare_message)
        self.metrics["messages_sent"] += 1
        
        # Wait for promises
        await asyncio.sleep(2.0)
        
        proposal_key = f"proposal_{proposal_number}"
        responses = self.pending_decisions.get(proposal_key, [])
        
        if proposal_key in self.pending_decisions:
            del self.pending_decisions[proposal_key]
        
        return responses
    
    async def _send_accept(self, proposal_number: int, value: Any) -> List[ConsensusMessage]:
        """Send accept messages for Paxos phase 2."""
        accept_message = ConsensusMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.ACCEPT,
            sender_id=self.node_id,
            receiver_id=None,
            view_number=self.current_view,
            proposal_number=proposal_number,
            value=value,
            timestamp=datetime.now()
        )
        
        await self.transport.broadcast_message(accept_message)
        self.metrics["messages_sent"] += 1
        
        # Wait for accepted responses
        await asyncio.sleep(2.0)
        
        accept_key = f"accept_{proposal_number}"
        responses = self.pending_decisions.get(accept_key, [])
        
        if accept_key in self.pending_decisions:
            del self.pending_decisions[accept_key]
        
        return responses
    
    async def _send_commit(self, decision: ConsensusDecision) -> None:
        """Send commit message."""
        commit_message = ConsensusMessage(
            message_id=str(uuid.uuid4()),
            message_type=MessageType.COMMIT,
            sender_id=self.node_id,
            receiver_id=None,
            view_number=self.current_view,
            proposal_number=decision.proposal_number,
            value={
                "decision_id": decision.decision_id,
                "value": decision.value,
                "committed_by": decision.committed_by,
                "timestamp": decision.timestamp.isoformat(),
                "confidence": decision.confidence
            },
            timestamp=datetime.now()
        )
        
        await self.transport.broadcast_message(commit_message)
        self.metrics["messages_sent"] += 1
    
    def _generate_proposal_number(self) -> int:
        """Generate unique proposal number."""
        self.highest_proposal += 1
        return self.highest_proposal
    
    def _quorum_size(self) -> int:
        """Calculate quorum size (majority)."""
        return (len(self.cluster_nodes) // 2) + 1
    
    def get_status(self) -> Dict[str, Any]:
        """Get node status."""
        return {
            "node_id": self.node_id,
            "state": self.state.value,
            "current_view": self.current_view,
            "current_leader": self.current_leader,
            "cluster_size": len(self.cluster_nodes),
            "quorum_size": self._quorum_size(),
            "committed_decisions": len(self.committed_decisions),
            "metrics": self.metrics
        }


class DistributedConsensusCoordinator:
    """Coordinator for distributed consensus-based multi-agent coordination."""
    
    def __init__(
        self,
        cluster_config: Dict[str, Any],
        node_id: Optional[str] = None
    ):
        self.cluster_config = cluster_config
        self.node_id = node_id or str(uuid.uuid4())
        self.logger = get_logger(f"consensus_coordinator.{self.node_id}")
        
        # Setup transport and consensus node
        self.transport = InMemoryTransport(self.node_id)
        self.consensus_node = PaxosConsensusNode(
            node_id=self.node_id,
            transport=self.transport,
            cluster_nodes=cluster_config.get("nodes", [self.node_id])
        )
        
        # Agent coordination state
        self.agent_assignments = {}
        self.task_queue = asyncio.Queue()
        self.coordination_policies = {}
        
    async def start(self) -> None:
        """Start the distributed coordinator."""
        self.logger.info("Starting distributed consensus coordinator")
        await self.consensus_node.start()
    
    async def stop(self) -> None:
        """Stop the distributed coordinator."""
        self.logger.info("Stopping distributed consensus coordinator")
        await self.consensus_node.stop()
    
    async def coordinate_agent_assignment(
        self,
        task_id: str,
        available_agents: List[str],
        assignment_criteria: Dict[str, Any]
    ) -> str:
        """Coordinate agent assignment using distributed consensus."""
        try:
            assignment_proposal = {
                "task_id": task_id,
                "available_agents": available_agents,
                "assignment_criteria": assignment_criteria,
                "proposed_agent": self._select_best_agent(available_agents, assignment_criteria),
                "proposer": self.node_id,
                "timestamp": datetime.now().isoformat()
            }
            
            # Propose assignment through consensus
            decision = await self.consensus_node.propose_value(
                value=assignment_proposal,
                decision_id=f"assignment_{task_id}"
            )
            
            assigned_agent = decision.value["proposed_agent"]
            self.agent_assignments[task_id] = assigned_agent
            
            self.logger.info(f"Consensus reached: Task {task_id} assigned to agent {assigned_agent}")
            return assigned_agent
            
        except Exception as e:
            self.logger.error(f"Agent assignment coordination failed: {e}")
            raise DataProcessingException(f"Agent assignment coordination failed: {e}")
    
    async def coordinate_resource_allocation(
        self,
        resource_type: str,
        requested_amount: float,
        requesting_agent: str,
        priority: int = 1
    ) -> bool:
        """Coordinate resource allocation using consensus."""
        try:
            allocation_proposal = {
                "resource_type": resource_type,
                "requested_amount": requested_amount,
                "requesting_agent": requesting_agent,
                "priority": priority,
                "current_allocations": self._get_current_allocations(resource_type),
                "available_capacity": self._get_available_capacity(resource_type),
                "timestamp": datetime.now().isoformat()
            }
            
            # Check if allocation is feasible
            if not self._is_allocation_feasible(allocation_proposal):
                self.logger.warning(f"Resource allocation not feasible: {allocation_proposal}")
                return False
            
            # Propose allocation through consensus
            decision = await self.consensus_node.propose_value(
                value=allocation_proposal,
                decision_id=f"resource_{resource_type}_{requesting_agent}_{int(time.time())}"
            )
            
            # Apply the allocation
            await self._apply_resource_allocation(decision.value)
            
            self.logger.info(f"Resource allocation consensus: {requested_amount} {resource_type} to {requesting_agent}")
            return True
            
        except Exception as e:
            self.logger.error(f"Resource allocation coordination failed: {e}")
            return False
    
    async def coordinate_policy_update(
        self,
        policy_name: str,
        new_policy: Dict[str, Any]
    ) -> bool:
        """Coordinate policy updates using consensus."""
        try:
            policy_proposal = {
                "policy_name": policy_name,
                "new_policy": new_policy,
                "previous_policy": self.coordination_policies.get(policy_name),
                "proposer": self.node_id,
                "timestamp": datetime.now().isoformat()
            }
            
            # Propose policy update through consensus
            decision = await self.consensus_node.propose_value(
                value=policy_proposal,
                decision_id=f"policy_{policy_name}_{int(time.time())}"
            )
            
            # Apply the policy update
            self.coordination_policies[policy_name] = decision.value["new_policy"]
            
            self.logger.info(f"Policy update consensus reached for {policy_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Policy update coordination failed: {e}")
            return False
    
    def _select_best_agent(
        self,
        available_agents: List[str],
        criteria: Dict[str, Any]
    ) -> str:
        """Select best agent based on criteria."""
        # Simple selection based on criteria
        # In a real implementation, this would use sophisticated agent capability matching
        
        if not available_agents:
            raise ValueError("No available agents")
        
        # For now, use round-robin with preference for less loaded agents
        # This would be replaced with actual agent capability and load assessment
        
        return available_agents[0]  # Simplified selection
    
    def _get_current_allocations(self, resource_type: str) -> Dict[str, float]:
        """Get current resource allocations."""
        # This would query the actual resource management system
        return {}
    
    def _get_available_capacity(self, resource_type: str) -> float:
        """Get available capacity for resource type."""
        # This would query the actual resource management system
        return 100.0  # Simplified
    
    def _is_allocation_feasible(self, proposal: Dict[str, Any]) -> bool:
        """Check if resource allocation is feasible."""
        requested = proposal["requested_amount"]
        available = proposal["available_capacity"]
        current_total = sum(proposal["current_allocations"].values())
        
        return (current_total + requested) <= available
    
    async def _apply_resource_allocation(self, allocation: Dict[str, Any]) -> None:
        """Apply resource allocation."""
        # This would interface with the actual resource management system
        self.logger.debug(f"Applied resource allocation: {allocation}")
    
    def get_coordination_status(self) -> Dict[str, Any]:
        """Get coordination status."""
        return {
            "coordinator_id": self.node_id,
            "consensus_status": self.consensus_node.get_status(),
            "active_assignments": len(self.agent_assignments),
            "coordination_policies": len(self.coordination_policies),
            "cluster_health": self._assess_cluster_health()
        }
    
    def _assess_cluster_health(self) -> Dict[str, Any]:
        """Assess cluster health."""
        status = self.consensus_node.get_status()
        
        return {
            "has_leader": status["current_leader"] is not None,
            "cluster_size": status["cluster_size"],
            "quorum_available": True,  # Simplified
            "consensus_success_rate": self._calculate_consensus_success_rate()
        }
    
    def _calculate_consensus_success_rate(self) -> float:
        """Calculate consensus success rate."""
        metrics = self.consensus_node.metrics
        total_attempts = metrics["consensus_rounds"] + metrics["failed_proposals"]
        
        if total_attempts == 0:
            return 1.0
        
        return metrics["consensus_rounds"] / total_attempts


# Integration with agent system
async def integrate_distributed_consensus(
    agent_orchestrator,
    cluster_config: Dict[str, Any]
) -> DistributedConsensusCoordinator:
    """Integrate distributed consensus with agent orchestrator."""
    try:
        coordinator = DistributedConsensusCoordinator(cluster_config)
        await coordinator.start()
        
        # This would hook into the existing agent orchestration system
        # to use consensus-based coordination for critical decisions
        
        get_logger("consensus_integration").info("Distributed consensus integrated with agent orchestrator")
        return coordinator
        
    except Exception as e:
        get_logger("consensus_integration").error(f"Consensus integration failed: {e}")
        raise DataProcessingException(f"Consensus integration failed: {e}")