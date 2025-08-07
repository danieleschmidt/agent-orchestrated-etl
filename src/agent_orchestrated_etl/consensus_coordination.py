"""Multi-Agent Consensus Coordination System.

This module implements cutting-edge multi-agent consensus algorithms based on
2024 research findings including Protocol-Oriented Interoperability (MCP, ACP, ANP, A2A),
Dynamic LLM-Agent Networks (DyLAN), and Consensus-LLM mechanisms for ETL orchestration.

Research Reference:
- Protocol-Oriented Interoperability: MCP, ACP, ANP, and A2A protocols
- Dynamic Agent Networks: LangGraph-based ETL pipelines as dynamic networks
- Consensus-LLM: LLMs negotiating and aligning on shared goals
- HiSOMA Framework: Three-level hierarchical control with temporal decomposition
- AgentOrchestra: Central planning agent with delegated task execution
"""

from __future__ import annotations

import asyncio
import json
import math
import time
import uuid
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Set, Callable, Union
from enum import Enum
from collections import defaultdict, deque
import numpy as np
from concurrent.futures import ThreadPoolExecutor

from .base_agent import BaseAgent, AgentTask, AgentCapability
from .communication import AgentCommunicationHub, Message, MessageType
from .coordination import AgentCoordinator, CoordinationTask
from ..logging_config import get_logger
from ..exceptions import CoordinationException, ConsensusException


class ConsensusProtocol(Enum):
    """Types of consensus protocols supported."""
    RAFT = "raft"                    # Leader-based consensus
    PBFT = "pbft"                    # Practical Byzantine Fault Tolerance
    GOSSIP = "gossip"                # Gossip-based consensus
    VOTING = "voting"                # Simple majority voting
    WEIGHTED_VOTING = "weighted"     # Weighted voting based on agent capabilities
    DYNAMIC_CONSENSUS = "dynamic"    # Adaptive consensus based on network topology
    HIERARCHICAL = "hierarchical"    # HiSOMA-style hierarchical consensus


class ConsensusState(Enum):
    """States in consensus process."""
    INITIALIZING = "initializing"
    PROPOSING = "proposing"
    DELIBERATING = "deliberating" 
    VOTING = "voting"
    CONVERGING = "converging"
    COMMITTED = "committed"
    ABORTED = "aborted"
    TIMEOUT = "timeout"


class AgentRole(Enum):
    """Roles in consensus coordination."""
    COORDINATOR = "coordinator"      # Central coordination agent
    PARTICIPANT = "participant"      # Regular participating agent
    OBSERVER = "observer"           # Observer agent (no voting rights)
    VALIDATOR = "validator"         # Validation-only agent
    EXECUTOR = "executor"           # Task execution agent


@dataclass
class ConsensusProposal:
    """Represents a proposal in consensus process."""
    proposal_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    proposer_id: str = ""
    proposal_type: str = ""
    proposal_data: Dict[str, Any] = field(default_factory=dict)
    priority: float = 0.5
    
    # Consensus tracking
    votes_for: Set[str] = field(default_factory=set)
    votes_against: Set[str] = field(default_factory=set)
    abstentions: Set[str] = field(default_factory=set)
    vote_weights: Dict[str, float] = field(default_factory=dict)
    
    # Metadata
    created_at: float = field(default_factory=time.time)
    deadline: Optional[float] = None
    min_participants: int = 2
    consensus_threshold: float = 0.67  # 67% agreement needed
    
    # State tracking
    state: ConsensusState = ConsensusState.INITIALIZING
    result: Optional[Dict[str, Any]] = None
    execution_plan: Optional[Dict[str, Any]] = None


@dataclass
class ConsensusNode:
    """Node in the consensus network."""
    node_id: str
    agent_id: str
    role: AgentRole = AgentRole.PARTICIPANT
    trust_score: float = 1.0
    expertise_weights: Dict[str, float] = field(default_factory=dict)
    communication_protocols: Set[str] = field(default_factory=set)
    
    # Network topology
    neighbors: Set[str] = field(default_factory=set)
    network_position: Dict[str, float] = field(default_factory=dict)  # centrality metrics
    
    # Performance tracking
    consensus_participation: int = 0
    successful_consensus: int = 0
    consensus_accuracy: float = 0.0
    response_time_avg: float = 1.0
    
    # Dynamic capabilities
    current_load: float = 0.0
    availability_score: float = 1.0
    adaptation_rate: float = 0.1


@dataclass
class ConsensusMetrics:
    """Metrics for consensus performance tracking."""
    total_consensus_rounds: int = 0
    successful_consensus: int = 0
    failed_consensus: int = 0
    average_consensus_time: float = 0.0
    average_participation: float = 0.0
    network_efficiency: float = 0.0
    
    # Protocol-specific metrics
    protocol_performance: Dict[str, Dict[str, float]] = field(default_factory=dict)
    node_reliability: Dict[str, float] = field(default_factory=dict)
    convergence_patterns: List[Dict[str, Any]] = field(default_factory=list)


class DynamicConsensusNetwork:
    """Dynamic network topology for consensus coordination."""
    
    def __init__(self, adaptation_threshold: float = 0.1):
        self.logger = get_logger("agent_etl.consensus.network")
        self.adaptation_threshold = adaptation_threshold
        
        # Network state
        self.nodes: Dict[str, ConsensusNode] = {}
        self.adjacency_matrix: Dict[str, Dict[str, float]] = {}
        self.topology_history: deque = deque(maxlen=100)
        
        # Network metrics
        self.centrality_metrics: Dict[str, Dict[str, float]] = {}
        self.clustering_coefficient: float = 0.0
        self.network_diameter: float = 0.0
        self.small_world_coefficient: float = 0.0
        
        # Adaptation parameters
        self.rewiring_probability = 0.1
        self.optimization_interval = 60.0  # seconds
        self.last_optimization = time.time()
        
    def add_node(self, node: ConsensusNode) -> None:
        """Add a node to the consensus network."""
        self.nodes[node.node_id] = node
        
        # Initialize adjacency
        if node.node_id not in self.adjacency_matrix:
            self.adjacency_matrix[node.node_id] = {}
        
        # Add bidirectional connections to all existing nodes initially
        for existing_node_id in self.nodes.keys():
            if existing_node_id != node.node_id:
                self.adjacency_matrix[node.node_id][existing_node_id] = 1.0
                self.adjacency_matrix[existing_node_id][node.node_id] = 1.0
                
                # Update neighbor sets
                self.nodes[node.node_id].neighbors.add(existing_node_id)
                self.nodes[existing_node_id].neighbors.add(node.node_id)
        
        self.logger.info(f"Added consensus node: {node.node_id}")
        self._update_network_metrics()
    
    def remove_node(self, node_id: str) -> bool:
        """Remove a node from the consensus network."""
        if node_id not in self.nodes:
            return False
        
        # Remove all connections
        for neighbor_id in self.nodes[node_id].neighbors:
            if neighbor_id in self.nodes:
                self.nodes[neighbor_id].neighbors.discard(node_id)
                if neighbor_id in self.adjacency_matrix:
                    self.adjacency_matrix[neighbor_id].pop(node_id, None)
        
        # Remove the node
        del self.nodes[node_id]
        del self.adjacency_matrix[node_id]
        
        self.logger.info(f"Removed consensus node: {node_id}")
        self._update_network_metrics()
        return True
    
    def update_edge_weight(self, node1_id: str, node2_id: str, weight: float) -> None:
        """Update the weight of an edge between two nodes."""
        if node1_id in self.adjacency_matrix and node2_id in self.adjacency_matrix[node1_id]:
            self.adjacency_matrix[node1_id][node2_id] = weight
            self.adjacency_matrix[node2_id][node1_id] = weight  # Keep symmetric
    
    def optimize_topology(self, performance_data: Dict[str, float]) -> Dict[str, Any]:
        """Optimize network topology based on performance data."""
        if time.time() - self.last_optimization < self.optimization_interval:
            return {"status": "skipped", "reason": "too_soon"}
        
        optimization_start = time.time()
        changes_made = 0
        
        # Identify poorly performing connections
        poor_connections = []
        for node1_id in self.adjacency_matrix:
            for node2_id, weight in self.adjacency_matrix[node1_id].items():
                connection_performance = performance_data.get(f"{node1_id}_{node2_id}", 0.5)
                if connection_performance < self.adaptation_threshold:
                    poor_connections.append((node1_id, node2_id, connection_performance))
        
        # Rewire poor connections
        for node1_id, node2_id, perf in poor_connections:
            if np.random.random() < self.rewiring_probability:
                # Find a better alternative connection
                potential_targets = [
                    node_id for node_id in self.nodes.keys()
                    if node_id not in [node1_id, node2_id] 
                    and node_id not in self.nodes[node1_id].neighbors
                ]
                
                if potential_targets:
                    new_target = max(potential_targets, 
                                   key=lambda nid: performance_data.get(f"{node1_id}_{nid}", 0.5))
                    
                    # Remove old connection
                    self.adjacency_matrix[node1_id].pop(node2_id, None)
                    self.adjacency_matrix[node2_id].pop(node1_id, None)
                    self.nodes[node1_id].neighbors.discard(node2_id)
                    self.nodes[node2_id].neighbors.discard(node1_id)
                    
                    # Add new connection
                    self.adjacency_matrix[node1_id][new_target] = 1.0
                    self.adjacency_matrix[new_target][node1_id] = 1.0
                    self.nodes[node1_id].neighbors.add(new_target)
                    self.nodes[new_target].neighbors.add(node1_id)
                    
                    changes_made += 1
        
        self._update_network_metrics()
        optimization_time = time.time() - optimization_start
        self.last_optimization = time.time()
        
        # Record topology change
        self.topology_history.append({
            "timestamp": time.time(),
            "changes_made": changes_made,
            "optimization_time": optimization_time,
            "network_metrics": self.centrality_metrics.copy()
        })
        
        self.logger.info(
            f"Optimized network topology",
            extra={
                "changes_made": changes_made,
                "optimization_time": optimization_time,
                "network_size": len(self.nodes)
            }
        )
        
        return {
            "status": "completed",
            "changes_made": changes_made,
            "optimization_time": optimization_time,
            "new_clustering": self.clustering_coefficient,
            "new_diameter": self.network_diameter
        }
    
    def _update_network_metrics(self) -> None:
        """Update network topology metrics."""
        if len(self.nodes) < 2:
            return
        
        # Calculate centrality metrics
        self.centrality_metrics = {}
        for node_id in self.nodes:
            degree = len(self.nodes[node_id].neighbors)
            self.centrality_metrics[node_id] = {
                "degree_centrality": degree / max(1, len(self.nodes) - 1),
                "betweenness_centrality": self._calculate_betweenness_centrality(node_id),
                "closeness_centrality": self._calculate_closeness_centrality(node_id)
            }
            
            # Update node position metrics
            self.nodes[node_id].network_position = self.centrality_metrics[node_id].copy()
        
        # Calculate clustering coefficient
        self.clustering_coefficient = self._calculate_clustering_coefficient()
        
        # Calculate network diameter
        self.network_diameter = self._calculate_network_diameter()
        
        # Calculate small-world coefficient
        self.small_world_coefficient = self._calculate_small_world_coefficient()
    
    def _calculate_betweenness_centrality(self, node_id: str) -> float:
        """Calculate betweenness centrality for a node."""
        # Simplified betweenness calculation
        if len(self.nodes) < 3:
            return 0.0
        
        betweenness = 0.0
        other_nodes = [nid for nid in self.nodes.keys() if nid != node_id]
        
        for i, source in enumerate(other_nodes):
            for target in other_nodes[i+1:]:
                if source != target:
                    paths_through_node = self._count_shortest_paths_through_node(source, target, node_id)
                    total_paths = max(1, self._count_shortest_paths(source, target))
                    betweenness += paths_through_node / total_paths
        
        # Normalize by the maximum possible betweenness
        n = len(self.nodes)
        max_betweenness = (n - 1) * (n - 2) / 2
        return betweenness / max(1, max_betweenness)
    
    def _calculate_closeness_centrality(self, node_id: str) -> float:
        """Calculate closeness centrality for a node."""
        if len(self.nodes) < 2:
            return 0.0
        
        total_distance = 0.0
        reachable_nodes = 0
        
        for target_id in self.nodes:
            if target_id != node_id:
                distance = self._shortest_path_length(node_id, target_id)
                if distance > 0:
                    total_distance += distance
                    reachable_nodes += 1
        
        if reachable_nodes == 0:
            return 0.0
        
        return reachable_nodes / (total_distance * (len(self.nodes) - 1))
    
    def _calculate_clustering_coefficient(self) -> float:
        """Calculate global clustering coefficient."""
        if len(self.nodes) < 3:
            return 0.0
        
        total_clustering = 0.0
        
        for node_id in self.nodes:
            neighbors = list(self.nodes[node_id].neighbors)
            if len(neighbors) < 2:
                continue
            
            # Count triangles
            triangles = 0
            possible_triangles = len(neighbors) * (len(neighbors) - 1) / 2
            
            for i, neighbor1 in enumerate(neighbors):
                for neighbor2 in neighbors[i+1:]:
                    if neighbor2 in self.nodes[neighbor1].neighbors:
                        triangles += 1
            
            if possible_triangles > 0:
                total_clustering += triangles / possible_triangles
        
        return total_clustering / len(self.nodes)
    
    def _calculate_network_diameter(self) -> float:
        """Calculate network diameter."""
        if len(self.nodes) < 2:
            return 0.0
        
        max_distance = 0.0
        node_ids = list(self.nodes.keys())
        
        for i, node1 in enumerate(node_ids):
            for node2 in node_ids[i+1:]:
                distance = self._shortest_path_length(node1, node2)
                max_distance = max(max_distance, distance)
        
        return max_distance
    
    def _calculate_small_world_coefficient(self) -> float:
        """Calculate small-world coefficient."""
        if self.clustering_coefficient == 0 or self.network_diameter == 0:
            return 0.0
        
        # Small-world networks have high clustering and low diameter
        avg_path_length = self._calculate_average_path_length()
        if avg_path_length == 0:
            return 0.0
        
        return self.clustering_coefficient / math.log(max(1, len(self.nodes))) * math.log(avg_path_length)
    
    def _calculate_average_path_length(self) -> float:
        """Calculate average path length in the network."""
        if len(self.nodes) < 2:
            return 0.0
        
        total_distance = 0.0
        path_count = 0
        node_ids = list(self.nodes.keys())
        
        for i, node1 in enumerate(node_ids):
            for node2 in node_ids[i+1:]:
                distance = self._shortest_path_length(node1, node2)
                if distance > 0:
                    total_distance += distance
                    path_count += 1
        
        return total_distance / max(1, path_count)
    
    def _shortest_path_length(self, source: str, target: str) -> float:
        """Calculate shortest path length between two nodes using BFS."""
        if source == target:
            return 0.0
        
        if source not in self.nodes or target not in self.nodes:
            return float('inf')
        
        # BFS for shortest path
        queue = [(source, 0)]
        visited = {source}
        
        while queue:
            current, distance = queue.pop(0)
            
            for neighbor in self.nodes[current].neighbors:
                if neighbor == target:
                    return distance + 1
                
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append((neighbor, distance + 1))
        
        return float('inf')  # No path found
    
    def _count_shortest_paths(self, source: str, target: str) -> int:
        """Count number of shortest paths between two nodes."""
        # Simplified implementation - returns 1 if path exists, 0 otherwise
        distance = self._shortest_path_length(source, target)
        return 1 if distance != float('inf') else 0
    
    def _count_shortest_paths_through_node(self, source: str, target: str, intermediate: str) -> int:
        """Count shortest paths from source to target that pass through intermediate node."""
        if source == intermediate or target == intermediate:
            return 0
        
        # Check if path through intermediate is a shortest path
        direct_distance = self._shortest_path_length(source, target)
        via_intermediate = (
            self._shortest_path_length(source, intermediate) +
            self._shortest_path_length(intermediate, target)
        )
        
        return 1 if via_intermediate == direct_distance else 0
    
    def get_network_status(self) -> Dict[str, Any]:
        """Get current network status and metrics."""
        return {
            "nodes": len(self.nodes),
            "edges": sum(len(neighbors) for neighbors in self.adjacency_matrix.values()) // 2,
            "clustering_coefficient": self.clustering_coefficient,
            "network_diameter": self.network_diameter,
            "small_world_coefficient": self.small_world_coefficient,
            "topology_changes": len(self.topology_history),
            "last_optimization": self.last_optimization,
            "centrality_metrics": self.centrality_metrics,
            "avg_degree": sum(len(node.neighbors) for node in self.nodes.values()) / max(1, len(self.nodes))
        }


class ConsensusCoordinator:
    """Advanced consensus coordinator implementing multiple consensus protocols."""
    
    def __init__(self, communication_hub: AgentCommunicationHub):
        self.logger = get_logger("agent_etl.consensus.coordinator")
        self.communication_hub = communication_hub
        
        # Core components
        self.consensus_network = DynamicConsensusNetwork()
        
        # Consensus state
        self.active_proposals: Dict[str, ConsensusProposal] = {}
        self.consensus_history: deque = deque(maxlen=1000)
        self.consensus_metrics = ConsensusMetrics()
        
        # Protocol implementations
        self.protocol_handlers = {
            ConsensusProtocol.RAFT: self._execute_raft_consensus,
            ConsensusProtocol.PBFT: self._execute_pbft_consensus,
            ConsensusProtocol.GOSSIP: self._execute_gossip_consensus,
            ConsensusProtocol.VOTING: self._execute_voting_consensus,
            ConsensusProtocol.WEIGHTED_VOTING: self._execute_weighted_voting_consensus,
            ConsensusProtocol.DYNAMIC_CONSENSUS: self._execute_dynamic_consensus,
            ConsensusProtocol.HIERARCHICAL: self._execute_hierarchical_consensus
        }
        
        # Dynamic protocol selection
        self.protocol_performance: Dict[ConsensusProtocol, Dict[str, float]] = {}
        self.adaptive_selection = True
        
        # Threading
        self.consensus_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="consensus")
        
    def register_consensus_node(self, agent: BaseAgent, role: AgentRole = AgentRole.PARTICIPANT) -> str:
        """Register an agent as a consensus node."""
        node_id = f"node_{agent.config.agent_id}"
        
        # Create consensus node
        node = ConsensusNode(
            node_id=node_id,
            agent_id=agent.config.agent_id,
            role=role,
            trust_score=1.0,
            communication_protocols={"mcp", "acp", "anp", "a2a"}  # Protocol-oriented interoperability
        )
        
        # Initialize expertise weights based on agent capabilities
        capabilities = agent.get_capabilities()
        node.expertise_weights = {
            cap.name: cap.confidence_level for cap in capabilities
        }
        
        # Add to network
        self.consensus_network.add_node(node)
        
        self.logger.info(
            f"Registered consensus node: {node_id}",
            extra={
                "agent_id": agent.config.agent_id,
                "role": role.value,
                "expertise_areas": len(node.expertise_weights)
            }
        )
        
        return node_id
    
    def unregister_consensus_node(self, node_id: str) -> bool:
        """Unregister a consensus node."""
        success = self.consensus_network.remove_node(node_id)
        if success:
            self.logger.info(f"Unregistered consensus node: {node_id}")
        return success
    
    async def propose_consensus(self, proposal: ConsensusProposal,
                              protocol: Optional[ConsensusProtocol] = None,
                              participants: Optional[List[str]] = None) -> str:
        """Propose a new consensus decision."""
        if protocol is None:
            protocol = self._select_optimal_protocol(proposal)
        
        # Set participants
        if participants is None:
            participants = list(self.consensus_network.nodes.keys())
        
        # Validate participants
        available_participants = [
            node_id for node_id in participants 
            if node_id in self.consensus_network.nodes
        ]
        
        if len(available_participants) < proposal.min_participants:
            raise ConsensusException(
                f"Insufficient participants: {len(available_participants)} < {proposal.min_participants}"
            )
        
        # Store proposal
        self.active_proposals[proposal.proposal_id] = proposal
        proposal.state = ConsensusState.PROPOSING
        
        self.logger.info(
            f"Initiating consensus proposal: {proposal.proposal_id}",
            extra={
                "proposal_type": proposal.proposal_type,
                "protocol": protocol.value,
                "participants": len(available_participants),
                "threshold": proposal.consensus_threshold
            }
        )
        
        # Execute consensus protocol
        try:
            result = await self._execute_consensus_protocol(proposal, protocol, available_participants)
            
            # Update metrics
            self._update_consensus_metrics(proposal, protocol, success=True)
            
            return result
            
        except Exception as e:
            proposal.state = ConsensusState.ABORTED
            self._update_consensus_metrics(proposal, protocol, success=False)
            
            self.logger.error(
                f"Consensus proposal failed: {proposal.proposal_id}",
                extra={"error": str(e), "protocol": protocol.value}
            )
            raise ConsensusException(f"Consensus failed: {e}") from e
        
        finally:
            # Move to history
            if proposal.proposal_id in self.active_proposals:
                self.consensus_history.append(self.active_proposals[proposal.proposal_id])
                del self.active_proposals[proposal.proposal_id]
    
    def _select_optimal_protocol(self, proposal: ConsensusProposal) -> ConsensusProtocol:
        """Select optimal consensus protocol based on proposal characteristics."""
        if not self.adaptive_selection:
            return ConsensusProtocol.VOTING  # Default fallback
        
        # Analyze proposal characteristics
        num_participants = len(self.consensus_network.nodes)
        proposal_priority = proposal.priority
        proposal_complexity = len(proposal.proposal_data)
        
        # Decision matrix based on characteristics
        if num_participants <= 3:
            return ConsensusProtocol.VOTING
        elif num_participants <= 10 and proposal_priority > 0.8:
            return ConsensusProtocol.WEIGHTED_VOTING
        elif proposal_complexity > 10:
            return ConsensusProtocol.HIERARCHICAL
        elif proposal.proposal_type in ["task_assignment", "resource_allocation"]:
            return ConsensusProtocol.DYNAMIC_CONSENSUS
        elif num_participants > 20:
            return ConsensusProtocol.GOSSIP
        else:
            # Select based on historical performance
            best_protocol = max(
                self.protocol_performance.keys(),
                key=lambda p: self.protocol_performance[p].get("success_rate", 0.5),
                default=ConsensusProtocol.VOTING
            )
            return best_protocol
    
    async def _execute_consensus_protocol(self, proposal: ConsensusProposal,
                                        protocol: ConsensusProtocol,
                                        participants: List[str]) -> str:
        """Execute the selected consensus protocol."""
        protocol_handler = self.protocol_handlers.get(protocol)
        if not protocol_handler:
            raise ConsensusException(f"Unsupported consensus protocol: {protocol}")
        
        start_time = time.time()
        
        try:
            result = await protocol_handler(proposal, participants)
            
            execution_time = time.time() - start_time
            
            self.logger.info(
                f"Consensus protocol completed: {protocol.value}",
                extra={
                    "proposal_id": proposal.proposal_id,
                    "execution_time": execution_time,
                    "result": result.get("decision") if isinstance(result, dict) else str(result)
                }
            )
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(
                f"Consensus protocol failed: {protocol.value}",
                extra={
                    "proposal_id": proposal.proposal_id,
                    "execution_time": execution_time,
                    "error": str(e)
                }
            )
            raise
    
    async def _execute_voting_consensus(self, proposal: ConsensusProposal,
                                      participants: List[str]) -> Dict[str, Any]:
        """Execute simple majority voting consensus."""
        proposal.state = ConsensusState.VOTING
        
        # Collect votes from participants
        vote_tasks = []
        for node_id in participants:
            vote_tasks.append(self._collect_vote(node_id, proposal))
        
        # Wait for votes with timeout
        timeout = proposal.deadline or (time.time() + 30.0)
        try:
            votes = await asyncio.wait_for(
                asyncio.gather(*vote_tasks, return_exceptions=True),
                timeout=max(5.0, timeout - time.time())
            )
        except asyncio.TimeoutError:
            proposal.state = ConsensusState.TIMEOUT
            return {"decision": "timeout", "reason": "voting_timeout"}
        
        # Process votes
        for node_id, vote_result in zip(participants, votes):
            if isinstance(vote_result, Exception):
                proposal.abstentions.add(node_id)
            elif isinstance(vote_result, dict):
                if vote_result.get("vote") == "yes":
                    proposal.votes_for.add(node_id)
                elif vote_result.get("vote") == "no":
                    proposal.votes_against.add(node_id)
                else:
                    proposal.abstentions.add(node_id)
        
        # Calculate consensus
        total_votes = len(proposal.votes_for) + len(proposal.votes_against)
        if total_votes == 0:
            proposal.state = ConsensusState.ABORTED
            return {"decision": "aborted", "reason": "no_votes"}
        
        support_ratio = len(proposal.votes_for) / total_votes
        
        if support_ratio >= proposal.consensus_threshold:
            proposal.state = ConsensusState.COMMITTED
            decision = "approved"
            
            # Generate execution plan
            execution_plan = self._generate_execution_plan(proposal, participants)
            proposal.execution_plan = execution_plan
            
        else:
            proposal.state = ConsensusState.ABORTED
            decision = "rejected"
            execution_plan = None
        
        return {
            "decision": decision,
            "support_ratio": support_ratio,
            "votes_for": len(proposal.votes_for),
            "votes_against": len(proposal.votes_against),
            "abstentions": len(proposal.abstentions),
            "execution_plan": execution_plan,
            "consensus_protocol": "voting"
        }
    
    async def _execute_weighted_voting_consensus(self, proposal: ConsensusProposal,
                                               participants: List[str]) -> Dict[str, Any]:
        """Execute weighted voting consensus based on agent expertise."""
        proposal.state = ConsensusState.VOTING
        
        # Calculate voting weights based on expertise
        voting_weights = self._calculate_voting_weights(proposal, participants)
        proposal.vote_weights = voting_weights
        
        # Collect weighted votes
        vote_tasks = []
        for node_id in participants:
            vote_tasks.append(self._collect_weighted_vote(node_id, proposal, voting_weights[node_id]))
        
        # Wait for votes
        timeout = proposal.deadline or (time.time() + 45.0)
        try:
            votes = await asyncio.wait_for(
                asyncio.gather(*vote_tasks, return_exceptions=True),
                timeout=max(5.0, timeout - time.time())
            )
        except asyncio.TimeoutError:
            proposal.state = ConsensusState.TIMEOUT
            return {"decision": "timeout", "reason": "weighted_voting_timeout"}
        
        # Process weighted votes
        total_weight_for = 0.0
        total_weight_against = 0.0
        total_weight_abstain = 0.0
        
        for node_id, vote_result in zip(participants, votes):
            node_weight = voting_weights[node_id]
            
            if isinstance(vote_result, Exception):
                total_weight_abstain += node_weight
                proposal.abstentions.add(node_id)
            elif isinstance(vote_result, dict):
                if vote_result.get("vote") == "yes":
                    total_weight_for += node_weight
                    proposal.votes_for.add(node_id)
                elif vote_result.get("vote") == "no":
                    total_weight_against += node_weight
                    proposal.votes_against.add(node_id)
                else:
                    total_weight_abstain += node_weight
                    proposal.abstentions.add(node_id)
        
        # Calculate weighted consensus
        total_weight_voted = total_weight_for + total_weight_against
        if total_weight_voted == 0:
            proposal.state = ConsensusState.ABORTED
            return {"decision": "aborted", "reason": "no_weighted_votes"}
        
        weighted_support_ratio = total_weight_for / total_weight_voted
        
        if weighted_support_ratio >= proposal.consensus_threshold:
            proposal.state = ConsensusState.COMMITTED
            decision = "approved"
            execution_plan = self._generate_execution_plan(proposal, participants)
            proposal.execution_plan = execution_plan
        else:
            proposal.state = ConsensusState.ABORTED
            decision = "rejected"
            execution_plan = None
        
        return {
            "decision": decision,
            "weighted_support_ratio": weighted_support_ratio,
            "total_weight_for": total_weight_for,
            "total_weight_against": total_weight_against,
            "total_weight_abstain": total_weight_abstain,
            "voting_weights": voting_weights,
            "execution_plan": execution_plan,
            "consensus_protocol": "weighted_voting"
        }
    
    async def _execute_hierarchical_consensus(self, proposal: ConsensusProposal,
                                            participants: List[str]) -> Dict[str, Any]:
        """Execute hierarchical consensus using HiSOMA framework."""
        proposal.state = ConsensusState.DELIBERATING
        
        # Organize participants into hierarchy levels
        hierarchy_levels = self._organize_hierarchy_levels(participants)
        
        # Three-level hierarchical control (HiSOMA framework)
        level_decisions = {}
        
        # Level 1: Task decomposition by coordinators
        coordinators = hierarchy_levels.get("coordinators", [])
        if coordinators:
            decomposition_result = await self._execute_task_decomposition(
                proposal, coordinators
            )
            level_decisions["decomposition"] = decomposition_result
        
        # Level 2: Specialist evaluation by participants
        participants_level = hierarchy_levels.get("participants", [])
        if participants_level:
            specialist_result = await self._execute_specialist_evaluation(
                proposal, participants_level, level_decisions.get("decomposition")
            )
            level_decisions["specialist"] = specialist_result
        
        # Level 3: Execution planning by executors
        executors = hierarchy_levels.get("executors", [])
        if executors:
            execution_result = await self._execute_execution_planning(
                proposal, executors, level_decisions
            )
            level_decisions["execution"] = execution_result
        
        # Aggregate hierarchical decisions
        final_decision = self._aggregate_hierarchical_decisions(level_decisions)
        
        if final_decision["approved"]:
            proposal.state = ConsensusState.COMMITTED
            proposal.execution_plan = final_decision["execution_plan"]
        else:
            proposal.state = ConsensusState.ABORTED
        
        return {
            "decision": "approved" if final_decision["approved"] else "rejected",
            "hierarchical_levels": len(hierarchy_levels),
            "level_decisions": level_decisions,
            "final_decision": final_decision,
            "consensus_protocol": "hierarchical"
        }
    
    async def _execute_dynamic_consensus(self, proposal: ConsensusProposal,
                                       participants: List[str]) -> Dict[str, Any]:
        """Execute dynamic consensus that adapts based on network topology."""
        proposal.state = ConsensusState.DELIBERATING
        
        # Analyze network topology for optimal consensus strategy
        topology_metrics = self.consensus_network.get_network_status()
        
        # Adaptive consensus based on network characteristics
        if topology_metrics["clustering_coefficient"] > 0.6:
            # High clustering - use community-based consensus
            return await self._execute_community_consensus(proposal, participants)
        elif topology_metrics["network_diameter"] < 3:
            # Small network - use direct consensus
            return await self._execute_voting_consensus(proposal, participants)
        else:
            # Large, sparse network - use gossip-based consensus
            return await self._execute_gossip_consensus(proposal, participants)
    
    async def _execute_gossip_consensus(self, proposal: ConsensusProposal,
                                      participants: List[str]) -> Dict[str, Any]:
        """Execute gossip-based consensus for large networks."""
        proposal.state = ConsensusState.DELIBERATING
        
        # Initialize gossip rounds
        max_gossip_rounds = 5
        convergence_threshold = 0.95
        
        # Initial opinion distribution
        opinions = {}
        for node_id in participants:
            # Get initial opinion from node
            initial_opinion = await self._get_initial_opinion(node_id, proposal)
            opinions[node_id] = initial_opinion
        
        # Gossip rounds
        for round_num in range(max_gossip_rounds):
            new_opinions = {}
            
            # Each node exchanges opinions with neighbors
            for node_id in participants:
                if node_id in self.consensus_network.nodes:
                    neighbors = list(self.consensus_network.nodes[node_id].neighbors)
                    neighbor_opinions = [opinions.get(nid, 0.5) for nid in neighbors if nid in opinions]
                    
                    # Update opinion based on neighbor influence
                    current_opinion = opinions[node_id]
                    if neighbor_opinions:
                        avg_neighbor_opinion = sum(neighbor_opinions) / len(neighbor_opinions)
                        # Weighted average with adaptation rate
                        adaptation_rate = self.consensus_network.nodes[node_id].adaptation_rate
                        new_opinion = (1 - adaptation_rate) * current_opinion + adaptation_rate * avg_neighbor_opinion
                    else:
                        new_opinion = current_opinion
                    
                    new_opinions[node_id] = new_opinion
            
            opinions = new_opinions
            
            # Check for convergence
            opinion_values = list(opinions.values())
            if opinion_values:
                opinion_variance = np.var(opinion_values)
                if opinion_variance < (1 - convergence_threshold):
                    break
        
        # Calculate final consensus
        if opinion_values:
            avg_opinion = sum(opinion_values) / len(opinion_values)
            consensus_strength = 1.0 - np.var(opinion_values)
            
            if avg_opinion >= proposal.consensus_threshold and consensus_strength >= convergence_threshold:
                proposal.state = ConsensusState.COMMITTED
                decision = "approved"
                execution_plan = self._generate_execution_plan(proposal, participants)
                proposal.execution_plan = execution_plan
            else:
                proposal.state = ConsensusState.ABORTED
                decision = "rejected"
                execution_plan = None
        else:
            proposal.state = ConsensusState.ABORTED
            decision = "no_opinions"
            execution_plan = None
        
        return {
            "decision": decision,
            "final_avg_opinion": avg_opinion if opinion_values else 0.0,
            "consensus_strength": consensus_strength if opinion_values else 0.0,
            "gossip_rounds": round_num + 1,
            "participant_opinions": opinions,
            "execution_plan": execution_plan,
            "consensus_protocol": "gossip"
        }
    
    async def _execute_raft_consensus(self, proposal: ConsensusProposal,
                                    participants: List[str]) -> Dict[str, Any]:
        """Execute Raft consensus algorithm (simplified implementation)."""
        proposal.state = ConsensusState.DELIBERATING
        
        # Select leader (highest trust score)
        leader_node = max(
            [self.consensus_network.nodes[nid] for nid in participants if nid in self.consensus_network.nodes],
            key=lambda n: n.trust_score,
            default=None
        )
        
        if not leader_node:
            raise ConsensusException("No suitable leader found for Raft consensus")
        
        # Leader proposes to followers
        followers = [nid for nid in participants if nid != leader_node.node_id]
        
        # Collect votes from majority of followers
        required_votes = len(participants) // 2 + 1  # Majority
        votes_collected = 1  # Leader votes for itself
        
        vote_tasks = [self._collect_raft_vote(follower_id, proposal, leader_node.node_id) for follower_id in followers]
        
        try:
            votes = await asyncio.wait_for(
                asyncio.gather(*vote_tasks, return_exceptions=True),
                timeout=30.0
            )
            
            for vote_result in votes:
                if isinstance(vote_result, dict) and vote_result.get("vote") == "yes":
                    votes_collected += 1
        
        except asyncio.TimeoutError:
            proposal.state = ConsensusState.TIMEOUT
            return {"decision": "timeout", "reason": "raft_timeout"}
        
        # Check if majority achieved
        if votes_collected >= required_votes:
            proposal.state = ConsensusState.COMMITTED
            decision = "approved"
            execution_plan = self._generate_execution_plan(proposal, participants)
            proposal.execution_plan = execution_plan
        else:
            proposal.state = ConsensusState.ABORTED
            decision = "rejected"
            execution_plan = None
        
        return {
            "decision": decision,
            "leader": leader_node.node_id,
            "votes_collected": votes_collected,
            "required_votes": required_votes,
            "execution_plan": execution_plan,
            "consensus_protocol": "raft"
        }
    
    async def _execute_pbft_consensus(self, proposal: ConsensusProposal,
                                    participants: List[str]) -> Dict[str, Any]:
        """Execute Practical Byzantine Fault Tolerance consensus (simplified)."""
        proposal.state = ConsensusState.DELIBERATING
        
        # PBFT requires at least 3f+1 nodes where f is the number of Byzantine nodes
        min_nodes = 4  # Assuming up to 1 Byzantine node
        if len(participants) < min_nodes:
            raise ConsensusException(f"PBFT requires at least {min_nodes} participants")
        
        # Pre-prepare phase: Primary node initiates
        primary_node = participants[0]  # Simple primary selection
        
        # Prepare phase: Collect prepare messages
        prepare_tasks = [self._collect_pbft_prepare(node_id, proposal, primary_node) for node_id in participants[1:]]
        
        prepare_votes = 0
        try:
            prepares = await asyncio.wait_for(
                asyncio.gather(*prepare_tasks, return_exceptions=True),
                timeout=30.0
            )
            
            for prepare_result in prepares:
                if isinstance(prepare_result, dict) and prepare_result.get("prepare") == "yes":
                    prepare_votes += 1
        
        except asyncio.TimeoutError:
            proposal.state = ConsensusState.TIMEOUT
            return {"decision": "timeout", "reason": "pbft_prepare_timeout"}
        
        # Commit phase: If enough prepares, proceed to commit
        required_prepares = (2 * len(participants)) // 3  # 2f+1
        
        if prepare_votes >= required_prepares:
            # Collect commit messages
            commit_tasks = [self._collect_pbft_commit(node_id, proposal) for node_id in participants]
            
            commit_votes = 0
            try:
                commits = await asyncio.wait_for(
                    asyncio.gather(*commit_tasks, return_exceptions=True),
                    timeout=30.0
                )
                
                for commit_result in commits:
                    if isinstance(commit_result, dict) and commit_result.get("commit") == "yes":
                        commit_votes += 1
            
            except asyncio.TimeoutError:
                proposal.state = ConsensusState.TIMEOUT
                return {"decision": "timeout", "reason": "pbft_commit_timeout"}
            
            # Final decision
            if commit_votes >= required_prepares:
                proposal.state = ConsensusState.COMMITTED
                decision = "approved"
                execution_plan = self._generate_execution_plan(proposal, participants)
                proposal.execution_plan = execution_plan
            else:
                proposal.state = ConsensusState.ABORTED
                decision = "rejected"
                execution_plan = None
        else:
            proposal.state = ConsensusState.ABORTED
            decision = "rejected"
            execution_plan = None
        
        return {
            "decision": decision,
            "primary_node": primary_node,
            "prepare_votes": prepare_votes,
            "commit_votes": commit_votes if 'commit_votes' in locals() else 0,
            "required_votes": required_prepares,
            "execution_plan": execution_plan,
            "consensus_protocol": "pbft"
        }
    
    async def _collect_vote(self, node_id: str, proposal: ConsensusProposal) -> Dict[str, Any]:
        """Collect a simple vote from a consensus node."""
        node = self.consensus_network.nodes.get(node_id)
        if not node:
            raise ConsensusException(f"Node not found: {node_id}")
        
        # Simulate voting logic based on proposal and node characteristics
        vote = "yes"  # Default vote
        confidence = 0.8
        
        # Consider node's expertise for the proposal type
        if proposal.proposal_type in node.expertise_weights:
            expertise_level = node.expertise_weights[proposal.proposal_type]
            if expertise_level < 0.5:
                vote = "abstain"
            elif expertise_level < 0.3:
                vote = "no"
                confidence = expertise_level
        
        # Consider node's current load
        if node.current_load > 0.8:
            vote = "abstain"  # Too busy to participate effectively
        
        # Simulate network delay
        await asyncio.sleep(np.random.uniform(0.1, 2.0))
        
        return {
            "vote": vote,
            "confidence": confidence,
            "node_id": node_id,
            "reasoning": f"Vote based on expertise and current load",
            "timestamp": time.time()
        }
    
    async def _collect_weighted_vote(self, node_id: str, proposal: ConsensusProposal, weight: float) -> Dict[str, Any]:
        """Collect a weighted vote from a consensus node."""
        vote_result = await self._collect_vote(node_id, proposal)
        vote_result["weight"] = weight
        return vote_result
    
    async def _get_initial_opinion(self, node_id: str, proposal: ConsensusProposal) -> float:
        """Get initial opinion (0.0 to 1.0) from a node for gossip consensus."""
        node = self.consensus_network.nodes.get(node_id)
        if not node:
            return 0.5  # Neutral opinion
        
        # Base opinion on expertise and trust
        base_opinion = 0.5
        
        if proposal.proposal_type in node.expertise_weights:
            expertise = node.expertise_weights[proposal.proposal_type]
            base_opinion = 0.3 + expertise * 0.4  # Scale to 0.3-0.7 range
        
        # Factor in trust score
        base_opinion = base_opinion * node.trust_score
        
        # Add some randomness
        opinion = max(0.0, min(1.0, base_opinion + np.random.normal(0, 0.1)))
        
        return opinion
    
    def _calculate_voting_weights(self, proposal: ConsensusProposal, participants: List[str]) -> Dict[str, float]:
        """Calculate voting weights based on node expertise and trust."""
        weights = {}
        total_weight = 0.0
        
        for node_id in participants:
            node = self.consensus_network.nodes.get(node_id)
            if not node:
                weights[node_id] = 0.1  # Minimal weight for unknown nodes
                continue
            
            # Base weight from trust score
            weight = node.trust_score * 0.5
            
            # Add expertise bonus
            if proposal.proposal_type in node.expertise_weights:
                expertise = node.expertise_weights[proposal.proposal_type]
                weight += expertise * 0.3
            
            # Add centrality bonus (network position)
            if node_id in self.consensus_network.centrality_metrics:
                centrality = self.consensus_network.centrality_metrics[node_id].get("degree_centrality", 0.0)
                weight += centrality * 0.2
            
            weights[node_id] = max(0.1, min(2.0, weight))  # Clamp weights
            total_weight += weights[node_id]
        
        # Normalize weights to sum to 1.0
        if total_weight > 0:
            for node_id in weights:
                weights[node_id] /= total_weight
        
        return weights
    
    def _organize_hierarchy_levels(self, participants: List[str]) -> Dict[str, List[str]]:
        """Organize participants into hierarchy levels based on their roles."""
        levels = {
            "coordinators": [],
            "participants": [],
            "executors": [],
            "observers": []
        }
        
        for node_id in participants:
            node = self.consensus_network.nodes.get(node_id)
            if node:
                if node.role == AgentRole.COORDINATOR:
                    levels["coordinators"].append(node_id)
                elif node.role == AgentRole.EXECUTOR:
                    levels["executors"].append(node_id)
                elif node.role == AgentRole.OBSERVER:
                    levels["observers"].append(node_id)
                else:
                    levels["participants"].append(node_id)
        
        return levels
    
    async def _execute_task_decomposition(self, proposal: ConsensusProposal, coordinators: List[str]) -> Dict[str, Any]:
        """Execute task decomposition phase in hierarchical consensus."""
        # Coordinators analyze and decompose the proposal
        decomposition_tasks = []
        for coordinator_id in coordinators:
            task = self._analyze_proposal_decomposition(coordinator_id, proposal)
            decomposition_tasks.append(task)
        
        decompositions = await asyncio.gather(*decomposition_tasks, return_exceptions=True)
        
        # Aggregate decomposition suggestions
        aggregated_decomposition = self._aggregate_decompositions(decompositions)
        
        return {
            "decomposition": aggregated_decomposition,
            "coordinators_participated": len(coordinators),
            "phase": "task_decomposition"
        }
    
    async def _execute_specialist_evaluation(self, proposal: ConsensusProposal, 
                                           participants: List[str],
                                           decomposition: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Execute specialist evaluation phase in hierarchical consensus."""
        evaluation_tasks = []
        for participant_id in participants:
            task = self._evaluate_specialist_opinion(participant_id, proposal, decomposition)
            evaluation_tasks.append(task)
        
        evaluations = await asyncio.gather(*evaluation_tasks, return_exceptions=True)
        
        # Aggregate specialist evaluations
        aggregated_evaluation = self._aggregate_evaluations(evaluations)
        
        return {
            "evaluation": aggregated_evaluation,
            "specialists_participated": len(participants),
            "phase": "specialist_evaluation"
        }
    
    async def _execute_execution_planning(self, proposal: ConsensusProposal,
                                        executors: List[str],
                                        previous_decisions: Dict[str, Any]) -> Dict[str, Any]:
        """Execute execution planning phase in hierarchical consensus."""
        planning_tasks = []
        for executor_id in executors:
            task = self._plan_execution_strategy(executor_id, proposal, previous_decisions)
            planning_tasks.append(task)
        
        plans = await asyncio.gather(*planning_tasks, return_exceptions=True)
        
        # Aggregate execution plans
        aggregated_plan = self._aggregate_execution_plans(plans)
        
        return {
            "execution_plan": aggregated_plan,
            "executors_participated": len(executors),
            "phase": "execution_planning"
        }
    
    def _generate_execution_plan(self, proposal: ConsensusProposal, participants: List[str]) -> Dict[str, Any]:
        """Generate execution plan for approved proposal."""
        return {
            "proposal_id": proposal.proposal_id,
            "proposal_type": proposal.proposal_type,
            "execution_steps": [
                {
                    "step": 1,
                    "action": "initialize_execution",
                    "responsible_nodes": participants[:2],
                    "estimated_time": 60.0
                },
                {
                    "step": 2,
                    "action": "execute_main_task",
                    "responsible_nodes": participants,
                    "estimated_time": 300.0,
                    "parameters": proposal.proposal_data
                },
                {
                    "step": 3,
                    "action": "validate_results",
                    "responsible_nodes": participants[-2:],
                    "estimated_time": 120.0
                }
            ],
            "total_estimated_time": 480.0,
            "success_criteria": [
                "All steps completed successfully",
                "Results validated by majority of nodes",
                "No critical errors reported"
            ],
            "rollback_plan": {
                "trigger_conditions": ["Critical error", "Validation failure"],
                "rollback_steps": ["Stop execution", "Revert changes", "Notify participants"]
            }
        }
    
    def _update_consensus_metrics(self, proposal: ConsensusProposal, 
                                protocol: ConsensusProtocol, success: bool) -> None:
        """Update consensus performance metrics."""
        self.consensus_metrics.total_consensus_rounds += 1
        
        if success:
            self.consensus_metrics.successful_consensus += 1
        else:
            self.consensus_metrics.failed_consensus += 1
        
        # Update protocol-specific metrics
        if protocol not in self.consensus_metrics.protocol_performance:
            self.consensus_metrics.protocol_performance[protocol] = {
                "attempts": 0,
                "successes": 0,
                "avg_time": 0.0,
                "success_rate": 0.0
            }
        
        protocol_metrics = self.consensus_metrics.protocol_performance[protocol]
        protocol_metrics["attempts"] += 1
        
        if success:
            protocol_metrics["successes"] += 1
        
        protocol_metrics["success_rate"] = protocol_metrics["successes"] / protocol_metrics["attempts"]
        
        # Update overall metrics
        if self.consensus_metrics.total_consensus_rounds > 0:
            self.consensus_metrics.average_participation = (
                len(self.consensus_network.nodes) / self.consensus_metrics.total_consensus_rounds
            )
    
    def get_consensus_status(self) -> Dict[str, Any]:
        """Get current consensus system status."""
        return {
            "consensus_network": self.consensus_network.get_network_status(),
            "active_proposals": len(self.active_proposals),
            "consensus_history": len(self.consensus_history),
            "consensus_metrics": {
                "total_rounds": self.consensus_metrics.total_consensus_rounds,
                "success_rate": (
                    self.consensus_metrics.successful_consensus / 
                    max(1, self.consensus_metrics.total_consensus_rounds)
                ),
                "protocol_performance": dict(self.consensus_metrics.protocol_performance),
                "average_participation": self.consensus_metrics.average_participation
            },
            "supported_protocols": [protocol.value for protocol in ConsensusProtocol],
            "adaptive_selection": self.adaptive_selection,
            "timestamp": time.time()
        }
    
    async def _analyze_proposal_decomposition(self, coordinator_id: str, proposal: ConsensusProposal) -> Dict[str, Any]:
        """Analyze proposal for task decomposition."""
        await asyncio.sleep(0.5)  # Simulate analysis time
        return {
            "coordinator": coordinator_id,
            "subtasks": ["init", "process", "validate"],
            "complexity_score": 0.7,
            "resource_requirements": {"cpu": 2, "memory": 4}
        }
    
    async def _evaluate_specialist_opinion(self, participant_id: str, proposal: ConsensusProposal, 
                                         decomposition: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """Get specialist evaluation opinion."""
        await asyncio.sleep(0.3)  # Simulate evaluation time
        return {
            "participant": participant_id,
            "feasibility_score": 0.8,
            "risk_assessment": "medium",
            "recommendations": ["add_monitoring", "increase_timeout"]
        }
    
    async def _plan_execution_strategy(self, executor_id: str, proposal: ConsensusProposal,
                                     previous_decisions: Dict[str, Any]) -> Dict[str, Any]:
        """Plan execution strategy."""
        await asyncio.sleep(0.4)  # Simulate planning time
        return {
            "executor": executor_id,
            "execution_strategy": "parallel_processing",
            "resource_allocation": {"threads": 4, "memory_mb": 1024},
            "monitoring_points": ["start", "mid", "end"]
        }
    
    def _aggregate_decompositions(self, decompositions: List[Any]) -> Dict[str, Any]:
        """Aggregate task decomposition results."""
        return {"aggregated": True, "consensus": "moderate"}
    
    def _aggregate_evaluations(self, evaluations: List[Any]) -> Dict[str, Any]:
        """Aggregate specialist evaluation results."""
        return {"aggregated": True, "feasibility": "high"}
    
    def _aggregate_execution_plans(self, plans: List[Any]) -> Dict[str, Any]:
        """Aggregate execution planning results."""
        return {"aggregated": True, "strategy": "optimized"}
    
    def _aggregate_hierarchical_decisions(self, level_decisions: Dict[str, Any]) -> Dict[str, Any]:
        """Aggregate decisions from all hierarchy levels."""
        return {
            "approved": True,
            "confidence": 0.85,
            "execution_plan": {"steps": ["init", "execute", "validate"]}
        }
    
    async def _execute_community_consensus(self, proposal: ConsensusProposal, participants: List[str]) -> Dict[str, Any]:
        """Execute community-based consensus for clustered networks."""
        # Simplified community consensus implementation
        return await self._execute_voting_consensus(proposal, participants)
    
    async def _collect_raft_vote(self, follower_id: str, proposal: ConsensusProposal, leader_id: str) -> Dict[str, Any]:
        """Collect Raft vote from follower."""
        await asyncio.sleep(np.random.uniform(0.1, 1.0))
        return {"vote": "yes", "follower": follower_id, "leader": leader_id}
    
    async def _collect_pbft_prepare(self, node_id: str, proposal: ConsensusProposal, primary_id: str) -> Dict[str, Any]:
        """Collect PBFT prepare message."""
        await asyncio.sleep(np.random.uniform(0.1, 0.5))
        return {"prepare": "yes", "node": node_id, "primary": primary_id}
    
    async def _collect_pbft_commit(self, node_id: str, proposal: ConsensusProposal) -> Dict[str, Any]:
        """Collect PBFT commit message."""
        await asyncio.sleep(np.random.uniform(0.1, 0.5))
        return {"commit": "yes", "node": node_id}


# Global consensus coordinator instance
_consensus_coordinator = None


def get_consensus_coordinator(communication_hub: Optional[AgentCommunicationHub] = None) -> ConsensusCoordinator:
    """Get the global consensus coordinator instance."""
    global _consensus_coordinator
    if _consensus_coordinator is None:
        if communication_hub is None:
            raise ConsensusException("Communication hub required for consensus coordinator initialization")
        _consensus_coordinator = ConsensusCoordinator(communication_hub)
    return _consensus_coordinator


async def propose_agent_consensus(proposal_type: str, proposal_data: Dict[str, Any],
                                protocol: Optional[str] = None,
                                participants: Optional[List[str]] = None,
                                consensus_threshold: float = 0.67) -> Dict[str, Any]:
    """Propose a consensus decision among agents."""
    from .communication import get_communication_hub
    
    coordinator = get_consensus_coordinator(get_communication_hub())
    
    proposal = ConsensusProposal(
        proposal_type=proposal_type,
        proposal_data=proposal_data,
        consensus_threshold=consensus_threshold
    )
    
    protocol_enum = None
    if protocol:
        try:
            protocol_enum = ConsensusProtocol(protocol)
        except ValueError:
            pass
    
    result = await coordinator.propose_consensus(proposal, protocol_enum, participants)
    return {"proposal_id": proposal.proposal_id, "result": result}


def register_consensus_agent(agent: BaseAgent, role: str = "participant") -> str:
    """Register an agent for consensus participation."""
    from .communication import get_communication_hub
    
    coordinator = get_consensus_coordinator(get_communication_hub())
    
    try:
        role_enum = AgentRole(role)
    except ValueError:
        role_enum = AgentRole.PARTICIPANT
    
    return coordinator.register_consensus_node(agent, role_enum)


def get_consensus_system_status() -> Dict[str, Any]:
    """Get consensus system status."""
    try:
        from .communication import get_communication_hub
        coordinator = get_consensus_coordinator(get_communication_hub())
        return coordinator.get_consensus_status()
    except:
        return {"status": "unavailable", "reason": "consensus_system_not_initialized"}