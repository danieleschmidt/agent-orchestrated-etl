"""Real-time Pipeline Adaptation using Reinforcement Learning for Dynamic Optimization."""

from __future__ import annotations

import asyncio
import json
import pickle
import time
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from enum import Enum
import numpy as np
import random
from abc import ABC, abstractmethod

from .logging_config import get_logger
from .exceptions import DataProcessingException


class ActionType(Enum):
    """Types of actions the RL agent can take."""
    INCREASE_PARALLELISM = "increase_parallelism"
    DECREASE_PARALLELISM = "decrease_parallelism"
    CHANGE_BATCH_SIZE = "change_batch_size"
    ADJUST_MEMORY_ALLOCATION = "adjust_memory"
    SWITCH_EXECUTION_ENGINE = "switch_engine"
    ENABLE_CACHING = "enable_caching"
    DISABLE_CACHING = "disable_caching"
    REORDER_OPERATIONS = "reorder_operations"
    NO_ACTION = "no_action"


@dataclass
class PipelineState:
    """Represents the current state of a pipeline."""
    pipeline_id: str
    cpu_utilization: float
    memory_utilization: float
    throughput: float  # records per second
    latency: float  # seconds
    error_rate: float
    queue_length: int
    parallelism_level: int
    batch_size: int
    memory_allocation: float  # GB
    caching_enabled: bool
    execution_engine: str
    timestamp: datetime
    
    def to_vector(self) -> np.ndarray:
        """Convert state to feature vector for ML model."""
        return np.array([
            self.cpu_utilization,
            self.memory_utilization,
            self.throughput / 1000.0,  # Normalize
            self.latency,
            self.error_rate,
            self.queue_length / 100.0,  # Normalize
            self.parallelism_level / 10.0,  # Normalize
            self.batch_size / 1000.0,  # Normalize
            self.memory_allocation / 32.0,  # Normalize (assuming max 32GB)
            1.0 if self.caching_enabled else 0.0,
            hash(self.execution_engine) % 100 / 100.0  # Simple hash normalization
        ])


@dataclass
class Action:
    """Represents an action taken by the RL agent."""
    action_type: ActionType
    parameters: Dict[str, Any]
    expected_impact: float
    confidence: float


@dataclass
class Experience:
    """Represents an experience tuple for RL training."""
    state: PipelineState
    action: Action
    reward: float
    next_state: PipelineState
    done: bool
    timestamp: datetime


class RewardFunction:
    """Calculates rewards for RL agent based on pipeline performance."""
    
    def __init__(self, weights: Optional[Dict[str, float]] = None):
        self.weights = weights or {
            "throughput": 0.3,
            "latency": -0.25,
            "cpu_efficiency": 0.2,
            "memory_efficiency": 0.15,
            "error_rate": -0.1
        }
        self.logger = get_logger("rl.reward")
    
    def calculate_reward(
        self, 
        prev_state: PipelineState, 
        current_state: PipelineState,
        action: Action
    ) -> float:
        """Calculate reward based on state transition."""
        try:
            # Throughput improvement
            throughput_delta = (current_state.throughput - prev_state.throughput) / max(prev_state.throughput, 1.0)
            
            # Latency improvement (negative delta is good)
            latency_delta = (prev_state.latency - current_state.latency) / max(prev_state.latency, 0.1)
            
            # Resource efficiency
            cpu_efficiency = 1.0 - abs(current_state.cpu_utilization - 0.75)  # Target 75% CPU
            memory_efficiency = 1.0 - abs(current_state.memory_utilization - 0.70)  # Target 70% memory
            
            # Error rate improvement (negative delta is good)
            error_delta = (prev_state.error_rate - current_state.error_rate) / max(prev_state.error_rate, 0.001)
            
            # Calculate weighted reward
            reward = (
                self.weights["throughput"] * throughput_delta +
                self.weights["latency"] * latency_delta +
                self.weights["cpu_efficiency"] * cpu_efficiency +
                self.weights["memory_efficiency"] * memory_efficiency +
                self.weights["error_rate"] * error_delta
            )
            
            # Penalty for extreme resource usage
            if current_state.cpu_utilization > 0.95 or current_state.memory_utilization > 0.95:
                reward -= 0.5
            
            # Bonus for maintaining stable performance
            if current_state.error_rate < 0.01 and current_state.throughput > prev_state.throughput:
                reward += 0.1
            
            self.logger.debug(f"Calculated reward: {reward:.3f} for action {action.action_type}")
            return reward
            
        except Exception as e:
            self.logger.error(f"Reward calculation failed: {e}")
            return 0.0


class DQNAgent:
    """Deep Q-Network agent for pipeline optimization."""
    
    def __init__(
        self,
        state_size: int = 11,
        action_size: int = len(ActionType),
        learning_rate: float = 0.001,
        epsilon: float = 1.0,
        epsilon_decay: float = 0.995,
        epsilon_min: float = 0.01,
        memory_size: int = 10000
    ):
        self.state_size = state_size
        self.action_size = action_size
        self.learning_rate = learning_rate
        self.epsilon = epsilon
        self.epsilon_decay = epsilon_decay
        self.epsilon_min = epsilon_min
        
        # Experience replay memory
        self.memory = deque(maxlen=memory_size)
        
        # Q-networks (simplified implementation)
        self.q_table = defaultdict(lambda: np.zeros(action_size))
        
        self.logger = get_logger("rl.dqn_agent")
        
    def act(self, state: PipelineState) -> ActionType:
        """Choose action using epsilon-greedy policy."""
        if np.random.random() <= self.epsilon:
            # Exploration: random action
            action = random.choice(list(ActionType))
            self.logger.debug(f"Exploration action: {action}")
            return action
        
        # Exploitation: best known action
        state_key = self._state_to_key(state)
        q_values = self.q_table[state_key]
        action_index = np.argmax(q_values)
        action = list(ActionType)[action_index]
        
        self.logger.debug(f"Exploitation action: {action} (Q-value: {q_values[action_index]:.3f})")
        return action
    
    def remember(self, experience: Experience) -> None:
        """Store experience in replay memory."""
        self.memory.append(experience)
    
    def replay(self, batch_size: int = 32) -> None:
        """Train the agent using experience replay."""
        if len(self.memory) < batch_size:
            return
        
        # Sample random batch
        batch = random.sample(self.memory, batch_size)
        
        for experience in batch:
            state_key = self._state_to_key(experience.state)
            next_state_key = self._state_to_key(experience.next_state)
            
            action_index = list(ActionType).index(experience.action.action_type)
            
            # Q-learning update
            target = experience.reward
            if not experience.done:
                target += 0.95 * np.max(self.q_table[next_state_key])
            
            self.q_table[state_key][action_index] = (
                (1 - self.learning_rate) * self.q_table[state_key][action_index] +
                self.learning_rate * target
            )
        
        # Decay epsilon
        if self.epsilon > self.epsilon_min:
            self.epsilon *= self.epsilon_decay
        
        self.logger.debug(f"Completed replay training on {batch_size} experiences")
    
    def _state_to_key(self, state: PipelineState) -> str:
        """Convert state to string key for Q-table."""
        # Discretize continuous values for Q-table lookup
        vector = state.to_vector()
        discretized = tuple(int(x * 10) for x in vector)  # Discretize to 1 decimal place
        return str(discretized)
    
    def save_model(self, file_path: str) -> None:
        """Save the trained model."""
        model_data = {
            "q_table": dict(self.q_table),
            "epsilon": self.epsilon,
            "state_size": self.state_size,
            "action_size": self.action_size
        }
        
        with open(file_path, 'wb') as f:
            pickle.dump(model_data, f)
        
        self.logger.info(f"Model saved to {file_path}")
    
    def load_model(self, file_path: str) -> None:
        """Load a trained model."""
        try:
            with open(file_path, 'rb') as f:
                model_data = pickle.load(f)
            
            self.q_table = defaultdict(lambda: np.zeros(self.action_size), model_data["q_table"])
            self.epsilon = model_data["epsilon"]
            
            self.logger.info(f"Model loaded from {file_path}")
            
        except FileNotFoundError:
            self.logger.warning(f"Model file not found: {file_path}")
        except Exception as e:
            self.logger.error(f"Failed to load model: {e}")


class PipelineActionExecutor:
    """Executes actions on the pipeline based on RL agent decisions."""
    
    def __init__(self):
        self.logger = get_logger("rl.action_executor")
        
    async def execute_action(
        self, 
        pipeline_id: str, 
        action: Action,
        current_state: PipelineState
    ) -> Dict[str, Any]:
        """Execute an action on the pipeline and return results."""
        try:
            self.logger.info(f"Executing action {action.action_type} on pipeline {pipeline_id}")
            
            if action.action_type == ActionType.INCREASE_PARALLELISM:
                return await self._increase_parallelism(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.DECREASE_PARALLELISM:
                return await self._decrease_parallelism(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.CHANGE_BATCH_SIZE:
                return await self._change_batch_size(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.ADJUST_MEMORY_ALLOCATION:
                return await self._adjust_memory(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.ENABLE_CACHING:
                return await self._enable_caching(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.DISABLE_CACHING:
                return await self._disable_caching(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.SWITCH_EXECUTION_ENGINE:
                return await self._switch_engine(pipeline_id, action.parameters)
            
            elif action.action_type == ActionType.REORDER_OPERATIONS:
                return await self._reorder_operations(pipeline_id, action.parameters)
            
            else:  # NO_ACTION
                return {"status": "success", "message": "No action taken"}
                
        except Exception as e:
            self.logger.error(f"Action execution failed: {e}")
            return {"status": "error", "message": str(e)}
    
    async def _increase_parallelism(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Increase pipeline parallelism."""
        increase_factor = params.get("factor", 1.5)
        self.logger.info(f"Increasing parallelism by factor {increase_factor}")
        
        # In a real implementation, this would interface with the execution engine
        await asyncio.sleep(0.1)  # Simulate execution time
        
        return {
            "status": "success",
            "message": f"Parallelism increased by {increase_factor}x",
            "new_parallelism": int(params.get("current_level", 4) * increase_factor)
        }
    
    async def _decrease_parallelism(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Decrease pipeline parallelism."""
        decrease_factor = params.get("factor", 0.7)
        self.logger.info(f"Decreasing parallelism by factor {decrease_factor}")
        
        await asyncio.sleep(0.1)
        
        return {
            "status": "success",
            "message": f"Parallelism decreased by {decrease_factor}x",
            "new_parallelism": max(1, int(params.get("current_level", 4) * decrease_factor))
        }
    
    async def _change_batch_size(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Change pipeline batch size."""
        new_batch_size = params.get("new_size", 1000)
        self.logger.info(f"Changing batch size to {new_batch_size}")
        
        await asyncio.sleep(0.1)
        
        return {
            "status": "success",
            "message": f"Batch size changed to {new_batch_size}",
            "new_batch_size": new_batch_size
        }
    
    async def _adjust_memory(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Adjust memory allocation."""
        memory_adjustment = params.get("adjustment", 1.0)  # GB
        self.logger.info(f"Adjusting memory by {memory_adjustment} GB")
        
        await asyncio.sleep(0.1)
        
        return {
            "status": "success",
            "message": f"Memory adjusted by {memory_adjustment} GB",
            "memory_change": memory_adjustment
        }
    
    async def _enable_caching(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Enable caching for pipeline."""
        cache_type = params.get("cache_type", "memory")
        self.logger.info(f"Enabling {cache_type} caching")
        
        await asyncio.sleep(0.1)
        
        return {
            "status": "success",
            "message": f"{cache_type} caching enabled",
            "cache_enabled": True
        }
    
    async def _disable_caching(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Disable caching for pipeline."""
        self.logger.info("Disabling caching")
        
        await asyncio.sleep(0.1)
        
        return {
            "status": "success",
            "message": "Caching disabled",
            "cache_enabled": False
        }
    
    async def _switch_engine(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Switch execution engine."""
        new_engine = params.get("engine", "spark")
        self.logger.info(f"Switching to {new_engine} engine")
        
        await asyncio.sleep(0.5)  # Engine switch takes longer
        
        return {
            "status": "success",
            "message": f"Switched to {new_engine} engine",
            "new_engine": new_engine
        }
    
    async def _reorder_operations(self, pipeline_id: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """Reorder pipeline operations."""
        optimization_type = params.get("optimization", "filter_first")
        self.logger.info(f"Reordering operations: {optimization_type}")
        
        await asyncio.sleep(0.2)
        
        return {
            "status": "success",
            "message": f"Operations reordered: {optimization_type}",
            "optimization_applied": optimization_type
        }


class AdaptivePipelineRL:
    """Main class for reinforcement learning-based pipeline adaptation."""
    
    def __init__(
        self,
        model_save_path: Optional[str] = None,
        training_enabled: bool = True,
        monitoring_interval: float = 10.0
    ):
        self.logger = get_logger("adaptive_pipeline_rl")
        self.model_save_path = model_save_path or "pipeline_rl_model.pkl"
        self.training_enabled = training_enabled
        self.monitoring_interval = monitoring_interval
        
        # Initialize components
        self.rl_agent = DQNAgent()
        self.reward_function = RewardFunction()
        self.action_executor = PipelineActionExecutor()
        
        # State tracking
        self.pipeline_states: Dict[str, PipelineState] = {}
        self.recent_actions: Dict[str, Tuple[Action, datetime]] = {}
        
        # Performance metrics
        self.metrics = {
            "total_actions": 0,
            "successful_actions": 0,
            "total_reward": 0.0,
            "episodes": 0
        }
        
        # Load existing model if available
        self._load_model()
        
    def _load_model(self) -> None:
        """Load existing RL model."""
        try:
            self.rl_agent.load_model(self.model_save_path)
        except Exception as e:
            self.logger.info(f"Starting with new model: {e}")
    
    async def observe_pipeline_state(
        self,
        pipeline_id: str,
        metrics: Dict[str, Any]
    ) -> None:
        """Observe current pipeline state and potentially take action."""
        try:
            # Create current state
            current_state = PipelineState(
                pipeline_id=pipeline_id,
                cpu_utilization=metrics.get("cpu_utilization", 0.5),
                memory_utilization=metrics.get("memory_utilization", 0.5),
                throughput=metrics.get("throughput", 100.0),
                latency=metrics.get("latency", 1.0),
                error_rate=metrics.get("error_rate", 0.0),
                queue_length=metrics.get("queue_length", 0),
                parallelism_level=metrics.get("parallelism_level", 4),
                batch_size=metrics.get("batch_size", 1000),
                memory_allocation=metrics.get("memory_allocation", 8.0),
                caching_enabled=metrics.get("caching_enabled", False),
                execution_engine=metrics.get("execution_engine", "pandas"),
                timestamp=datetime.now()
            )
            
            # Check if we have a previous state for this pipeline
            prev_state = self.pipeline_states.get(pipeline_id)
            
            if prev_state:
                # Calculate reward if we took an action
                if pipeline_id in self.recent_actions:
                    action, action_time = self.recent_actions[pipeline_id]
                    
                    # Only calculate reward if enough time has passed for the action to take effect
                    if (datetime.now() - action_time).total_seconds() > 5.0:
                        reward = self.reward_function.calculate_reward(prev_state, current_state, action)
                        
                        # Store experience for training
                        if self.training_enabled:
                            experience = Experience(
                                state=prev_state,
                                action=action,
                                reward=reward,
                                next_state=current_state,
                                done=False,
                                timestamp=datetime.now()
                            )
                            self.rl_agent.remember(experience)
                            
                            # Update metrics
                            self.metrics["total_reward"] += reward
                            
                            # Train agent periodically
                            if len(self.rl_agent.memory) >= 32:
                                self.rl_agent.replay()
                        
                        # Remove action from recent actions
                        del self.recent_actions[pipeline_id]
                        
                        self.logger.debug(f"Calculated reward {reward:.3f} for pipeline {pipeline_id}")
            
            # Decide whether to take action
            should_act = self._should_take_action(current_state)
            
            if should_act:
                await self._take_action(pipeline_id, current_state)
            
            # Update stored state
            self.pipeline_states[pipeline_id] = current_state
            
        except Exception as e:
            self.logger.error(f"Failed to observe pipeline state: {e}")
    
    def _should_take_action(self, state: PipelineState) -> bool:
        """Determine if an action should be taken based on current state."""
        # Take action if performance is suboptimal
        if state.cpu_utilization > 0.9 or state.memory_utilization > 0.9:
            return True
        
        if state.error_rate > 0.05:  # 5% error rate threshold
            return True
        
        if state.latency > 5.0:  # 5 second latency threshold
            return True
        
        if state.queue_length > 1000:  # Large queue backlog
            return True
        
        # Occasionally take action for exploration (during training)
        if self.training_enabled and random.random() < 0.1:
            return True
        
        return False
    
    async def _take_action(self, pipeline_id: str, state: PipelineState) -> None:
        """Take an action to optimize the pipeline."""
        try:
            # Get action from RL agent
            action_type = self.rl_agent.act(state)
            
            # Create action with appropriate parameters
            action = self._create_action(action_type, state)
            
            # Execute action
            result = await self.action_executor.execute_action(pipeline_id, action, state)
            
            # Track action
            self.recent_actions[pipeline_id] = (action, datetime.now())
            self.metrics["total_actions"] += 1
            
            if result.get("status") == "success":
                self.metrics["successful_actions"] += 1
                self.logger.info(f"Action {action_type} executed successfully on {pipeline_id}")
            else:
                self.logger.warning(f"Action {action_type} failed on {pipeline_id}: {result.get('message')}")
            
        except Exception as e:
            self.logger.error(f"Failed to take action: {e}")
    
    def _create_action(self, action_type: ActionType, state: PipelineState) -> Action:
        """Create action with appropriate parameters based on current state."""
        parameters = {}
        expected_impact = 0.5
        confidence = 0.8
        
        if action_type == ActionType.INCREASE_PARALLELISM:
            if state.cpu_utilization < 0.6:
                parameters = {"factor": 1.5, "current_level": state.parallelism_level}
                expected_impact = 0.7
            else:
                parameters = {"factor": 1.2, "current_level": state.parallelism_level}
                expected_impact = 0.3
        
        elif action_type == ActionType.DECREASE_PARALLELISM:
            parameters = {"factor": 0.8, "current_level": state.parallelism_level}
            expected_impact = 0.4
        
        elif action_type == ActionType.CHANGE_BATCH_SIZE:
            if state.latency > 3.0:
                # Increase batch size to improve throughput
                new_size = min(state.batch_size * 2, 5000)
            else:
                # Decrease batch size to reduce latency
                new_size = max(state.batch_size // 2, 100)
            
            parameters = {"new_size": new_size}
            expected_impact = 0.6
        
        elif action_type == ActionType.ADJUST_MEMORY_ALLOCATION:
            if state.memory_utilization > 0.85:
                adjustment = 2.0  # Add 2GB
            else:
                adjustment = -1.0  # Remove 1GB
            
            parameters = {"adjustment": adjustment}
            expected_impact = 0.5
        
        elif action_type == ActionType.ENABLE_CACHING:
            parameters = {"cache_type": "memory"}
            expected_impact = 0.8
        
        elif action_type == ActionType.SWITCH_EXECUTION_ENGINE:
            # Simple engine selection logic
            current_engine = state.execution_engine
            if current_engine == "pandas":
                new_engine = "spark" if state.throughput > 1000 else "dask"
            else:
                new_engine = "pandas"
            
            parameters = {"engine": new_engine}
            expected_impact = 0.9
            confidence = 0.6  # Engine switches are riskier
        
        return Action(
            action_type=action_type,
            parameters=parameters,
            expected_impact=expected_impact,
            confidence=confidence
        )
    
    async def save_model(self) -> None:
        """Save the trained RL model."""
        try:
            self.rl_agent.save_model(self.model_save_path)
            
            # Save metrics
            metrics_path = self.model_save_path.replace(".pkl", "_metrics.json")
            with open(metrics_path, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            
            self.logger.info("RL model and metrics saved successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to save RL model: {e}")
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get performance summary of the RL system."""
        success_rate = (
            self.metrics["successful_actions"] / max(self.metrics["total_actions"], 1)
        )
        
        avg_reward = (
            self.metrics["total_reward"] / max(self.metrics["episodes"], 1)
        )
        
        return {
            "total_actions": self.metrics["total_actions"],
            "success_rate": success_rate,
            "average_reward": avg_reward,
            "current_epsilon": self.rl_agent.epsilon,
            "memory_size": len(self.rl_agent.memory),
            "tracked_pipelines": len(self.pipeline_states)
        }
    
    async def start_monitoring(self) -> None:
        """Start continuous monitoring and adaptation."""
        self.logger.info("Starting adaptive pipeline monitoring")
        
        while True:
            try:
                # Periodic model saving
                if self.metrics["total_actions"] % 100 == 0 and self.metrics["total_actions"] > 0:
                    await self.save_model()
                
                # Log performance metrics
                if self.metrics["total_actions"] % 50 == 0 and self.metrics["total_actions"] > 0:
                    summary = self.get_performance_summary()
                    self.logger.info(f"RL Performance: {summary}")
                
                await asyncio.sleep(self.monitoring_interval)
                
            except asyncio.CancelledError:
                self.logger.info("Monitoring cancelled")
                break
            except Exception as e:
                self.logger.error(f"Monitoring error: {e}")
                await asyncio.sleep(self.monitoring_interval)


# Integration with existing pipeline system
async def integrate_rl_adaptation(
    pipeline_monitor,
    rl_adapter: AdaptivePipelineRL
) -> None:
    """Integrate RL adaptation with existing pipeline monitoring."""
    try:
        # Start RL monitoring
        monitoring_task = asyncio.create_task(rl_adapter.start_monitoring())
        
        # Connect to pipeline monitor events
        # This would hook into the existing monitoring system
        get_logger("rl_integration").info("RL adaptation integrated with pipeline monitoring")
        
        # Wait for monitoring to complete
        await monitoring_task
        
    except Exception as e:
        get_logger("rl_integration").error(f"RL integration failed: {e}")
        raise DataProcessingException(f"RL integration failed: {e}")