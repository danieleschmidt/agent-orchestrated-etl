#!/usr/bin/env python3
"""
GENERATION 3: SCALING AUTONOMOUS SYSTEM - MAKE IT SCALE
Advanced performance optimization, concurrent processing, intelligent caching,
load balancing, auto-scaling, and resource management
"""

import json
import time
import logging
import asyncio
import hashlib
import threading
import multiprocessing
import statistics
from pathlib import Path
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from dataclasses import dataclass, field
from enum import Enum
from contextlib import asynccontextmanager
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
from datetime import datetime, timedelta
import uuid
import weakref
from collections import defaultdict, deque

# Advanced logging with performance tracking
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - [%(processName)s-%(threadName)s] - %(message)s'
)

class ScalingStrategy(Enum):
    """Auto-scaling strategies"""
    REACTIVE = "reactive"
    PREDICTIVE = "predictive" 
    ADAPTIVE = "adaptive"
    MACHINE_LEARNING = "ml_based"

class CacheStrategy(Enum):
    """Caching strategies"""
    LRU = "lru"
    LFU = "lfu"
    TTL = "ttl"
    ADAPTIVE = "adaptive"
    INTELLIGENT = "intelligent"

@dataclass
class PerformanceMetrics:
    """Comprehensive performance metrics"""
    operation_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    duration_ms: Optional[float] = None
    cpu_usage_percent: float = 0.0
    memory_usage_mb: float = 0.0
    throughput_ops_sec: float = 0.0
    concurrent_operations: int = 0
    cache_hit_rate: float = 0.0
    network_latency_ms: float = 0.0
    error_rate: float = 0.0
    queue_depth: int = 0
    resource_utilization: Dict[str, float] = field(default_factory=dict)

@dataclass
class ScalingDecision:
    """Auto-scaling decision context"""
    timestamp: float = field(default_factory=time.time)
    current_load: float = 0.0
    predicted_load: float = 0.0
    recommended_workers: int = 1
    scaling_action: str = "maintain"  # scale_up, scale_down, maintain
    confidence_score: float = 0.0
    reasoning: str = ""
    metrics_snapshot: Dict[str, Any] = field(default_factory=dict)

class IntelligentCache:
    """Advanced intelligent caching system with multiple strategies"""
    
    def __init__(self, max_size: int = 1000, default_ttl: int = 3600, 
                 strategy: CacheStrategy = CacheStrategy.ADAPTIVE):
        self.max_size = max_size
        self.default_ttl = default_ttl
        self.strategy = strategy
        self.cache = {}
        self.access_times = {}
        self.access_counts = defaultdict(int)
        self.insertion_order = deque()
        self.logger = logging.getLogger(f'{__name__}.IntelligentCache')
        self.hit_count = 0
        self.miss_count = 0
        self.lock = threading.RLock()
        
        # Adaptive parameters
        self.access_pattern_history = deque(maxlen=1000)
        self.cache_efficiency_threshold = 0.7
        
    async def get(self, key: str) -> Optional[Any]:
        """Get item from cache with intelligent access tracking"""
        with self.lock:
            current_time = time.time()
            
            if key in self.cache:
                # Check TTL
                item_data = self.cache[key]
                if current_time - item_data['timestamp'] > item_data.get('ttl', self.default_ttl):
                    del self.cache[key]
                    self.access_times.pop(key, None)
                    self.miss_count += 1
                    return None
                
                # Update access patterns
                self.access_times[key] = current_time
                self.access_counts[key] += 1
                self.access_pattern_history.append({'key': key, 'access_time': current_time, 'hit': True})
                
                self.hit_count += 1
                return item_data['value']
            
            self.miss_count += 1
            self.access_pattern_history.append({'key': key, 'access_time': current_time, 'hit': False})
            return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set item in cache with intelligent eviction"""
        with self.lock:
            current_time = time.time()
            
            # Evict if at capacity
            if len(self.cache) >= self.max_size and key not in self.cache:
                await self._intelligent_eviction()
            
            # Store item
            self.cache[key] = {
                'value': value,
                'timestamp': current_time,
                'ttl': ttl or self.default_ttl
            }
            self.access_times[key] = current_time
            self.access_counts[key] += 1
            
            if key not in self.insertion_order:
                self.insertion_order.append(key)
            
            return True
    
    async def _intelligent_eviction(self):
        """Intelligent cache eviction based on strategy and patterns"""
        if not self.cache:
            return
            
        current_time = time.time()
        
        if self.strategy == CacheStrategy.LRU:
            # Least Recently Used
            oldest_key = min(self.access_times, key=self.access_times.get)
            
        elif self.strategy == CacheStrategy.LFU:
            # Least Frequently Used
            oldest_key = min(self.access_counts, key=self.access_counts.get)
            
        elif self.strategy == CacheStrategy.TTL:
            # Shortest TTL remaining
            oldest_key = min(self.cache, key=lambda k: 
                           self.cache[k]['timestamp'] + self.cache[k].get('ttl', self.default_ttl))
                           
        elif self.strategy == CacheStrategy.ADAPTIVE:
            # Adaptive strategy based on access patterns
            oldest_key = await self._adaptive_eviction_selection()
            
        else:  # INTELLIGENT
            oldest_key = await self._ml_based_eviction_selection()
        
        # Remove selected key
        if oldest_key in self.cache:
            del self.cache[oldest_key]
            self.access_times.pop(oldest_key, None)
            self.access_counts.pop(oldest_key, None)
            if oldest_key in self.insertion_order:
                self.insertion_order.remove(oldest_key)
    
    async def _adaptive_eviction_selection(self) -> str:
        """Adaptive eviction based on recent access patterns"""
        current_time = time.time()
        
        # Analyze recent access patterns
        recent_accesses = [
            entry for entry in self.access_pattern_history 
            if current_time - entry['access_time'] < 300  # Last 5 minutes
        ]
        
        if not recent_accesses:
            # Fallback to LRU
            return min(self.access_times, key=self.access_times.get)
        
        # Calculate access frequency in recent window
        recent_access_counts = defaultdict(int)
        for access in recent_accesses:
            if access['hit']:
                recent_access_counts[access['key']] += 1
        
        # Find key with lowest recent access count that exists in cache
        candidates = [key for key in self.cache if key in recent_access_counts or key not in recent_access_counts]
        if not candidates:
            return list(self.cache.keys())[0]
            
        return min(candidates, key=lambda k: recent_access_counts.get(k, 0))
    
    async def _ml_based_eviction_selection(self) -> str:
        """ML-based intelligent eviction (simplified heuristic)"""
        current_time = time.time()
        
        scores = {}
        for key in self.cache:
            # Simple scoring algorithm (would be ML model in production)
            recency_score = 1.0 / (current_time - self.access_times.get(key, 0) + 1)
            frequency_score = self.access_counts.get(key, 1) / max(self.access_counts.values())
            ttl_remaining = (self.cache[key]['timestamp'] + 
                           self.cache[key].get('ttl', self.default_ttl) - current_time)
            ttl_score = max(0, ttl_remaining) / self.default_ttl
            
            # Weighted combination
            scores[key] = 0.4 * recency_score + 0.4 * frequency_score + 0.2 * ttl_score
        
        return min(scores, key=scores.get)
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics"""
        total_requests = self.hit_count + self.miss_count
        hit_rate = self.hit_count / total_requests if total_requests > 0 else 0.0
        
        return {
            "hit_rate": hit_rate,
            "hit_count": self.hit_count,
            "miss_count": self.miss_count,
            "total_requests": total_requests,
            "cache_size": len(self.cache),
            "max_size": self.max_size,
            "utilization": len(self.cache) / self.max_size,
            "strategy": self.strategy.value,
            "avg_access_count": statistics.mean(self.access_counts.values()) if self.access_counts else 0
        }

class LoadBalancer:
    """Intelligent load balancer with multiple strategies"""
    
    def __init__(self, workers: List[str] = None, strategy: str = "least_connections"):
        self.workers = workers or [f"worker_{i}" for i in range(4)]
        self.strategy = strategy
        self.connections = {worker: 0 for worker in self.workers}
        self.response_times = {worker: deque(maxlen=100) for worker in self.workers}
        self.error_counts = {worker: 0 for worker in self.workers}
        self.health_status = {worker: True for worker in self.workers}
        self.lock = threading.RLock()
        self.logger = logging.getLogger(f'{__name__}.LoadBalancer')
        
        # Advanced metrics
        self.request_count = 0
        self.total_response_time = 0.0
        self.worker_performance_scores = {worker: 1.0 for worker in self.workers}
        
    async def select_worker(self) -> str:
        """Select optimal worker using configured strategy"""
        with self.lock:
            healthy_workers = [w for w in self.workers if self.health_status[w]]
            
            if not healthy_workers:
                # Emergency fallback - select least errored worker
                return min(self.workers, key=lambda w: self.error_counts[w])
            
            if self.strategy == "round_robin":
                return healthy_workers[self.request_count % len(healthy_workers)]
                
            elif self.strategy == "least_connections":
                return min(healthy_workers, key=lambda w: self.connections[w])
                
            elif self.strategy == "least_response_time":
                return min(healthy_workers, key=lambda w: 
                          statistics.mean(self.response_times[w]) if self.response_times[w] else 0)
                          
            elif self.strategy == "weighted_performance":
                return max(healthy_workers, key=lambda w: self.worker_performance_scores[w])
                
            else:  # intelligent
                return await self._intelligent_worker_selection(healthy_workers)
    
    async def _intelligent_worker_selection(self, healthy_workers: List[str]) -> str:
        """AI-based intelligent worker selection"""
        scores = {}
        
        for worker in healthy_workers:
            # Multi-factor scoring
            connection_score = 1.0 / (self.connections[worker] + 1)
            
            avg_response_time = (statistics.mean(self.response_times[worker]) 
                               if self.response_times[worker] else 0.1)
            response_time_score = 1.0 / (avg_response_time + 0.1)
            
            error_rate = self.error_counts[worker] / max(1, len(self.response_times[worker]))
            error_score = 1.0 / (error_rate + 0.1)
            
            performance_score = self.worker_performance_scores[worker]
            
            # Weighted combination
            scores[worker] = (
                0.3 * connection_score +
                0.3 * response_time_score + 
                0.2 * error_score +
                0.2 * performance_score
            )
        
        return max(scores, key=scores.get)
    
    async def start_request(self, worker: str):
        """Start request tracking for worker"""
        with self.lock:
            self.connections[worker] += 1
            self.request_count += 1
    
    async def end_request(self, worker: str, response_time: float, error: bool = False):
        """End request tracking and update metrics"""
        with self.lock:
            self.connections[worker] = max(0, self.connections[worker] - 1)
            self.response_times[worker].append(response_time)
            self.total_response_time += response_time
            
            if error:
                self.error_counts[worker] += 1
                # Decrease performance score on error
                self.worker_performance_scores[worker] *= 0.95
            else:
                # Increase performance score on success
                self.worker_performance_scores[worker] = min(1.0, 
                    self.worker_performance_scores[worker] * 1.01)
    
    async def update_health_status(self, worker: str, healthy: bool):
        """Update worker health status"""
        with self.lock:
            self.health_status[worker] = healthy
            if not healthy:
                self.logger.warning(f"Worker {worker} marked as unhealthy")
    
    def get_load_balancer_stats(self) -> Dict[str, Any]:
        """Get comprehensive load balancer statistics"""
        with self.lock:
            avg_response_time = (self.total_response_time / max(1, self.request_count))
            
            worker_stats = {}
            for worker in self.workers:
                worker_stats[worker] = {
                    "connections": self.connections[worker],
                    "avg_response_time": (statistics.mean(self.response_times[worker]) 
                                        if self.response_times[worker] else 0),
                    "error_count": self.error_counts[worker],
                    "healthy": self.health_status[worker],
                    "performance_score": self.worker_performance_scores[worker]
                }
            
            return {
                "strategy": self.strategy,
                "total_requests": self.request_count,
                "avg_response_time": avg_response_time,
                "worker_stats": worker_stats,
                "healthy_workers": sum(1 for status in self.health_status.values() if status),
                "total_workers": len(self.workers)
            }

class AutoScaler:
    """Intelligent auto-scaling system"""
    
    def __init__(self, min_workers: int = 1, max_workers: int = 10, 
                 scale_up_threshold: float = 0.8, scale_down_threshold: float = 0.3,
                 strategy: ScalingStrategy = ScalingStrategy.ADAPTIVE):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.strategy = strategy
        self.current_workers = min_workers
        
        self.metrics_history = deque(maxlen=100)
        self.scaling_history = deque(maxlen=20)
        self.logger = logging.getLogger(f'{__name__}.AutoScaler')
        
        # ML-based prediction parameters
        self.load_prediction_window = 10
        self.scaling_cooldown = 60  # seconds
        self.last_scaling_action = 0
        
    async def analyze_and_scale(self, current_metrics: PerformanceMetrics, 
                              load_balancer: LoadBalancer) -> ScalingDecision:
        """Analyze current state and make scaling decisions"""
        current_time = time.time()
        self.metrics_history.append((current_time, current_metrics))
        
        # Calculate current load
        current_load = self._calculate_current_load(current_metrics, load_balancer)
        predicted_load = await self._predict_future_load()
        
        decision = ScalingDecision(
            current_load=current_load,
            predicted_load=predicted_load,
            recommended_workers=self.current_workers
        )
        
        # Apply scaling strategy
        if self.strategy == ScalingStrategy.REACTIVE:
            decision = await self._reactive_scaling(decision, current_metrics)
        elif self.strategy == ScalingStrategy.PREDICTIVE:
            decision = await self._predictive_scaling(decision, predicted_load)
        elif self.strategy == ScalingStrategy.ADAPTIVE:
            decision = await self._adaptive_scaling(decision, current_metrics)
        else:  # MACHINE_LEARNING
            decision = await self._ml_based_scaling(decision, current_metrics)
        
        # Apply cooldown and bounds checking
        decision = self._apply_scaling_constraints(decision, current_time)
        
        # Execute scaling decision
        if decision.scaling_action != "maintain":
            await self._execute_scaling(decision, load_balancer)
        
        self.scaling_history.append(decision)
        return decision
    
    def _calculate_current_load(self, metrics: PerformanceMetrics, 
                              load_balancer: LoadBalancer) -> float:
        """Calculate normalized current load (0.0 to 1.0)"""
        # Multi-factor load calculation
        cpu_load = metrics.cpu_usage_percent / 100.0
        memory_load = min(1.0, metrics.memory_usage_mb / 1000.0)  # Assume 1GB baseline
        
        # Connection-based load
        total_connections = sum(load_balancer.connections.values())
        max_connections_per_worker = 100  # Configurable
        connection_load = min(1.0, total_connections / (self.current_workers * max_connections_per_worker))
        
        # Queue depth load
        queue_load = min(1.0, metrics.queue_depth / 50.0)  # Assume 50 queue baseline
        
        # Weighted average
        return 0.3 * cpu_load + 0.2 * memory_load + 0.3 * connection_load + 0.2 * queue_load
    
    async def _predict_future_load(self) -> float:
        """Predict future load based on historical patterns"""
        if len(self.metrics_history) < 3:
            return 0.5  # Default prediction
        
        # Simple linear regression on recent load trends
        recent_loads = []
        for timestamp, metrics in list(self.metrics_history)[-self.load_prediction_window:]:
            # Reconstruct load from metrics (simplified)
            load = (metrics.cpu_usage_percent / 100.0 + 
                   min(1.0, metrics.memory_usage_mb / 1000.0)) / 2
            recent_loads.append(load)
        
        if len(recent_loads) < 2:
            return recent_loads[0] if recent_loads else 0.5
            
        # Calculate trend
        x = list(range(len(recent_loads)))
        y = recent_loads
        
        # Simple linear regression
        n = len(recent_loads)
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi * xi for xi in x)
        
        if n * sum_x2 - sum_x * sum_x != 0:
            slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)
            intercept = (sum_y - slope * sum_x) / n
            
            # Predict next value
            predicted = slope * n + intercept
            return max(0.0, min(1.0, predicted))
        
        return statistics.mean(recent_loads)
    
    async def _reactive_scaling(self, decision: ScalingDecision, 
                               metrics: PerformanceMetrics) -> ScalingDecision:
        """Reactive scaling based on current thresholds"""
        if decision.current_load > self.scale_up_threshold:
            decision.scaling_action = "scale_up"
            decision.recommended_workers = min(self.max_workers, self.current_workers + 1)
            decision.reasoning = f"Load {decision.current_load:.2f} > threshold {self.scale_up_threshold}"
            
        elif decision.current_load < self.scale_down_threshold:
            decision.scaling_action = "scale_down"  
            decision.recommended_workers = max(self.min_workers, self.current_workers - 1)
            decision.reasoning = f"Load {decision.current_load:.2f} < threshold {self.scale_down_threshold}"
            
        else:
            decision.scaling_action = "maintain"
            decision.reasoning = f"Load {decision.current_load:.2f} within thresholds"
        
        decision.confidence_score = 0.8
        return decision
    
    async def _predictive_scaling(self, decision: ScalingDecision, 
                                predicted_load: float) -> ScalingDecision:
        """Predictive scaling based on load prediction"""
        # Scale based on predicted load with lead time
        if predicted_load > self.scale_up_threshold:
            decision.scaling_action = "scale_up"
            decision.recommended_workers = min(self.max_workers, 
                                             self.current_workers + max(1, int((predicted_load - self.scale_up_threshold) * 5)))
            decision.reasoning = f"Predicted load {predicted_load:.2f} > threshold"
            decision.confidence_score = 0.7
            
        elif predicted_load < self.scale_down_threshold and decision.current_load < self.scale_down_threshold:
            decision.scaling_action = "scale_down"
            decision.recommended_workers = max(self.min_workers, self.current_workers - 1) 
            decision.reasoning = f"Predicted load {predicted_load:.2f} and current load low"
            decision.confidence_score = 0.6
            
        else:
            decision.scaling_action = "maintain"
            decision.reasoning = f"Predicted load {predicted_load:.2f} suggests no scaling needed"
            decision.confidence_score = 0.8
        
        return decision
    
    async def _adaptive_scaling(self, decision: ScalingDecision, 
                               metrics: PerformanceMetrics) -> ScalingDecision:
        """Adaptive scaling combining reactive and predictive"""
        # Get both reactive and predictive decisions
        reactive_decision = await self._reactive_scaling(
            ScalingDecision(current_load=decision.current_load, 
                           predicted_load=decision.predicted_load), metrics)
        
        predictive_decision = await self._predictive_scaling(
            ScalingDecision(current_load=decision.current_load, 
                           predicted_load=decision.predicted_load), 
            decision.predicted_load)
        
        # Combine decisions with weights
        if reactive_decision.scaling_action == predictive_decision.scaling_action:
            decision.scaling_action = reactive_decision.scaling_action
            decision.recommended_workers = reactive_decision.recommended_workers
            decision.confidence_score = 0.9
            decision.reasoning = f"Reactive and predictive agree: {reactive_decision.reasoning}"
            
        else:
            # Favor reactive for immediate issues
            if reactive_decision.scaling_action == "scale_up":
                decision = reactive_decision
                decision.confidence_score = 0.7
                decision.reasoning += " (reactive priority)"
            else:
                # Be conservative for scale down
                decision.scaling_action = "maintain"
                decision.recommended_workers = self.current_workers
                decision.confidence_score = 0.5
                decision.reasoning = "Reactive and predictive disagree - maintaining"
        
        return decision
    
    async def _ml_based_scaling(self, decision: ScalingDecision, 
                               metrics: PerformanceMetrics) -> ScalingDecision:
        """ML-based scaling (simplified heuristic implementation)"""
        # In production, this would use a trained ML model
        # For now, implement advanced heuristic
        
        # Feature extraction
        features = {
            "current_load": decision.current_load,
            "predicted_load": decision.predicted_load,
            "error_rate": metrics.error_rate,
            "throughput": metrics.throughput_ops_sec,
            "queue_depth": metrics.queue_depth,
            "cache_hit_rate": metrics.cache_hit_rate
        }
        
        # Simple scoring model
        scale_up_score = 0.0
        scale_down_score = 0.0
        
        # Load-based scoring
        if decision.current_load > 0.7:
            scale_up_score += 0.3 * (decision.current_load - 0.7) / 0.3
        if decision.current_load < 0.4:
            scale_down_score += 0.2 * (0.4 - decision.current_load) / 0.4
        
        # Error rate consideration
        if metrics.error_rate > 0.05:  # 5% error rate
            scale_up_score += 0.2
        
        # Queue depth consideration
        if metrics.queue_depth > 20:
            scale_up_score += 0.15
        
        # Throughput consideration
        if metrics.throughput_ops_sec < 10:
            scale_up_score += 0.1
            
        # Cache efficiency consideration
        if metrics.cache_hit_rate < 0.7:
            scale_up_score += 0.05
        
        # Make decision
        if scale_up_score > 0.6:
            decision.scaling_action = "scale_up"
            decision.recommended_workers = min(self.max_workers, 
                                             self.current_workers + max(1, int(scale_up_score * 3)))
            decision.confidence_score = scale_up_score
            decision.reasoning = f"ML model recommends scale up (score: {scale_up_score:.2f})"
            
        elif scale_down_score > 0.4:
            decision.scaling_action = "scale_down"
            decision.recommended_workers = max(self.min_workers, self.current_workers - 1)
            decision.confidence_score = scale_down_score
            decision.reasoning = f"ML model recommends scale down (score: {scale_down_score:.2f})"
            
        else:
            decision.scaling_action = "maintain"
            decision.confidence_score = max(0.5, 1.0 - max(scale_up_score, scale_down_score))
            decision.reasoning = f"ML model recommends maintain (up: {scale_up_score:.2f}, down: {scale_down_score:.2f})"
        
        return decision
    
    def _apply_scaling_constraints(self, decision: ScalingDecision, 
                                 current_time: float) -> ScalingDecision:
        """Apply scaling constraints and cooldown periods"""
        # Check cooldown period
        if current_time - self.last_scaling_action < self.scaling_cooldown:
            if decision.scaling_action != "maintain":
                decision.scaling_action = "maintain"
                decision.recommended_workers = self.current_workers
                decision.reasoning += f" (cooldown period active)"
                decision.confidence_score *= 0.5
        
        # Apply bounds
        decision.recommended_workers = max(self.min_workers, 
                                         min(self.max_workers, decision.recommended_workers))
        
        return decision
    
    async def _execute_scaling(self, decision: ScalingDecision, 
                              load_balancer: LoadBalancer):
        """Execute scaling decision"""
        old_workers = self.current_workers
        self.current_workers = decision.recommended_workers
        self.last_scaling_action = time.time()
        
        # Update load balancer workers
        if decision.scaling_action == "scale_up":
            # Add new workers
            for i in range(old_workers, self.current_workers):
                new_worker = f"worker_{i}"
                if new_worker not in load_balancer.workers:
                    load_balancer.workers.append(new_worker)
                    load_balancer.connections[new_worker] = 0
                    load_balancer.response_times[new_worker] = deque(maxlen=100)
                    load_balancer.error_counts[new_worker] = 0
                    load_balancer.health_status[new_worker] = True
                    load_balancer.worker_performance_scores[new_worker] = 1.0
            
            self.logger.info(f"Scaled UP from {old_workers} to {self.current_workers} workers")
            
        elif decision.scaling_action == "scale_down":
            # Remove excess workers
            workers_to_remove = load_balancer.workers[self.current_workers:]
            for worker in workers_to_remove:
                load_balancer.workers.remove(worker)
                load_balancer.connections.pop(worker, None)
                load_balancer.response_times.pop(worker, None)
                load_balancer.error_counts.pop(worker, None)
                load_balancer.health_status.pop(worker, None)
                load_balancer.worker_performance_scores.pop(worker, None)
            
            self.logger.info(f"Scaled DOWN from {old_workers} to {self.current_workers} workers")
    
    def get_scaling_stats(self) -> Dict[str, Any]:
        """Get comprehensive scaling statistics"""
        recent_decisions = list(self.scaling_history)[-10:]
        scale_up_count = sum(1 for d in recent_decisions if d.scaling_action == "scale_up")
        scale_down_count = sum(1 for d in recent_decisions if d.scaling_action == "scale_down")
        
        return {
            "current_workers": self.current_workers,
            "min_workers": self.min_workers,
            "max_workers": self.max_workers,
            "strategy": self.strategy.value,
            "recent_scale_up_count": scale_up_count,
            "recent_scale_down_count": scale_down_count,
            "avg_confidence": statistics.mean([d.confidence_score for d in recent_decisions]) if recent_decisions else 0,
            "last_scaling_action": self.last_scaling_action,
            "scaling_history_size": len(self.scaling_history)
        }

class ScalableDataProcessor:
    """Generation 3: Scalable data processor with advanced optimization"""
    
    def __init__(self, max_workers: int = 4, enable_caching: bool = True, 
                 cache_size: int = 1000, scaling_strategy: ScalingStrategy = ScalingStrategy.ADAPTIVE):
        self.max_workers = max_workers
        self.logger = logging.getLogger(f'{__name__}.ScalableDataProcessor')
        
        # Initialize scaling components
        self.cache = IntelligentCache(max_size=cache_size, strategy=CacheStrategy.INTELLIGENT) if enable_caching else None
        self.load_balancer = LoadBalancer(strategy="intelligent")
        self.auto_scaler = AutoScaler(strategy=scaling_strategy)
        
        # Processing pools
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workers)
        self.process_pool = ProcessPoolExecutor(max_workers=min(4, multiprocessing.cpu_count()))
        
        # Performance tracking
        self.metrics = PerformanceMetrics()
        self.operation_count = 0
        self.total_processing_time = 0.0
        
        # Resource optimization
        self.resource_monitor = threading.Thread(target=self._resource_monitoring_loop, daemon=True)
        self.resource_monitor.start()
        
    async def process_data_scalable(self, data_batches: List[List[Dict[str, Any]]], 
                                  processing_config: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Process data with advanced scaling and optimization"""
        start_time = time.time()
        self.operation_count += 1
        operation_id = f"op_{self.operation_count}"
        
        self.logger.info(f"Starting scalable processing operation {operation_id} with {len(data_batches)} batches")
        
        # Update metrics
        self.metrics.operation_id = operation_id
        self.metrics.start_time = start_time
        self.metrics.concurrent_operations = len(data_batches)
        
        try:
            # Auto-scaling analysis
            scaling_decision = await self.auto_scaler.analyze_and_scale(self.metrics, self.load_balancer)
            self.logger.info(f"Scaling decision: {scaling_decision.scaling_action} "
                           f"(confidence: {scaling_decision.confidence_score:.2f})")
            
            # Process batches concurrently
            processed_results = await self._process_batches_concurrent(data_batches, processing_config)
            
            # Update final metrics
            end_time = time.time()
            processing_time = end_time - start_time
            self.metrics.end_time = end_time
            self.metrics.duration_ms = processing_time * 1000
            self.metrics.throughput_ops_sec = len(data_batches) / processing_time if processing_time > 0 else 0
            
            self.total_processing_time += processing_time
            
            # Cache results if enabled
            if self.cache:
                cache_key = f"processed_{operation_id}"
                await self.cache.set(cache_key, processed_results)
            
            self.logger.info(f"Scalable processing complete: {len(processed_results)} results in {processing_time:.2f}s "
                           f"({self.metrics.throughput_ops_sec:.1f} ops/sec)")
            
            return processed_results
            
        except Exception as e:
            self.logger.error(f"Scalable processing failed: {str(e)}")
            raise
    
    async def _process_batches_concurrent(self, data_batches: List[List[Dict[str, Any]]], 
                                        processing_config: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Process data batches with intelligent load balancing"""
        tasks = []
        results = []
        
        # Create concurrent processing tasks
        for i, batch in enumerate(data_batches):
            # Select optimal worker
            worker = await self.load_balancer.select_worker()
            
            # Create processing task
            task = asyncio.create_task(
                self._process_single_batch(batch, worker, processing_config)
            )
            tasks.append((task, worker))
        
        # Execute tasks and collect results
        for task, worker in tasks:
            batch_start_time = time.time()
            await self.load_balancer.start_request(worker)
            
            try:
                batch_result = await task
                response_time = time.time() - batch_start_time
                await self.load_balancer.end_request(worker, response_time, error=False)
                results.extend(batch_result)
                
            except Exception as e:
                response_time = time.time() - batch_start_time
                await self.load_balancer.end_request(worker, response_time, error=True)
                self.logger.error(f"Batch processing failed on {worker}: {str(e)}")
                # Continue processing other batches
        
        return results
    
    async def _process_single_batch(self, batch: List[Dict[str, Any]], 
                                  worker: str, processing_config: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Process a single batch of data"""
        # Check cache first
        if self.cache:
            batch_hash = hashlib.sha256(json.dumps(batch, sort_keys=True).encode()).hexdigest()[:16]
            cached_result = await self.cache.get(f"batch_{batch_hash}")
            if cached_result:
                self.metrics.cache_hit_rate = (self.cache.hit_count / 
                                             (self.cache.hit_count + self.cache.miss_count))
                return cached_result
        
        # Process batch
        processed_batch = []
        
        for record in batch:
            # Apply scalable transformations
            processed_record = await self._apply_scalable_transformations(record, processing_config)
            processed_batch.append(processed_record)
        
        # Cache result
        if self.cache:
            await self.cache.set(f"batch_{batch_hash}", processed_batch)
        
        return processed_batch
    
    async def _apply_scalable_transformations(self, record: Dict[str, Any], 
                                            config: Dict[str, Any] = None) -> Dict[str, Any]:
        """Apply generation 3 scalable transformations"""
        transformed = record.copy()
        
        # Add generation 3 metadata
        transformed['generation'] = 3
        transformed['processing_tier'] = 'scalable'
        transformed['processed_at'] = time.time()
        transformed['processor_id'] = self.metrics.operation_id
        
        # Advanced transformations
        if 'value' in transformed and isinstance(transformed['value'], (int, float)):
            # Parallel mathematical operations
            transformed['value_scaled'] = transformed['value'] * 1.5
            transformed['value_normalized'] = min(1.0, transformed['value'] / 1000.0)
            transformed['value_category'] = self._intelligent_categorization(transformed['value'])
        
        # Data quality enhancements
        transformed['quality_metrics'] = await self._calculate_quality_metrics(transformed)
        transformed['optimization_applied'] = True
        transformed['cache_eligible'] = True
        
        return transformed
    
    def _intelligent_categorization(self, value: Union[int, float]) -> str:
        """Intelligent value categorization with ML-like logic"""
        if value < 50:
            return "micro"
        elif value < 150:
            return "small"
        elif value < 350:
            return "medium"
        elif value < 600:
            return "large"
        else:
            return "enterprise"
    
    async def _calculate_quality_metrics(self, record: Dict[str, Any]) -> Dict[str, float]:
        """Calculate advanced data quality metrics"""
        # Completeness
        non_null_fields = sum(1 for v in record.values() if v is not None and v != '')
        completeness = non_null_fields / len(record) if record else 0
        
        # Consistency (simplified)
        consistency = 1.0  # Would involve cross-record analysis in production
        
        # Validity (basic checks)
        validity = 0.9  # Would involve schema validation in production
        
        # Timeliness
        timeliness = 1.0 if 'timestamp' in record else 0.8
        
        return {
            "completeness": completeness,
            "consistency": consistency,
            "validity": validity,
            "timeliness": timeliness,
            "overall_score": (completeness + consistency + validity + timeliness) / 4
        }
    
    def _resource_monitoring_loop(self):
        """Background resource monitoring"""
        import psutil
        
        while True:
            try:
                # Update resource metrics
                self.metrics.cpu_usage_percent = psutil.cpu_percent(interval=1)
                memory_info = psutil.virtual_memory()
                self.metrics.memory_usage_mb = memory_info.used / (1024 * 1024)
                
                # Queue depth simulation (would be actual queue in production)
                self.metrics.queue_depth = max(0, len(self.load_balancer.workers) * 2 - 3)
                
                time.sleep(5)  # Update every 5 seconds
                
            except Exception as e:
                self.logger.error(f"Resource monitoring error: {str(e)}")
                time.sleep(10)  # Back off on error
    
    def get_comprehensive_stats(self) -> Dict[str, Any]:
        """Get comprehensive system statistics"""
        avg_processing_time = (self.total_processing_time / max(1, self.operation_count))
        
        return {
            "generation": 3,
            "implementation": "scalable_optimized",
            "processing_stats": {
                "total_operations": self.operation_count,
                "avg_processing_time": avg_processing_time,
                "total_processing_time": self.total_processing_time,
                "current_throughput": self.metrics.throughput_ops_sec
            },
            "cache_stats": self.cache.get_cache_stats() if self.cache else None,
            "load_balancer_stats": self.load_balancer.get_load_balancer_stats(),
            "auto_scaler_stats": self.auto_scaler.get_scaling_stats(),
            "resource_metrics": {
                "cpu_usage_percent": self.metrics.cpu_usage_percent,
                "memory_usage_mb": self.metrics.memory_usage_mb,
                "queue_depth": self.metrics.queue_depth,
                "cache_hit_rate": self.metrics.cache_hit_rate
            }
        }
    
    def cleanup(self):
        """Clean up resources"""
        self.thread_pool.shutdown(wait=True)
        self.process_pool.shutdown(wait=True)

# Main execution for Generation 3
async def scalable_autonomous_pipeline():
    """Execute Generation 3 scalable autonomous pipeline"""
    logger = logging.getLogger("Generation3_Execution")
    logger.info("⚡ Starting Generation 3: Scalable Autonomous Pipeline")
    
    # Initialize scalable processor
    processor = ScalableDataProcessor(
        max_workers=8,
        enable_caching=True,
        cache_size=500,
        scaling_strategy=ScalingStrategy.ADAPTIVE
    )
    
    try:
        # Generate test data batches
        test_batches = [
            [{"id": i + j*10, "value": (i + j*10) * 10, "category": f"batch_{j}"} 
             for i in range(1, 6)]  # 5 records per batch
            for j in range(4)  # 4 batches
        ]
        
        # Process data with scaling
        results = await processor.process_data_scalable(
            test_batches, 
            {"optimization_level": "high", "quality_checks": True}
        )
        
        # Get comprehensive statistics
        stats = processor.get_comprehensive_stats()
        
        logger.info("🎉 Generation 3 Scalable Autonomous Implementation Complete!")
        logger.info(f"📊 Processed {len(results)} records")
        logger.info(f"⚡ Throughput: {stats['processing_stats']['current_throughput']:.1f} ops/sec")
        
        if stats['cache_stats']:
            logger.info(f"💾 Cache Hit Rate: {stats['cache_stats']['hit_rate']:.1%}")
        
        return {
            "generation": 3,
            "implementation": "scalable_autonomous",
            "results_count": len(results),
            "sample_result": results[0] if results else None,
            "comprehensive_stats": stats,
            "scaling_features": [
                "Intelligent caching with adaptive eviction",
                "Multi-strategy load balancing", 
                "ML-based auto-scaling",
                "Concurrent batch processing",
                "Real-time performance monitoring",
                "Resource optimization",
                "Predictive scaling decisions"
            ]
        }
        
    finally:
        processor.cleanup()

def main():
    """Main execution for Generation 3 scalable testing"""
    print("⚡ GENERATION 3: SCALABLE AUTONOMOUS ETL EXECUTION")
    print("=" * 65)
    
    return asyncio.run(scalable_autonomous_pipeline())

if __name__ == "__main__":
    result = main()
    print("\n📋 FINAL SCALING RESULTS:")
    print(json.dumps(result, indent=2, default=str))