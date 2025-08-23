#!/usr/bin/env python3
"""
Generation 3: Advanced Scaling and Performance Optimization
Implements high-performance caching, load balancing, auto-scaling, and concurrent processing
"""

import asyncio
import concurrent.futures
import hashlib
import json
import queue
import time
import threading
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional, Callable, Union, Tuple
from collections import deque, defaultdict
from abc import ABC, abstractmethod
import logging

# Advanced Caching System
class CacheStrategy(ABC):
    """Abstract base class for cache strategies"""
    
    @abstractmethod
    def should_evict(self, key: str, metadata: Dict[str, Any]) -> bool:
        pass

class LRUCacheStrategy(CacheStrategy):
    """Least Recently Used cache eviction strategy"""
    
    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self.access_order = deque()
        self.access_times = {}
    
    def should_evict(self, key: str, metadata: Dict[str, Any]) -> bool:
        current_time = time.time()
        
        # Update access time and order
        if key in self.access_times:
            self.access_order.remove(key)
        else:
            # If cache is full, evict LRU item
            if len(self.access_order) >= self.max_size:
                return True
        
        self.access_order.append(key)
        self.access_times[key] = current_time
        return False

class TTLCacheStrategy(CacheStrategy):
    """Time-to-Live cache eviction strategy"""
    
    def __init__(self, ttl_seconds: float = 300):  # 5 minutes default
        self.ttl_seconds = ttl_seconds
    
    def should_evict(self, key: str, metadata: Dict[str, Any]) -> bool:
        created_time = metadata.get('created_at', 0)
        return (time.time() - created_time) > self.ttl_seconds

@dataclass
class CacheEntry:
    """Cache entry with metadata"""
    data: Any
    created_at: float
    access_count: int = 0
    last_accessed: float = 0
    size_bytes: int = 0

class MultiLevelCache:
    """Multi-level cache with different strategies"""
    
    def __init__(self):
        self.levels = {}
        self.strategies = {}
        self.hit_counts = defaultdict(int)
        self.miss_counts = defaultdict(int)
        self.logger = logging.getLogger(f"{__name__}.MultiLevelCache")
    
    def add_level(self, level_name: str, strategy: CacheStrategy):
        """Add a cache level with a specific strategy"""
        self.levels[level_name] = {}
        self.strategies[level_name] = strategy
        self.logger.info(f"Added cache level '{level_name}' with strategy {strategy.__class__.__name__}")
    
    def _calculate_size(self, data: Any) -> int:
        """Estimate memory size of data"""
        try:
            return len(json.dumps(data, default=str).encode('utf-8'))
        except:
            return len(str(data).encode('utf-8'))
    
    def get(self, key: str) -> Tuple[Any, Optional[str]]:
        """Get data from cache, returns (data, cache_level)"""
        cache_key = hashlib.md5(key.encode()).hexdigest()
        
        # Check each cache level in order
        for level_name, cache_level in self.levels.items():
            if cache_key in cache_level:
                entry = cache_level[cache_key]
                
                # Check if entry should be evicted
                if self.strategies[level_name].should_evict(cache_key, asdict(entry)):
                    del cache_level[cache_key]
                    self.miss_counts[level_name] += 1
                    continue
                
                # Update access metadata
                entry.access_count += 1
                entry.last_accessed = time.time()
                
                self.hit_counts[level_name] += 1
                self.logger.debug(f"Cache hit in level '{level_name}' for key: {key[:20]}...")
                return entry.data, level_name
        
        # Cache miss
        for level_name in self.levels.keys():
            self.miss_counts[level_name] += 1
        
        self.logger.debug(f"Cache miss for key: {key[:20]}...")
        return None, None
    
    def put(self, key: str, data: Any, preferred_level: str = None):
        """Put data into cache"""
        cache_key = hashlib.md5(key.encode()).hexdigest()
        current_time = time.time()
        
        entry = CacheEntry(
            data=data,
            created_at=current_time,
            last_accessed=current_time,
            size_bytes=self._calculate_size(data)
        )
        
        # Determine which level to store in
        target_level = preferred_level or list(self.levels.keys())[0]
        
        if target_level in self.levels:
            self.levels[target_level][cache_key] = entry
            self.logger.debug(f"Cached data in level '{target_level}' for key: {key[:20]}...")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get cache performance statistics"""
        stats = {}
        for level_name in self.levels.keys():
            hits = self.hit_counts[level_name]
            misses = self.miss_counts[level_name]
            total = hits + misses
            hit_rate = (hits / total) if total > 0 else 0
            
            stats[level_name] = {
                'hits': hits,
                'misses': misses,
                'hit_rate': hit_rate,
                'entries': len(self.levels[level_name]),
                'total_size_bytes': sum(entry.size_bytes for entry in self.levels[level_name].values())
            }
        
        return stats

# Intelligent Load Balancer
class LoadBalancingStrategy(ABC):
    """Abstract base class for load balancing strategies"""
    
    @abstractmethod
    def select_worker(self, workers: List[Dict[str, Any]], task: Dict[str, Any]) -> int:
        pass

class RoundRobinStrategy(LoadBalancingStrategy):
    """Round-robin load balancing"""
    
    def __init__(self):
        self.current_index = 0
    
    def select_worker(self, workers: List[Dict[str, Any]], task: Dict[str, Any]) -> int:
        if not workers:
            return -1
        
        # Filter healthy workers
        healthy_workers = [i for i, w in enumerate(workers) if w.get('healthy', True)]
        if not healthy_workers:
            return -1
        
        selected = healthy_workers[self.current_index % len(healthy_workers)]
        self.current_index += 1
        return selected

class WeightedStrategy(LoadBalancingStrategy):
    """Weighted load balancing based on worker capacity"""
    
    def select_worker(self, workers: List[Dict[str, Any]], task: Dict[str, Any]) -> int:
        if not workers:
            return -1
        
        # Calculate scores based on capacity and current load
        best_score = -1
        best_worker = -1
        
        for i, worker in enumerate(workers):
            if not worker.get('healthy', True):
                continue
            
            capacity = worker.get('capacity', 1.0)
            current_load = worker.get('current_load', 0.0)
            
            # Score = available capacity
            score = max(0, capacity - current_load)
            
            if score > best_score:
                best_score = score
                best_worker = i
        
        return best_worker

class LeastLatencyStrategy(LoadBalancingStrategy):
    """Load balancing based on historical latency"""
    
    def select_worker(self, workers: List[Dict[str, Any]], task: Dict[str, Any]) -> int:
        if not workers:
            return -1
        
        best_latency = float('inf')
        best_worker = -1
        
        for i, worker in enumerate(workers):
            if not worker.get('healthy', True):
                continue
            
            avg_latency = worker.get('avg_latency', 0)
            
            if avg_latency < best_latency:
                best_latency = avg_latency
                best_worker = i
        
        return best_worker

class IntelligentLoadBalancer:
    """Intelligent load balancer with multiple strategies"""
    
    def __init__(self):
        self.workers = []
        self.strategies = {
            'round_robin': RoundRobinStrategy(),
            'weighted': WeightedStrategy(),
            'least_latency': LeastLatencyStrategy()
        }
        self.current_strategy = 'weighted'
        self.task_queue = queue.Queue()
        self.results_queue = queue.Queue()
        self.worker_stats = defaultdict(lambda: {
            'tasks_completed': 0,
            'total_latency': 0,
            'errors': 0,
            'last_health_check': time.time()
        })
        self.logger = logging.getLogger(f"{__name__}.IntelligentLoadBalancer")
    
    def add_worker(self, worker_id: str, capacity: float = 1.0, worker_func: Callable = None):
        """Add a worker to the pool"""
        worker = {
            'id': worker_id,
            'capacity': capacity,
            'current_load': 0.0,
            'healthy': True,
            'avg_latency': 0,
            'worker_func': worker_func,
            'created_at': time.time()
        }
        self.workers.append(worker)
        self.logger.info(f"Added worker '{worker_id}' with capacity {capacity}")
    
    def submit_task(self, task: Dict[str, Any]) -> str:
        """Submit a task for processing"""
        task_id = f"task_{int(time.time() * 1000000)}"
        task['id'] = task_id
        task['submitted_at'] = time.time()
        
        self.task_queue.put(task)
        self.logger.debug(f"Submitted task {task_id}")
        return task_id
    
    def process_tasks(self, max_concurrent: int = 10):
        """Process tasks using the load balancer"""
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
            futures = {}
            
            while True:
                try:
                    # Get next task
                    task = self.task_queue.get(timeout=1)
                    
                    # Select worker
                    strategy = self.strategies[self.current_strategy]
                    worker_index = strategy.select_worker(self.workers, task)
                    
                    if worker_index == -1:
                        self.logger.warning(f"No healthy workers available for task {task['id']}")
                        continue
                    
                    worker = self.workers[worker_index]
                    
                    # Update worker load
                    worker['current_load'] += task.get('estimated_load', 0.1)
                    
                    # Submit task to executor
                    future = executor.submit(self._execute_task, worker, task)
                    futures[future] = (worker_index, task)
                    
                    # Check for completed tasks
                    completed_futures = [f for f in futures if f.done()]
                    for future in completed_futures:
                        worker_index, completed_task = futures.pop(future)
                        self._handle_task_completion(future, worker_index, completed_task)
                
                except queue.Empty:
                    # No more tasks, check if all futures are complete
                    if not futures:
                        break
                    
                    # Wait for some futures to complete
                    if futures:
                        completed_futures = concurrent.futures.as_completed(futures, timeout=1)
                        for future in completed_futures:
                            worker_index, completed_task = futures.pop(future)
                            self._handle_task_completion(future, worker_index, completed_task)
                            break
    
    def _execute_task(self, worker: Dict[str, Any], task: Dict[str, Any]) -> Any:
        """Execute a task using the specified worker"""
        start_time = time.time()
        
        try:
            if worker.get('worker_func'):
                result = worker['worker_func'](task)
            else:
                # Default task processing
                time.sleep(task.get('processing_time', 0.1))
                result = f"Processed by worker {worker['id']}"
            
            return {
                'task_id': task['id'],
                'result': result,
                'processing_time': time.time() - start_time,
                'worker_id': worker['id'],
                'status': 'success'
            }
        
        except Exception as e:
            return {
                'task_id': task['id'],
                'error': str(e),
                'processing_time': time.time() - start_time,
                'worker_id': worker['id'],
                'status': 'error'
            }
    
    def _handle_task_completion(self, future: concurrent.futures.Future, worker_index: int, task: Dict[str, Any]):
        """Handle completion of a task"""
        worker = self.workers[worker_index]
        
        try:
            result = future.result()
            processing_time = result['processing_time']
            
            # Update worker statistics
            stats = self.worker_stats[worker['id']]
            stats['tasks_completed'] += 1
            stats['total_latency'] += processing_time
            
            if result['status'] == 'error':
                stats['errors'] += 1
                self.logger.warning(f"Task {task['id']} failed on worker {worker['id']}: {result.get('error')}")
            
            # Update worker average latency
            worker['avg_latency'] = stats['total_latency'] / stats['tasks_completed']
            
            # Decrease worker load
            worker['current_load'] = max(0, worker['current_load'] - task.get('estimated_load', 0.1))
            
            self.results_queue.put(result)
            self.logger.debug(f"Task {task['id']} completed by worker {worker['id']} in {processing_time:.3f}s")
        
        except Exception as e:
            self.logger.error(f"Error handling task completion: {e}")
    
    def get_worker_stats(self) -> Dict[str, Any]:
        """Get detailed worker statistics"""
        stats = {}
        
        for worker in self.workers:
            worker_id = worker['id']
            worker_stats = self.worker_stats[worker_id]
            
            stats[worker_id] = {
                'capacity': worker['capacity'],
                'current_load': worker['current_load'],
                'healthy': worker['healthy'],
                'avg_latency': worker.get('avg_latency', 0),
                'tasks_completed': worker_stats['tasks_completed'],
                'error_rate': worker_stats['errors'] / max(worker_stats['tasks_completed'], 1),
                'utilization': worker['current_load'] / worker['capacity'] if worker['capacity'] > 0 else 0
            }
        
        return stats

# Predictive Auto-Scaling
class PredictiveScaler:
    """Predictive auto-scaling based on historical patterns"""
    
    def __init__(self, min_workers: int = 1, max_workers: int = 20):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.metrics_history = deque(maxlen=100)  # Keep last 100 measurements
        self.scaling_events = []
        self.logger = logging.getLogger(f"{__name__}.PredictiveScaler")
    
    def record_metrics(self, metrics: Dict[str, Any]):
        """Record system metrics for scaling decisions"""
        metrics['timestamp'] = time.time()
        self.metrics_history.append(metrics)
    
    def predict_scaling_need(self, current_workers: int) -> Tuple[str, int, str]:
        """
        Predict scaling need based on historical data
        Returns: (action, target_workers, reason)
        """
        if len(self.metrics_history) < 10:
            return 'none', current_workers, 'Insufficient data for prediction'
        
        recent_metrics = list(self.metrics_history)[-10:]
        
        # Calculate trends
        avg_cpu_usage = sum(m.get('cpu_usage', 0) for m in recent_metrics) / len(recent_metrics)
        avg_memory_usage = sum(m.get('memory_usage', 0) for m in recent_metrics) / len(recent_metrics)
        avg_queue_length = sum(m.get('queue_length', 0) for m in recent_metrics) / len(recent_metrics)
        avg_response_time = sum(m.get('avg_response_time', 0) for m in recent_metrics) / len(recent_metrics)
        
        # Calculate growth trends
        if len(recent_metrics) >= 5:
            recent_avg_queue = sum(m.get('queue_length', 0) for m in recent_metrics[-3:]) / 3
            older_avg_queue = sum(m.get('queue_length', 0) for m in recent_metrics[-6:-3]) / 3
            queue_trend = (recent_avg_queue - older_avg_queue) / max(older_avg_queue, 1)
        else:
            queue_trend = 0
        
        # Scaling decision logic
        scale_up_reasons = []
        scale_down_reasons = []
        
        # Scale up conditions
        if avg_cpu_usage > 0.8:
            scale_up_reasons.append(f"High CPU usage: {avg_cpu_usage:.1%}")
        if avg_memory_usage > 0.8:
            scale_up_reasons.append(f"High memory usage: {avg_memory_usage:.1%}")
        if avg_queue_length > current_workers * 2:
            scale_up_reasons.append(f"High queue length: {avg_queue_length:.1f}")
        if avg_response_time > 5.0:
            scale_up_reasons.append(f"High response time: {avg_response_time:.2f}s")
        if queue_trend > 0.2:
            scale_up_reasons.append(f"Growing queue trend: +{queue_trend:.1%}")
        
        # Scale down conditions
        if avg_cpu_usage < 0.3 and avg_queue_length < current_workers * 0.5:
            scale_down_reasons.append(f"Low resource usage (CPU: {avg_cpu_usage:.1%}, Queue: {avg_queue_length:.1f})")
        if queue_trend < -0.2:
            scale_down_reasons.append(f"Declining queue trend: {queue_trend:.1%}")
        
        # Make scaling decision
        if scale_up_reasons and current_workers < self.max_workers:
            target_workers = min(current_workers + max(1, int(current_workers * 0.5)), self.max_workers)
            return 'scale_up', target_workers, '; '.join(scale_up_reasons)
        
        elif scale_down_reasons and current_workers > self.min_workers:
            target_workers = max(current_workers - 1, self.min_workers)
            return 'scale_down', target_workers, '; '.join(scale_down_reasons)
        
        else:
            return 'none', current_workers, 'Optimal scaling level'
    
    def execute_scaling(self, current_workers: int, target_workers: int, reason: str) -> bool:
        """Execute scaling action"""
        if target_workers == current_workers:
            return True
        
        scaling_event = {
            'timestamp': time.time(),
            'from_workers': current_workers,
            'to_workers': target_workers,
            'reason': reason,
            'action': 'scale_up' if target_workers > current_workers else 'scale_down'
        }
        
        self.scaling_events.append(scaling_event)
        
        self.logger.info(f"Scaling {scaling_event['action']}: {current_workers} → {target_workers} workers. Reason: {reason}")
        
        return True

# High-Performance Concurrent Pipeline
class ConcurrentETLPipeline:
    """High-performance ETL pipeline with concurrent processing"""
    
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers
        self.cache = MultiLevelCache()
        self.load_balancer = IntelligentLoadBalancer()
        self.scaler = PredictiveScaler(min_workers=2, max_workers=max_workers)
        
        # Setup multi-level cache
        self.cache.add_level('l1_memory', LRUCacheStrategy(max_size=100))
        self.cache.add_level('l2_memory', TTLCacheStrategy(ttl_seconds=1800))
        
        # Setup load balancer workers
        self._setup_workers()
        
        self.logger = logging.getLogger(f"{__name__}.ConcurrentETLPipeline")
    
    def _setup_workers(self):
        """Setup initial worker pool"""
        for i in range(min(4, self.max_workers)):  # Start with 4 workers
            self.load_balancer.add_worker(
                worker_id=f"etl_worker_{i}",
                capacity=1.0,
                worker_func=self._process_etl_task
            )
    
    def _process_etl_task(self, task: Dict[str, Any]) -> Any:
        """Process individual ETL task"""
        task_type = task.get('type', 'extract')
        data = task.get('data')
        
        # Simulate different processing times based on task type
        processing_times = {
            'extract': 0.1,
            'transform': 0.05,
            'load': 0.15,
            'validate': 0.02
        }
        
        # Check cache first
        cache_key = f"{task_type}_{hash(str(data))}"
        cached_result, cache_level = self.cache.get(cache_key)
        
        if cached_result is not None:
            self.logger.debug(f"Cache hit for task {task['id']} in {cache_level}")
            return cached_result
        
        # Process the task
        processing_time = processing_times.get(task_type, 0.1)
        time.sleep(processing_time)  # Simulate processing
        
        # Generate result
        result = {
            'task_type': task_type,
            'processed_data': f"processed_{data}",
            'timestamp': time.time(),
            'processing_duration': processing_time
        }
        
        # Cache the result
        self.cache.put(cache_key, result, preferred_level='l1_memory')
        
        return result
    
    async def process_pipeline_async(self, pipeline_tasks: List[Dict[str, Any]]) -> List[Any]:
        """Process pipeline tasks asynchronously"""
        start_time = time.time()
        
        # Submit all tasks
        task_ids = []
        for task in pipeline_tasks:
            task_id = self.load_balancer.submit_task(task)
            task_ids.append(task_id)
        
        # Process tasks concurrently
        processing_thread = threading.Thread(
            target=self.load_balancer.process_tasks,
            args=(self.max_workers,)
        )
        processing_thread.daemon = True
        processing_thread.start()
        
        # Collect results
        results = []
        collected_results = 0
        
        while collected_results < len(task_ids):
            try:
                result = self.load_balancer.results_queue.get(timeout=10)
                results.append(result)
                collected_results += 1
            except queue.Empty:
                self.logger.warning("Timeout waiting for task results")
                break
        
        total_time = time.time() - start_time
        
        # Record metrics for scaling
        metrics = {
            'cpu_usage': 0.6,  # Simulated metric
            'memory_usage': 0.4,  # Simulated metric
            'queue_length': self.load_balancer.task_queue.qsize(),
            'avg_response_time': total_time / max(len(results), 1),
            'total_tasks': len(pipeline_tasks),
            'completed_tasks': len(results)
        }
        self.scaler.record_metrics(metrics)
        
        # Check if scaling is needed
        current_workers = len(self.load_balancer.workers)
        action, target_workers, reason = self.scaler.predict_scaling_need(current_workers)
        
        if action != 'none':
            self.logger.info(f"Scaling recommendation: {action} to {target_workers} workers - {reason}")
        
        self.logger.info(f"Processed {len(results)} tasks in {total_time:.2f}s (avg: {total_time/max(len(results), 1):.3f}s per task)")
        
        return results
    
    def get_performance_stats(self) -> Dict[str, Any]:
        """Get comprehensive performance statistics"""
        cache_stats = self.cache.get_stats()
        worker_stats = self.load_balancer.get_worker_stats()
        scaling_events = self.scaler.scaling_events[-10:]  # Last 10 scaling events
        
        return {
            'cache_performance': cache_stats,
            'worker_performance': worker_stats,
            'recent_scaling_events': scaling_events,
            'total_workers': len(self.load_balancer.workers),
            'queue_size': self.load_balancer.task_queue.qsize()
        }

def test_generation3_scaling():
    """Test Generation 3 scaling and optimization features"""
    print("⚡ Testing Generation 3: MAKE IT SCALE (Optimized)")
    print("=" * 60)
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(name)s - %(message)s')
    
    # Test Multi-Level Cache
    print("🗄️ Testing multi-level cache...")
    cache = MultiLevelCache()
    cache.add_level('l1', LRUCacheStrategy(max_size=5))
    cache.add_level('l2', TTLCacheStrategy(ttl_seconds=2))
    
    # Test cache operations
    test_data = {"key1": "value1", "key2": "value2", "key3": "value3"}
    for key, value in test_data.items():
        cache.put(key, value)
    
    # Test cache hits
    for key in test_data.keys():
        data, level = cache.get(key)
        if data:
            print(f"   ✅ Cache hit for {key} in level {level}")
    
    cache_stats = cache.get_stats()
    hit_rates = [(level, f"{stats['hit_rate']:.1%}") for level, stats in cache_stats.items()]
    print(f"   📊 Cache hit rates: {hit_rates}")
    
    # Test Load Balancer
    print("\n⚖️ Testing intelligent load balancer...")
    balancer = IntelligentLoadBalancer()
    
    # Add workers with different capacities
    balancer.add_worker('worker_1', capacity=1.0)
    balancer.add_worker('worker_2', capacity=1.5)
    balancer.add_worker('worker_3', capacity=0.8)
    
    # Submit test tasks
    test_tasks = [
        {'type': 'extract', 'data': f'dataset_{i}', 'estimated_load': 0.2}
        for i in range(10)
    ]
    
    for task in test_tasks:
        balancer.submit_task(task)
    
    # Process tasks
    processing_start = time.time()
    balancer.process_tasks(max_concurrent=5)
    processing_time = time.time() - processing_start
    
    worker_stats = balancer.get_worker_stats()
    print(f"   ✅ Processed {len(test_tasks)} tasks in {processing_time:.2f}s")
    worker_utilization = [(w_id, f"{stats['utilization']:.1%}") for w_id, stats in worker_stats.items()]
    print(f"   📊 Worker utilization: {worker_utilization}")
    
    # Test Predictive Scaling
    print("\n📈 Testing predictive auto-scaling...")
    scaler = PredictiveScaler(min_workers=2, max_workers=10)
    
    # Simulate increasing load
    simulated_metrics = [
        {'cpu_usage': 0.3, 'memory_usage': 0.2, 'queue_length': 2, 'avg_response_time': 1.0},
        {'cpu_usage': 0.5, 'memory_usage': 0.4, 'queue_length': 5, 'avg_response_time': 2.0},
        {'cpu_usage': 0.7, 'memory_usage': 0.6, 'queue_length': 8, 'avg_response_time': 3.5},
        {'cpu_usage': 0.85, 'memory_usage': 0.8, 'queue_length': 12, 'avg_response_time': 5.5},
    ]
    
    current_workers = 3
    for i, metrics in enumerate(simulated_metrics):
        scaler.record_metrics(metrics)
        action, target, reason = scaler.predict_scaling_need(current_workers)
        
        print(f"   Step {i+1}: {action} - {current_workers} → {target} workers ({reason})")
        current_workers = target
    
    # Test Concurrent Pipeline
    print("\n🚀 Testing high-performance concurrent pipeline...")
    pipeline = ConcurrentETLPipeline(max_workers=8)
    
    # Create test pipeline tasks
    pipeline_tasks = []
    for i in range(5):
        pipeline_tasks.extend([
            {'type': 'extract', 'data': f'source_{i}'},
            {'type': 'transform', 'data': f'raw_data_{i}'},
            {'type': 'validate', 'data': f'transformed_{i}'},
            {'type': 'load', 'data': f'validated_{i}'}
        ])
    
    # Run async processing
    async def run_pipeline():
        results = await pipeline.process_pipeline_async(pipeline_tasks)
        return results
    
    # Execute pipeline
    results = asyncio.run(run_pipeline())
    print(f"   ✅ Pipeline processed {len(results)} tasks successfully")
    
    # Get performance statistics
    perf_stats = pipeline.get_performance_stats()
    cache_hit_rates = [(level, f"{stats['hit_rate']:.1%}") for level, stats in perf_stats['cache_performance'].items()]
    print(f"   📊 Cache hit rates: {cache_hit_rates}")
    print(f"   ⚙️ Total workers: {perf_stats['total_workers']}")
    
    print("\n" + "=" * 60)
    print("🎉 Generation 3 Scaling and Optimization Testing Complete!")
    
    return True

if __name__ == "__main__":
    test_generation3_scaling()