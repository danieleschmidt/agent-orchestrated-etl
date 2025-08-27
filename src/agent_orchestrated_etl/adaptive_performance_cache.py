"""
Adaptive Performance Cache System
Intelligent multi-tier caching with ML-driven optimization and predictive pre-loading.
"""
import asyncio
import hashlib
import json
import logging
import pickle
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import statistics
import zlib

logger = logging.getLogger(__name__)


class CachePolicy(Enum):
    LRU = "lru"  # Least Recently Used
    LFU = "lfu"  # Least Frequently Used
    ADAPTIVE = "adaptive"  # ML-driven adaptive policy
    TTL = "ttl"  # Time To Live
    PREDICTIVE = "predictive"  # Predictive pre-loading


class CacheLevel(Enum):
    MEMORY = "memory"
    DISK = "disk"
    DISTRIBUTED = "distributed"
    PERSISTENT = "persistent"


@dataclass
class CacheEntry:
    key: str
    value: Any
    created_at: datetime
    last_accessed: datetime
    access_count: int
    size_bytes: int
    ttl: Optional[float]
    compression_ratio: float = 1.0
    prediction_score: float = 0.0


@dataclass
class CacheMetrics:
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    size_bytes: int = 0
    avg_access_time: float = 0.0
    hit_rate: float = 0.0
    memory_efficiency: float = 0.0


@dataclass
class AccessPattern:
    key: str
    timestamp: datetime
    access_type: str  # read, write, delete
    execution_time: float
    context: Dict[str, Any]


class AdaptivePerformanceCache:
    """Intelligent multi-tier cache with ML-driven optimization."""
    
    def __init__(self, name: str, config: Optional[Dict[str, Any]] = None):
        self.name = name
        self.config = config or {}
        
        # Cache storage layers
        self.memory_cache: Dict[str, CacheEntry] = {}
        self.disk_cache_path = Path(f"cache/{name}")
        self.disk_cache_path.mkdir(parents=True, exist_ok=True)
        
        # Configuration
        self.max_memory_size = self.config.get("max_memory_mb", 100) * 1024 * 1024
        self.max_disk_size = self.config.get("max_disk_mb", 500) * 1024 * 1024
        self.default_ttl = self.config.get("default_ttl", 3600)  # 1 hour
        self.compression_threshold = self.config.get("compression_threshold", 1024)
        
        # Metrics and learning
        self.metrics = CacheMetrics()
        self.access_patterns: List[AccessPattern] = []
        self.prediction_models: Dict[str, Any] = {}
        
        # Adaptive parameters
        self.cache_policy = CachePolicy(self.config.get("policy", "adaptive"))
        self.learning_window = self.config.get("learning_window", 1000)
        self.prediction_threshold = self.config.get("prediction_threshold", 0.7)
        
        # Background tasks
        self._background_tasks: List[asyncio.Task] = []
        self._start_background_tasks()
        
        logger.info(f"Initialized adaptive cache '{name}' with policy {self.cache_policy.value}")
    
    def _start_background_tasks(self) -> None:
        """Start background maintenance tasks."""
        loop = asyncio.get_event_loop()
        
        # Cache cleanup and optimization
        cleanup_task = loop.create_task(self._background_cleanup())
        self._background_tasks.append(cleanup_task)
        
        # Pattern analysis and prediction model updates
        analysis_task = loop.create_task(self._background_analysis())
        self._background_tasks.append(analysis_task)
        
        # Predictive pre-loading
        preload_task = loop.create_task(self._background_preload())
        self._background_tasks.append(preload_task)
    
    async def get(self, key: str, context: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        """Retrieve value from cache with adaptive optimization."""
        start_time = time.time()
        context = context or {}
        
        # Record access pattern
        await self._record_access_pattern(key, "read", 0.0, context)
        
        # Try memory cache first
        if key in self.memory_cache:
            entry = self.memory_cache[key]
            
            # Check TTL expiration
            if self._is_expired(entry):
                await self._evict_entry(key)
                self.metrics.misses += 1
                return None
            
            # Update access metadata
            entry.last_accessed = datetime.now()
            entry.access_count += 1
            
            # Update metrics
            self.metrics.hits += 1
            access_time = time.time() - start_time
            self._update_avg_access_time(access_time)
            
            logger.debug(f"Cache hit for key '{key}' in memory")
            return entry.value
        
        # Try disk cache
        disk_value = await self._get_from_disk(key)
        if disk_value is not None:
            # Promote to memory cache if it fits
            await self._promote_to_memory(key, disk_value, context)
            
            self.metrics.hits += 1
            access_time = time.time() - start_time
            self._update_avg_access_time(access_time)
            
            logger.debug(f"Cache hit for key '{key}' from disk")
            return disk_value
        
        # Cache miss
        self.metrics.misses += 1
        self._update_hit_rate()
        
        # Trigger predictive loading for similar keys
        await self._trigger_predictive_loading(key, context)
        
        logger.debug(f"Cache miss for key '{key}'")
        return None
    
    async def set(self, key: str, value: Any, 
                  ttl: Optional[float] = None,
                  context: Optional[Dict[str, Any]] = None) -> None:
        """Store value in cache with intelligent placement."""
        start_time = time.time()
        context = context or {}
        ttl = ttl or self.default_ttl
        
        # Serialize and potentially compress the value
        serialized_value, size_bytes, compression_ratio = await self._serialize_value(value)
        
        # Create cache entry
        entry = CacheEntry(
            key=key,
            value=value,
            created_at=datetime.now(),
            last_accessed=datetime.now(),
            access_count=1,
            size_bytes=size_bytes,
            ttl=ttl,
            compression_ratio=compression_ratio,
            prediction_score=self._calculate_prediction_score(key, context)
        )
        
        # Decide on cache level placement
        cache_level = await self._decide_cache_level(entry, context)
        
        if cache_level == CacheLevel.MEMORY:
            # Ensure memory capacity
            await self._ensure_memory_capacity(size_bytes)
            self.memory_cache[key] = entry
            self.metrics.size_bytes += size_bytes
            logger.debug(f"Stored '{key}' in memory cache ({size_bytes} bytes)")
        
        elif cache_level == CacheLevel.DISK:
            await self._store_to_disk(key, serialized_value, entry)
            logger.debug(f"Stored '{key}' in disk cache ({size_bytes} bytes)")
        
        # Record access pattern
        execution_time = time.time() - start_time
        await self._record_access_pattern(key, "write", execution_time, context)
        
        # Update prediction models
        await self._update_prediction_models(key, context, entry)
    
    async def delete(self, key: str, context: Optional[Dict[str, Any]] = None) -> bool:
        """Delete entry from cache."""
        context = context or {}
        deleted = False
        
        # Remove from memory cache
        if key in self.memory_cache:
            entry = self.memory_cache[key]
            del self.memory_cache[key]
            self.metrics.size_bytes -= entry.size_bytes
            deleted = True
            logger.debug(f"Deleted '{key}' from memory cache")
        
        # Remove from disk cache
        disk_path = self.disk_cache_path / f"{self._hash_key(key)}.cache"
        if disk_path.exists():
            disk_path.unlink()
            deleted = True
            logger.debug(f"Deleted '{key}' from disk cache")
        
        # Record access pattern
        await self._record_access_pattern(key, "delete", 0.0, context)
        
        return deleted
    
    async def _get_from_disk(self, key: str) -> Optional[Any]:
        """Retrieve value from disk cache."""
        try:
            disk_path = self.disk_cache_path / f"{self._hash_key(key)}.cache"
            if not disk_path.exists():
                return None
            
            # Load metadata and check expiration
            metadata_path = self.disk_cache_path / f"{self._hash_key(key)}.meta"
            if metadata_path.exists():
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                created_at = datetime.fromisoformat(metadata['created_at'])
                ttl = metadata.get('ttl')
                
                if ttl and (datetime.now() - created_at).total_seconds() > ttl:
                    # Expired - clean up
                    disk_path.unlink()
                    metadata_path.unlink()
                    return None
            
            # Load the actual data
            with open(disk_path, 'rb') as f:
                compressed_data = f.read()
            
            # Decompress if needed
            try:
                data = zlib.decompress(compressed_data)
                return pickle.loads(data)
            except zlib.error:
                # Not compressed
                return pickle.loads(compressed_data)
        
        except Exception as e:
            logger.warning(f"Failed to load '{key}' from disk cache: {e}")
            return None
    
    async def _store_to_disk(self, key: str, serialized_value: bytes, 
                           entry: CacheEntry) -> None:
        """Store value to disk cache."""
        try:
            disk_path = self.disk_cache_path / f"{self._hash_key(key)}.cache"
            metadata_path = self.disk_cache_path / f"{self._hash_key(key)}.meta"
            
            # Store the data
            with open(disk_path, 'wb') as f:
                f.write(serialized_value)
            
            # Store metadata
            metadata = {
                'key': key,
                'created_at': entry.created_at.isoformat(),
                'ttl': entry.ttl,
                'size_bytes': entry.size_bytes,
                'compression_ratio': entry.compression_ratio
            }
            
            with open(metadata_path, 'w') as f:
                json.dump(metadata, f)
        
        except Exception as e:
            logger.error(f"Failed to store '{key}' to disk cache: {e}")
    
    async def _serialize_value(self, value: Any) -> Tuple[bytes, int, float]:
        """Serialize and optionally compress value."""
        try:
            # Serialize
            serialized = pickle.dumps(value)
            original_size = len(serialized)
            
            # Compress if above threshold
            if original_size > self.compression_threshold:
                compressed = zlib.compress(serialized, level=6)
                if len(compressed) < original_size * 0.9:  # Only if significant compression
                    compression_ratio = len(compressed) / original_size
                    return compressed, len(compressed), compression_ratio
            
            return serialized, original_size, 1.0
        
        except Exception as e:
            logger.error(f"Failed to serialize value: {e}")
            raise
    
    def _hash_key(self, key: str) -> str:
        """Generate hash for key to use as filename."""
        return hashlib.md5(key.encode()).hexdigest()
    
    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired."""
        if not entry.ttl:
            return False
        
        age = (datetime.now() - entry.created_at).total_seconds()
        return age > entry.ttl
    
    async def _decide_cache_level(self, entry: CacheEntry, 
                                context: Dict[str, Any]) -> CacheLevel:
        """Intelligently decide which cache level to use."""
        # High-priority or frequently accessed items go to memory
        if (context.get('priority') == 'high' or 
            entry.prediction_score > self.prediction_threshold):
            return CacheLevel.MEMORY
        
        # Large items or lower priority go to disk
        if entry.size_bytes > self.max_memory_size * 0.1:  # > 10% of memory limit
            return CacheLevel.DISK
        
        # Check memory availability
        if self.metrics.size_bytes + entry.size_bytes <= self.max_memory_size:
            return CacheLevel.MEMORY
        
        return CacheLevel.DISK
    
    async def _promote_to_memory(self, key: str, value: Any, 
                               context: Dict[str, Any]) -> None:
        """Promote frequently accessed disk items to memory."""
        try:
            serialized_value, size_bytes, compression_ratio = await self._serialize_value(value)
            
            # Only promote if there's space and it's worthwhile
            if (self.metrics.size_bytes + size_bytes <= self.max_memory_size and
                context.get('promote', True)):
                
                entry = CacheEntry(
                    key=key,
                    value=value,
                    created_at=datetime.now(),
                    last_accessed=datetime.now(),
                    access_count=1,
                    size_bytes=size_bytes,
                    ttl=self.default_ttl,
                    compression_ratio=compression_ratio
                )
                
                await self._ensure_memory_capacity(size_bytes)
                self.memory_cache[key] = entry
                self.metrics.size_bytes += size_bytes
                
                logger.debug(f"Promoted '{key}' from disk to memory cache")
        
        except Exception as e:
            logger.warning(f"Failed to promote '{key}' to memory: {e}")
    
    async def _ensure_memory_capacity(self, required_bytes: int) -> None:
        """Ensure sufficient memory capacity by evicting entries if needed."""
        while (self.metrics.size_bytes + required_bytes > self.max_memory_size and 
               self.memory_cache):
            
            # Select victim based on cache policy
            victim_key = await self._select_eviction_victim()
            if victim_key:
                await self._evict_entry(victim_key)
                self.metrics.evictions += 1
            else:
                break  # No more victims available
    
    async def _select_eviction_victim(self) -> Optional[str]:
        """Select entry to evict based on cache policy."""
        if not self.memory_cache:
            return None
        
        if self.cache_policy == CachePolicy.LRU:
            # Least Recently Used
            oldest_key = min(
                self.memory_cache.keys(),
                key=lambda k: self.memory_cache[k].last_accessed
            )
            return oldest_key
        
        elif self.cache_policy == CachePolicy.LFU:
            # Least Frequently Used
            least_used_key = min(
                self.memory_cache.keys(),
                key=lambda k: self.memory_cache[k].access_count
            )
            return least_used_key
        
        elif self.cache_policy == CachePolicy.ADAPTIVE:
            # ML-driven adaptive eviction
            return await self._adaptive_eviction_selection()
        
        else:
            # Default to LRU
            oldest_key = min(
                self.memory_cache.keys(),
                key=lambda k: self.memory_cache[k].last_accessed
            )
            return oldest_key
    
    async def _adaptive_eviction_selection(self) -> Optional[str]:
        """Use ML-driven approach to select eviction victim."""
        if not self.memory_cache:
            return None
        
        # Calculate eviction scores for each entry
        scores = {}
        
        for key, entry in self.memory_cache.items():
            # Factors for eviction score (lower = more likely to evict)
            age_score = (datetime.now() - entry.last_accessed).total_seconds()
            frequency_score = 1.0 / max(1, entry.access_count)
            size_score = entry.size_bytes / self.max_memory_size
            prediction_score = 1.0 - entry.prediction_score
            
            # Weighted combination
            eviction_score = (
                age_score * 0.3 +
                frequency_score * 0.3 +
                size_score * 0.2 +
                prediction_score * 0.2
            )
            
            scores[key] = eviction_score
        
        # Return key with highest eviction score (most suitable for eviction)
        return max(scores.keys(), key=lambda k: scores[k])
    
    async def _evict_entry(self, key: str) -> None:
        """Evict entry from memory cache."""
        if key in self.memory_cache:
            entry = self.memory_cache[key]
            
            # Consider storing to disk if valuable
            if (entry.access_count > 1 and 
                entry.prediction_score > 0.3):
                try:
                    serialized_value, _, _ = await self._serialize_value(entry.value)
                    await self._store_to_disk(key, serialized_value, entry)
                    logger.debug(f"Evicted '{key}' from memory to disk")
                except Exception as e:
                    logger.warning(f"Failed to store evicted entry to disk: {e}")
            
            # Remove from memory
            self.metrics.size_bytes -= entry.size_bytes
            del self.memory_cache[key]
            
            logger.debug(f"Evicted '{key}' from memory cache")
    
    def _calculate_prediction_score(self, key: str, context: Dict[str, Any]) -> float:
        """Calculate prediction score for how likely this key is to be accessed again."""
        score = 0.0
        
        # Context-based scoring
        if context.get('priority') == 'high':
            score += 0.3
        elif context.get('priority') == 'medium':
            score += 0.1
        
        # Pattern-based scoring (simplified)
        recent_patterns = self.access_patterns[-100:] if self.access_patterns else []
        similar_keys = [p.key for p in recent_patterns if key in p.key or p.key in key]
        
        if similar_keys:
            score += min(0.4, len(similar_keys) / 10)
        
        # Time-based patterns (simplified)
        current_hour = datetime.now().hour
        hour_patterns = [p for p in recent_patterns if p.timestamp.hour == current_hour]
        
        if hour_patterns:
            score += min(0.3, len(hour_patterns) / 20)
        
        return min(1.0, score)
    
    async def _record_access_pattern(self, key: str, access_type: str,
                                   execution_time: float, 
                                   context: Dict[str, Any]) -> None:
        """Record access pattern for learning."""
        pattern = AccessPattern(
            key=key,
            timestamp=datetime.now(),
            access_type=access_type,
            execution_time=execution_time,
            context=context.copy()
        )
        
        self.access_patterns.append(pattern)
        
        # Limit pattern history
        if len(self.access_patterns) > self.learning_window:
            self.access_patterns.pop(0)
    
    async def _trigger_predictive_loading(self, key: str, 
                                        context: Dict[str, Any]) -> None:
        """Trigger predictive loading for related keys."""
        if not self.cache_policy == CachePolicy.PREDICTIVE:
            return
        
        # Find related keys based on patterns (simplified implementation)
        related_keys = await self._find_related_keys(key, context)
        
        for related_key in related_keys[:3]:  # Limit to top 3
            if related_key not in self.memory_cache:
                # This would trigger loading from external source
                logger.debug(f"Would predictively load '{related_key}'")
    
    async def _find_related_keys(self, key: str, context: Dict[str, Any]) -> List[str]:
        """Find keys that are likely to be accessed together."""
        related = []
        
        # Simple pattern matching (in production, this would use ML models)
        key_parts = key.split('_')
        for pattern in self.access_patterns[-50:]:
            pattern_parts = pattern.key.split('_')
            
            # Find keys with similar patterns
            if (len(set(key_parts) & set(pattern_parts)) > 0 and 
                pattern.key != key):
                related.append(pattern.key)
        
        # Return unique related keys
        return list(set(related))
    
    async def _update_prediction_models(self, key: str, context: Dict[str, Any],
                                      entry: CacheEntry) -> None:
        """Update ML prediction models based on new data."""
        # Simplified model update (in production, this would train actual ML models)
        model_key = context.get('operation', 'default')
        
        if model_key not in self.prediction_models:
            self.prediction_models[model_key] = {
                'patterns': [],
                'accuracy': 0.0,
                'last_updated': datetime.now()
            }
        
        model = self.prediction_models[model_key]
        model['patterns'].append({
            'key': key,
            'context': context,
            'prediction_score': entry.prediction_score,
            'timestamp': datetime.now().isoformat()
        })
        
        # Limit model data
        if len(model['patterns']) > 100:
            model['patterns'].pop(0)
        
        model['last_updated'] = datetime.now()
    
    def _update_avg_access_time(self, access_time: float) -> None:
        """Update average access time with exponential moving average."""
        alpha = 0.1
        if self.metrics.avg_access_time == 0:
            self.metrics.avg_access_time = access_time
        else:
            self.metrics.avg_access_time = (
                alpha * access_time + 
                (1 - alpha) * self.metrics.avg_access_time
            )
    
    def _update_hit_rate(self) -> None:
        """Update cache hit rate."""
        total_requests = self.metrics.hits + self.metrics.misses
        if total_requests > 0:
            self.metrics.hit_rate = self.metrics.hits / total_requests
    
    async def _background_cleanup(self) -> None:
        """Background task for cache cleanup and maintenance."""
        while True:
            try:
                await asyncio.sleep(300)  # Run every 5 minutes
                
                # Clean up expired entries
                expired_keys = []
                for key, entry in self.memory_cache.items():
                    if self._is_expired(entry):
                        expired_keys.append(key)
                
                for key in expired_keys:
                    await self._evict_entry(key)
                
                if expired_keys:
                    logger.info(f"Cleaned up {len(expired_keys)} expired entries")
                
                # Clean up disk cache
                await self._cleanup_disk_cache()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background cleanup: {e}")
    
    async def _cleanup_disk_cache(self) -> None:
        """Clean up expired disk cache entries."""
        try:
            for cache_file in self.disk_cache_path.glob("*.cache"):
                meta_file = cache_file.with_suffix('.meta')
                
                if meta_file.exists():
                    with open(meta_file, 'r') as f:
                        metadata = json.load(f)
                    
                    created_at = datetime.fromisoformat(metadata['created_at'])
                    ttl = metadata.get('ttl')
                    
                    if ttl and (datetime.now() - created_at).total_seconds() > ttl:
                        cache_file.unlink()
                        meta_file.unlink()
        
        except Exception as e:
            logger.warning(f"Error cleaning disk cache: {e}")
    
    async def _background_analysis(self) -> None:
        """Background task for pattern analysis and model updates."""
        while True:
            try:
                await asyncio.sleep(600)  # Run every 10 minutes
                
                if len(self.access_patterns) >= 50:
                    await self._analyze_access_patterns()
                    await self._optimize_cache_policy()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background analysis: {e}")
    
    async def _analyze_access_patterns(self) -> None:
        """Analyze access patterns for insights."""
        if not self.access_patterns:
            return
        
        recent_patterns = self.access_patterns[-100:]
        
        # Analyze access frequency by hour
        hourly_access = {}
        for pattern in recent_patterns:
            hour = pattern.timestamp.hour
            hourly_access[hour] = hourly_access.get(hour, 0) + 1
        
        # Find peak hours
        if hourly_access:
            peak_hour = max(hourly_access.keys(), key=lambda h: hourly_access[h])
            logger.debug(f"Peak access hour: {peak_hour} with {hourly_access[peak_hour]} accesses")
    
    async def _optimize_cache_policy(self) -> None:
        """Optimize cache policy based on performance metrics."""
        # Simple optimization logic
        if self.metrics.hit_rate < 0.5 and self.cache_policy != CachePolicy.ADAPTIVE:
            logger.info(f"Low hit rate ({self.metrics.hit_rate:.2f}), considering adaptive policy")
        
        # Update memory efficiency
        if self.metrics.size_bytes > 0:
            self.metrics.memory_efficiency = (
                self.metrics.hits / 
                (self.metrics.size_bytes / (1024 * 1024))  # hits per MB
            )
    
    async def _background_preload(self) -> None:
        """Background task for predictive preloading."""
        while True:
            try:
                await asyncio.sleep(900)  # Run every 15 minutes
                
                if self.cache_policy == CachePolicy.PREDICTIVE:
                    await self._predictive_preload()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in background preload: {e}")
    
    async def _predictive_preload(self) -> None:
        """Perform predictive preloading based on patterns."""
        # This would implement actual predictive logic
        # For now, it's a placeholder
        logger.debug("Predictive preload analysis completed")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        total_requests = self.metrics.hits + self.metrics.misses
        
        return {
            "name": self.name,
            "policy": self.cache_policy.value,
            "metrics": {
                "hits": self.metrics.hits,
                "misses": self.metrics.misses,
                "hit_rate": self.metrics.hit_rate,
                "total_requests": total_requests,
                "evictions": self.metrics.evictions,
                "avg_access_time_ms": self.metrics.avg_access_time * 1000,
                "memory_efficiency": self.metrics.memory_efficiency
            },
            "memory_usage": {
                "size_bytes": self.metrics.size_bytes,
                "size_mb": self.metrics.size_bytes / (1024 * 1024),
                "max_size_mb": self.max_memory_size / (1024 * 1024),
                "utilization": (self.metrics.size_bytes / self.max_memory_size) * 100,
                "entries": len(self.memory_cache)
            },
            "learning": {
                "access_patterns": len(self.access_patterns),
                "prediction_models": len(self.prediction_models),
                "pattern_window": self.learning_window
            },
            "configuration": {
                "max_memory_mb": self.max_memory_size / (1024 * 1024),
                "max_disk_mb": self.max_disk_size / (1024 * 1024),
                "default_ttl": self.default_ttl,
                "compression_threshold": self.compression_threshold
            }
        }
    
    async def close(self) -> None:
        """Close cache and clean up resources."""
        # Cancel background tasks
        for task in self._background_tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Closed adaptive cache '{self.name}'")


class CacheManager:
    """Manager for multiple adaptive performance caches."""
    
    def __init__(self):
        self.caches: Dict[str, AdaptivePerformanceCache] = {}
    
    def get_cache(self, name: str, config: Optional[Dict[str, Any]] = None) -> AdaptivePerformanceCache:
        """Get or create a cache instance."""
        if name not in self.caches:
            self.caches[name] = AdaptivePerformanceCache(name, config)
        return self.caches[name]
    
    async def get_all_stats(self) -> Dict[str, Any]:
        """Get statistics for all caches."""
        stats = {}
        total_hits = 0
        total_misses = 0
        total_size = 0
        
        for name, cache in self.caches.items():
            cache_stats = cache.get_cache_stats()
            stats[name] = cache_stats
            
            total_hits += cache_stats["metrics"]["hits"]
            total_misses += cache_stats["metrics"]["misses"]
            total_size += cache_stats["memory_usage"]["size_bytes"]
        
        # Add global summary
        total_requests = total_hits + total_misses
        global_hit_rate = (total_hits / total_requests) if total_requests > 0 else 0
        
        stats["_global_summary"] = {
            "total_caches": len(self.caches),
            "global_hit_rate": global_hit_rate,
            "total_memory_mb": total_size / (1024 * 1024),
            "total_requests": total_requests
        }
        
        return stats
    
    async def close_all(self) -> None:
        """Close all caches."""
        for cache in self.caches.values():
            await cache.close()
        self.caches.clear()


# Global cache manager
cache_manager = CacheManager()


# Convenience functions
def get_cache(name: str, config: Optional[Dict[str, Any]] = None) -> AdaptivePerformanceCache:
    """Get adaptive performance cache instance."""
    return cache_manager.get_cache(name, config)


async def get_cache_stats() -> Dict[str, Any]:
    """Get statistics for all caches."""
    return await cache_manager.get_all_stats()