"""Advanced caching system for ETL performance optimization."""

from __future__ import annotations

import asyncio
import hashlib
import pickle
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional

from .logging_config import get_logger


@dataclass
class CacheConfig:
    """Configuration for caching system."""
    max_size: int = 1000
    ttl_seconds: int = 3600  # 1 hour
    enable_persistent: bool = False
    cache_directory: str = "/tmp/etl_cache"
    compression_enabled: bool = True
    memory_threshold_mb: int = 100


@dataclass
class CacheEntry:
    """Represents a cached entry."""
    key: str
    value: Any
    created_at: float
    last_accessed: float
    access_count: int = 0
    size_bytes: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class InMemoryCache:
    """Thread-safe in-memory cache with LRU eviction."""

    def __init__(self, config: CacheConfig):
        self.config = config
        self.logger = get_logger("memory_cache")
        self._cache: Dict[str, CacheEntry] = {}
        self._lock = threading.RLock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "total_size": 0
        }

    def get(self, key: str) -> Optional[Any]:
        """Get value from cache."""
        with self._lock:
            if key not in self._cache:
                self._stats["misses"] += 1
                return None

            entry = self._cache[key]

            # Check TTL
            if self._is_expired(entry):
                del self._cache[key]
                self._stats["misses"] += 1
                return None

            # Update access info
            entry.last_accessed = time.time()
            entry.access_count += 1
            self._stats["hits"] += 1

            return entry.value

    def put(self, key: str, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """Put value in cache."""
        with self._lock:
            # Calculate size
            size_bytes = self._calculate_size(value)

            # Check if we need to evict entries
            self._evict_if_needed(size_bytes)

            entry = CacheEntry(
                key=key,
                value=value,
                created_at=time.time(),
                last_accessed=time.time(),
                size_bytes=size_bytes,
                metadata=metadata or {}
            )

            self._cache[key] = entry
            self._stats["total_size"] += size_bytes

    def _is_expired(self, entry: CacheEntry) -> bool:
        """Check if cache entry is expired."""
        return (time.time() - entry.created_at) > self.config.ttl_seconds

    def _evict_if_needed(self, new_entry_size: int) -> None:
        """Evict entries if cache is full."""
        # Check size limit
        while (len(self._cache) >= self.config.max_size or
               self._stats["total_size"] + new_entry_size > self.config.memory_threshold_mb * 1024 * 1024):

            if not self._cache:
                break

            # LRU eviction - find least recently accessed
            lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].last_accessed)
            evicted_entry = self._cache.pop(lru_key)
            self._stats["total_size"] -= evicted_entry.size_bytes
            self._stats["evictions"] += 1

    def _calculate_size(self, value: Any) -> int:
        """Calculate approximate size of value in bytes."""
        try:
            return len(pickle.dumps(value))
        except Exception:
            # Fallback estimation
            return len(str(value).encode('utf-8'))

    def clear(self) -> None:
        """Clear all cache entries."""
        with self._lock:
            self._cache.clear()
            self._stats["total_size"] = 0

    def get_stats(self) -> Dict[str, Any]:
        """Get cache statistics."""
        with self._lock:
            hit_rate = (self._stats["hits"] /
                       (self._stats["hits"] + self._stats["misses"])
                       if (self._stats["hits"] + self._stats["misses"]) > 0 else 0)

            return {
                **self._stats,
                "hit_rate": hit_rate,
                "entry_count": len(self._cache),
                "memory_usage_mb": self._stats["total_size"] / (1024 * 1024)
            }


class PerformanceCache:
    """High-performance caching system for ETL operations."""

    def __init__(self, config: Optional[CacheConfig] = None):
        self.config = config or CacheConfig()
        self.logger = get_logger("performance_cache")
        self.memory_cache = InMemoryCache(self.config)
        self.executor = ThreadPoolExecutor(max_workers=4)

    def generate_key(self, operation: str, *args, **kwargs) -> str:
        """Generate cache key from operation and parameters."""
        key_data = {
            "operation": operation,
            "args": str(args),
            "kwargs": str(sorted(kwargs.items()))
        }

        key_string = str(key_data)
        return hashlib.md5(key_string.encode()).hexdigest()

    async def get_or_compute(
        self,
        operation: str,
        compute_func: Callable,
        *args,
        use_cache: bool = True,
        **kwargs
    ) -> Any:
        """Get from cache or compute and cache the result."""

        if not use_cache:
            return await self._compute_value(compute_func, *args, **kwargs)

        cache_key = self.generate_key(operation, *args, **kwargs)

        # Try to get from cache first
        cached_value = self.memory_cache.get(cache_key)
        if cached_value is not None:
            self.logger.debug(f"Cache hit for operation: {operation}")
            return cached_value

        # Compute and cache
        self.logger.debug(f"Cache miss for operation: {operation}, computing...")
        computed_value = await self._compute_value(compute_func, *args, **kwargs)

        # Cache the result
        self.memory_cache.put(
            cache_key,
            computed_value,
            metadata={
                "operation": operation,
                "computed_at": time.time()
            }
        )

        return computed_value

    async def _compute_value(self, compute_func: Callable, *args, **kwargs) -> Any:
        """Compute value using the provided function."""
        if asyncio.iscoroutinefunction(compute_func):
            return await compute_func(*args, **kwargs)
        else:
            # Run sync function in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(self.executor, compute_func, *args, **kwargs)

    def invalidate(self, operation: str, *args, **kwargs) -> None:
        """Invalidate cache entry for specific operation."""
        cache_key = self.generate_key(operation, *args, **kwargs)
        with self.memory_cache._lock:
            if cache_key in self.memory_cache._cache:
                entry = self.memory_cache._cache.pop(cache_key)
                self.memory_cache._stats["total_size"] -= entry.size_bytes
                self.logger.debug(f"Invalidated cache for operation: {operation}")

    def clear_all(self) -> None:
        """Clear all cache entries."""
        self.memory_cache.clear()
        self.logger.info("Cleared all cache entries")

    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get detailed performance metrics."""
        cache_stats = self.memory_cache.get_stats()

        return {
            "cache_performance": cache_stats,
            "config": {
                "max_size": self.config.max_size,
                "ttl_seconds": self.config.ttl_seconds,
                "memory_threshold_mb": self.config.memory_threshold_mb
            },
            "recommendations": self._get_performance_recommendations(cache_stats)
        }

    def _get_performance_recommendations(self, stats: Dict[str, Any]) -> List[str]:
        """Generate performance recommendations based on cache stats."""
        recommendations = []

        if stats["hit_rate"] < 0.5:
            recommendations.append("Low cache hit rate - consider increasing TTL or cache size")

        if stats["evictions"] > stats["hits"] * 0.1:
            recommendations.append("High eviction rate - consider increasing memory threshold")

        if stats["memory_usage_mb"] > self.config.memory_threshold_mb * 0.9:
            recommendations.append("Memory usage high - consider optimizing data structures")

        return recommendations


def cached_operation(
    cache_key: Optional[str] = None,
    ttl_seconds: Optional[int] = None,
    use_cache: bool = True
):
    """Decorator to cache function results."""

    def decorator(func: Callable) -> Callable:
        _cache = PerformanceCache()

        async def async_wrapper(*args, **kwargs):
            operation_key = cache_key or f"{func.__module__}.{func.__name__}"

            return await _cache.get_or_compute(
                operation_key,
                func,
                *args,
                use_cache=use_cache,
                **kwargs
            )

        def sync_wrapper(*args, **kwargs):
            if asyncio.iscoroutinefunction(func):
                return async_wrapper(*args, **kwargs)

            operation_key = cache_key or f"{func.__module__}.{func.__name__}"
            cache_key_full = _cache.generate_key(operation_key, *args, **kwargs)

            if use_cache:
                cached_result = _cache.memory_cache.get(cache_key_full)
                if cached_result is not None:
                    return cached_result

            result = func(*args, **kwargs)

            if use_cache:
                _cache.memory_cache.put(cache_key_full, result)

            return result

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator
