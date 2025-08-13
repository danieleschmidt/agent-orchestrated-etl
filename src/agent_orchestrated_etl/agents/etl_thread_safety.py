"""Thread safety utilities for ETL operations."""

import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager
from typing import Any, Dict, Optional


class ThreadSafeState:
    """Thread-safe state manager for ETL operations."""

    def __init__(self):
        """Initialize thread-safe state."""
        self._lock = threading.RLock()
        self._async_lock = asyncio.Lock()
        self._data: Dict[str, Any] = {}

    @contextmanager
    def sync_lock(self):
        """Context manager for synchronous thread-safe operations."""
        with self._lock:
            yield

    @asynccontextmanager
    async def async_lock(self):
        """Context manager for asynchronous thread-safe operations."""
        async with self._async_lock:
            yield

    def get(self, key: str, default: Any = None) -> Any:
        """Thread-safe get operation."""
        with self._lock:
            return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Thread-safe set operation."""
        with self._lock:
            self._data[key] = value

    def update(self, key: str, updates: Dict[str, Any]) -> None:
        """Thread-safe update operation for nested dictionaries."""
        with self._lock:
            if key not in self._data:
                self._data[key] = {}
            if isinstance(self._data[key], dict):
                self._data[key].update(updates)
            else:
                self._data[key] = updates

    def delete(self, key: str) -> bool:
        """Thread-safe delete operation."""
        with self._lock:
            if key in self._data:
                del self._data[key]
                return True
            return False

    def copy(self) -> Dict[str, Any]:
        """Thread-safe copy operation."""
        with self._lock:
            return {k: v.copy() if isinstance(v, dict) else v for k, v in self._data.items()}

    def clear(self) -> None:
        """Thread-safe clear operation."""
        with self._lock:
            self._data.clear()


class ThreadSafeMetrics:
    """Thread-safe metrics counter for ETL operations."""

    def __init__(self):
        """Initialize thread-safe metrics."""
        self._lock = threading.RLock()
        self._metrics = {
            "records_processed": 0,
            "total_processing_time": 0.0,
            "average_throughput": 0.0,
            "data_quality_score": 1.0,
            "error_rate": 0.0,
        }

    def increment(self, metric: str, value: float = 1.0) -> None:
        """Thread-safe increment operation."""
        with self._lock:
            if metric in self._metrics:
                self._metrics[metric] += value

    def set_metric(self, metric: str, value: float) -> None:
        """Thread-safe set metric operation."""
        with self._lock:
            self._metrics[metric] = value

    def get_metric(self, metric: str) -> float:
        """Thread-safe get metric operation."""
        with self._lock:
            return self._metrics.get(metric, 0.0)

    def update_throughput(self) -> None:
        """Thread-safe throughput calculation."""
        with self._lock:
            if self._metrics["total_processing_time"] > 0:
                self._metrics["average_throughput"] = (
                    self._metrics["records_processed"] / self._metrics["total_processing_time"]
                )

    def copy(self) -> Dict[str, float]:
        """Thread-safe copy of all metrics."""
        with self._lock:
            return self._metrics.copy()

    def reset(self) -> None:
        """Thread-safe reset of all metrics."""
        with self._lock:
            self._metrics = {
                "records_processed": 0,
                "total_processing_time": 0.0,
                "average_throughput": 0.0,
                "data_quality_score": 1.0,
                "error_rate": 0.0,
            }


class ThreadSafeOperationTracker:
    """Thread-safe operation tracker for ETL operations."""

    def __init__(self):
        """Initialize thread-safe operation tracker."""
        self._lock = threading.RLock()
        self._operations: Dict[str, Dict[str, Any]] = {}

    def start_operation(self, operation_id: str, operation_info: Dict[str, Any]) -> None:
        """Thread-safe operation start tracking."""
        with self._lock:
            self._operations[operation_id] = operation_info.copy()

    def update_operation(self, operation_id: str, updates: Dict[str, Any]) -> None:
        """Thread-safe operation update."""
        with self._lock:
            if operation_id in self._operations:
                self._operations[operation_id].update(updates)

    def finish_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Thread-safe operation completion."""
        with self._lock:
            return self._operations.pop(operation_id, None)

    def get_operation(self, operation_id: str) -> Optional[Dict[str, Any]]:
        """Thread-safe operation retrieval."""
        with self._lock:
            operation = self._operations.get(operation_id)
            return operation.copy() if operation else None

    def get_all_operations(self) -> Dict[str, Dict[str, Any]]:
        """Thread-safe retrieval of all operations."""
        with self._lock:
            return {k: v.copy() for k, v in self._operations.items()}

    def count(self) -> int:
        """Thread-safe count of active operations."""
        with self._lock:
            return len(self._operations)

    def clear(self) -> None:
        """Thread-safe clear all operations."""
        with self._lock:
            self._operations.clear()


def thread_safe_operation(func):
    """Decorator to make operations thread-safe."""
    def wrapper(self, *args, **kwargs):
        # Use the instance's lock if available
        if hasattr(self, '_etl_lock'):
            with self._etl_lock:
                return func(self, *args, **kwargs)
        else:
            return func(self, *args, **kwargs)
    return wrapper


async def async_thread_safe_operation(func):
    """Decorator to make async operations thread-safe."""
    async def wrapper(self, *args, **kwargs):
        # Use the instance's async lock if available
        if hasattr(self, '_etl_async_lock'):
            async with self._etl_async_lock:
                return await func(self, *args, **kwargs)
        else:
            return await func(self, *args, **kwargs)
    return wrapper
