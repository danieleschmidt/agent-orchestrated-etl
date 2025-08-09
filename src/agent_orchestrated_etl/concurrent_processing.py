"""Concurrent processing capabilities for high-performance ETL operations."""

from __future__ import annotations

import asyncio
import concurrent.futures
import multiprocessing as mp
from typing import Any, Dict, List, Optional, Callable, Iterator
from dataclasses import dataclass
import time
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed

from .logging_config import get_logger
from .performance_cache import PerformanceCache


@dataclass
class ConcurrencyConfig:
    """Configuration for concurrent processing."""
    max_workers: int = None  # None = CPU count
    chunk_size: int = 1000
    use_processes: bool = False  # True for CPU-bound, False for I/O-bound
    enable_caching: bool = True
    timeout_seconds: float = 300.0
    memory_limit_mb: int = 500


class ConcurrentProcessor:
    """High-performance concurrent processor for ETL operations."""
    
    def __init__(self, config: Optional[ConcurrencyConfig] = None):
        self.config = config or ConcurrencyConfig()
        self.logger = get_logger("concurrent_processor")
        
        # Set default workers based on CPU count
        if self.config.max_workers is None:
            self.config.max_workers = min(mp.cpu_count(), 8)
        
        self.cache = PerformanceCache() if self.config.enable_caching else None
        self._thread_executor = None
        self._process_executor = None
        
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()
    
    def cleanup(self):
        """Clean up executors."""
        if self._thread_executor:
            self._thread_executor.shutdown(wait=True)
            self._thread_executor = None
        
        if self._process_executor:
            self._process_executor.shutdown(wait=True)
            self._process_executor = None
    
    @property
    def executor(self):
        """Get appropriate executor based on configuration."""
        if self.config.use_processes:
            if self._process_executor is None:
                self._process_executor = ProcessPoolExecutor(
                    max_workers=self.config.max_workers
                )
            return self._process_executor
        else:
            if self._thread_executor is None:
                self._thread_executor = ThreadPoolExecutor(
                    max_workers=self.config.max_workers
                )
            return self._thread_executor
    
    async def process_batch_async(
        self,
        data: List[Any],
        processor_func: Callable,
        **kwargs
    ) -> List[Any]:
        """Process data in batches asynchronously."""
        
        start_time = time.time()
        self.logger.info(f"Starting async batch processing of {len(data)} items")
        
        # Split data into chunks
        chunks = self._create_chunks(data, self.config.chunk_size)
        
        # Process chunks concurrently
        loop = asyncio.get_event_loop()
        tasks = []
        
        for chunk_idx, chunk in enumerate(chunks):
            task = loop.run_in_executor(
                self.executor,
                self._process_chunk,
                chunk,
                processor_func,
                chunk_idx,
                kwargs
            )
            tasks.append(task)
        
        # Wait for all tasks to complete
        try:
            chunk_results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=self.config.timeout_seconds
            )
        except asyncio.TimeoutError:
            self.logger.error(f"Batch processing timed out after {self.config.timeout_seconds}s")
            raise
        
        # Flatten results and handle exceptions
        results = []
        for chunk_result in chunk_results:
            if isinstance(chunk_result, Exception):
                self.logger.error(f"Chunk processing failed: {str(chunk_result)}")
                raise chunk_result
            results.extend(chunk_result)
        
        processing_time = time.time() - start_time
        self.logger.info(f"Completed batch processing in {processing_time:.2f}s")
        
        return results
    
    def process_batch_sync(
        self,
        data: List[Any],
        processor_func: Callable,
        **kwargs
    ) -> List[Any]:
        """Process data in batches synchronously."""
        
        start_time = time.time()
        self.logger.info(f"Starting sync batch processing of {len(data)} items")
        
        # Split data into chunks
        chunks = self._create_chunks(data, self.config.chunk_size)
        
        # Process chunks concurrently using executor
        future_to_chunk = {}
        
        with self.executor as executor:
            # Submit all chunks
            for chunk_idx, chunk in enumerate(chunks):
                future = executor.submit(
                    self._process_chunk,
                    chunk,
                    processor_func,
                    chunk_idx,
                    kwargs
                )
                future_to_chunk[future] = chunk_idx
            
            # Collect results
            results = []
            for future in as_completed(future_to_chunk, timeout=self.config.timeout_seconds):
                chunk_idx = future_to_chunk[future]
                try:
                    chunk_result = future.result()
                    results.extend(chunk_result)
                except Exception as e:
                    self.logger.error(f"Chunk {chunk_idx} processing failed: {str(e)}")
                    raise
        
        processing_time = time.time() - start_time
        self.logger.info(f"Completed sync batch processing in {processing_time:.2f}s")
        
        return results
    
    def _create_chunks(self, data: List[Any], chunk_size: int) -> List[List[Any]]:
        """Split data into chunks for parallel processing."""
        chunks = []
        for i in range(0, len(data), chunk_size):
            chunks.append(data[i:i + chunk_size])
        return chunks
    
    def _process_chunk(
        self,
        chunk: List[Any],
        processor_func: Callable,
        chunk_idx: int,
        kwargs: Dict[str, Any]
    ) -> List[Any]:
        """Process a single chunk of data."""
        
        try:
            # Apply processor function to each item in chunk
            results = []
            for item in chunk:
                try:
                    if kwargs:
                        result = processor_func(item, **kwargs)
                    else:
                        result = processor_func(item)
                    results.append(result)
                except Exception as e:
                    self.logger.warning(f"Failed to process item in chunk {chunk_idx}: {str(e)}")
                    # Continue processing other items
                    continue
            
            return results
            
        except Exception as e:
            self.logger.error(f"Chunk {chunk_idx} processing failed: {str(e)}")
            raise
    
    async def stream_process(
        self,
        data_stream: Iterator[Any],
        processor_func: Callable,
        **kwargs
    ) -> Iterator[Any]:
        """Process streaming data with backpressure control."""
        
        self.logger.info("Starting stream processing")
        buffer = []
        
        async for item in self._async_iterator(data_stream):
            buffer.append(item)
            
            # Process when buffer is full
            if len(buffer) >= self.config.chunk_size:
                processed_chunk = await self.process_batch_async(
                    buffer, processor_func, **kwargs
                )
                
                for result in processed_chunk:
                    yield result
                
                buffer = []
        
        # Process remaining items
        if buffer:
            processed_chunk = await self.process_batch_async(
                buffer, processor_func, **kwargs
            )
            
            for result in processed_chunk:
                yield result
    
    async def _async_iterator(self, sync_iterator: Iterator) -> Iterator:
        """Convert sync iterator to async iterator."""
        for item in sync_iterator:
            yield item
            # Allow other coroutines to run
            await asyncio.sleep(0)
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics for the processor."""
        return {
            "config": {
                "max_workers": self.config.max_workers,
                "chunk_size": self.config.chunk_size,
                "use_processes": self.config.use_processes,
                "timeout_seconds": self.config.timeout_seconds
            },
            "system_info": {
                "cpu_count": mp.cpu_count(),
                "available_memory_mb": self._get_available_memory_mb()
            },
            "cache_metrics": self.cache.get_performance_metrics() if self.cache else None
        }
    
    def _get_available_memory_mb(self) -> float:
        """Get available system memory in MB."""
        try:
            import psutil
            return psutil.virtual_memory().available / (1024 * 1024)
        except ImportError:
            return -1  # Unknown


class ParallelETLProcessor:
    """Specialized parallel processor for ETL operations."""
    
    def __init__(self, config: Optional[ConcurrencyConfig] = None):
        self.processor = ConcurrentProcessor(config)
        self.logger = get_logger("parallel_etl")
        
    async def parallel_extract(
        self,
        sources: List[Dict[str, Any]],
        extractor_func: Callable
    ) -> Dict[str, List[Any]]:
        """Extract data from multiple sources in parallel."""
        
        self.logger.info(f"Starting parallel extraction from {len(sources)} sources")
        
        extraction_results = {}
        
        # Process each source
        for source in sources:
            source_id = source.get("id", f"source_{len(extraction_results)}")
            
            try:
                extracted_data = await self.processor.process_batch_async(
                    [source],  # Single source as batch
                    extractor_func
                )
                extraction_results[source_id] = extracted_data[0] if extracted_data else []
                
            except Exception as e:
                self.logger.error(f"Extraction failed for source {source_id}: {str(e)}")
                extraction_results[source_id] = []
        
        return extraction_results
    
    async def parallel_transform(
        self,
        data: List[Any],
        transformation_rules: List[Callable]
    ) -> List[Any]:
        """Apply multiple transformations in parallel."""
        
        self.logger.info(f"Starting parallel transformation with {len(transformation_rules)} rules")
        
        # Apply each transformation rule in parallel
        transformed_data = data
        
        for rule_idx, rule in enumerate(transformation_rules):
            self.logger.debug(f"Applying transformation rule {rule_idx + 1}")
            
            transformed_data = await self.processor.process_batch_async(
                transformed_data,
                rule
            )
        
        return transformed_data
    
    async def parallel_load(
        self,
        data: List[Any],
        destinations: List[Dict[str, Any]],
        loader_func: Callable
    ) -> Dict[str, Dict[str, Any]]:
        """Load data to multiple destinations in parallel."""
        
        self.logger.info(f"Starting parallel loading to {len(destinations)} destinations")
        
        load_results = {}
        
        # Create loading tasks for each destination
        tasks = []
        for dest in destinations:
            dest_id = dest.get("id", f"dest_{len(tasks)}")
            
            task = asyncio.create_task(
                self._load_to_destination(data, dest, loader_func, dest_id)
            )
            tasks.append((dest_id, task))
        
        # Wait for all loading tasks
        for dest_id, task in tasks:
            try:
                load_result = await task
                load_results[dest_id] = load_result
            except Exception as e:
                self.logger.error(f"Loading failed for destination {dest_id}: {str(e)}")
                load_results[dest_id] = {"status": "error", "error": str(e)}
        
        return load_results
    
    async def _load_to_destination(
        self,
        data: List[Any],
        destination: Dict[str, Any],
        loader_func: Callable,
        dest_id: str
    ) -> Dict[str, Any]:
        """Load data to a single destination."""
        
        try:
            result = await self.processor.process_batch_async(
                data,
                lambda item: loader_func(item, destination)
            )
            
            return {
                "status": "success",
                "records_loaded": len(result),
                "destination": dest_id
            }
            
        except Exception as e:
            self.logger.error(f"Failed to load to {dest_id}: {str(e)}")
            raise
    
    def cleanup(self):
        """Clean up resources."""
        self.processor.cleanup()


# Utility functions for common parallel operations

async def parallel_map(
    data: List[Any],
    func: Callable,
    max_workers: int = None,
    chunk_size: int = 1000
) -> List[Any]:
    """Apply function to data in parallel (like map() but concurrent)."""
    
    config = ConcurrencyConfig(
        max_workers=max_workers,
        chunk_size=chunk_size
    )
    
    with ConcurrentProcessor(config) as processor:
        return await processor.process_batch_async(data, func)


def parallel_map_sync(
    data: List[Any],
    func: Callable,
    max_workers: int = None,
    chunk_size: int = 1000
) -> List[Any]:
    """Apply function to data in parallel (synchronous version)."""
    
    config = ConcurrencyConfig(
        max_workers=max_workers,
        chunk_size=chunk_size
    )
    
    with ConcurrentProcessor(config) as processor:
        return processor.process_batch_sync(data, func)