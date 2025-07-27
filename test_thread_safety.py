"""Test ETL thread safety under concurrent operations."""

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent
from src.agent_orchestrated_etl.agents.etl_thread_safety import (
    ThreadSafeMetrics, ThreadSafeOperationTracker, ThreadSafeState
)


def test_thread_safe_metrics():
    """Test thread-safe metrics under concurrent operations."""
    print("\n🧵 Testing ThreadSafeMetrics...")
    
    metrics = ThreadSafeMetrics()
    
    def increment_worker(worker_id: int, iterations: int):
        """Worker function to increment metrics concurrently."""
        for i in range(iterations):
            metrics.increment("records_processed", 1)
            metrics.increment("total_processing_time", 0.1)
            metrics.update_throughput()
            time.sleep(0.001)  # Simulate work
    
    # Run 10 workers concurrently, each doing 100 increments
    workers = 10
    iterations = 100
    expected_total = workers * iterations
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(increment_worker, i, iterations) for i in range(workers)]
        for future in as_completed(futures):
            future.result()  # Wait for completion
    
    # Verify results
    final_records = metrics.get_metric("records_processed")
    final_time = metrics.get_metric("total_processing_time")
    final_throughput = metrics.get_metric("average_throughput")
    
    print(f"   Expected records: {expected_total}")
    print(f"   Actual records: {final_records}")
    print(f"   Processing time: {final_time:.2f}s")
    print(f"   Throughput: {final_throughput:.2f} records/sec")
    
    assert final_records == expected_total, f"Expected {expected_total}, got {final_records}"
    assert final_time > 0, "Processing time should be greater than 0"
    assert final_throughput > 0, "Throughput should be greater than 0"
    
    print("   ✅ ThreadSafeMetrics passed concurrent test")


def test_thread_safe_operation_tracker():
    """Test thread-safe operation tracker under concurrent operations."""
    print("\n🧵 Testing ThreadSafeOperationTracker...")
    
    tracker = ThreadSafeOperationTracker()
    
    def operation_worker(worker_id: int, operations: int):
        """Worker function to manage operations concurrently."""
        for i in range(operations):
            op_id = f"worker_{worker_id}_op_{i}"
            
            # Start operation
            tracker.start_operation(op_id, {
                "worker_id": worker_id,
                "operation_index": i,
                "status": "started",
                "start_time": time.time()
            })
            
            # Update operation
            tracker.update_operation(op_id, {
                "status": "processing",
                "progress": 50
            })
            
            time.sleep(0.001)  # Simulate work
            
            # Finish operation
            result = tracker.finish_operation(op_id)
            assert result is not None, f"Operation {op_id} should exist"
    
    # Run 5 workers concurrently, each managing 20 operations
    workers = 5
    operations_per_worker = 20
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(operation_worker, i, operations_per_worker) for i in range(workers)]
        for future in as_completed(futures):
            future.result()  # Wait for completion
    
    # Verify all operations are completed
    active_count = tracker.count()
    all_operations = tracker.get_all_operations()
    
    print(f"   Active operations remaining: {active_count}")
    print(f"   Operations in tracker: {len(all_operations)}")
    
    assert active_count == 0, f"Expected 0 active operations, got {active_count}"
    
    print("   ✅ ThreadSafeOperationTracker passed concurrent test")


def test_etl_agent_thread_safety():
    """Test ETL agent thread safety under concurrent operations."""
    print("\n🧵 Testing ETLAgent thread safety...")
    
    agent = ETLAgent()
    
    def etl_worker(worker_id: int, operations: int):
        """Worker function to perform ETL operations concurrently."""
        for i in range(operations):
            # Simulate extraction operation
            extraction_id = f"extract_{worker_id}_{i}"
            extraction_info = {
                "worker_id": worker_id,
                "operation_index": i,
                "status": "started",
                "started_at": time.time(),
                "records_extracted": 10 + i
            }
            
            agent.active_extractions.start_operation(extraction_id, extraction_info)
            
            # Update metrics
            agent.etl_metrics.increment("records_processed", 10 + i)
            
            time.sleep(0.001)  # Simulate work
            
            # Finish extraction
            agent.active_extractions.finish_operation(extraction_id)
            
            # Simulate transformation operation
            transformation_id = f"transform_{worker_id}_{i}"
            transformation_info = {
                "worker_id": worker_id,
                "operation_index": i,
                "status": "started",
                "started_at": time.time(),
                "records_transformed": 10 + i
            }
            
            agent.active_transformations.start_operation(transformation_id, transformation_info)
            
            time.sleep(0.001)  # Simulate work
            
            # Finish transformation
            agent.active_transformations.finish_operation(transformation_id)
    
    # Run 8 workers concurrently, each doing 15 operations
    workers = 8
    operations_per_worker = 15
    expected_records = sum(range(10, 10 + operations_per_worker)) * workers
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(etl_worker, i, operations_per_worker) for i in range(workers)]
        for future in as_completed(futures):
            future.result()  # Wait for completion
    
    # Verify results
    active_extractions = agent.active_extractions.count()
    active_transformations = agent.active_transformations.count()
    total_records = agent.etl_metrics.get_metric("records_processed")
    
    print(f"   Active extractions: {active_extractions}")
    print(f"   Active transformations: {active_transformations}")
    print(f"   Total records processed: {total_records}")
    print(f"   Expected records: {expected_records}")
    
    assert active_extractions == 0, f"Expected 0 active extractions, got {active_extractions}"
    assert active_transformations == 0, f"Expected 0 active transformations, got {active_transformations}"
    assert total_records == expected_records, f"Expected {expected_records} records, got {total_records}"
    
    print("   ✅ ETLAgent passed concurrent test")


def test_race_condition_prevention():
    """Test that race conditions are prevented in shared state updates."""
    print("\n🧵 Testing race condition prevention...")
    
    state = ThreadSafeState()
    
    def race_worker(worker_id: int, iterations: int):
        """Worker function that attempts to create race conditions."""
        for i in range(iterations):
            key = f"shared_counter"
            
            # Read-modify-write operation that could cause race conditions
            current_value = state.get(key, 0)
            new_value = current_value + 1
            state.set(key, new_value)
            
            # Also test nested updates
            nested_key = f"nested_data"
            state.update(nested_key, {f"worker_{worker_id}": i})
    
    # Run many workers to increase chance of race conditions
    workers = 20
    iterations = 50
    expected_final_value = workers * iterations
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(race_worker, i, iterations) for i in range(workers)]
        for future in as_completed(futures):
            future.result()  # Wait for completion
    
    # Check results
    final_counter = state.get("shared_counter", 0)
    nested_data = state.get("nested_data", {})
    
    print(f"   Expected counter value: {expected_final_value}")
    print(f"   Actual counter value: {final_counter}")
    print(f"   Nested data keys: {len(nested_data)}")
    
    # Note: This test would fail with regular dict due to race conditions
    # but should pass with ThreadSafeState
    assert final_counter == expected_final_value, f"Race condition detected: {final_counter} != {expected_final_value}"
    assert len(nested_data) == workers, f"Expected {workers} nested entries, got {len(nested_data)}"
    
    print("   ✅ Race condition prevention successful")


if __name__ == "__main__":
    print("🧵 Running ETL Thread Safety Tests...")
    
    test_thread_safe_metrics()
    test_thread_safe_operation_tracker()
    test_etl_agent_thread_safety()
    test_race_condition_prevention()
    
    print("\n🎉 All thread safety tests passed!")
    print("✅ ETL-019: Thread Safety Issues - RESOLVED")