"""Performance tests for pipeline execution."""

import time
from unittest.mock import Mock, patch

import pytest

from agent_orchestrated_etl.orchestrator import DataOrchestrator


@pytest.mark.slow
class TestPipelinePerformance:
    """Performance benchmarks for pipeline operations."""

    def test_pipeline_creation_performance(self):
        """Test pipeline creation time is within acceptable limits."""
        orchestrator = DataOrchestrator()
        
        start_time = time.time()
        
        # Create multiple pipelines to test performance
        for i in range(10):
            pipeline = orchestrator.create_pipeline(
                source=f"mock://test-source-{i}"
            )
            assert pipeline is not None
        
        end_time = time.time()
        creation_time = end_time - start_time
        
        # Should create 10 pipelines in less than 1 second
        assert creation_time < 1.0, f"Pipeline creation took {creation_time:.2f}s"

    @patch('agent_orchestrated_etl.agents.etl_agent.ETLAgent')
    def test_large_dataset_processing(self, mock_etl_agent):
        """Test processing performance with large datasets."""
        # Mock large dataset
        mock_data = [{"id": i, "value": f"data_{i}"} for i in range(10000)]
        mock_etl_agent.return_value.extract.return_value = mock_data
        
        orchestrator = DataOrchestrator()
        pipeline = orchestrator.create_pipeline(source="mock://large-dataset")
        
        start_time = time.time()
        result = pipeline.execute()
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        # Should process 10k records in less than 5 seconds
        assert processing_time < 5.0, f"Large dataset processing took {processing_time:.2f}s"
        assert len(result) == 10000

    def test_concurrent_pipeline_execution(self):
        """Test performance with multiple concurrent pipelines."""
        import concurrent.futures
        
        orchestrator = DataOrchestrator()
        
        def create_and_execute_pipeline(source_id):
            pipeline = orchestrator.create_pipeline(
                source=f"mock://concurrent-test-{source_id}"
            )
            return pipeline.execute()
        
        start_time = time.time()
        
        # Run 5 pipelines concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(create_and_execute_pipeline, i) 
                for i in range(5)
            ]
            results = [future.result() for future in futures]
        
        end_time = time.time()
        concurrent_time = end_time - start_time
        
        # Concurrent execution should be efficient
        assert concurrent_time < 10.0, f"Concurrent execution took {concurrent_time:.2f}s"
        assert len(results) == 5

    def test_memory_usage_during_processing(self):
        """Test memory usage remains reasonable during processing."""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        orchestrator = DataOrchestrator()
        
        # Process multiple pipelines
        for i in range(20):
            pipeline = orchestrator.create_pipeline(
                source=f"mock://memory-test-{i}"
            )
            pipeline.execute()
        
        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable (less than 100MB)
        assert memory_increase < 100, f"Memory increased by {memory_increase:.2f}MB"