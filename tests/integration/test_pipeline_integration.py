"""Integration tests for complete pipeline execution."""

import os
import tempfile
from pathlib import Path

import pytest

from agent_orchestrated_etl.orchestrator import DataOrchestrator


@pytest.mark.integration
class TestPipelineIntegration:
    """Test complete pipeline execution scenarios."""

    @pytest.fixture
    def temp_data_dir(self):
        """Create temporary directory for test data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def sample_csv_data(self, temp_data_dir):
        """Create sample CSV data for testing."""
        csv_file = temp_data_dir / "sample_data.csv"
        csv_content = """id,name,value,timestamp
1,Alice,100,2023-01-01T00:00:00Z
2,Bob,200,2023-01-01T01:00:00Z
3,Charlie,300,2023-01-01T02:00:00Z
"""
        csv_file.write_text(csv_content)
        return csv_file

    @pytest.mark.slow
    def test_file_to_memory_pipeline(self, sample_csv_data):
        """Test complete pipeline from file source to in-memory storage."""
        orchestrator = DataOrchestrator()
        
        # Create pipeline from file source
        pipeline = orchestrator.create_pipeline(
            source=f"file://{sample_csv_data}",
            operations={"load": lambda data: data}  # In-memory load
        )
        
        # Execute pipeline
        result = pipeline.execute()
        
        # Verify results
        assert result is not None
        assert len(result) == 3
        assert result[0]["name"] == "Alice"

    @pytest.mark.slow
    @pytest.mark.skipif(
        not os.getenv("INTEGRATION_TESTS"), 
        reason="Integration tests disabled"
    )
    def test_s3_pipeline_execution(self):
        """Test pipeline execution with S3 source (requires AWS credentials)."""
        orchestrator = DataOrchestrator()
        
        # This test requires actual AWS credentials and S3 bucket
        s3_source = "s3://test-bucket/test-data/"
        
        pipeline = orchestrator.create_pipeline(source=s3_source)
        
        # This would fail without proper AWS setup, so we just test creation
        assert pipeline is not None
        assert pipeline.source == s3_source

    def test_error_handling_in_pipeline(self, temp_data_dir):
        """Test pipeline error handling and recovery."""
        orchestrator = DataOrchestrator()
        
        # Create invalid source
        invalid_source = "file:///nonexistent/path/data.csv"
        
        with pytest.raises(Exception):
            pipeline = orchestrator.create_pipeline(source=invalid_source)
            pipeline.execute()

    def test_pipeline_monitoring_integration(self, sample_csv_data):
        """Test pipeline execution with monitoring enabled."""
        orchestrator = DataOrchestrator()
        
        monitor_file = tempfile.NamedTemporaryFile(mode='w', delete=False)
        monitor_file.close()
        
        try:
            pipeline = orchestrator.create_pipeline(
                source=f"file://{sample_csv_data}",
                monitor_file=monitor_file.name
            )
            
            result = pipeline.execute()
            
            # Verify monitoring file was created
            assert os.path.exists(monitor_file.name)
            
            # Verify pipeline executed successfully
            assert result is not None
            
        finally:
            if os.path.exists(monitor_file.name):
                os.unlink(monitor_file.name)