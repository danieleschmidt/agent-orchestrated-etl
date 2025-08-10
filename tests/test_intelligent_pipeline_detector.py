"""Tests for the intelligent pipeline detector."""

import pytest
from unittest.mock import Mock, patch

from src.agent_orchestrated_etl.intelligent_pipeline_detector import (
    IntelligentPipelineDetector,
    DataSourceType,
    TransformationStrategy,
    OutputDestinationType,
    DataPatterns,
    PipelineRecommendation
)
from src.agent_orchestrated_etl.models.pipeline_models import PipelineType, OptimizationStrategy


@pytest.fixture
def detector():
    """Create a detector instance for testing."""
    return IntelligentPipelineDetector()


@pytest.fixture
def sample_s3_metadata():
    """Sample S3 metadata for testing."""
    return {
        "tables": ["data.csv", "records.json"],
        "fields": ["id", "name", "timestamp", "latitude", "longitude", "email"],
        "analysis_metadata": {
            "total_files": 10,
            "file_formats": {"csv": 7, "json": 3},
            "total_size_mb": 150,
            "data_quality": {
                "data_types": {
                    "id": "string",
                    "name": "string", 
                    "timestamp": "datetime",
                    "latitude": "numeric",
                    "longitude": "numeric",
                    "email": "string"
                },
                "potential_pii": ["email"]
            },
            "schema_info": {
                "csv": {"columns": ["id", "name", "timestamp"]},
                "json": {"columns": ["id", "latitude", "longitude", "email"]}
            }
        }
    }


class TestDataSourceTypeDetection:
    """Test data source type detection."""
    
    def test_detect_api_source(self, detector):
        """Test API source detection."""
        assert detector.detect_data_source_type("https://api.example.com/v1/data") == DataSourceType.API
        assert detector.detect_data_source_type("http://service.com/rest/users") == DataSourceType.API
    
    def test_detect_s3_source(self, detector):
        """Test S3 source detection."""
        assert detector.detect_data_source_type("s3://my-bucket/data/") == DataSourceType.S3
        assert detector.detect_data_source_type("s3://bucket-name/file.csv") == DataSourceType.S3
    
    def test_detect_database_source(self, detector):
        """Test database source detection."""
        assert detector.detect_data_source_type("postgresql://user:pass@host/db") == DataSourceType.DATABASE
        assert detector.detect_data_source_type("mysql://localhost:3306/database") == DataSourceType.DATABASE
        assert detector.detect_data_source_type("sqlite:///path/to/db.sqlite") == DataSourceType.DATABASE
    
    def test_detect_file_source(self, detector):
        """Test file source detection."""
        assert detector.detect_data_source_type("/path/to/data.csv") == DataSourceType.FILE
        assert detector.detect_data_source_type("data.json") == DataSourceType.FILE
        assert detector.detect_data_source_type("records.parquet") == DataSourceType.FILE
    
    def test_detect_streaming_source(self, detector):
        """Test streaming source detection."""
        assert detector.detect_data_source_type("kafka://broker:9092/topic") == DataSourceType.STREAMING
        assert detector.detect_data_source_type("kinesis-stream-name") == DataSourceType.STREAMING
    
    def test_detect_unknown_source(self, detector):
        """Test unknown source detection."""
        assert detector.detect_data_source_type("unknown-identifier") == DataSourceType.UNKNOWN


class TestDataPatternAnalysis:
    """Test data pattern analysis."""
    
    def test_analyze_patterns_with_timestamps(self, detector, sample_s3_metadata):
        """Test timestamp detection."""
        patterns = detector.analyze_data_patterns("s3://test-bucket", sample_s3_metadata)
        
        assert patterns.has_timestamps is True
        assert patterns.has_incremental_keys is True  # 'id' field
        assert patterns.has_geospatial_data is True  # lat/lon fields
        assert patterns.has_numeric_data is True
        assert patterns.has_categorical_data is True
    
    def test_analyze_patterns_volume_estimation(self, detector, sample_s3_metadata):
        """Test volume estimation."""
        patterns = detector.analyze_data_patterns("s3://test-bucket", sample_s3_metadata)
        
        # Should estimate medium volume based on file count and types
        assert patterns.estimated_volume in ["small", "medium"]
        assert patterns.estimated_record_size is not None
    
    def test_analyze_patterns_schema_complexity(self, detector, sample_s3_metadata):
        """Test schema complexity detection."""
        patterns = detector.analyze_data_patterns("s3://test-bucket", sample_s3_metadata)
        
        # Based on schema info in metadata
        assert patterns.schema_complexity in ["simple", "moderate", "complex"]
    
    def test_analyze_patterns_data_quality_issues(self, detector, sample_s3_metadata):
        """Test data quality issue detection."""
        patterns = detector.analyze_data_patterns("s3://test-bucket", sample_s3_metadata)
        
        # Should detect PII in email field
        assert len(patterns.data_quality_issues) > 0
        assert any("PII" in issue for issue in patterns.data_quality_issues)


class TestTransformationStrategyRecommendation:
    """Test transformation strategy recommendations."""
    
    def test_recommend_streaming_for_real_time(self, detector):
        """Test streaming recommendation for real-time data."""
        patterns = DataPatterns(
            has_timestamps=True,
            has_incremental_keys=True,
            update_frequency="real_time"
        )
        
        strategy = detector.recommend_transformation_strategy(patterns, DataSourceType.STREAMING)
        assert strategy == TransformationStrategy.REAL_TIME_PROCESSING
    
    def test_recommend_incremental_for_incremental_keys(self, detector):
        """Test incremental processing recommendation."""
        patterns = DataPatterns(
            has_timestamps=True,
            has_incremental_keys=True,
            update_frequency="batch"
        )
        
        strategy = detector.recommend_transformation_strategy(patterns, DataSourceType.DATABASE)
        assert strategy == TransformationStrategy.INCREMENTAL_PROCESSING
    
    def test_recommend_aggregation_for_large_numeric(self, detector):
        """Test aggregation strategy for large numeric data."""
        patterns = DataPatterns(
            has_numeric_data=True,
            estimated_volume="large",
            schema_complexity="simple"
        )
        
        strategy = detector.recommend_transformation_strategy(patterns, DataSourceType.S3)
        assert strategy == TransformationStrategy.AGGREGATION_FOCUSED
    
    def test_recommend_cleansing_for_quality_issues(self, detector):
        """Test cleansing strategy for data quality issues."""
        patterns = DataPatterns(
            data_quality_issues=["Missing values", "Invalid formats"]
        )
        
        strategy = detector.recommend_transformation_strategy(patterns, DataSourceType.FILE)
        assert strategy == TransformationStrategy.CLEANSING_FOCUSED


class TestOutputDestinationRecommendation:
    """Test output destination recommendations."""
    
    def test_recommend_streaming_destinations(self, detector):
        """Test streaming destinations for real-time processing."""
        patterns = DataPatterns()
        
        destinations = detector.recommend_output_destinations(
            patterns, TransformationStrategy.REAL_TIME_PROCESSING
        )
        
        assert OutputDestinationType.STREAMING_PLATFORM in destinations
        assert OutputDestinationType.CACHE in destinations
    
    def test_recommend_analytics_destinations(self, detector):
        """Test analytics destinations for numeric data."""
        patterns = DataPatterns(
            has_numeric_data=True,
            estimated_volume="large"
        )
        
        destinations = detector.recommend_output_destinations(
            patterns, TransformationStrategy.AGGREGATION_FOCUSED
        )
        
        assert OutputDestinationType.DATA_WAREHOUSE in destinations
        assert OutputDestinationType.DATA_LAKE in destinations
    
    def test_recommend_search_for_text_data(self, detector):
        """Test search engine destination for text data."""
        patterns = DataPatterns(
            has_large_text_fields=True,
            has_categorical_data=True
        )
        
        destinations = detector.recommend_output_destinations(
            patterns, TransformationStrategy.BATCH_PROCESSING
        )
        
        assert OutputDestinationType.SEARCH_ENGINE in destinations


class TestPerformanceOptimizations:
    """Test performance optimization recommendations."""
    
    def test_optimizations_for_large_data(self, detector):
        """Test optimizations for large datasets."""
        patterns = DataPatterns(estimated_volume="large")
        
        optimizations = detector.generate_performance_optimizations(
            patterns, DataSourceType.S3, TransformationStrategy.BATCH_PROCESSING
        )
        
        assert any("parallel processing" in opt.lower() for opt in optimizations)
        assert any("partitioning" in opt.lower() for opt in optimizations)
    
    def test_optimizations_for_api_source(self, detector):
        """Test API-specific optimizations."""
        patterns = DataPatterns()
        
        optimizations = detector.generate_performance_optimizations(
            patterns, DataSourceType.API, TransformationStrategy.BATCH_PROCESSING
        )
        
        assert any("rate limiting" in opt.lower() for opt in optimizations)
        assert any("connection pooling" in opt.lower() for opt in optimizations)
    
    def test_optimizations_for_complex_schema(self, detector):
        """Test optimizations for complex schemas."""
        patterns = DataPatterns(schema_complexity="complex")
        
        optimizations = detector.generate_performance_optimizations(
            patterns, DataSourceType.DATABASE, TransformationStrategy.NORMALIZATION_FOCUSED
        )
        
        assert any("schema evolution" in opt.lower() for opt in optimizations)
        assert any("nested data" in opt.lower() for opt in optimizations)


class TestResourceEstimation:
    """Test resource estimation."""
    
    def test_resource_estimation_scaling(self, detector):
        """Test resource scaling based on volume."""
        small_patterns = DataPatterns(estimated_volume="small")
        large_patterns = DataPatterns(estimated_volume="large")
        
        small_resources = detector.estimate_resources(
            small_patterns, TransformationStrategy.BATCH_PROCESSING
        )
        large_resources = detector.estimate_resources(
            large_patterns, TransformationStrategy.BATCH_PROCESSING
        )
        
        assert large_resources["cpu_cores"] > small_resources["cpu_cores"]
        assert large_resources["memory_gb"] > small_resources["memory_gb"]
        assert large_resources["storage_gb"] > small_resources["storage_gb"]
    
    def test_resource_estimation_for_aggregation(self, detector):
        """Test resource estimation for aggregation workloads."""
        patterns = DataPatterns(estimated_volume="medium")
        
        resources = detector.estimate_resources(
            patterns, TransformationStrategy.AGGREGATION_FOCUSED
        )
        
        # Aggregation should require more memory
        base_resources = detector.estimate_resources(
            patterns, TransformationStrategy.BATCH_PROCESSING
        )
        
        assert resources["memory_gb"] >= base_resources["memory_gb"]
    
    def test_resource_estimation_includes_metadata(self, detector):
        """Test that resource estimation includes additional metadata."""
        patterns = DataPatterns(estimated_volume="medium")
        
        resources = detector.estimate_resources(
            patterns, TransformationStrategy.BATCH_PROCESSING
        )
        
        required_keys = [
            "estimated_execution_time_minutes",
            "recommended_worker_count", 
            "estimated_cost_per_run",
            "scaling_recommendations"
        ]
        
        for key in required_keys:
            assert key in resources


class TestPipelineRecommendationGeneration:
    """Test complete pipeline recommendation generation."""
    
    @patch('src.agent_orchestrated_etl.intelligent_pipeline_detector.analyze_source')
    def test_generate_recommendation_with_metadata(self, mock_analyze, detector, sample_s3_metadata):
        """Test recommendation generation with provided metadata."""
        mock_analyze.return_value = sample_s3_metadata
        
        recommendation = detector.generate_pipeline_recommendation(
            "s3://test-bucket/data/", sample_s3_metadata
        )
        
        assert isinstance(recommendation, PipelineRecommendation)
        assert recommendation.confidence_score > 0.5
        assert len(recommendation.reasoning) > 0
        assert len(recommendation.performance_optimizations) > 0
    
    @patch('src.agent_orchestrated_etl.intelligent_pipeline_detector.analyze_source')
    def test_generate_recommendation_without_metadata(self, mock_analyze, detector, sample_s3_metadata):
        """Test recommendation generation without provided metadata."""
        mock_analyze.return_value = sample_s3_metadata
        
        recommendation = detector.generate_pipeline_recommendation("s3://test-bucket/data/")
        
        assert isinstance(recommendation, PipelineRecommendation)
        assert recommendation.confidence_score > 0.3
        mock_analyze.assert_called_once()
    
    def test_generate_fallback_recommendation(self, detector):
        """Test fallback recommendation generation."""
        with patch('src.agent_orchestrated_etl.intelligent_pipeline_detector.analyze_source') as mock_analyze:
            mock_analyze.side_effect = Exception("Analysis failed")
            
            recommendation = detector.generate_pipeline_recommendation("unknown://source")
            
            assert isinstance(recommendation, PipelineRecommendation)
            assert recommendation.confidence_score < 0.5
            assert len(recommendation.warnings) > 0
            assert any("fallback" in reason.lower() for reason in recommendation.reasoning)


class TestPipelineConfigCreation:
    """Test pipeline configuration creation."""
    
    def test_create_pipeline_config(self, detector):
        """Test creation of PipelineConfig from recommendations."""
        # Create a sample recommendation
        recommendation = PipelineRecommendation(
            recommended_type=PipelineType.BATCH_PROCESSING,
            optimization_strategy=OptimizationStrategy.BALANCED,
            transformation_strategy=TransformationStrategy.BATCH_PROCESSING,
            output_destinations=[OutputDestinationType.DATABASE],
            performance_optimizations=["Use parallel processing"],
            estimated_resources={
                "cpu_cores": 4,
                "memory_gb": 8,
                "storage_gb": 20,
                "estimated_execution_time_minutes": 15,
                "recommended_worker_count": 3
            },
            confidence_score=0.8,
            reasoning=["Test reasoning"],
            warnings=[],
            alternative_approaches=[]
        )
        
        config = detector.create_pipeline_config(
            "s3://test-bucket", recommendation, "test_pipeline"
        )
        
        assert config.name == "test_pipeline"
        assert config.pipeline_type == PipelineType.BATCH_PROCESSING
        assert config.optimization_strategy == OptimizationStrategy.BALANCED
        assert len(config.tasks) >= 2  # At least extract and load
        assert config.resource_allocation["cpu_cores"] == 4
        assert config.max_parallel_tasks == 3
    
    def test_create_pipeline_config_with_multiple_destinations(self, detector):
        """Test pipeline config with multiple output destinations."""
        recommendation = PipelineRecommendation(
            recommended_type=PipelineType.DATA_WAREHOUSE,
            optimization_strategy=OptimizationStrategy.ANALYTICS_OPTIMIZED,
            transformation_strategy=TransformationStrategy.AGGREGATION_FOCUSED,
            output_destinations=[
                OutputDestinationType.DATA_WAREHOUSE,
                OutputDestinationType.DATABASE
            ],
            performance_optimizations=[],
            estimated_resources={
                "cpu_cores": 2,
                "memory_gb": 4,
                "storage_gb": 10,
                "estimated_execution_time_minutes": 30,
                "recommended_worker_count": 2
            },
            confidence_score=0.7,
            reasoning=[],
            warnings=[],
            alternative_approaches=[]
        )
        
        config = detector.create_pipeline_config("api://test", recommendation)
        
        # Should have extract, transform, and multiple load tasks
        load_tasks = [task for task in config.tasks if task.task_type == "loading"]
        assert len(load_tasks) == 2  # One for each destination


class TestIntegration:
    """Integration tests."""
    
    @patch('src.agent_orchestrated_etl.intelligent_pipeline_detector.analyze_source')
    def test_end_to_end_pipeline_generation(self, mock_analyze, detector, sample_s3_metadata):
        """Test complete end-to-end pipeline generation."""
        mock_analyze.return_value = sample_s3_metadata
        
        # Generate recommendation
        recommendation = detector.generate_pipeline_recommendation("s3://test-bucket/data/")
        
        # Create pipeline config
        config = detector.create_pipeline_config(
            "s3://test-bucket/data/", recommendation, "integration_test_pipeline"
        )
        
        # Validate complete pipeline
        assert config.name == "integration_test_pipeline"
        assert len(config.tasks) >= 2
        assert config.pipeline_type in [member for member in PipelineType]
        assert config.optimization_strategy in [member for member in OptimizationStrategy]
        assert config.resource_allocation["cpu_cores"] >= 2
        assert config.timeout_minutes > 0
        
        # Validate task dependencies
        extract_tasks = [task for task in config.tasks if task.task_type == "extraction"]
        transform_tasks = [task for task in config.tasks if task.task_type == "transformation"]
        load_tasks = [task for task in config.tasks if task.task_type == "loading"]
        
        assert len(extract_tasks) >= 1
        assert len(load_tasks) >= 1
        
        # If there are transform tasks, they should depend on extract tasks
        if transform_tasks:
            for transform_task in transform_tasks:
                assert any(dep in [t.task_id for t in extract_tasks] for dep in transform_task.dependencies or [])