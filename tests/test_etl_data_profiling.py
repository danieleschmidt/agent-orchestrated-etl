"""Tests for ETL Agent advanced data profiling capabilities."""

import pytest
from unittest.mock import patch

from src.agent_orchestrated_etl.agents.etl_agent import (
    ETLAgent, 
    ProfilingConfig, 
    ColumnProfile
)
from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole, AgentTask
from src.agent_orchestrated_etl.exceptions import DataProcessingException


class TestProfilingConfig:
    """Test ProfilingConfig dataclass."""
    
    def test_default_config(self):
        """Test ProfilingConfig with default values."""
        config = ProfilingConfig()
        
        assert config.statistical_analysis is True
        assert config.anomaly_detection is True
        assert config.data_quality_scoring is True
        assert config.pattern_detection is True
        assert config.sample_size == 10000
        assert config.sample_percentage == 10.0
        assert config.outlier_method == "iqr"
        assert config.outlier_threshold == 1.5
        assert config.zscore_threshold == 3.0
        assert config.completeness_threshold == 0.95
        assert config.correlation_analysis is False
        assert config.distribution_analysis is True
    
    def test_custom_config(self):
        """Test ProfilingConfig with custom values."""
        config = ProfilingConfig(
            statistical_analysis=False,
            anomaly_detection=True,
            sample_size=5000,
            outlier_method="zscore",
            outlier_threshold=2.0,
            zscore_threshold=2.5,
            correlation_analysis=True
        )
        
        assert config.statistical_analysis is False
        assert config.anomaly_detection is True
        assert config.sample_size == 5000
        assert config.outlier_method == "zscore"
        assert config.outlier_threshold == 2.0
        assert config.zscore_threshold == 2.5
        assert config.correlation_analysis is True


class TestColumnProfile:
    """Test ColumnProfile dataclass."""
    
    def test_column_profile_creation(self):
        """Test ColumnProfile creation with basic fields."""
        profile = ColumnProfile(
            name="test_column",
            data_type="integer",
            null_count=5,
            null_percentage=5.0,
            unique_count=90,
            unique_percentage=90.0
        )
        
        assert profile.name == "test_column"
        assert profile.data_type == "integer"
        assert profile.null_count == 5
        assert profile.null_percentage == 5.0
        assert profile.unique_count == 90
        assert profile.unique_percentage == 90.0
        
        # Test default values
        assert profile.completeness_score == 1.0
        assert profile.validity_score == 1.0
        assert profile.consistency_score == 1.0
        assert profile.outlier_count == 0
        assert profile.outlier_percentage == 0.0
    
    def test_column_profile_post_init(self):
        """Test ColumnProfile post-init mutable field initialization."""
        profile = ColumnProfile(
            name="test_column",
            data_type="string",
            null_count=0,
            null_percentage=0.0,
            unique_count=100,
            unique_percentage=100.0
        )
        
        assert isinstance(profile.detected_patterns, list)
        assert isinstance(profile.outlier_values, list)
        assert isinstance(profile.percentiles, dict)
        assert len(profile.detected_patterns) == 0
        assert len(profile.outlier_values) == 0
        assert len(profile.percentiles) == 0


class TestETLDataProfiling:
    """Test ETL Agent data profiling functionality."""
    
    @pytest.fixture
    def etl_agent(self):
        """Create ETL agent for testing."""
        config = AgentConfig(
            name="TestETLAgent", 
            role=AgentRole.ETL_SPECIALIST
        )
        agent = ETLAgent(config, specialization="general")
        return agent
    
    @pytest.fixture
    def profiling_task(self):
        """Create profiling task for testing."""
        return AgentTask(
            task_type="profile_data",
            description="Profile test dataset",
            inputs={
                "data_source": "test_source",
                "profiling_config": {
                    "statistical_analysis": True,
                    "anomaly_detection": True,
                    "data_quality_scoring": True,
                    "sample_size": 1000,
                    "outlier_method": "iqr"
                }
            }
        )
    
    @pytest.mark.asyncio
    async def test_profile_data_task(self, etl_agent, profiling_task):
        """Test the main profile_data task execution."""
        # Mock the comprehensive profiling method
        expected_result = {
            "data_source": "test_source",
            "profiling_timestamp": 1640995200.0,
            "execution_time_seconds": 0.5,
            "overall_quality_score": 0.85,
            "column_profiles": [],
            "recommendations": []
        }
        
        with patch.object(etl_agent, '_perform_comprehensive_profiling', return_value=expected_result):
            with patch.object(etl_agent, '_store_etl_memory'):
                result = await etl_agent._profile_data(profiling_task)
                
                assert result == expected_result
                assert result["data_source"] == "test_source"
                assert result["overall_quality_score"] == 0.85
    
    @pytest.mark.asyncio 
    async def test_profile_data_error_handling(self, etl_agent, profiling_task):
        """Test error handling in profile_data task."""
        with patch.object(etl_agent, '_perform_comprehensive_profiling', side_effect=Exception("Profiling failed")):
            with pytest.raises(DataProcessingException) as exc_info:
                await etl_agent._profile_data(profiling_task)
            
            assert "Data profiling failed" in str(exc_info.value)
            assert "Profiling failed" in str(exc_info.value)
    
    def test_infer_data_type(self, etl_agent):
        """Test data type inference."""
        # Test integer data
        assert etl_agent._infer_data_type([1, 2, 3, 4, 5]) == "integer"
        
        # Test float data
        assert etl_agent._infer_data_type([1.1, 2.2, 3.3]) == "float"
        
        # Test string data
        assert etl_agent._infer_data_type(["a", "b", "c"]) == "string"
        
        # Test boolean data
        assert etl_agent._infer_data_type([True, False, True]) == "boolean"
        
        # Test mixed data (should return string)
        assert etl_agent._infer_data_type([1, "a", 2.5]) == "string"
        
        # Test empty data
        assert etl_agent._infer_data_type([]) == "unknown"
    
    def test_detect_outliers_iqr(self, etl_agent):
        """Test IQR outlier detection method."""
        # Test data with clear outliers
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 100]  # 100 is an outlier
        
        outliers = etl_agent._detect_outliers_iqr(data, 1.5)
        assert 100 in outliers
        assert len(outliers) >= 1
        
        # Test normal data without outliers
        normal_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        outliers = etl_agent._detect_outliers_iqr(normal_data, 1.5)
        assert len(outliers) == 0
        
        # Test insufficient data
        small_data = [1, 2, 3]
        outliers = etl_agent._detect_outliers_iqr(small_data, 1.5)
        assert len(outliers) == 0
    
    def test_detect_outliers_zscore(self, etl_agent):
        """Test Z-score outlier detection method."""
        # Test data with outliers
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 50]  # 50 might be an outlier
        
        outliers = etl_agent._detect_outliers_zscore(data, 2.0)
        assert isinstance(outliers, list)
        
        # Test data with no variance
        uniform_data = [5, 5, 5, 5, 5]
        outliers = etl_agent._detect_outliers_zscore(uniform_data, 2.0)
        assert len(outliers) == 0
        
        # Test insufficient data
        small_data = [1]
        outliers = etl_agent._detect_outliers_zscore(small_data, 2.0)
        assert len(outliers) == 0
    
    def test_detect_outliers_simple_isolation(self, etl_agent):
        """Test simple isolation forest outlier detection."""
        # Test sufficient data
        data = list(range(1, 20)) + [100, 200]  # Add some outliers
        
        outliers = etl_agent._detect_outliers_simple_isolation(data)
        assert isinstance(outliers, list)
        # Should detect the extreme values
        assert 100 in outliers or 200 in outliers
        
        # Test insufficient data
        small_data = [1, 2, 3, 4, 5]
        outliers = etl_agent._detect_outliers_simple_isolation(small_data)
        assert len(outliers) == 0
    
    def test_calculate_percentile(self, etl_agent):
        """Test percentile calculation."""
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        
        # Test various percentiles
        p25 = etl_agent._calculate_percentile(data, 25)
        p50 = etl_agent._calculate_percentile(data, 50)
        p75 = etl_agent._calculate_percentile(data, 75)
        
        assert p25 < p50 < p75
        assert p50 == 5.5  # Median of 1-10
        
        # Test edge cases
        assert etl_agent._calculate_percentile([1], 50) == 1
        assert etl_agent._calculate_percentile([1, 2], 50) == 1.5
    
    def test_profile_column_numeric(self, etl_agent):
        """Test column profiling for numeric data."""
        config = ProfilingConfig(
            statistical_analysis=True,
            anomaly_detection=True,
            data_quality_scoring=True,
            percentiles=[25, 50, 75, 90]
        )
        
        # Test data with some nulls and outliers
        column_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 100, None, None]
        
        profile = etl_agent._profile_column("test_numeric", column_data, config)
        
        assert profile.name == "test_numeric"
        assert profile.data_type == "integer"
        assert profile.null_count == 2
        assert profile.null_percentage == (2/12 * 100)
        assert profile.unique_count == 10  # 10 unique non-null values
        
        # Check statistical measures
        assert profile.mean is not None
        assert profile.median is not None
        assert profile.std_dev is not None
        assert profile.min_value == 1
        assert profile.max_value == 100
        
        # Check percentiles
        assert len(profile.percentiles) > 0
        assert "p50" in profile.percentiles  # Median should be calculated
        
        # Check outlier detection
        assert profile.outlier_count > 0  # 100 should be detected as outlier
        assert profile.outlier_percentage > 0
        assert 100 in profile.outlier_values
        
        # Check quality scores
        assert 0 <= profile.completeness_score <= 1
        assert 0 <= profile.validity_score <= 1
        assert 0 <= profile.consistency_score <= 1
    
    def test_profile_column_string(self, etl_agent):
        """Test column profiling for string data."""
        config = ProfilingConfig(
            statistical_analysis=True,
            pattern_detection=True,
            data_quality_scoring=True
        )
        
        column_data = ["hello", "world", "test", "hello", None, "a", "longer string"]
        
        profile = etl_agent._profile_column("test_string", column_data, config)
        
        assert profile.name == "test_string"
        assert profile.data_type == "string"
        assert profile.null_count == 1
        assert profile.unique_count == 5  # 5 unique non-null values (hello appears twice)
        
        # Check string-specific measures
        assert profile.avg_length is not None
        assert profile.min_length == 1  # "a"
        assert profile.max_length == 13  # "longer string"
        
        # Check quality scores
        assert profile.completeness_score < 1.0  # Due to null value
        assert profile.validity_score > 0
    
    def test_calculate_overall_quality_score(self, etl_agent):
        """Test overall quality score calculation."""
        # Create mock column profiles
        profiles = [
            ColumnProfile(
                name="col1", data_type="integer", null_count=0, null_percentage=0,
                unique_count=100, unique_percentage=100,
                completeness_score=1.0, validity_score=0.9, consistency_score=0.8
            ),
            ColumnProfile(
                name="col2", data_type="string", null_count=5, null_percentage=5,
                unique_count=95, unique_percentage=95,
                completeness_score=0.95, validity_score=0.85, consistency_score=0.9
            )
        ]
        
        quality_score = etl_agent._calculate_overall_quality_score(profiles)
        
        assert 0 <= quality_score <= 1
        assert isinstance(quality_score, float)
        
        # Test empty profiles
        assert etl_agent._calculate_overall_quality_score([]) == 1.0
    
    def test_generate_profiling_recommendations(self, etl_agent):
        """Test profiling recommendations generation."""
        # Create profiles with various issues
        profiles = [
            ColumnProfile(
                name="high_nulls", data_type="string", null_count=50, null_percentage=50,
                unique_count=50, unique_percentage=50,
                completeness_score=0.5, validity_score=0.8, consistency_score=0.9
            ),
            ColumnProfile(
                name="many_outliers", data_type="integer", null_count=0, null_percentage=0,
                unique_count=100, unique_percentage=100,
                completeness_score=1.0, validity_score=0.7, consistency_score=0.8,
                outlier_count=20, outlier_percentage=20.0
            )
        ]
        
        recommendations = etl_agent._generate_profiling_recommendations(profiles, 0.7)
        
        assert isinstance(recommendations, list)
        assert len(recommendations) > 0
        
        # Check that recommendations contain actionable advice
        rec_text = " ".join(recommendations)
        assert any(keyword in rec_text.lower() for keyword in ["null", "missing", "outlier", "quality"])
    
    def test_analyze_correlations(self, etl_agent):
        """Test correlation analysis between columns."""
        # Mock sample data with numeric columns
        sample_data = {
            "columns": ["col1", "col2", "col3"],
            "data": {
                "col1": [1, 2, 3, 4, 5],
                "col2": [2, 4, 6, 8, 10],  # Perfect correlation with col1
                "col3": ["a", "b", "c", "d", "e"]  # Non-numeric
            }
        }
        
        # Create column profiles
        profiles = [
            ColumnProfile(name="col1", data_type="integer", null_count=0, null_percentage=0, unique_count=5, unique_percentage=100),
            ColumnProfile(name="col2", data_type="integer", null_count=0, null_percentage=0, unique_count=5, unique_percentage=100),
            ColumnProfile(name="col3", data_type="string", null_count=0, null_percentage=0, unique_count=5, unique_percentage=100)
        ]
        
        correlations = etl_agent._analyze_correlations(sample_data, profiles)
        
        assert isinstance(correlations, dict)
        # Should find correlation between col1 and col2 (both numeric)
        if correlations:  # Only check if correlations were calculated
            assert any("col1" in key and "col2" in key for key in correlations.keys())
    
    @pytest.mark.asyncio
    async def test_load_sample_data(self, etl_agent):
        """Test sample data loading for profiling (mock implementation)."""
        config = ProfilingConfig(sample_size=100)
        
        sample_data = await etl_agent._load_sample_data("test_source", config)
        
        assert isinstance(sample_data, dict)
        assert "columns" in sample_data
        assert "data" in sample_data
        assert isinstance(sample_data["columns"], list)
        assert isinstance(sample_data["data"], dict)
        
        # Check that sample data contains expected columns (based on actual implementation)
        expected_columns = ["user_id", "email", "age", "salary", "registration_date", "is_premium", "last_login"]
        for col in expected_columns:
            assert col in sample_data["columns"]
            assert col in sample_data["data"]
        
        # Check sample size constraint
        if sample_data["data"]:
            first_column = list(sample_data["data"].values())[0]
            assert len(first_column) <= config.sample_size
    
    @pytest.mark.asyncio
    async def test_load_real_sample_data_database(self, etl_agent):
        """Test real database sampling functionality - ETL-011 implementation."""
        config = ProfilingConfig(sample_size=5, sampling_strategy='random')
        
        # Mock database configuration
        data_source_config = {
            "type": "database",
            "connection_string": "sqlite:///test.db",
            "table_name": "test_table",
            "sampling_strategy": "random",
            "sample_size": 5
        }
        
        # Mock the database sampling method to test the interface
        with patch.object(etl_agent, '_sample_database_data') as mock_db_sample:
            mock_db_sample.return_value = {
                "columns": ["id", "name", "value"],
                "total_records": 5,
                "data": {
                    "id": [1, 2, 3, 4, 5],
                    "name": ["A", "B", "C", "D", "E"],
                    "value": [10.1, 20.2, 30.3, 40.4, 50.5]
                },
                "sampling_metadata": {
                    "strategy": "random",
                    "requested_size": 5,
                    "actual_size": 5,
                    "source_type": "database"
                }
            }
            
            result = await etl_agent._load_real_sample_data(data_source_config, config)
            
            # Verify the method was called with correct parameters
            mock_db_sample.assert_called_once_with(data_source_config, 'random', 5)
            
            # Verify result structure
            assert result["columns"] == ["id", "name", "value"]
            assert result["total_records"] == 5
            assert "sampling_metadata" in result
            assert result["sampling_metadata"]["strategy"] == "random"
            assert result["sampling_metadata"]["source_type"] == "database"
    
    @pytest.mark.asyncio 
    async def test_load_real_sample_data_file(self, etl_agent):
        """Test real file sampling functionality - ETL-011 implementation."""
        config = ProfilingConfig(sample_size=3, sampling_strategy='systematic')
        
        # Mock file configuration
        data_source_config = {
            "type": "file",
            "file_path": "/path/to/test.csv",
            "file_format": "csv",
            "sampling_strategy": "systematic",
            "sample_size": 3
        }
        
        # Mock the file sampling method
        with patch.object(etl_agent, '_sample_file_data') as mock_file_sample:
            mock_file_sample.return_value = {
                "columns": ["product_id", "name", "price"],
                "total_records": 3,
                "data": {
                    "product_id": ["1", "3", "5"],
                    "name": ["Product A", "Product C", "Product E"],
                    "price": ["10.99", "30.99", "50.99"]
                },
                "sampling_metadata": {
                    "strategy": "systematic",
                    "requested_size": 3,
                    "actual_size": 3,
                    "source_type": "csv_file"
                }
            }
            
            result = await etl_agent._load_real_sample_data(data_source_config, config)
            
            # Verify the method was called with correct parameters
            mock_file_sample.assert_called_once_with(data_source_config, 'systematic', 3)
            
            # Verify result structure
            assert result["columns"] == ["product_id", "name", "price"]
            assert result["total_records"] == 3
            assert result["sampling_metadata"]["strategy"] == "systematic"
            assert result["sampling_metadata"]["source_type"] == "csv_file"
    
    @pytest.mark.asyncio
    async def test_load_real_sample_data_unsupported_type(self, etl_agent):
        """Test error handling for unsupported data source types."""
        config = ProfilingConfig(sample_size=10)
        
        data_source_config = {
            "type": "unsupported_type",
            "sample_size": 10
        }
        
        with pytest.raises(ValueError) as exc_info:
            await etl_agent._load_real_sample_data(data_source_config, config)
        
        assert "Unsupported data source type for sampling: unsupported_type" in str(exc_info.value)
    
    def test_sampling_strategies_support(self, etl_agent):
        """Test that all required sampling strategies are supported."""
        # This test validates that the sampling strategies mentioned in acceptance criteria are implemented
        
        # Test that ProfilingConfig supports sampling_strategy
        config = ProfilingConfig(sampling_strategy='random')
        assert config.sampling_strategy == 'random'
        
        config = ProfilingConfig(sampling_strategy='systematic') 
        assert config.sampling_strategy == 'systematic'
        
        config = ProfilingConfig(sampling_strategy='stratified')
        assert config.sampling_strategy == 'stratified'
        
        config = ProfilingConfig(sampling_strategy='reservoir')
        assert config.sampling_strategy == 'reservoir'
        
        # Test default sampling strategy
        config = ProfilingConfig()
        assert config.sampling_strategy == 'random'
    
    def test_analyze_dataset_structure(self, etl_agent):
        """Test dataset structure analysis."""
        sample_data = {
            "columns": ["col1", "col2", "col3"],
            "total_records": 5,  # Include total_records as expected by implementation
            "data": {
                "col1": [1, 2, None, 4, 5],
                "col2": [1, 2, 3, 4, 5],
                "col3": [None, None, None, None, None]
            }
        }
        
        stats = etl_agent._analyze_dataset_structure(sample_data)
        
        assert stats["total_records"] == 5
        assert stats["total_columns"] == 3
        assert stats["columns"] == ["col1", "col2", "col3"]
        assert stats["has_missing_data"] is True
        assert "sparsity" in stats
        assert isinstance(stats["sparsity"], float)
    
    def test_column_profile_to_dict(self, etl_agent):
        """Test column profile serialization to dictionary."""
        profile = ColumnProfile(
            name="test_col",
            data_type="integer", 
            null_count=5,
            null_percentage=5.0,
            unique_count=95,
            unique_percentage=95.0,
            mean=50.0,
            median=45.0,
            std_dev=15.5,
            min_value=1,
            max_value=100
        )
        profile.percentiles = {"p25": 25, "p75": 75}
        profile.outlier_values = [100, 99]
        
        profile_dict = etl_agent._column_profile_to_dict(profile)
        
        assert isinstance(profile_dict, dict)
        assert profile_dict["name"] == "test_col"
        assert profile_dict["data_type"] == "integer"
        assert profile_dict["null_count"] == 5
        assert profile_dict["mean"] == 50.0
        assert profile_dict["percentiles"] == {"p25": 25, "p75": 75}
        assert profile_dict["outlier_values"] == [100, 99]


class TestProfilingIntegration:
    """Integration tests for complete profiling workflow."""
    
    @pytest.fixture
    def etl_agent(self):
        """Create ETL agent for integration testing."""
        config = AgentConfig(
            name="IntegrationETLAgent", 
            role=AgentRole.ETL_SPECIALIST
        )
        return ETLAgent(config, specialization="general")
    
    @pytest.mark.asyncio
    async def test_comprehensive_profiling_workflow(self, etl_agent):
        """Test the complete profiling workflow end-to-end."""
        config = ProfilingConfig(
            statistical_analysis=True,
            anomaly_detection=True,
            data_quality_scoring=True,
            pattern_detection=True,
            correlation_analysis=True,
            sample_size=500,
            outlier_method="iqr",
            percentiles=[10, 25, 50, 75, 90]
        )
        
        result = await etl_agent._perform_comprehensive_profiling("test_source", config)
        
        # Verify top-level structure
        assert "data_source" in result
        assert "profiling_timestamp" in result
        assert "execution_time_seconds" in result
        assert "profiling_config" in result
        assert "dataset_statistics" in result
        assert "column_profiles" in result
        assert "overall_quality_score" in result
        assert "quality_breakdown" in result
        assert "recommendations" in result
        assert "anomaly_summary" in result
        
        # Verify profiling config
        assert result["profiling_config"]["statistical_analysis"] is True
        assert result["profiling_config"]["anomaly_detection"] is True
        assert result["profiling_config"]["outlier_method"] == "iqr"
        
        # Verify dataset statistics
        dataset_stats = result["dataset_statistics"]
        assert "total_records" in dataset_stats
        assert "total_columns" in dataset_stats
        assert "columns" in dataset_stats
        
        # Verify column profiles
        assert isinstance(result["column_profiles"], list)
        assert len(result["column_profiles"]) > 0
        
        # Check first column profile structure
        if result["column_profiles"]:
            profile = result["column_profiles"][0]
            assert "name" in profile
            assert "data_type" in profile
            assert "null_count" in profile
            assert "completeness_score" in profile
        
        # Verify quality breakdown
        quality = result["quality_breakdown"]
        assert "completeness" in quality
        assert "validity" in quality
        assert "consistency" in quality
        assert "outlier_impact" in quality
        
        # Verify anomaly summary
        anomaly = result["anomaly_summary"] 
        assert "total_outliers" in anomaly
        assert "columns_with_outliers" in anomaly
        assert "avg_outlier_percentage" in anomaly
        
        # Verify overall quality score is reasonable
        assert 0 <= result["overall_quality_score"] <= 1
        
        # Verify execution time is recorded
        assert result["execution_time_seconds"] > 0
    
    @pytest.mark.asyncio
    async def test_profiling_with_different_configurations(self, etl_agent):
        """Test profiling with various configuration combinations."""
        configs = [
            ProfilingConfig(statistical_analysis=False, anomaly_detection=True),
            ProfilingConfig(statistical_analysis=True, anomaly_detection=False),
            ProfilingConfig(outlier_method="zscore", zscore_threshold=2.0),
            ProfilingConfig(sample_size=100),
            ProfilingConfig(correlation_analysis=True)
        ]
        
        for config in configs:
            result = await etl_agent._perform_comprehensive_profiling("test_source", config)
            
            # Each configuration should produce valid results
            assert isinstance(result, dict)
            assert "data_source" in result
            assert "overall_quality_score" in result
            assert 0 <= result["overall_quality_score"] <= 1
    
    def test_profiling_performance_metrics(self, etl_agent):
        """Test that profiling captures performance metrics."""
        # This test verifies that timing and performance data is captured
        config = ProfilingConfig(sample_size=1000)
        
        # Use synchronous version for testing
        import asyncio
        result = asyncio.run(etl_agent._perform_comprehensive_profiling("perf_test", config))
        
        assert "execution_time_seconds" in result
        assert isinstance(result["execution_time_seconds"], float)
        assert result["execution_time_seconds"] > 0
        
        # Verify profiling metadata
        assert result["profiling_config"]["sample_size"] == 1000
        assert "profiling_timestamp" in result