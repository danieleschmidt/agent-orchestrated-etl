"""Comprehensive tests for enhanced ETL features."""

import pytest
import asyncio
import time
from unittest.mock import Mock, patch, MagicMock

from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator, PipelineConfig
from agent_orchestrated_etl.security import SecurityValidator, SecurityConfig
from agent_orchestrated_etl.enhanced_validation import EnhancedDataValidator
from agent_orchestrated_etl.performance_cache import PerformanceCache, CacheConfig
from agent_orchestrated_etl.concurrent_processing import ConcurrentProcessor, ConcurrencyConfig
from agent_orchestrated_etl.error_recovery import ErrorRecoveryManager, RecoveryStrategy, RecoveryConfig


class TestEnhancedOrchestrator:
    """Test the enhanced orchestrator functionality."""
    
    @pytest.fixture
    def orchestrator(self):
        return EnhancedDataOrchestrator(enable_security=False, enable_validation=False)
    
    @pytest.fixture
    def secure_orchestrator(self):
        return EnhancedDataOrchestrator(enable_security=True, enable_validation=True)
    
    def test_orchestrator_initialization(self, orchestrator):
        """Test orchestrator initializes correctly."""
        assert orchestrator is not None
        assert orchestrator.logger is not None
        assert orchestrator.pipeline_history == []
    
    def test_secure_orchestrator_initialization(self, secure_orchestrator):
        """Test secure orchestrator with security features."""
        assert secure_orchestrator.security_validator is not None
        assert secure_orchestrator.data_validator is not None
        assert secure_orchestrator.audit_logger is not None
    
    def test_create_pipeline_s3_source(self, orchestrator):
        """Test creating pipeline with S3 source."""
        pipeline = orchestrator.create_pipeline("s3://test-bucket/data/")
        
        assert pipeline is not None
        assert pipeline.config.source_config["type"] == "s3"
        assert pipeline.config.source_config["bucket"] == "test-bucket"
        assert pipeline.config.source_config["key"] == "data/"
    
    def test_create_pipeline_api_source(self, orchestrator):
        """Test creating pipeline with API source."""
        pipeline = orchestrator.create_pipeline("api://example.com/data")
        
        assert pipeline.config.source_config["type"] == "api"
        assert pipeline.config.source_config["endpoint"] == "https://example.com/data"
    
    def test_create_pipeline_database_source(self, orchestrator):
        """Test creating pipeline with database source."""
        pipeline = orchestrator.create_pipeline("db://postgres://user:pass@host:5432/db")
        
        assert pipeline.config.source_config["type"] == "database"
        assert "postgres" in pipeline.config.source_config["connection_string"]
    
    def test_create_pipeline_file_source(self, orchestrator):
        """Test creating pipeline with file source."""
        pipeline = orchestrator.create_pipeline("/path/to/data.json")
        
        assert pipeline.config.source_config["type"] == "file"
        assert pipeline.config.source_config["path"] == "/path/to/data.json"
    
    def test_pipeline_preview_tasks(self, orchestrator):
        """Test pipeline task preview functionality."""
        pipeline = orchestrator.create_pipeline("s3://test-bucket/data/")
        tasks = pipeline.preview_tasks()
        
        assert len(tasks) > 0
        assert any("Extract data" in task for task in tasks)
        assert any("transformation" in task.lower() for task in tasks)
        assert any("Load data" in task for task in tasks)


class TestSecurityFeatures:
    """Test security validation and features."""
    
    @pytest.fixture
    def security_validator(self):
        return SecurityValidator(SecurityConfig())
    
    def test_sql_injection_detection(self, security_validator):
        """Test SQL injection pattern detection."""
        malicious_input = "'; DROP TABLE users; --"
        
        with pytest.raises(Exception):
            security_validator.validate_input(malicious_input, "test_context")
    
    def test_xss_detection(self, security_validator):
        """Test XSS pattern detection."""
        malicious_input = "<script>alert('xss')</script>"
        
        with pytest.raises(Exception):
            security_validator.validate_input(malicious_input, "test_context")
    
    def test_path_traversal_detection(self, security_validator):
        """Test path traversal detection."""
        malicious_input = "../../../etc/passwd"
        
        with pytest.raises(Exception):
            security_validator.validate_input(malicious_input, "test_context")
    
    def test_valid_input(self, security_validator):
        """Test that valid input passes validation."""
        valid_input = "normal user input data"
        
        result = security_validator.validate_input(valid_input, "test_context")
        assert result is True
    
    def test_rate_limiting(self, security_validator):
        """Test rate limiting functionality."""
        client_id = "test_client"
        
        # First requests should pass
        for _ in range(50):
            assert security_validator.check_rate_limit(client_id) is True
        
        # Exceeding limit should fail
        for _ in range(51):
            security_validator.check_rate_limit(client_id)
        
        assert security_validator.check_rate_limit(client_id) is False
    
    def test_api_key_generation(self, security_validator):
        """Test API key generation and validation."""
        client_name = "test_client"
        api_key = security_validator.generate_api_key(client_name)
        
        assert len(api_key) > 20
        assert security_validator.validate_api_key(api_key) is True
        assert security_validator.validate_api_key("invalid_key") is False


class TestDataValidation:
    """Test data validation features."""
    
    @pytest.fixture
    def validator(self):
        return EnhancedDataValidator()
    
    def test_email_validation(self, validator):
        """Test email format validation."""
        test_data = [
            {"email": "valid@example.com", "name": "User1"},
            {"email": "invalid-email", "name": "User2"},
            {"email": "another@valid.co.uk", "name": "User3"}
        ]
        
        report = validator.validate_dataset(test_data)
        
        assert report.total_records == 3
        assert len(report.validation_errors) > 0
        assert any(error["field"] == "email" for error in report.validation_errors)
    
    def test_non_empty_validation(self, validator):
        """Test non-empty field validation."""
        test_data = [
            {"name": "Valid Name", "title": "Valid Title"},
            {"name": "", "title": "Valid Title"},
            {"name": "Valid Name", "title": None}
        ]
        
        report = validator.validate_dataset(test_data)
        
        assert report.total_records == 3
        assert len(report.warnings) > 0
    
    def test_positive_number_validation(self, validator):
        """Test positive number validation."""
        test_data = [
            {"amount": 100, "quantity": 5},
            {"amount": -50, "quantity": 0},
            {"amount": 200, "quantity": 10}
        ]
        
        report = validator.validate_dataset(test_data)
        
        assert report.total_records == 3
        assert len(report.validation_errors) > 0
    
    def test_quality_score_calculation(self, validator):
        """Test data quality score calculation."""
        perfect_data = [
            {"email": "user1@example.com", "amount": 100},
            {"email": "user2@example.com", "amount": 200}
        ]
        
        report = validator.validate_dataset(perfect_data)
        
        assert report.quality_score > 90
        assert report.valid_records == report.total_records
    
    def test_field_statistics(self, validator):
        """Test field statistics calculation."""
        test_data = [
            {"name": "Alice", "age": 25, "score": 95.5},
            {"name": "Bob", "age": 30, "score": 87.2},
            {"name": "", "age": None, "score": 92.1}
        ]
        
        report = validator.validate_dataset(test_data)
        
        assert "name" in report.field_statistics
        assert "age" in report.field_statistics
        assert report.field_statistics["name"]["null_count"] > 0


class TestPerformanceCache:
    """Test caching functionality."""
    
    @pytest.fixture
    def cache(self):
        return PerformanceCache(CacheConfig(max_size=10, ttl_seconds=1))
    
    def test_cache_miss_and_hit(self, cache):
        """Test cache miss followed by cache hit."""
        
        def expensive_computation(x):
            time.sleep(0.01)  # Simulate expensive operation
            return x * 2
        
        # First call should be cache miss
        start_time = time.time()
        result1 = cache.memory_cache.get("test_key")
        assert result1 is None
        
        # Cache the result
        cache.memory_cache.put("test_key", expensive_computation(5))
        
        # Second call should be cache hit
        result2 = cache.memory_cache.get("test_key")
        assert result2 == 10
    
    @pytest.mark.asyncio
    async def test_get_or_compute(self, cache):
        """Test get_or_compute functionality."""
        
        async def async_computation(x):
            await asyncio.sleep(0.01)
            return x * 3
        
        # First call computes
        result1 = await cache.get_or_compute("test_op", async_computation, 5)
        assert result1 == 15
        
        # Second call uses cache
        start_time = time.time()
        result2 = await cache.get_or_compute("test_op", async_computation, 5)
        end_time = time.time()
        
        assert result2 == 15
        assert (end_time - start_time) < 0.005  # Should be much faster
    
    def test_cache_ttl_expiration(self, cache):
        """Test cache TTL expiration."""
        cache.memory_cache.put("expiring_key", "test_value")
        
        # Should be available immediately
        assert cache.memory_cache.get("expiring_key") == "test_value"
        
        # Wait for expiration
        time.sleep(1.1)
        
        # Should be expired
        assert cache.memory_cache.get("expiring_key") is None
    
    def test_cache_size_limit(self, cache):
        """Test cache size limit enforcement."""
        # Fill cache beyond limit
        for i in range(15):
            cache.memory_cache.put(f"key_{i}", f"value_{i}")
        
        # Should have evicted some entries
        stats = cache.memory_cache.get_stats()
        assert stats["entry_count"] <= 10
        assert stats["evictions"] > 0


class TestConcurrentProcessing:
    """Test concurrent processing capabilities."""
    
    @pytest.fixture
    def processor(self):
        config = ConcurrencyConfig(max_workers=2, chunk_size=3)
        return ConcurrentProcessor(config)
    
    @pytest.mark.asyncio
    async def test_async_batch_processing(self, processor):
        """Test asynchronous batch processing."""
        
        def double_value(x):
            return x * 2
        
        data = list(range(10))
        results = await processor.process_batch_async(data, double_value)
        
        assert len(results) == 10
        assert results == [i * 2 for i in range(10)]
    
    def test_sync_batch_processing(self, processor):
        """Test synchronous batch processing."""
        
        def square_value(x):
            return x ** 2
        
        data = list(range(5))
        results = processor.process_batch_sync(data, square_value)
        
        assert len(results) == 5
        assert results == [i ** 2 for i in range(5)]
    
    def test_chunk_creation(self, processor):
        """Test data chunking functionality."""
        data = list(range(10))
        chunks = processor._create_chunks(data, 3)
        
        assert len(chunks) == 4  # 3 + 3 + 3 + 1
        assert chunks[0] == [0, 1, 2]
        assert chunks[1] == [3, 4, 5]
        assert chunks[3] == [9]
    
    def test_performance_metrics(self, processor):
        """Test performance metrics collection."""
        metrics = processor.get_performance_metrics()
        
        assert "config" in metrics
        assert "system_info" in metrics
        assert metrics["config"]["max_workers"] == 2
        assert metrics["config"]["chunk_size"] == 3


class TestErrorRecovery:
    """Test error recovery mechanisms."""
    
    @pytest.fixture
    def recovery_manager(self):
        return ErrorRecoveryManager(RecoveryConfig(max_retries=2))
    
    @pytest.mark.asyncio
    async def test_successful_operation(self, recovery_manager):
        """Test successful operation execution."""
        
        async def successful_op():
            return "success"
        
        result = await recovery_manager.execute_with_recovery(
            successful_op,
            "test_operation",
            [RecoveryStrategy.RETRY]
        )
        
        assert result == "success"
    
    @pytest.mark.asyncio
    async def test_retry_mechanism(self, recovery_manager):
        """Test retry mechanism for failing operations."""
        
        call_count = 0
        
        async def failing_then_success():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary failure")
            return "recovered"
        
        result = await recovery_manager.execute_with_recovery(
            failing_then_success,
            "test_retry",
            [RecoveryStrategy.RETRY]
        )
        
        assert result == "recovered"
        assert call_count == 2
    
    @pytest.mark.asyncio
    async def test_fallback_recovery(self, recovery_manager):
        """Test fallback data recovery strategy."""
        
        async def always_failing():
            raise Exception("Persistent failure")
        
        result = await recovery_manager.execute_with_recovery(
            always_failing,
            "test_fallback",
            [RecoveryStrategy.FALLBACK_DATA]
        )
        
        assert result["status"] == "fallback"
        assert "data" in result
    
    def test_error_statistics(self, recovery_manager):
        """Test error statistics collection."""
        
        # Simulate some errors
        from agent_orchestrated_etl.error_recovery import ErrorContext
        
        error1 = ErrorContext(
            error=Exception("Test error 1"),
            operation="test_op",
            attempt_number=1,
            timestamp=time.time(),
            metadata={}
        )
        
        recovery_manager.error_history.append(error1)
        
        stats = recovery_manager.get_error_statistics()
        
        assert stats["total_errors"] == 1
        assert "test_op" in stats["operations"]


@pytest.mark.integration
class TestIntegrationScenarios:
    """Integration tests for complete workflows."""
    
    @pytest.mark.asyncio
    async def test_full_secure_pipeline_execution(self):
        """Test complete pipeline execution with security features."""
        
        orchestrator = EnhancedDataOrchestrator(enable_security=True, enable_validation=True)
        
        # Mock the agent initialization to avoid complex setup
        with patch.object(orchestrator, 'initialize_agents'):
            
            config = PipelineConfig(
                source_config={"type": "sample", "path": "test_data"},
                transformation_rules=[],
                destination_config={"type": "memory"}
            )
            
            # Mock the core functions to avoid dependencies
            with patch('agent_orchestrated_etl.enhanced_orchestrator.primary_data_extraction') as mock_extract, \
                 patch('agent_orchestrated_etl.enhanced_orchestrator.transform_data') as mock_transform, \
                 patch('agent_orchestrated_etl.enhanced_orchestrator.load_data') as mock_load:
                
                mock_extract.return_value = [{"id": 1, "data": "test"}]
                mock_transform.return_value = [{"id": 1, "data": "transformed"}]
                mock_load.return_value = {"status": "completed", "records_loaded": 1}
                
                result = await orchestrator.execute_pipeline(config, client_id="test_client")
                
                assert result["status"] == "success"
                assert result["metrics"]["records_processed"] == 1
                assert "execution_id" in result
    
    def test_pipeline_with_custom_operations(self):
        """Test pipeline with custom transformation and loading operations."""
        
        orchestrator = EnhancedDataOrchestrator(enable_security=False)
        
        def custom_transform(data):
            return [{"custom": True, **item} for item in data]
        
        def custom_load(data):
            return {"custom_load": True, "count": len(data)}
        
        pipeline = orchestrator.create_pipeline(
            "s3://test-bucket/data/",
            operations={
                "transform": custom_transform,
                "load": custom_load
            }
        )
        
        # Mock the extraction
        with patch('agent_orchestrated_etl.enhanced_orchestrator.primary_data_extraction') as mock_extract:
            mock_extract.return_value = [{"id": 1}]
            
            result = pipeline.execute()
            
            assert result["status"] == "success"
            assert result["data"][0]["custom"] is True


if __name__ == "__main__":
    pytest.main([__file__, "-v"])