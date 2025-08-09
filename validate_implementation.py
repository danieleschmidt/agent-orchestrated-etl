"""Validation script for the enhanced ETL implementation."""

import sys
import os
import time

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_imports():
    """Test that all new modules can be imported."""
    print("🧪 Testing module imports...")
    
    try:
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator, PipelineConfig
        print("✅ Enhanced orchestrator - OK")
        
        from agent_orchestrated_etl.security import SecurityValidator, SecurityConfig
        print("✅ Security module - OK")
        
        from agent_orchestrated_etl.enhanced_validation import EnhancedDataValidator
        print("✅ Enhanced validation - OK")
        
        from agent_orchestrated_etl.performance_cache import PerformanceCache
        print("✅ Performance cache - OK")
        
        from agent_orchestrated_etl.concurrent_processing import ConcurrentProcessor
        print("✅ Concurrent processing - OK")
        
        from agent_orchestrated_etl.error_recovery import ErrorRecoveryManager
        print("✅ Error recovery - OK")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_basic_functionality():
    """Test basic functionality of key components."""
    print("\n🧪 Testing basic functionality...")
    
    try:
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator
        
        # Test orchestrator creation
        orchestrator = EnhancedDataOrchestrator(enable_security=False, enable_validation=False)
        print("✅ Orchestrator creation - OK")
        
        # Test pipeline creation
        pipeline = orchestrator.create_pipeline("s3://test-bucket/data/")
        assert pipeline.config.source_config["type"] == "s3"
        print("✅ Pipeline creation - OK")
        
        # Test task preview
        tasks = pipeline.preview_tasks()
        assert len(tasks) > 0
        print("✅ Task preview - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Functionality test failed: {e}")
        return False

def test_security_features():
    """Test security validation features."""
    print("\n🔒 Testing security features...")
    
    try:
        from agent_orchestrated_etl.security import SecurityValidator, SecurityConfig
        
        validator = SecurityValidator(SecurityConfig())
        
        # Test valid input
        assert validator.validate_input("normal input", "test") == True
        print("✅ Valid input validation - OK")
        
        # Test malicious input detection
        try:
            validator.validate_input("'; DROP TABLE users; --", "test")
            print("❌ SQL injection not detected!")
            return False
        except:
            print("✅ SQL injection detection - OK")
        
        # Test API key generation
        api_key = validator.generate_api_key("test_client")
        assert len(api_key) > 20
        print("✅ API key generation - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Security test failed: {e}")
        return False

def test_data_validation():
    """Test data validation capabilities."""
    print("\n📊 Testing data validation...")
    
    try:
        from agent_orchestrated_etl.enhanced_validation import EnhancedDataValidator
        
        validator = EnhancedDataValidator()
        
        # Test dataset validation
        test_data = [
            {"email": "valid@example.com", "amount": 100},
            {"email": "invalid-email", "amount": -50}
        ]
        
        report = validator.validate_dataset(test_data)
        
        assert report.total_records == 2
        assert len(report.validation_errors) > 0  # Should catch invalid email and negative amount
        print("✅ Dataset validation - OK")
        
        # Test quality score
        assert 0 <= report.quality_score <= 100
        print("✅ Quality score calculation - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Data validation test failed: {e}")
        return False

def test_caching():
    """Test caching functionality."""
    print("\n⚡ Testing caching...")
    
    try:
        from agent_orchestrated_etl.performance_cache import PerformanceCache, CacheConfig
        
        cache = PerformanceCache(CacheConfig(max_size=5, ttl_seconds=1))
        
        # Test cache miss
        result = cache.memory_cache.get("test_key")
        assert result is None
        print("✅ Cache miss - OK")
        
        # Test cache put/get
        cache.memory_cache.put("test_key", "test_value")
        result = cache.memory_cache.get("test_key")
        assert result == "test_value"
        print("✅ Cache put/get - OK")
        
        # Test TTL expiration
        time.sleep(1.1)
        result = cache.memory_cache.get("test_key")
        assert result is None
        print("✅ Cache TTL expiration - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Caching test failed: {e}")
        return False

def test_concurrent_processing():
    """Test concurrent processing capabilities."""
    print("\n🚀 Testing concurrent processing...")
    
    try:
        from agent_orchestrated_etl.concurrent_processing import ConcurrentProcessor, ConcurrencyConfig
        
        config = ConcurrencyConfig(max_workers=2, chunk_size=3)
        processor = ConcurrentProcessor(config)
        
        # Test sync batch processing
        def double_value(x):
            return x * 2
        
        data = [1, 2, 3, 4, 5]
        results = processor.process_batch_sync(data, double_value)
        
        assert len(results) == 5
        assert results == [2, 4, 6, 8, 10]
        print("✅ Sync batch processing - OK")
        
        # Test chunking
        chunks = processor._create_chunks(data, 2)
        assert len(chunks) == 3  # [1,2], [3,4], [5]
        print("✅ Data chunking - OK")
        
        processor.cleanup()
        return True
        
    except Exception as e:
        print(f"❌ Concurrent processing test failed: {e}")
        return False

def main():
    """Run all validation tests."""
    print("🚀 Enhanced ETL Implementation Validation")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_basic_functionality,
        test_security_features,
        test_data_validation,
        test_caching,
        test_concurrent_processing
    ]
    
    passed = 0
    failed = 0
    
    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} crashed: {e}")
            failed += 1
    
    print("\n" + "=" * 50)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All tests passed! Implementation validated successfully.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    exit(main())