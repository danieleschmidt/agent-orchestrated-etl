"""Minimal validation script that tests only the core functionality without heavy dependencies."""

import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

def test_core_imports():
    """Test core module imports."""
    print("🧪 Testing core imports...")
    
    try:
        # Test basic core functionality
        from agent_orchestrated_etl.core import primary_data_extraction, transform_data
        print("✅ Core functions - OK")
        
        # Test enhanced orchestrator
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator, PipelineConfig
        print("✅ Enhanced orchestrator - OK")
        
        return True
        
    except ImportError as e:
        print(f"❌ Import failed: {e}")
        return False

def test_enhanced_orchestrator():
    """Test enhanced orchestrator basic functionality."""
    print("\n🧪 Testing enhanced orchestrator...")
    
    try:
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator, PipelineConfig
        
        # Create orchestrator without heavy dependencies
        orchestrator = EnhancedDataOrchestrator(enable_security=False, enable_validation=False)
        print("✅ Orchestrator creation - OK")
        
        # Test pipeline creation with different sources
        s3_pipeline = orchestrator.create_pipeline("s3://bucket/data/")
        assert s3_pipeline.config.source_config["type"] == "s3"
        assert s3_pipeline.config.source_config["bucket"] == "bucket"
        print("✅ S3 pipeline creation - OK")
        
        api_pipeline = orchestrator.create_pipeline("api://example.com/data")
        assert api_pipeline.config.source_config["type"] == "api"
        assert api_pipeline.config.source_config["endpoint"] == "https://example.com/data"
        print("✅ API pipeline creation - OK")
        
        file_pipeline = orchestrator.create_pipeline("/path/to/file.json")
        assert file_pipeline.config.source_config["type"] == "file"
        assert file_pipeline.config.source_config["path"] == "/path/to/file.json"
        print("✅ File pipeline creation - OK")
        
        # Test task preview
        tasks = s3_pipeline.preview_tasks()
        assert len(tasks) >= 3  # Extract, Transform, Load
        print("✅ Task preview - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Enhanced orchestrator test failed: {e}")
        return False

def test_core_etl_functions():
    """Test core ETL functionality."""
    print("\n🧪 Testing core ETL functions...")
    
    try:
        from agent_orchestrated_etl.core import primary_data_extraction, transform_data
        
        # Test data extraction with sample config
        sample_config = {"type": "sample"}
        extracted_data = primary_data_extraction(source_config=sample_config)
        
        assert isinstance(extracted_data, list)
        assert len(extracted_data) > 0
        print("✅ Data extraction - OK")
        
        # Test data transformation
        transformation_rules = [{"type": "filter", "field": "category", "value": "test"}]
        transformed_data = transform_data(extracted_data, transformation_rules)
        
        assert isinstance(transformed_data, list)
        print("✅ Data transformation - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Core ETL test failed: {e}")
        return False

def test_data_pipeline():
    """Test data pipeline functionality."""
    print("\n🧪 Testing data pipeline...")
    
    try:
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator
        
        orchestrator = EnhancedDataOrchestrator(enable_security=False, enable_validation=False)
        
        # Test pipeline with custom operations
        def custom_transform(data):
            return [{"transformed": True, **item} for item in data]
        
        def custom_load(data):
            return {
                "status": "loaded",
                "records_loaded": len(data),
                "custom": True
            }
        
        pipeline = orchestrator.create_pipeline(
            "sample://test",
            operations={
                "transform": custom_transform,
                "load": custom_load
            }
        )
        
        print("✅ Custom pipeline creation - OK")
        
        # Test pipeline configuration
        config = pipeline.config
        assert isinstance(config.source_config, dict)
        assert isinstance(config.transformation_rules, list)
        assert isinstance(config.destination_config, dict)
        print("✅ Pipeline configuration - OK")
        
        return True
        
    except Exception as e:
        print(f"❌ Data pipeline test failed: {e}")
        return False

def test_architecture_components():
    """Test basic architecture components."""
    print("\n🧪 Testing architecture components...")
    
    try:
        # Test that we can import key architectural components
        from agent_orchestrated_etl import config, core, orchestrator
        print("✅ Core architecture modules - OK")
        
        # Test enhanced features are available
        from agent_orchestrated_etl.enhanced_orchestrator import EnhancedDataOrchestrator
        from agent_orchestrated_etl.performance_cache import PerformanceCache
        from agent_orchestrated_etl.concurrent_processing import ConcurrentProcessor
        print("✅ Enhanced feature modules - OK")
        
        return True
        
    except ImportError as e:
        print(f"❌ Architecture test failed: {e}")
        return False

def main():
    """Run minimal validation tests."""
    print("🚀 Minimal ETL Implementation Validation")
    print("=" * 50)
    
    tests = [
        test_core_imports,
        test_enhanced_orchestrator,
        test_core_etl_functions,
        test_data_pipeline,
        test_architecture_components
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
        print("🎉 All core tests passed! Implementation validated successfully.")
        return 0
    else:
        print("⚠️  Some tests failed. Check the output above.")
        return 1

if __name__ == "__main__":
    exit(main())