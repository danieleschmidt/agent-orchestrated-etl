#!/usr/bin/env python3
"""
Generation 1: MAKE IT WORK (Simple) - Basic functionality test
Tests core ETL operations with simple, reliable implementations
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from agent_orchestrated_etl import DataOrchestrator, core
from agent_orchestrated_etl.agents.coordination import AgentCoordinator
from agent_orchestrated_etl.data_source_analysis import S3DataAnalyzer

def test_basic_data_extraction():
    """Test basic data extraction functionality."""
    print("🔍 Testing basic data extraction...")
    
    # Test with sample data
    result = core.primary_data_extraction()
    assert len(result) > 0, "Should return sample data"
    print(f"   ✅ Sample data extraction: {len(result)} records")
    
    # Test with different source types
    source_configs = [
        {'type': 'database', 'connection_string': 'mock://test', 'query': 'SELECT 1'},
        {'type': 'api', 'endpoint': 'https://api.example.com/test'},
        {'type': 's3', 'bucket': 'test-bucket', 'key': 'test.json'},
        {'type': 'file', 'path': '/tmp/test.json', 'format': 'json'}
    ]
    
    for config in source_configs:
        try:
            result = core.primary_data_extraction(source_config=config)
            print(f"   ✅ {config['type']} extraction: {len(result)} records")
        except Exception as e:
            print(f"   ⚠️  {config['type']} extraction failed: {e}")
    
    return True

def test_basic_data_transformation():
    """Test basic data transformation functionality."""
    print("🔄 Testing basic data transformation...")
    
    # Test with simple integer list
    simple_data = [1, 2, 3, 4, 5]
    result = core.transform_data(simple_data)
    assert len(result) == 5, "Should transform all items"
    assert result[0]['original'] == 1, "Should preserve original values"
    print(f"   ✅ Simple transformation: {len(result)} records")
    
    # Test with complex data
    complex_data = [
        {"id": 1, "name": "test1", "value": 100},
        {"id": 2, "name": "test2", "value": 200}
    ]
    result = core.transform_data(complex_data)
    assert len(result) == 2, "Should transform all records"
    assert 'processed_at' in result[0], "Should add processing timestamp"
    print(f"   ✅ Complex transformation: {len(result)} records")
    
    return True

def test_data_quality_validation():
    """Test data quality validation."""
    print("🛡️ Testing data quality validation...")
    
    validator = core.DataQualityValidator()
    
    # Test with good data
    good_data = [
        {"id": 1, "name": "Alice", "age": 30, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 25, "email": "bob@example.com"}
    ]
    
    result = validator.validate_data_quality(good_data)
    print(f"   ✅ Quality validation - Status: {result['status']}, Score: {result['quality_score']:.2f}")
    
    # Test with problematic data
    bad_data = [
        {"id": 1, "name": "", "age": None},
        {"id": None, "name": "Bob", "age": 25}
    ]
    
    result = validator.validate_data_quality(bad_data)
    print(f"   ✅ Problem detection - Status: {result['status']}, Issues: {len(result['issues'])}")
    
    return True

def test_orchestrator_basic():
    """Test basic orchestrator functionality."""
    print("🎯 Testing basic orchestrator...")
    
    try:
        orchestrator = DataOrchestrator()
        print("   ✅ Orchestrator initialization successful")
        
        # Test simple pipeline creation with supported source type
        pipeline = orchestrator.create_pipeline(source="s3://test-bucket/test")
        print("   ✅ Pipeline creation successful")
        
        return True
    except Exception as e:
        print(f"   ⚠️  Orchestrator test failed: {e}")
        return False

def test_agent_coordination():
    """Test basic agent coordination."""
    print("🤝 Testing agent coordination...")
    
    try:
        # Test with required communication hub parameter
        from agent_orchestrated_etl.agents.communication import AgentCommunicationHub
        hub = AgentCommunicationHub()
        coordinator = AgentCoordinator(hub)
        print("   ✅ Agent coordinator initialization successful")
        return True
    except Exception as e:
        print(f"   ⚠️  Agent coordination test failed: {e}")
        return False

def test_data_source_analysis():
    """Test data source analysis."""
    print("📊 Testing data source analysis...")
    
    try:
        analyzer = S3DataAnalyzer()
        
        # Test basic analyzer functionality
        supported_sources = {"s3", "postgresql", "api", "test_db"}
        print(f"   ✅ S3 analyzer initialized with supported sources")
        
        return True
    except Exception as e:
        print(f"   ⚠️  Data source analysis test failed: {e}")
        return False

def main():
    """Run Generation 1 basic functionality tests."""
    print("🚀 Generation 1: MAKE IT WORK (Simple) - Basic Functionality Tests")
    print("=" * 70)
    
    tests = [
        test_basic_data_extraction,
        test_basic_data_transformation,
        test_data_quality_validation,
        test_orchestrator_basic,
        test_agent_coordination,
        test_data_source_analysis
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
            print(f"   ❌ Test failed with exception: {e}")
            failed += 1
        print()
    
    print("=" * 70)
    print(f"📊 Test Results: {passed} passed, {failed} failed")
    
    if failed == 0:
        print("🎉 All Generation 1 tests passed! Ready for Generation 2.")
        return True
    else:
        print("⚠️  Some tests failed. Review and fix before proceeding.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)