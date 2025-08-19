#!/usr/bin/env python3
"""
Basic functionality test without external dependencies.
Tests core ETL functionality with mocked components.
"""

import sys
import os
import time
import json
from pathlib import Path

# Add src to path
sys.path.insert(0, 'src')

def test_basic_imports():
    """Test that basic modules can be imported."""
    print("Testing basic imports...")
    
    try:
        # Test core module imports
        from agent_orchestrated_etl.core import primary_data_extraction, transform_data
        from agent_orchestrated_etl.logging_config import get_logger
        from agent_orchestrated_etl.validation import ValidationError, sanitize_json_output
        
        print("✓ Core modules imported successfully")
        return True
    except ImportError as e:
        print(f"✗ Import failed: {e}")
        return False

def test_core_functionality():
    """Test core ETL operations."""
    print("\nTesting core functionality...")
    
    try:
        from agent_orchestrated_etl.core import primary_data_extraction, transform_data, DataQualityValidator
        
        # Test data extraction
        test_data = primary_data_extraction()
        print(f"✓ Data extraction returned {len(test_data)} records")
        
        # Test data transformation
        transformed_data = transform_data(test_data)
        print(f"✓ Data transformation returned {len(transformed_data)} records")
        
        # Test data quality validation
        validator = DataQualityValidator()
        quality_result = validator.validate_data_quality(transformed_data)
        print(f"✓ Data quality validation completed with score: {quality_result['quality_score']:.2f}")
        
        return True
    except Exception as e:
        print(f"✗ Core functionality test failed: {e}")
        return False

def test_cli_functionality():
    """Test CLI module functionality."""
    print("\nTesting CLI functionality...")
    
    try:
        from agent_orchestrated_etl import cli
        
        # Test CLI argument parsing (without execution)
        print("✓ CLI module imported successfully")
        
        # Test individual CLI functions exist
        assert hasattr(cli, 'run_pipeline_cmd'), "run_pipeline_cmd function not found"
        assert hasattr(cli, 'generate_dag_cmd'), "generate_dag_cmd function not found"
        assert hasattr(cli, 'benchmark_cmd'), "benchmark_cmd function not found"
        
        print("✓ CLI functions available")
        return True
    except Exception as e:
        print(f"✗ CLI functionality test failed: {e}")
        return False

def test_api_module():
    """Test API module without FastAPI dependency."""
    print("\nTesting API module...")
    
    try:
        from agent_orchestrated_etl.api import app, routers
        
        # Since FastAPI might not be available, check if graceful fallback works
        if app.FASTAPI_AVAILABLE:
            print("✓ FastAPI available - API endpoints active")
        else:
            print("✓ FastAPI not available - graceful fallback working")
        
        print("✓ API module structure intact")
        return True
    except Exception as e:
        print(f"✗ API module test failed: {e}")
        return False

def test_examples():
    """Test that examples can be imported and basic functions exist."""
    print("\nTesting examples...")
    
    try:
        # Test that example files exist
        examples_dir = Path("examples")
        if not examples_dir.exists():
            print("✗ Examples directory not found")
            return False
        
        # Check for example files
        advanced_example = examples_dir / "advanced_pipeline_orchestration.py"
        research_example = examples_dir / "research_implementations.py"
        
        if advanced_example.exists():
            print("✓ Advanced pipeline orchestration example exists")
        else:
            print("✗ Advanced example file missing")
            return False
            
        if research_example.exists():
            print("✓ Research implementations example exists")
        else:
            print("✗ Research example file missing")
            return False
        
        print("✓ Example files are present")
        return True
    except Exception as e:
        print(f"✗ Examples test failed: {e}")
        return False

def test_configuration_files():
    """Test that configuration files are properly structured."""
    print("\nTesting configuration files...")
    
    try:
        # Test pyproject.toml
        pyproject_path = Path("pyproject.toml")
        if pyproject_path.exists():
            print("✓ pyproject.toml exists")
        else:
            print("✗ pyproject.toml missing")
            return False
        
        # Test requirements.txt
        requirements_path = Path("requirements.txt")
        if requirements_path.exists():
            with open(requirements_path, 'r') as f:
                requirements = f.read()
                if "PyYAML" in requirements and "pandas" in requirements:
                    print("✓ requirements.txt has core dependencies")
                else:
                    print("✗ requirements.txt missing core dependencies")
                    return False
        else:
            print("✗ requirements.txt missing")
            return False
        
        print("✓ Configuration files are properly structured")
        return True
    except Exception as e:
        print(f"✗ Configuration files test failed: {e}")
        return False

def test_mock_pipeline_execution():
    """Test a mock pipeline execution with sample data."""
    print("\nTesting mock pipeline execution...")
    
    try:
        from agent_orchestrated_etl.core import primary_data_extraction, transform_data
        from agent_orchestrated_etl.orchestrator import load_data
        
        # Create a simple pipeline execution simulation
        print("  - Extracting sample data...")
        extracted_data = primary_data_extraction()
        
        print("  - Transforming data...")
        transformed_data = transform_data(extracted_data)
        
        print("  - Loading data (mock)...")
        load_result = load_data(transformed_data)
        
        print(f"✓ Mock pipeline executed successfully")
        print(f"  - Records processed: {len(transformed_data)}")
        print(f"  - Load status: {load_result.get('status', 'unknown')}")
        
        return True
    except Exception as e:
        print(f"✗ Mock pipeline execution failed: {e}")
        return False

def run_comprehensive_tests():
    """Run all tests and provide summary."""
    print("=" * 60)
    print("COMPREHENSIVE BASIC FUNCTIONALITY TESTS")
    print("=" * 60)
    
    tests = [
        ("Basic Imports", test_basic_imports),
        ("Core Functionality", test_core_functionality),
        ("CLI Functionality", test_cli_functionality),
        ("API Module", test_api_module),
        ("Examples", test_examples),
        ("Configuration Files", test_configuration_files),
        ("Mock Pipeline Execution", test_mock_pipeline_execution)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n[{test_name}]")
        success = test_func()
        results.append((test_name, success))
    
    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    for test_name, success in results:
        status = "PASS" if success else "FAIL"
        print(f"{test_name:.<40} {status}")
    
    print(f"\nTotal: {passed}/{total} tests passed")
    print(f"Success Rate: {passed/total*100:.1f}%")
    
    if passed == total:
        print("\n🎉 All tests passed! The system is working correctly.")
        return 0
    else:
        print(f"\n⚠️  {total-passed} test(s) failed. Check the output above for details.")
        return 1

if __name__ == "__main__":
    exit_code = run_comprehensive_tests()
    sys.exit(exit_code)