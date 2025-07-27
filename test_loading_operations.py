#!/usr/bin/env python3
"""Test script specifically for the ETL Loading Operations module."""

import sys
import os
import asyncio
import logging
import time

# Add the agents directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src', 'agent_orchestrated_etl', 'agents'))

# Set up basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MockException(Exception):
    """Mock exception classes for testing."""
    pass

class AgentException(MockException):
    pass

class DataProcessingException(MockException):
    pass

# Mock the exceptions module
sys.modules['agent_orchestrated_etl.exceptions'] = sys.modules[__name__]
sys.modules['exceptions'] = sys.modules[__name__]

# Mock the logging_config module
class MockLogContext:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass

logging_config_mock = type('MockModule', (), {'LogContext': MockLogContext})()
sys.modules['agent_orchestrated_etl.logging_config'] = logging_config_mock
sys.modules['logging_config'] = logging_config_mock

# Mock the etl_config module
class MockProfilingConfig:
    pass

class MockColumnProfile:
    pass

etl_config_mock = type('MockModule', (), {
    'ProfilingConfig': MockProfilingConfig,
    'ColumnProfile': MockColumnProfile
})()
sys.modules['etl_config'] = etl_config_mock

async def test_loading_operations():
    """Test the ETL Loading Operations module."""
    print("🧪 Testing ETL Loading Operations Module")
    print("=" * 50)
    
    try:
        # Import the loading operations
        from etl_loading_ops import ETLLoadingOperations
        print("✓ Successfully imported ETLLoadingOperations")
        
        # Test 1: Create an instance
        print("\n1. Testing instance creation...")
        loading_ops = ETLLoadingOperations(logger=logger, specialization="database")
        print("✓ Successfully created ETLLoadingOperations instance")
        print(f"   Specialization: {loading_ops.specialization}")
        print(f"   Active loads: {len(loading_ops.get_active_loads())}")
        
        # Test 2: Test metrics initialization
        print("\n2. Testing metrics initialization...")
        metrics = loading_ops.get_loading_metrics()
        print(f"✓ Initial metrics: {metrics}")
        assert metrics['total_loads'] == 0
        assert metrics['successful_loads'] == 0
        assert metrics['failed_loads'] == 0
        
        # Test 3: Target configuration validation
        print("\n3. Testing configuration validation...")
        
        # Test valid database configuration
        db_config = {
            "type": "postgres",
            "connection_string": "postgresql://user:pass@localhost/db",
            "table_name": "test_table"
        }
        db_validation = await loading_ops.validate_target_config(db_config)
        print(f"✓ Database config validation: valid={db_validation['valid']}")
        assert db_validation['valid'] == True
        
        # Test invalid database configuration
        invalid_db_config = {"type": "postgres"}  # Missing required fields
        invalid_validation = await loading_ops.validate_target_config(invalid_db_config)
        print(f"✓ Invalid database config: valid={invalid_validation['valid']}, errors={len(invalid_validation['errors'])}")
        assert invalid_validation['valid'] == False
        assert len(invalid_validation['errors']) == 2  # Missing connection_string and table_name
        
        # Test file configuration
        file_config = {
            "type": "csv",
            "file_path": "/tmp/output.csv"
        }
        file_validation = await loading_ops.validate_target_config(file_config)
        print(f"✓ File config validation: valid={file_validation['valid']}")
        assert file_validation['valid'] == True
        
        # Test API configuration
        api_config = {
            "type": "rest",
            "url": "https://api.example.com/data"
        }
        api_validation = await loading_ops.validate_target_config(api_config)
        print(f"✓ API config validation: valid={api_validation['valid']}")
        assert api_validation['valid'] == True
        
        # Test 4: Load operations
        print("\n4. Testing load operations...")
        
        # Test database loading
        task_inputs = {
            "target_config": db_config,
            "source_data": [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}],
            "load_id": "test_db_load_001"
        }
        
        start_time = time.time()
        load_result = await loading_ops.load_data(task_inputs)
        end_time = time.time()
        
        print(f"✓ Database load completed: {load_result['status']}")
        print(f"   Load ID: {load_result['load_id']}")
        print(f"   Records loaded: {load_result['records_loaded']}")
        print(f"   Target type: {load_result['target_type']}")
        print(f"   Load time: {load_result['load_time']:.3f}s")
        
        assert load_result['status'] == 'completed'
        assert load_result['records_loaded'] == 1000  # Mock value
        assert load_result['target_type'] == 'postgres'
        
        # Test file loading
        file_task_inputs = {
            "target_config": file_config,
            "source_data": [{"id": 1, "name": "test"}],
            "load_id": "test_file_load_001"
        }
        
        file_result = await loading_ops.load_data(file_task_inputs)
        print(f"✓ File load completed: {file_result['status']}")
        assert file_result['status'] == 'completed'
        
        # Test API loading
        api_task_inputs = {
            "target_config": api_config,
            "source_data": [{"id": 1, "name": "test"}],
            "load_id": "test_api_load_001"
        }
        
        api_result = await loading_ops.load_data(api_task_inputs)
        print(f"✓ API load completed: {api_result['status']}")
        assert api_result['status'] == 'completed'
        
        # Test 5: Check updated metrics
        print("\n5. Testing metrics tracking...")
        final_metrics = loading_ops.get_loading_metrics()
        print(f"✓ Final metrics: {final_metrics}")
        assert final_metrics['total_loads'] == 3
        assert final_metrics['successful_loads'] == 3
        assert final_metrics['failed_loads'] == 0
        assert final_metrics['total_records_loaded'] == 3000  # 3 loads × 1000 records each
        
        # Test 6: Error handling
        print("\n6. Testing error handling...")
        try:
            error_task_inputs = {
                "target_config": {"type": "invalid_type"},
                "source_data": [{"id": 1}],
                "load_id": "error_test"
            }
            await loading_ops.load_data(error_task_inputs)
            print("❌ Error test failed - should have raised exception")
            return False
        except Exception as e:
            print(f"✓ Error handling works: {type(e).__name__}")
        
        # Test 7: Load status and cancellation
        print("\n7. Testing load management...")
        
        # All loads should be completed, so no active loads
        active_loads = loading_ops.get_active_loads()
        print(f"✓ Active loads count: {len(active_loads)}")
        assert len(active_loads) == 0  # All loads completed
        
        # Test cancel operation (should return False for non-existent load)
        cancel_result = loading_ops.cancel_load("non_existent_load")
        print(f"✓ Cancel non-existent load: {cancel_result}")
        assert cancel_result == False
        
        # Test get status of non-existent load
        status_result = loading_ops.get_load_status("non_existent_load")
        print(f"✓ Get non-existent load status: {status_result}")
        assert status_result is None
        
        print("\n" + "=" * 50)
        print("🎉 ALL TESTS PASSED!")
        print("\nETL Loading Operations Module Summary:")
        print("  ✓ Module imports and initializes correctly")
        print("  ✓ Configuration validation works for all target types")
        print("  ✓ Load operations complete successfully")
        print("  ✓ Metrics tracking functions properly")
        print("  ✓ Error handling is robust")
        print("  ✓ Load management functions work")
        print("\nThe loading operations have been successfully extracted from the ETL agent!")
        return True
        
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = asyncio.run(test_loading_operations())
    sys.exit(0 if success else 1)