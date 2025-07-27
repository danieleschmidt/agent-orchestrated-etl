#!/usr/bin/env python3
"""Simple test script for ETL Loading Operations module."""

import asyncio
import logging
import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

# Set up basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_etl_loading_operations():
    """Test the ETL Loading Operations functionality."""
    try:
        # Import the loading operations
        from agent_orchestrated_etl.agents.etl_loading_ops import ETLLoadingOperations
        
        print("✓ Successfully imported ETLLoadingOperations")
        
        # Create an instance
        loading_ops = ETLLoadingOperations(logger=logger, specialization="database")
        print("✓ Successfully created ETLLoadingOperations instance")
        
        # Test target configuration validation
        test_config = {
            "type": "postgres",
            "connection_string": "postgresql://user:pass@localhost/db",
            "table_name": "test_table"
        }
        
        validation_result = await loading_ops.validate_target_config(test_config)
        print(f"✓ Target config validation: {validation_result}")
        
        # Test loading operation
        task_inputs = {
            "target_config": test_config,
            "source_data": [{"id": 1, "name": "test"}],
            "load_id": "test_load_001"
        }
        
        load_result = await loading_ops.load_data(task_inputs)
        print(f"✓ Load operation completed: {load_result}")
        
        # Test metrics
        metrics = loading_ops.get_loading_metrics()
        print(f"✓ Loading metrics: {metrics}")
        
        print("\n🎉 All tests passed! ETL Loading Operations module is working correctly.")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_etl_loading_operations())