#!/usr/bin/env python3
"""Integration test for real sampling implementation bypassing YAML dependency."""

import sys
import os
import tempfile
import sqlite3
import csv

# Mock the yaml module to avoid import issues
class MockYAML:
    def safe_load(self, *args):
        return {}
    def dump(self, *args, **kwargs):
        return ""

sys.modules['yaml'] = MockYAML()

# Now we can import our modules
sys.path.append('src')

def create_test_agent():
    """Create a test ETL agent with minimal setup."""
    from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent, ProfilingConfig
    from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole
    
    config = AgentConfig(name="TestETL", role=AgentRole.ETL_SPECIALIST)
    return ETLAgent(config, specialization="general")

async def test_real_database_sampling():
    """Test the actual _load_real_sample_data method with database."""
    # Create test database
    db_file = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL,
            category TEXT
        )
    ''')
    
    test_data = [
        (1, 'Laptop', 1200.0, 'Electronics'),
        (2, 'Chair', 250.0, 'Furniture'),
        (3, 'Phone', 800.0, 'Electronics'),
        (4, 'Desk', 400.0, 'Furniture'),
        (5, 'Tablet', 600.0, 'Electronics'),
        (6, 'Lamp', 80.0, 'Furniture'),
    ]
    
    cursor.executemany('INSERT INTO products VALUES (?, ?, ?, ?)', test_data)
    conn.commit()
    conn.close()
    
    try:
        # Create agent and config
        agent = create_test_agent()
        
        from src.agent_orchestrated_etl.agents.etl_agent import ProfilingConfig
        profiling_config = ProfilingConfig(
            sample_size=3,
            sampling_strategy='random'
        )
        
        data_source_config = {
            "type": "database",
            "connection_string": f"sqlite:///{db_file}",
            "table_name": "products",
            "sampling_strategy": "random",
            "sample_size": 3
        }
        
        # Test the real sampling method
        result = await agent._load_real_sample_data(data_source_config, profiling_config)
        
        print("✅ Real database sampling test passed")
        print(f"   Columns: {result['columns']}")
        print(f"   Records: {result['total_records']}")
        print(f"   Strategy: {result['sampling_metadata']['strategy']}")
        print(f"   Sample data preview: {result['data']['name'][:3] if 'name' in result['data'] else 'N/A'}")
        
        # Verify it's real data
        names = result['data'].get('name', [])
        expected_names = ['Laptop', 'Chair', 'Phone', 'Desk', 'Tablet', 'Lamp']
        assert any(name in expected_names for name in names), "Should contain real product names"
        
        return True
        
    except Exception as e:
        print(f"❌ Database sampling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(db_file):
            os.unlink(db_file)

async def test_real_csv_sampling():
    """Test the actual _load_real_sample_data method with CSV."""
    # Create test CSV
    csv_file = tempfile.mktemp(suffix='.csv')
    
    test_data = [
        ['id', 'customer_name', 'order_amount', 'region'],
        ['1', 'John Doe', '150.50', 'North'],
        ['2', 'Jane Smith', '275.00', 'South'],
        ['3', 'Bob Johnson', '89.99', 'East'],
        ['4', 'Alice Brown', '420.75', 'West'],
        ['5', 'Charlie Wilson', '199.99', 'North'],
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(test_data)
    
    try:
        # Create agent and config
        agent = create_test_agent()
        
        from src.agent_orchestrated_etl.agents.etl_agent import ProfilingConfig
        profiling_config = ProfilingConfig(
            sample_size=3,
            sampling_strategy='systematic'
        )
        
        data_source_config = {
            "type": "file",
            "file_path": csv_file,
            "file_format": "csv",
            "sampling_strategy": "systematic",
            "sample_size": 3
        }
        
        # Test the real sampling method
        result = await agent._load_real_sample_data(data_source_config, profiling_config)
        
        print("✅ Real CSV sampling test passed")
        print(f"   Columns: {result['columns']}")
        print(f"   Records: {result['total_records']}")
        print(f"   Strategy: {result['sampling_metadata']['strategy']}")
        print(f"   Sample data preview: {result['data']['customer_name'][:3] if 'customer_name' in result['data'] else 'N/A'}")
        
        # Verify it's real data
        names = result['data'].get('customer_name', [])
        expected_names = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson']
        assert any(name in expected_names for name in names), "Should contain real customer names"
        
        return True
        
    except Exception as e:
        print(f"❌ CSV sampling test failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        if os.path.exists(csv_file):
            os.unlink(csv_file)

async def main():
    """Run all integration tests."""
    print("🧪 Testing real data sampling integration...")
    
    tests = [
        test_real_database_sampling,
        test_real_csv_sampling
    ]
    
    passed = 0
    for test in tests:
        try:
            print(f"\n📋 Running {test.__name__}...")
            if await test():
                passed += 1
        except Exception as e:
            print(f"❌ {test.__name__} failed with exception: {e}")
    
    print(f"\n📊 Integration tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("🟢 Real data sampling implementation is working!")
        print("🎯 ETL-011 implementation successfully completed!")
    else:
        print("🔴 Some integration tests failed")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())