#!/usr/bin/env python3
"""Test script for real data sampling implementation - ETL-011.

This script tests the new real data sampling functionality without requiring
the full pytest framework setup.
"""

import sys
import os
sys.path.append('src')

def test_real_database_sampling():
    """Test real database sampling with SQLite."""
    # Create a temporary SQLite database with test data
    import sqlite3
    import tempfile
    from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent, ProfilingConfig
    from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole
    
    # Create test database
    db_file = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Create test table with realistic data
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            salary REAL,
            department TEXT,
            created_date TEXT
        )
    ''')
    
    # Insert test data
    test_data = [
        (1, 'Alice Johnson', 28, 75000.0, 'Engineering', '2023-01-15'),
        (2, 'Bob Smith', 35, 82000.0, 'Marketing', '2023-02-20'),
        (3, 'Carol Davis', 42, 95000.0, 'Engineering', '2023-01-10'),
        (4, 'David Wilson', None, 67000.0, 'Sales', '2023-03-05'),  # Missing age
        (5, 'Eve Brown', 29, None, 'Engineering', '2023-01-25'),    # Missing salary
        (6, 'Frank Miller', 55, 120000.0, 'Management', '2023-02-01'),
        (7, 'Grace Lee', 31, 78000.0, 'Marketing', '2023-02-15'),
        (8, 'Henry Taylor', 24, 58000.0, 'Sales', '2023-03-10'),
        (9, 'Ivy Chen', 33, 85000.0, 'Engineering', '2023-01-30'),
        (10, 'Jack Anderson', 45, 110000.0, 'Management', '2023-02-05'),
    ]
    
    cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?, ?, ?)', test_data)
    conn.commit()
    conn.close()
    
    # Test real data sampling
    config = AgentConfig(name="TestETL", role=AgentRole.ETL_SPECIALIST)
    agent = ETLAgent(config, specialization="database")
    
    profiling_config = ProfilingConfig(
        sample_size=5,
        sampling_strategy="random"
    )
    
    # Create data source configuration for database
    data_source_config = {
        "type": "database",
        "connection_string": f"sqlite:///{db_file}",
        "table_name": "users",
        "sampling_strategy": "random",
        "sample_size": 5
    }
    
    try:
        # This should use real database sampling, not mock data
        import asyncio
        sample_data = asyncio.run(agent._load_real_sample_data(data_source_config, profiling_config))
        
        print("✅ Real database sampling test")
        print(f"   Sample size: {len(sample_data.get('data', {}).get('id', []))}")
        print(f"   Columns: {sample_data.get('columns', [])}")
        print(f"   Has real data: {not all('user_' in str(val) for val in sample_data.get('data', {}).get('name', []) if val)}")
        
        # Verify it's real data, not mock
        names = sample_data.get('data', {}).get('name', [])
        assert any(name in ['Alice Johnson', 'Bob Smith', 'Carol Davis'] for name in names if name), \
               "Should contain real names from database, not mock data"
        
        return True
        
    except Exception as e:
        print(f"❌ Database sampling test failed: {e}")
        return False
    finally:
        # Cleanup
        if os.path.exists(db_file):
            os.unlink(db_file)

def test_real_file_sampling():
    """Test real file sampling with CSV."""
    import tempfile
    import csv
    from src.agent_orchestrated_etl.agents.etl_agent import ETLAgent, ProfilingConfig
    from src.agent_orchestrated_etl.agents.base_agent import AgentConfig, AgentRole
    
    # Create test CSV file
    csv_file = tempfile.mktemp(suffix='.csv')
    
    test_data = [
        ['product_id', 'product_name', 'price', 'category', 'in_stock'],
        ['1', 'Laptop Pro', '1299.99', 'Electronics', 'True'],
        ['2', 'Office Chair', '249.50', 'Furniture', 'True'],
        ['3', 'Coffee Maker', '89.99', 'Appliances', 'False'],
        ['4', 'Desk Lamp', '34.99', 'Furniture', 'True'],
        ['5', 'Smartphone', '699.00', 'Electronics', 'True'],
        ['6', 'Monitor', '299.99', 'Electronics', 'True'],
        ['7', 'Keyboard', '79.99', 'Electronics', 'False'],
        ['8', 'Mouse', '29.99', 'Electronics', 'True'],
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(test_data)
    
    # Test real file sampling
    config = AgentConfig(name="TestETL", role=AgentRole.ETL_SPECIALIST)
    agent = ETLAgent(config, specialization="file")
    
    profiling_config = ProfilingConfig(
        sample_size=4,
        sampling_strategy="systematic"
    )
    
    data_source_config = {
        "type": "file",
        "file_path": csv_file,
        "file_format": "csv",
        "sampling_strategy": "systematic",
        "sample_size": 4
    }
    
    try:
        import asyncio
        sample_data = asyncio.run(agent._load_real_sample_data(data_source_config, profiling_config))
        
        print("✅ Real file sampling test")
        print(f"   Sample size: {len(sample_data.get('data', {}).get('product_id', []))}")
        print(f"   Columns: {sample_data.get('columns', [])}")
        
        # Verify it's real data, not mock  
        product_names = sample_data.get('data', {}).get('product_name', [])
        assert any(name in ['Laptop Pro', 'Office Chair', 'Coffee Maker'] for name in product_names if name), \
               "Should contain real product names from CSV, not mock data"
        
        return True
        
    except Exception as e:
        print(f"❌ File sampling test failed: {e}")
        return False
    finally:
        # Cleanup
        if os.path.exists(csv_file):
            os.unlink(csv_file)

if __name__ == "__main__":
    print("🔴 Testing real data sampling (should fail initially)...")
    
    tests = [
        test_real_database_sampling,
        test_real_file_sampling
    ]
    
    passed = 0
    for test in tests:
        try:
            if test():
                passed += 1
        except Exception as e:
            print(f"❌ Test {test.__name__} failed with exception: {e}")
    
    print(f"\n📊 Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("🟢 All tests passed - implementation complete!")
    else:
        print("🔴 Tests failing - need to implement real data sampling")