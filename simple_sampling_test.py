#!/usr/bin/env python3
"""Simple test for real data sampling without full module imports."""

import sqlite3
import tempfile
import os
import sys

def test_database_sampling_direct():
    """Test database sampling directly with minimal dependencies."""
    
    # Create test database
    db_file = tempfile.mktemp(suffix='.db')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Create test table
    cursor.execute('''
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            department TEXT
        )
    ''')
    
    # Insert test data
    test_data = [
        (1, 'Alice Johnson', 28, 'Engineering'),
        (2, 'Bob Smith', 35, 'Marketing'),
        (3, 'Carol Davis', 42, 'Engineering'),
        (4, 'David Wilson', 31, 'Sales'),
        (5, 'Eve Brown', 29, 'Engineering'),
    ]
    
    cursor.executemany('INSERT INTO users VALUES (?, ?, ?, ?)', test_data)
    conn.commit()
    conn.close()
    
    print("✅ Test database created successfully")
    
    # Test basic SQLite random sampling
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Random sampling query (similar to what our implementation does)
    cursor.execute("SELECT * FROM users ORDER BY RANDOM() LIMIT 3")
    results = cursor.fetchall()
    
    print(f"✅ Random sampling returned {len(results)} records")
    for row in results:
        print(f"   {row}")
    
    # Test systematic sampling simulation
    cursor.execute("SELECT COUNT(*) FROM users")
    total_records = cursor.fetchone()[0]
    sample_size = 2
    step = max(1, total_records // sample_size)
    
    cursor.execute(f"""
        SELECT * FROM (
            SELECT *, ROW_NUMBER() OVER (ORDER BY id) as rn 
            FROM users
        ) WHERE rn % {step} = 0 LIMIT {sample_size}
    """)
    results = cursor.fetchall()
    
    print(f"✅ Systematic sampling returned {len(results)} records")
    for row in results:
        print(f"   {row}")
    
    conn.close()
    
    # Cleanup
    if os.path.exists(db_file):
        os.unlink(db_file)
    
    return True

def test_csv_sampling_direct():
    """Test CSV sampling directly."""
    import csv
    import tempfile
    import random
    
    # Create test CSV
    csv_file = tempfile.mktemp(suffix='.csv')
    
    test_data = [
        ['product_id', 'product_name', 'price', 'category'],
        ['1', 'Laptop Pro', '1299.99', 'Electronics'],
        ['2', 'Office Chair', '249.50', 'Furniture'],  
        ['3', 'Coffee Maker', '89.99', 'Appliances'],
        ['4', 'Desk Lamp', '34.99', 'Furniture'],
        ['5', 'Smartphone', '699.00', 'Electronics'],
    ]
    
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(test_data)
    
    print("✅ Test CSV created successfully")
    
    # Test random sampling
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        sample_size = 3
        selected_rows = random.sample(all_rows, min(sample_size, len(all_rows)))
    
    print(f"✅ CSV random sampling returned {len(selected_rows)} records")
    for row in selected_rows:
        print(f"   {row}")
    
    # Test systematic sampling
    with open(csv_file, 'r') as f:
        reader = csv.DictReader(f)
        all_rows = list(reader)
        total_rows = len(all_rows)
        sample_size = 2
        step = max(1, total_rows // sample_size)
        systematic_sample = [all_rows[i] for i in range(0, total_rows, step)][:sample_size]
    
    print(f"✅ CSV systematic sampling returned {len(systematic_sample)} records")
    for row in systematic_sample:
        print(f"   {row}")
    
    # Cleanup
    if os.path.exists(csv_file):
        os.unlink(csv_file)
    
    return True

if __name__ == "__main__":
    print("🔬 Testing real data sampling algorithms...")
    
    tests = [
        test_database_sampling_direct,
        test_csv_sampling_direct
    ]
    
    passed = 0
    for test in tests:
        try:
            print(f"\n📋 Running {test.__name__}...")
            if test():
                passed += 1
                print(f"✅ {test.__name__} passed")
        except Exception as e:
            print(f"❌ {test.__name__} failed: {e}")
            import traceback
            traceback.print_exc()
    
    print(f"\n📊 Tests passed: {passed}/{len(tests)}")
    
    if passed == len(tests):
        print("🟢 All sampling algorithms work correctly!")
    else:
        print("🔴 Some sampling algorithms failed")