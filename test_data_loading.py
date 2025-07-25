#!/usr/bin/env python3
"""Test data loading functionality."""

import tempfile
import os
from src.agent_orchestrated_etl.orchestrator import load_data, DataLoader


def test_data_validation():
    """Test data quality validation."""
    
    loader = DataLoader()
    
    # Test data
    test_data = [
        {"id": 1, "name": "Alice", "age": 25, "email": "alice@example.com"},
        {"id": 2, "name": "Bob", "age": 30, "email": "bob@example.com"},
        {"id": 3, "name": None, "age": 35, "email": "charlie@example.com"}  # Invalid
    ]
    
    # Test validation rules
    validation_rules = [
        {"type": "not_null", "fields": ["name", "email"]},
        {"type": "data_type", "field": "age", "expected_type": "integer"},
        {"type": "range", "field": "age", "min": 18, "max": 65}
    ]
    
    result = loader._validate_data_quality(test_data, validation_rules)
    
    print("Data Validation Test:")
    print(f"Status: {result['status']}")
    print(f"Errors: {result['errors']}")
    print(f"Rules Checked: {result['rules_checked']}")
    
    assert result['status'] == 'failed'
    assert len(result['errors']) > 0
    assert "Field 'name' cannot be null" in result['errors'][0]
    
    print("✅ Data validation test passed!\n")


def test_file_loading():
    """Test loading data to files."""
    
    # Test data
    test_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30},
        {"id": 3, "name": "Charlie", "age": 35}
    ]
    
    # Test CSV file loading
    with tempfile.TemporaryDirectory() as temp_dir:
        csv_path = os.path.join(temp_dir, "test_data.csv")
        
        config = {
            "type": "file",
            "file_path": csv_path,
            "format": "csv"
        }
        
        result = load_data(test_data, config)
        
        print("File Loading Test (CSV):")
        print(f"Status: {result['status']}")
        print(f"Records Loaded: {result['records_loaded']}")
        print(f"File Path: {result['file_path']}")
        
        assert result['status'] == 'completed'
        assert result['records_loaded'] == 3
        assert os.path.exists(csv_path)
        
        # Verify file contents
        with open(csv_path, 'r') as f:
            content = f.read()
            assert 'Alice' in content
            assert 'Bob' in content
            assert 'Charlie' in content
        
        print("✅ CSV file loading test passed!")
    
    # Test JSON file loading
    with tempfile.TemporaryDirectory() as temp_dir:
        json_path = os.path.join(temp_dir, "test_data.json")
        
        config = {
            "type": "file",
            "file_path": json_path,
            "format": "json"
        }
        
        result = load_data(test_data, config)
        
        print(f"JSON Status: {result['status']}")
        print(f"JSON Records: {result['records_loaded']}")
        
        assert result['status'] == 'completed'
        assert result['records_loaded'] == 3
        assert os.path.exists(json_path)
        
        print("✅ JSON file loading test passed!\n")


def test_database_loading_sqlite():
    """Test loading data to SQLite database."""
    
    test_data = [
        {"id": 1, "name": "Alice", "age": 25},
        {"id": 2, "name": "Bob", "age": 30}
    ]
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
        temp_db.close()
        
        try:
            config = {
                "type": "database",
                "connection_string": f"sqlite:///{temp_db.name}",
                "table_name": "users",
                "operation": "insert",
                "create_table": True,
                "batch_size": 100
            }
            
            result = load_data(test_data, config)
            
            print("Database Loading Test (SQLite):")
            print(f"Status: {result['status']}")
            print(f"Records Loaded: {result['records_loaded']}")
            print(f"Table: {result['table_name']}")
            print(f"Operation: {result['operation']}")
            
            assert result['status'] == 'completed'
            assert result['records_loaded'] == 2
            assert result['table_name'] == 'users'
            
            print("✅ SQLite database loading test passed!\n")
            
        finally:
            # Clean up
            if os.path.exists(temp_db.name):
                os.unlink(temp_db.name)


def test_validation_failure():
    """Test data loading with validation failure."""
    
    test_data = [
        {"id": 1, "name": None, "age": 25}  # Invalid - name is null
    ]
    
    config = {
        "type": "file",
        "file_path": "/tmp/test.csv",
        "validation_rules": [
            {"type": "not_null", "fields": ["name"]}
        ]
    }
    
    result = load_data(test_data, config)
    
    print("Validation Failure Test:")
    print(f"Status: {result['status']}")
    print(f"Validation Errors: {result.get('validation_errors', [])}")
    
    assert result['status'] == 'validation_failed'
    assert len(result['validation_errors']) > 0
    
    print("✅ Validation failure test passed!\n")


def test_error_handling():
    """Test error handling in data loading."""
    
    # Test missing configuration
    result = load_data([{"test": "data"}], {"type": "database"})  # Missing required fields
    
    print("Error Handling Test:")
    print(f"Status: {result['status']}")
    print(f"Error Message: {result['error_message']}")
    
    assert result['status'] == 'error'
    assert 'connection_string' in result['error_message']
    
    print("✅ Error handling test passed!\n")


def test_empty_data():
    """Test handling of empty data."""
    
    config = {
        "type": "database",
        "connection_string": "sqlite:///:memory:",
        "table_name": "empty_table",
        "create_table": True
    }
    
    result = load_data([], config)
    
    print("Empty Data Test:")
    print(f"Status: {result['status']}")
    print(f"Records Loaded: {result['records_loaded']}")
    print(f"Message: {result.get('message', '')}")
    
    assert result['status'] == 'completed'
    assert result['records_loaded'] == 0
    
    print("✅ Empty data test passed!\n")


def main():
    """Run all tests."""
    print("Testing Data Loading Implementation...\n")
    
    test_data_validation()
    test_file_loading()
    test_database_loading_sqlite()
    test_validation_failure()
    test_error_handling()
    test_empty_data()
    
    print("🎉 All data loading tests passed!")


if __name__ == '__main__':
    main()