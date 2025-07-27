# ETL Loading Operations Module Usage

## Overview

The `ETLLoadingOperations` class has been extracted from the main ETL agent to provide modular and reusable data loading functionality. This module handles loading data to various destinations including databases, files, and APIs.

## Features

### 1. **Modular Design**
- Separated loading operations from the main ETL agent
- Easy to test, maintain, and extend
- Supports multiple target types (database, file, API)

### 2. **Comprehensive Error Handling**
- Proper exception handling and logging
- Graceful failure recovery
- Detailed error messages

### 3. **Performance Monitoring**
- Load operation metrics tracking
- Active load monitoring
- Throughput calculations

### 4. **Configuration Validation**
- Pre-load target configuration validation
- Type-specific validation rules
- Clear error reporting

## Basic Usage

```python
from agent_orchestrated_etl.agents.etl_loading_ops import ETLLoadingOperations
import asyncio

# Initialize the loading operations
loading_ops = ETLLoadingOperations(
    logger=your_logger,
    specialization="database"  # or "file", "api", "general"
)

# Example: Load data to PostgreSQL database
async def load_to_postgres():
    task_inputs = {
        "target_config": {
            "type": "postgres",
            "connection_string": "postgresql://user:pass@localhost/mydb",
            "table_name": "customers",
            "batch_size": 1000
        },
        "source_data": [
            {"id": 1, "name": "John Doe", "email": "john@example.com"},
            {"id": 2, "name": "Jane Smith", "email": "jane@example.com"}
        ],
        "load_id": "customer_load_001"
    }
    
    result = await loading_ops.load_data(task_inputs)
    print(f"Load completed: {result}")

# Run the load operation
asyncio.run(load_to_postgres())
```

## Supported Target Types

### Database Targets
- **PostgreSQL**: `type: "postgres"`
- **MySQL**: `type: "mysql"`
- **SQLite**: `type: "sqlite"`

Required configuration:
- `connection_string`: Database connection string
- `table_name`: Target table name
- `batch_size`: Optional batch size (default: 1000)

### File Targets
- **CSV**: `type: "csv"`
- **JSON**: `type: "json"`
- **Parquet**: `type: "parquet"`

Required configuration:
- `file_path`: Output file path
- `compression`: Optional compression type

### API Targets
- **REST API**: `type: "rest"`
- **Webhook**: `type: "webhook"`

Required configuration:
- `url`: API endpoint URL
- `headers`: Optional HTTP headers
- `authentication`: Optional authentication config
- `batch_size`: Optional batch size (default: 100)

## Configuration Validation

```python
# Validate target configuration before loading
validation_result = await loading_ops.validate_target_config({
    "type": "postgres",
    "connection_string": "postgresql://user:pass@localhost/db",
    "table_name": "orders"
})

if validation_result["valid"]:
    print("Configuration is valid")
else:
    print(f"Validation errors: {validation_result['errors']}")
```

## Monitoring and Metrics

```python
# Get active load operations
active_loads = loading_ops.get_active_loads()
print(f"Active loads: {len(active_loads)}")

# Get loading metrics
metrics = loading_ops.get_loading_metrics()
print(f"Total loads: {metrics['total_loads']}")
print(f"Success rate: {metrics['successful_loads'] / metrics['total_loads'] * 100:.2f}%")

# Cancel a load operation
success = loading_ops.cancel_load("load_id_123")
if success:
    print("Load operation cancelled")
```

## Error Handling

The module provides comprehensive error handling:

```python
try:
    result = await loading_ops.load_data(task_inputs)
except AgentException as e:
    print(f"Configuration error: {e}")
except DataProcessingException as e:
    print(f"Loading failed: {e}")
except Exception as e:
    print(f"Unexpected error: {e}")
```

## Integration with ETL Agent

The loading operations are automatically integrated with the main ETL agent:

```python
from agent_orchestrated_etl.agents.etl_agent import ETLAgent

# Create ETL agent with database specialization
etl_agent = ETLAgent(specialization="database")

# The loading operations are available via etl_agent.loading_ops
metrics = etl_agent.loading_ops.get_loading_metrics()
```

## Advanced Features

### Custom Specialization
```python
# Create loading operations with custom specialization
loading_ops = ETLLoadingOperations(
    logger=logger,
    specialization="custom"
)
```

### Batch Processing
```python
# Configure batch processing for large datasets
target_config = {
    "type": "postgres",
    "connection_string": "postgresql://user:pass@localhost/db",
    "table_name": "large_dataset",
    "batch_size": 5000  # Process 5000 records at a time
}
```

### Load Status Tracking
```python
# Check status of specific load operation
load_status = loading_ops.get_load_status("load_id_123")
if load_status:
    print(f"Load status: {load_status['status']}")
    print(f"Records loaded: {load_status['records_loaded']}")
```

## Migration from ETL Agent

If you were previously using loading methods directly on the ETL agent, they are still available for backward compatibility:

```python
# These methods still work but now delegate to loading_ops
result = await etl_agent._load_to_database(data, config)
result = await etl_agent._load_to_file(data, config)
result = await etl_agent._load_to_api(data, config)
```

However, it's recommended to use the loading operations directly for new code:

```python
# Preferred approach
result = await etl_agent.loading_ops._load_to_database(data, config)
```