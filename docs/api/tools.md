# Tools API Reference

## Overview

The Tools API provides comprehensive interfaces for data processing tools within the Agent Orchestrated ETL platform. This includes query execution, pipeline orchestration, data validation, monitoring, and export capabilities.

## Core Tool Classes

### ETLTool (Base Class)

Base class for all ETL tools providing common functionality and interfaces.

#### Methods

##### `async execute(self, **kwargs) -> Dict[str, Any]`
Abstract method for tool execution. Must be implemented by subclasses.

##### `get_capabilities(self) -> List[str]`
Return list of capabilities provided by this tool.

##### `validate_inputs(self, inputs: Dict[str, Any]) -> bool`
Validate tool inputs before execution.

## Data Processing Tools

### QueryDataTool

Comprehensive SQL query execution tool with security, caching, and export capabilities.

#### Core Methods

##### `async _execute(self, data_source: str, query: str, limit: int = 100, format: str = "json", use_cache: bool = True, security_level: str = "standard") -> Dict[str, Any]`

Execute SQL queries with comprehensive security and performance features.

**Parameters:**
- `data_source`: Database connection identifier
- `query`: SQL query string (validated for security)
- `limit`: Maximum number of results (default: 100)
- `format`: Output format ("json", "csv", "excel")
- `use_cache`: Enable query result caching (default: True)
- `security_level`: Security validation level ("strict", "standard", "permissive")

**Returns:**
```python
{
    "status": "success",
    "data": [...],
    "metadata": {
        "rows_returned": 150,
        "execution_time_ms": 45.2,
        "cache_hit": false,
        "query_plan": {...}
    },
    "export_options": {
        "csv_download_url": "/exports/query_123.csv",
        "excel_download_url": "/exports/query_123.xlsx"
    }
}
```

**Security Features:**
- SQL injection prevention with 13 dangerous patterns detected
- Multi-level security validation (strict/standard/permissive)
- Query sanitization and parameterization
- Comprehensive audit logging

#### Caching System

##### `_generate_cache_key(self, data_source: str, query: str, limit: int) -> str`
Generate MD5-based cache keys for deterministic lookup.

##### `_cleanup_cache(self) -> None`
Automatic LRU cache cleanup to prevent memory bloat (1000 entry limit).

**Cache Features:**
- MD5-based deterministic cache keys
- LRU cleanup with configurable limits
- Cache metadata tracking (age, hits, performance)
- Per-query cache control

#### Security Validation

##### `_validate_query_security(self, query: str, security_level: str) -> Dict[str, Any]`
Comprehensive security validation with pattern detection.

**Detected Patterns:**
- DROP/DELETE/TRUNCATE operations
- UNION-based injection attacks
- Comment-based attacks (-- and /* */)
- Stacked queries with semicolons
- System function access attempts
- Information schema access
- File system operations

**Security Levels:**
- **Strict**: SELECT-only queries, maximum validation
- **Standard**: Common operations with validation
- **Permissive**: Development mode with basic validation

#### Performance Features

##### `_execute_sql_query(self, data_source: str, query: str, limit: int) -> Dict[str, Any]`
High-performance SQL execution with connection pooling and optimization.

**Performance Features:**
- SQLAlchemy connection pooling
- Automatic LIMIT injection for large queries
- Query plan extraction and analysis
- Execution timing with millisecond precision
- Resource usage monitoring

### PipelineExecutionTool

Orchestrate complex data processing pipelines with DAG support and monitoring.

#### Pipeline Methods

##### `async execute_pipeline(self, pipeline_config: Dict[str, Any]) -> Dict[str, Any]`
Execute data processing pipeline with dependency resolution.

**Pipeline Features:**
- DAG-based task dependency resolution
- Parallel task execution where possible
- Real-time progress monitoring
- Failure recovery and retry logic
- Resource management and optimization

##### `async monitor_pipeline(self, pipeline_id: str) -> Dict[str, Any]`
Real-time pipeline monitoring with health checks and metrics.

**Returns:**
```python
{
    "pipeline_id": "pipeline_123",
    "status": "running",
    "progress": 0.65,
    "tasks_completed": 13,
    "tasks_total": 20,
    "current_tasks": ["extract_api_data", "validate_quality"],
    "estimated_completion": "2025-07-24T15:30:00Z",
    "performance_metrics": {
        "throughput": 1500.0,
        "error_rate": 0.02
    }
}
```

### DataValidationTool

Comprehensive data quality validation with configurable rules and anomaly detection.

#### Validation Methods

##### `async validate_data(self, data: Any, validation_rules: Dict[str, Any]) -> Dict[str, Any]`
Execute comprehensive data quality validation.

**Validation Categories:**
- **Data Type Validation**: Type checking and conversion
- **Format Validation**: Pattern matching and structure validation
- **Business Rule Validation**: Custom business logic validation
- **Statistical Validation**: Outlier and anomaly detection
- **Completeness Validation**: Missing value and null analysis
- **Integrity Validation**: Referential integrity and consistency

**Example Validation Rules:**
```python
validation_rules = {
    "email": {
        "type": "string",
        "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        "required": True
    },
    "age": {
        "type": "integer",
        "min_value": 0,
        "max_value": 150,
        "statistical_outlier_check": True
    },
    "revenue": {
        "type": "float",
        "min_value": 0.0,
        "business_rule": "revenue > cost",
        "anomaly_detection": True
    }
}
```

##### `async detect_anomalies(self, data: List[Any], column_name: str) -> Dict[str, Any]`
Statistical anomaly detection using multiple algorithms.

**Anomaly Detection Methods:**
- Interquartile Range (IQR) method for outlier detection
- Z-score analysis for statistical outliers
- Pattern-based anomaly detection
- Time-series anomaly detection (for temporal data)

### MonitoringTool

Real-time system monitoring with metrics collection and alerting.

#### Monitoring Methods

##### `async collect_metrics(self, component: str) -> Dict[str, Any]`
Collect comprehensive system metrics for monitoring and alerting.

**Metric Categories:**
- **Performance Metrics**: Throughput, latency, resource usage
- **Health Metrics**: System availability, error rates, uptime
- **Business Metrics**: Data quality scores, pipeline success rates
- **Security Metrics**: Access patterns, security events, compliance

##### `async generate_alerts(self, metrics: Dict[str, Any], thresholds: Dict[str, Any]) -> List[Dict[str, Any]]`
Generate intelligent alerts based on configurable thresholds and patterns.

**Alert Types:**
- **Threshold Alerts**: Metric-based threshold violations
- **Trend Alerts**: Statistical trend analysis and deviation
- **Pattern Alerts**: Anomalous behavior pattern detection
- **Predictive Alerts**: ML-based predictive alerting

## Export and Formatting Tools

### DataExportTool

Multi-format data export with compression and delivery options.

#### Export Methods

##### `async export_data(self, data: Any, format: str, options: Dict[str, Any] = None) -> Dict[str, Any]`
Export data to multiple formats with configurable options.

**Supported Formats:**
- **JSON**: Structured JSON with optional pretty-printing
- **CSV**: CSV with configurable delimiters and encoding
- **Excel**: Multi-sheet Excel workbooks with formatting
- **Parquet**: Columnar storage for big data applications
- **XML**: Structured XML with schema validation

**Export Options:**
```python
export_options = {
    "compression": "gzip",  # gzip, zip, bzip2, none
    "encoding": "utf-8",
    "include_metadata": True,
    "delivery_method": "download",  # download, email, s3, ftp
    "notification": {
        "email": "user@example.com",
        "webhook": "https://api.example.com/webhook"
    }
}
```

## Configuration and Utilities

### ToolRegistry

Central registry for tool discovery and management.

#### Registry Methods

##### `register_tool(self, tool_class: Type[ETLTool], metadata: Dict[str, Any]) -> None`
Register new tools with metadata for discovery.

##### `get_tool(self, tool_name: str) -> ETLTool`
Retrieve tool instance by name with lazy loading.

##### `list_available_tools(self) -> List[Dict[str, Any]]`
List all available tools with capabilities and metadata.

## Error Handling

### ToolException
Base exception for all tool-related errors.

### SecurityException
Raised for security validation failures and unauthorized access.

### ValidationException
Raised for data validation and rule violation errors.

### PerformanceException  
Raised for performance threshold violations and timeouts.

## Performance Optimization

### Caching Strategies
- **Query Result Caching**: MD5-based caching with LRU cleanup
- **Connection Pooling**: Efficient database connection management
- **Result Streaming**: Memory-efficient handling of large datasets
- **Parallel Processing**: Concurrent execution for independent operations

### Resource Management
- **Memory Optimization**: Automatic cleanup and garbage collection
- **Connection Limits**: Configurable connection pool sizing
- **Timeout Management**: Intelligent timeout handling for operations
- **Load Balancing**: Resource distribution across available resources

## Security Features

### Input Validation
- **SQL Injection Prevention**: Comprehensive pattern detection
- **Parameter Sanitization**: Automatic input sanitization
- **Schema Validation**: Input schema validation against defined contracts
- **Authorization Checks**: Role-based access control for tool operations

### Audit and Compliance
- **Operation Logging**: Complete audit trail for all tool operations
- **Security Event Tracking**: Security violation logging and monitoring
- **Data Lineage**: Full data processing history and lineage tracking
- **Compliance Reporting**: Automated compliance report generation

### Data Protection
- **Encryption**: Data encryption in transit and at rest
- **Access Control**: Fine-grained access control for data operations
- **Privacy Protection**: PII detection and protection mechanisms
- **Data Masking**: Automatic data masking for sensitive information

## Integration Examples

### Basic Query Execution
```python
query_tool = QueryDataTool()
result = await query_tool._execute(
    data_source="production_db",
    query="SELECT * FROM customers WHERE region = %s",
    limit=100,
    format="json",
    security_level="standard"
)
```

### Pipeline Orchestration
```python
pipeline_tool = PipelineExecutionTool()
result = await pipeline_tool.execute_pipeline({
    "name": "customer_etl_pipeline",
    "tasks": [
        {"type": "extract", "source": "crm_api"},
        {"type": "validate", "rules": "customer_validation"},
        {"type": "transform", "mappings": "customer_mappings"},
        {"type": "load", "destination": "data_warehouse"}
    ]
})
```

### Data Validation
```python
validation_tool = DataValidationTool()
result = await validation_tool.validate_data(
    data=customer_data,
    validation_rules=customer_validation_rules
)
```

---

*For advanced usage patterns and custom tool development, see the Developer Guide.*