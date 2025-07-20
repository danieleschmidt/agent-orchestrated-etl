# S3 Data Source Analysis Guide

This guide explains how to use the enhanced S3 data source analysis capabilities in the Agent-Orchestrated ETL system.

## Overview

The S3 Data Source Analysis feature provides comprehensive analysis of data stored in Amazon S3 buckets. Instead of returning placeholder metadata, the system now inspects actual S3 contents to infer data structure, quality, and characteristics.

## Features

### Automatic Schema Detection
- **File Format Recognition**: Automatically detects CSV, JSON, Parquet, XML, Text, Avro, and ORC files
- **Column Inference**: Extracts column names and data types from sample files
- **Data Type Detection**: Identifies numeric, date, string, and null data types
- **Row Count Estimation**: Provides row counts for analyzed files

### Data Quality Assessment
- **PII Detection**: Identifies potential personally identifiable information
- **Null Value Analysis**: Detects and reports null indicators
- **Data Type Consistency**: Validates data type consistency across samples
- **Quality Scoring**: Provides data quality metrics

### Partition Detection
- **Hive-style Partitioning**: Detects `key=value` partition patterns
- **Date-based Partitioning**: Identifies `year/month/day` directory structures
- **Custom Patterns**: Recognizes various partitioning schemes

### Performance Optimization
- **Sample-based Analysis**: Analyzes representative samples for efficiency
- **Partial File Reading**: Downloads only necessary data portions
- **Caching Support**: Reduces redundant S3 API calls
- **Error Resilience**: Graceful fallback when analysis fails

## Usage

### Basic S3 Analysis

```python
from agent_orchestrated_etl.data_source_analysis import analyze_source

# Analyze an S3 location
metadata = analyze_source("s3", "s3://my-bucket/data/")

print(f"Tables found: {metadata['tables']}")
print(f"Fields detected: {metadata['fields']}")
print(f"Analysis details: {metadata['analysis_metadata']}")
```

### Direct S3 Analyzer Usage

```python
from agent_orchestrated_etl.data_source_analysis import S3DataAnalyzer

# Create analyzer instance
analyzer = S3DataAnalyzer(
    region_name="us-west-2",
    max_sample_files=10
)

# Analyze S3 source
result = analyzer.analyze_s3_source("s3://my-bucket/data/csv-files/")

# Access detailed results
print(f"Bucket: {result.bucket_name}")
print(f"Total files: {result.total_files}")
print(f"Total size: {result.total_size_bytes / (1024*1024):.2f} MB")
print(f"File formats: {result.file_formats}")
print(f"Schema info: {result.schema_info}")
print(f"Data quality: {result.data_quality}")
print(f"Partitions: {result.partitions}")

# Convert to standard metadata format
metadata = result.to_metadata()
```

## Configuration

### Environment Variables

```bash
# AWS credentials (required)
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1

# Optional: Configure S3 analysis behavior
export AGENT_ETL_S3_MAX_SAMPLE_FILES=20
export AGENT_ETL_S3_ANALYSIS_TIMEOUT=300
```

### Python Configuration

```python
# Custom analyzer configuration
analyzer = S3DataAnalyzer(
    region_name="eu-west-1",
    max_sample_files=15
)

# Use with specific credentials
import boto3
from botocore.credentials import Credentials

# Custom session
session = boto3.Session(
    aws_access_key_id='your-key',
    aws_secret_access_key='your-secret',
    region_name='us-west-2'
)

analyzer._s3_client = session.client('s3')
```

## Supported File Formats

### CSV Files (.csv, .tsv)
- **Column Detection**: First row used as header
- **Data Type Inference**: Numeric, date, string classification
- **Quality Checks**: Null value detection, PII identification
- **Sample Output**:
  ```json
  {
    "format": "csv",
    "columns": ["id", "name", "email", "created_date"],
    "row_count": 1000,
    "data_types": {
      "id": "numeric",
      "name": "string", 
      "email": "string",
      "created_date": "date"
    },
    "potential_pii": ["name", "email"]
  }
  ```

### JSON Files (.json, .jsonl, .ndjson)
- **Schema Extraction**: Field names from JSON objects
- **Nested Structure**: Flattened field analysis
- **Array Support**: Analysis of JSON arrays
- **JSON Lines**: Support for newline-delimited JSON
- **Sample Output**:
  ```json
  {
    "format": "json",
    "columns": ["user_id", "profile", "timestamp"],
    "row_count": 500,
    "data_types": {
      "user_id": "numeric",
      "profile": "string",
      "timestamp": "date"
    }
  }
  ```

### Parquet Files (.parquet, .pq)
- **Metadata Reading**: Schema extraction without full download
- **Column Types**: Native Parquet type detection
- **Compression Info**: Compression algorithm detection
- **Note**: Requires `pyarrow` package for full functionality

### Other Formats
- **XML**: Basic structure detection
- **Text/Log**: Content-based analysis
- **Avro/ORC**: Format recognition with basic metadata

## Data Quality Features

### PII Detection

The system automatically identifies potential personally identifiable information:

```python
# PII detection patterns
pii_indicators = [
    "email", "phone", "ssn", "credit_card", 
    "address", "name", "firstname", "lastname"
]

# Content-based detection
email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
phone_pattern = r'^\+?[\d\s\-\(\)]{10,}$'
```

### Data Type Inference

```python
# Type detection examples
result.data_quality['data_types'] = {
    'id': 'numeric',           # 1, 2, 3.14
    'created_date': 'date',    # 2023-01-01, 01/01/2023
    'description': 'string',   # Text content
    'status': 'null'           # All NULL/empty values
}
```

### Quality Metrics

```python
# Example quality assessment
quality_info = {
    'total_rows_sampled': 1000,
    'null_percentages': {
        'email': 0.05,      # 5% null values
        'phone': 0.15       # 15% null values
    },
    'potential_pii': ['email', 'phone', 'full_name'],
    'data_types': {...}
}
```

## Partition Detection

### Hive-style Partitioning
```
s3://bucket/data/year=2023/month=01/day=15/file.csv
s3://bucket/data/department=sales/region=west/file.json
```

Detected partitions: `["year", "month", "day", "department", "region"]`

### Date-based Partitioning
```
s3://bucket/logs/2023/01/15/log.txt
s3://bucket/data/2023/12/25/events.json
```

Detected partitions: `["year/month/day"]`

## Error Handling and Fallbacks

### Graceful Degradation
```python
# If analysis fails, system provides fallback metadata
fallback_metadata = {
    "tables": ["objects"],
    "fields": ["key", "size", "last_modified"],
    "analysis_metadata": {
        "error": "Failed to analyze S3 source: Access denied",
        "fallback_used": True
    }
}
```

### Common Error Scenarios

1. **Access Denied**
   ```python
   # Result includes error information
   if result.errors:
       print(f"Analysis errors: {result.errors}")
   ```

2. **Network Issues**
   ```python
   # Timeout or connectivity problems
   try:
       result = analyzer.analyze_s3_source("s3://bucket/data/")
   except ValidationError as e:
       print(f"Analysis failed: {e}")
   ```

3. **Invalid File Formats**
   ```python
   # Corrupted or unsupported files are skipped
   # Analysis continues with valid files
   ```

## Performance Considerations

### Optimization Strategies

1. **Limit Sample Size**
   ```python
   analyzer = S3DataAnalyzer(max_sample_files=5)  # Faster analysis
   ```

2. **Regional Configuration**
   ```python
   # Use same region as S3 bucket
   analyzer = S3DataAnalyzer(region_name="us-west-2")
   ```

3. **Partial File Reading**
   ```python
   # Only first 10KB of each file is downloaded
   # Configurable via Range header
   ```

### Cost Optimization

- **API Call Reduction**: Analysis uses list operations and partial downloads
- **Sample-based**: Only analyzes representative files, not entire datasets
- **Caching**: Results can be cached to avoid repeated analysis
- **Regional Access**: Use same region as data to reduce transfer costs

## Advanced Usage

### Custom Analysis Pipeline

```python
from agent_orchestrated_etl.data_source_analysis import S3DataAnalyzer

class CustomS3Analyzer(S3DataAnalyzer):
    def _analyze_custom_format(self, content: bytes):
        # Custom file format analysis
        return custom_schema, custom_quality_info
    
    def _detect_custom_partitions(self, result):
        # Custom partition detection logic
        pass

# Use custom analyzer
custom_analyzer = CustomS3Analyzer()
result = custom_analyzer.analyze_s3_source("s3://my-bucket/custom-data/")
```

### Integration with DAG Generation

```python
# Use analysis results for DAG generation
metadata = analyze_source("s3", "s3://data-lake/sales/")

# Create tasks based on detected file formats
for table in metadata['tables']:
    if 'csv' in table:
        create_csv_processing_task(metadata)
    elif 'json' in table:
        create_json_processing_task(metadata)
```

### Monitoring and Alerting

```python
# Monitor data quality changes
def monitor_data_quality(s3_uri: str):
    result = analyzer.analyze_s3_source(s3_uri)
    
    # Alert on PII detection
    if result.data_quality.get('potential_pii'):
        send_alert(f"PII detected in {s3_uri}")
    
    # Alert on data quality issues
    error_rate = len(result.errors) / max(result.total_files, 1)
    if error_rate > 0.1:  # 10% error threshold
        send_alert(f"High error rate in {s3_uri}: {error_rate}")
```

## Troubleshooting

### Common Issues

1. **"boto3 not available"**
   ```bash
   pip install boto3 pandas pyarrow
   ```

2. **"AWS credentials not configured"**
   ```bash
   aws configure
   # or
   export AWS_ACCESS_KEY_ID=your-key
   export AWS_SECRET_ACCESS_KEY=your-secret
   ```

3. **"S3 bucket does not exist"**
   - Verify bucket name spelling
   - Check region configuration
   - Ensure bucket exists and is accessible

4. **"Access denied"**
   - Verify IAM permissions
   - Check bucket policies
   - Ensure correct region

### Debug Mode

```python
import logging

# Enable debug logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger('agent_orchestrated_etl.data_source_analysis')

# Analyze with detailed logging
result = analyzer.analyze_s3_source("s3://bucket/data/")
```

### Performance Debugging

```python
import time

start_time = time.time()
result = analyzer.analyze_s3_source("s3://large-bucket/data/")
analysis_time = time.time() - start_time

print(f"Analysis completed in {analysis_time:.2f} seconds")
print(f"Files analyzed: {len(result.sample_files)}")
print(f"Total files found: {result.total_files}")
print(f"Errors encountered: {len(result.errors)}")
```

## Security Considerations

### Data Privacy
- **Sample Size Limitation**: Only small samples are downloaded for analysis
- **PII Detection**: Automatically flags potential sensitive data
- **Access Logging**: All S3 access is logged via CloudTrail
- **Encryption**: Supports S3 server-side encryption

### IAM Permissions
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetObject"
            ],
            "Resource": [
                "arn:aws:s3:::your-bucket",
                "arn:aws:s3:::your-bucket/*"
            ]
        }
    ]
}
```

### Best Practices
1. **Least Privilege**: Grant minimal required S3 permissions
2. **Region Consistency**: Use same region for compute and storage
3. **Error Handling**: Always handle analysis failures gracefully
4. **Data Sampling**: Limit analysis to representative samples
5. **Monitoring**: Track analysis performance and errors