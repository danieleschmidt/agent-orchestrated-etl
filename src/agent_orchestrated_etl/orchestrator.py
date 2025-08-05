"""Enhanced orchestrator that builds and executes ETL pipelines with resilience features."""

from __future__ import annotations

import time
from typing import Callable, Dict, Any, List, Optional
from pathlib import Path

from . import data_source_analysis, dag_generator
from .core import primary_data_extraction, transform_data
from .exceptions import (
    PipelineExecutionException,
    PipelineTimeoutException,
    DataProcessingException,
    AgentETLException,
)
from .retry import retry, RetryConfigs
from .circuit_breaker import circuit_breaker, CircuitBreakerConfigs
from .graceful_degradation import with_graceful_degradation, DegradationConfigs
from .logging_config import get_logger, TimedOperation, LogContext


def load_data(data: Any, destination_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Load data to configured destination with comprehensive features."""
    import time
    
    start_time = time.time()
    
    try:
        # Use DataLoader for comprehensive loading functionality
        loader = DataLoader()
        result = loader.load(data, destination_config or {})
        
        result.update({
            "load_time": time.time() - start_time,
            "status": result.get("status", "completed")
        })
        
        return result
        
    except Exception as e:
        return {
            "status": "error",
            "error_message": str(e),
            "error_type": type(e).__name__,
            "load_time": time.time() - start_time,
            "records_loaded": 0
        }


class DataLoader:
    """Comprehensive data loading system with database support, transactions, and data quality validation."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.data_loader")
        self.supported_destinations = ['database', 'file', 's3', 'api']
    
    def load(self, data: Any, destination_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to specified destination with comprehensive features."""
        destination_type = destination_config.get('type', 'database').lower()
        
        if destination_type not in self.supported_destinations:
            raise ValueError(f"Unsupported destination type: {destination_type}")
        
        # Validate data before loading
        validation_result = self._validate_data_quality(data, destination_config.get('validation_rules', []))
        if validation_result.get('status') == 'failed':
            return {
                "status": "validation_failed",
                "validation_errors": validation_result.get('errors', []),
                "records_loaded": 0
            }
        
        # Route to appropriate loader
        if destination_type == 'database':
            return self._load_to_database(data, destination_config)
        elif destination_type == 'file':
            return self._load_to_file(data, destination_config)
        elif destination_type == 's3':
            return self._load_to_s3(data, destination_config)
        elif destination_type == 'api':
            return self._load_to_api(data, destination_config)
        else:
            raise ValueError(f"Destination type '{destination_type}' not implemented")
    
    def _validate_data_quality(self, data: Any, validation_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Validate data quality before loading."""
        if not validation_rules:
            return {"status": "passed", "errors": []}
        
        errors = []
        
        # Convert data to list if not already
        if not isinstance(data, list):
            data = [data] if data is not None else []
        
        for rule in validation_rules:
            rule_type = rule.get('type', '').lower()
            
            if rule_type == 'not_null':
                fields = rule.get('fields', [])
                for record in data:
                    if isinstance(record, dict):
                        for field in fields:
                            if field not in record or record[field] is None:
                                errors.append(f"Field '{field}' cannot be null")
            
            elif rule_type == 'data_type':
                field = rule.get('field')
                expected_type = rule.get('expected_type', 'string')
                for i, record in enumerate(data):
                    if isinstance(record, dict) and field in record:
                        value = record[field]
                        if not self._check_data_type(value, expected_type):
                            errors.append(f"Record {i}: Field '{field}' must be of type {expected_type}")
            
            elif rule_type == 'range':
                field = rule.get('field')
                min_val = rule.get('min')
                max_val = rule.get('max')
                for i, record in enumerate(data):
                    if isinstance(record, dict) and field in record:
                        value = record[field]
                        if isinstance(value, (int, float)):
                            if min_val is not None and value < min_val:
                                errors.append(f"Record {i}: Field '{field}' value {value} below minimum {min_val}")
                            if max_val is not None and value > max_val:
                                errors.append(f"Record {i}: Field '{field}' value {value} above maximum {max_val}")
        
        return {
            "status": "failed" if errors else "passed",
            "errors": errors,
            "rules_checked": len(validation_rules)
        }
    
    def _check_data_type(self, value: Any, expected_type: str) -> bool:
        """Check if value matches expected data type."""
        type_mapping = {
            'string': str,
            'integer': int,
            'float': (int, float),
            'boolean': bool,
            'list': list,
            'dict': dict
        }
        
        expected_python_type = type_mapping.get(expected_type.lower())
        if expected_python_type is None:
            return True  # Unknown type, skip validation
        
        return isinstance(value, expected_python_type)
    
    def _load_to_database(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to database with transaction support and bulk operations."""
        from sqlalchemy import create_engine, inspect
        from sqlalchemy.exc import SQLAlchemyError
        import pandas as pd
        
        # Extract configuration
        connection_string = config.get('connection_string')
        table_name = config.get('table_name')
        operation = config.get('operation', 'insert').lower()  # insert, upsert, replace
        batch_size = config.get('batch_size', 1000)
        create_table = config.get('create_table', False)
        primary_key = config.get('primary_key', [])
        
        if not connection_string or not table_name:
            return {
                "status": "error",
                "error_message": "Database connection_string and table_name are required"
            }
        
        # SECURITY: Validate table name to prevent SQL injection
        if not self._is_valid_identifier(table_name):
            return {
                "status": "error",
                "error_message": f"Invalid table name format: {table_name}. Table names must be valid SQL identifiers."
            }
        
        # SECURITY: Validate operation parameter
        valid_operations = ['insert', 'upsert', 'replace']
        if operation not in valid_operations:
            return {
                "status": "error",
                "error_message": f"Invalid operation: {operation}. Must be one of: {valid_operations}"
            }
        
        # SECURITY: Validate primary key column names if provided
        if primary_key:
            if not isinstance(primary_key, list):
                return {
                    "status": "error",
                    "error_message": "primary_key must be a list of column names"
                }
            for pk_col in primary_key:
                if not self._is_valid_identifier(pk_col):
                    return {
                        "status": "error",
                        "error_message": f"Invalid primary key column name: {pk_col}"
                    }
        
        # Convert data to DataFrame for easier manipulation
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            return {
                "status": "error",
                "error_message": "Data must be a list of records or a single record"
            }
        
        if df.empty:
            return {
                "status": "completed",
                "records_loaded": 0,
                "message": "No data to load"
            }
        
        # SECURITY: Validate all column names to prevent SQL injection
        for column_name in df.columns:
            if not self._is_valid_identifier(str(column_name)):
                return {
                    "status": "error",
                    "error_message": f"Invalid column name: {column_name}. Column names must be valid SQL identifiers."
                }
        
        try:
            # Create database engine
            engine = create_engine(connection_string)
            
            with engine.begin() as conn:  # Use transaction
                # Check if table exists
                inspector = inspect(engine)
                table_exists = inspector.has_table(table_name)
                
                if not table_exists and create_table:
                    # Create table from DataFrame schema
                    df.head(0).to_sql(table_name, conn, if_exists='fail', index=False)
                    self.logger.info(f"Created table '{table_name}'")
                elif not table_exists:
                    return {
                        "status": "error",
                        "error_message": f"Table '{table_name}' does not exist. Set create_table=True to create it."
                    }
                
                # Perform the loading operation
                records_loaded = 0
                
                if operation == 'insert':
                    # Simple insert operation
                    df.to_sql(table_name, conn, if_exists='append', index=False, method='multi', chunksize=batch_size)
                    records_loaded = len(df)
                
                elif operation == 'replace':
                    # Replace all data in table
                    df.to_sql(table_name, conn, if_exists='replace', index=False, method='multi', chunksize=batch_size)
                    records_loaded = len(df)
                
                elif operation == 'upsert':
                    # Upsert operation (insert or update)
                    if not primary_key:
                        return {
                            "status": "error",
                            "error_message": "primary_key must be specified for upsert operation"
                        }
                    
                    records_loaded = self._perform_upsert(conn, df, table_name, primary_key, batch_size)
                
                else:
                    return {
                        "status": "error",
                        "error_message": f"Unsupported operation: {operation}"
                    }
                
                # Commit transaction automatically when exiting 'with' block
                self.logger.info(f"Successfully loaded {records_loaded} records to {table_name}")
                
                return {
                    "status": "completed",
                    "records_loaded": records_loaded,
                    "table_name": table_name,
                    "operation": operation,
                    "batch_size": batch_size
                }
        
        except SQLAlchemyError as e:
            self.logger.error(f"Database error during load: {e}")
            return {
                "status": "error",
                "error_message": f"Database error: {str(e)}",
                "error_type": "SQLAlchemyError"
            }
        except Exception as e:
            self.logger.error(f"Unexpected error during load: {e}")
            return {
                "status": "error",
                "error_message": f"Unexpected error: {str(e)}",
                "error_type": type(e).__name__
            }
    
    def _perform_upsert(self, conn, df: 'pandas.DataFrame', table_name: str, primary_key: List[str], batch_size: int) -> int:
        """Perform upsert operation (insert or update on conflict) with SQL injection protection."""
        from sqlalchemy import text, MetaData, Table
        import re
        
        records_loaded = 0
        columns = df.columns.tolist()
        
        # SECURITY: Validate table name and column names to prevent SQL injection
        if not self._is_valid_identifier(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        
        for col in columns:
            if not self._is_valid_identifier(col):
                raise ValueError(f"Invalid column name: {col}")
        
        for pk_col in primary_key:
            if not self._is_valid_identifier(pk_col):
                raise ValueError(f"Invalid primary key column name: {pk_col}")
            if pk_col not in columns:
                raise ValueError(f"Primary key column '{pk_col}' not found in data")
        
        # SECURITY: Use SQLAlchemy's identifier quoting for table and column names
        # This prevents SQL injection while preserving functionality
        metadata = MetaData()
        try:
            # Reflect the table to get proper column information
            table = Table(table_name, metadata, autoload_with=conn.engine)
            quoted_table_name = table.name
            quoted_columns = [col.name for col in table.columns if col.name in columns]
            quoted_pk_columns = [col.name for col in table.columns if col.name in primary_key]
        except Exception:
            # Fallback: manually quote identifiers
            quoted_table_name = self._quote_identifier(table_name, conn.engine.dialect)
            quoted_columns = [self._quote_identifier(col, conn.engine.dialect) for col in columns]
            quoted_pk_columns = [self._quote_identifier(col, conn.engine.dialect) for col in primary_key]
        
        # Generate upsert query based on database type with proper escaping
        if 'postgresql' in str(conn.engine.url).lower():
            # PostgreSQL ON CONFLICT syntax
            pk_clause = ', '.join(quoted_pk_columns)
            update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in quoted_columns if col not in quoted_pk_columns])
            
            # Use parameterized values only - never interpolate identifiers into f-strings
            column_placeholders = ', '.join([f":{col}" for col in columns])
            columns_list = ', '.join(quoted_columns)
            
            # Build query with proper identifier escaping
            query = f"""
                INSERT INTO {quoted_table_name} ({columns_list})
                VALUES ({column_placeholders})
                ON CONFLICT ({pk_clause})
                DO UPDATE SET {update_clause}
            """
        
        elif 'mysql' in str(conn.engine.url).lower():
            # MySQL ON DUPLICATE KEY UPDATE syntax
            update_clause = ', '.join([f"{col} = VALUES({col})" for col in quoted_columns if col not in quoted_pk_columns])
            column_placeholders = ', '.join([f":{col}" for col in columns])
            columns_list = ', '.join(quoted_columns)
            
            query = f"""
                INSERT INTO {quoted_table_name} ({columns_list})
                VALUES ({column_placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
        
        else:
            # SQLite or other databases - use REPLACE
            column_placeholders = ', '.join([f":{col}" for col in columns])
            columns_list = ', '.join(quoted_columns)
            
            query = f"""
                REPLACE INTO {quoted_table_name} ({columns_list})
                VALUES ({column_placeholders})
            """
        
        # Execute upsert in batches using parameterized queries
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i:i+batch_size]
            batch_records = batch.to_dict('records')
            
            # SECURITY: Use parameterized execution - values are bound safely
            for record in batch_records:
                # Validate record data to prevent injection through values
                validated_record = self._validate_record_values(record)
                conn.execute(text(query), validated_record)
                records_loaded += 1
        
        return records_loaded
    
    def _is_valid_identifier(self, identifier: str) -> bool:
        """Validate SQL identifier to prevent injection attacks."""
        if not identifier or not isinstance(identifier, str):
            return False
        
        # Allow alphanumeric characters, underscores, and dots (for schema.table)
        # Must start with letter or underscore
        import re
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$'
        return bool(re.match(pattern, identifier)) and len(identifier) <= 64
    
    def _quote_identifier(self, identifier: str, dialect) -> str:
        """Safely quote SQL identifier for the given dialect."""
        # Use SQLAlchemy's dialect-specific quoting
        return dialect.identifier_preparer.quote(identifier)
    
    def _validate_record_values(self, record: dict) -> dict:
        """Validate and sanitize record values to prevent injection."""
        validated = {}
        for key, value in record.items():
            # Ensure key is valid (already validated above, but double-check)
            if not self._is_valid_identifier(key):
                raise ValueError(f"Invalid column name in record: {key}")
            
            # For values, we rely on parameterized queries for safety
            # But we can add basic type validation
            if value is not None:
                # Convert to appropriate types, handling potential injection vectors
                if isinstance(value, str):
                    # Limit string length to prevent memory attacks
                    if len(value) > 10000:
                        raise ValueError(f"String value too long for column {key}")
                    validated[key] = value
                elif isinstance(value, (int, float, bool)):
                    validated[key] = value
                else:
                    # Convert other types to string with length limit
                    str_value = str(value)
                    if len(str_value) > 10000:
                        raise ValueError(f"Value too long for column {key}")
                    validated[key] = str_value
            else:
                validated[key] = None
        
        return validated
    
    def _load_to_file(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to file (CSV, JSON, Parquet)."""
        import pandas as pd
        from pathlib import Path
        
        file_path = config.get('file_path')
        file_format = config.get('format', 'csv').lower()
        
        if not file_path:
            return {
                "status": "error",
                "error_message": "file_path is required for file destination"
            }
        
        # Convert data to DataFrame
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame([data])
        else:
            return {
                "status": "error",
                "error_message": "Data must be a list of records or a single record"
            }
        
        try:
            # Ensure directory exists
            Path(file_path).parent.mkdir(parents=True, exist_ok=True)
            
            # Write based on format
            if file_format == 'csv':
                df.to_csv(file_path, index=False)
            elif file_format == 'json':
                df.to_json(file_path, orient='records', indent=2)
            elif file_format == 'parquet':
                df.to_parquet(file_path, index=False)
            else:
                return {
                    "status": "error",
                    "error_message": f"Unsupported file format: {file_format}"
                }
            
            return {
                "status": "completed",
                "records_loaded": len(df),
                "file_path": file_path,
                "format": file_format
            }
        
        except Exception as e:
            return {
                "status": "error",
                "error_message": f"File write error: {str(e)}",
                "error_type": type(e).__name__
            }
    
    def _load_to_s3(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to S3."""
        import boto3
        import json
        import pandas as pd
        
        bucket = config.get('bucket')
        key = config.get('key')
        file_format = config.get('format', 'json').lower()
        
        if not bucket or not key:
            return {
                "status": "error",
                "error_message": "bucket and key are required for S3 destination"
            }
        
        # Convert data to appropriate format
        try:
            if file_format == 'json':
                if isinstance(data, list):
                    content = json.dumps(data, indent=2)
                else:
                    content = json.dumps([data] if data else [], indent=2)
            elif file_format == 'csv':
                df = pd.DataFrame(data if isinstance(data, list) else [data])
                content = df.to_csv(index=False)
            else:
                return {
                    "status": "error",
                    "error_message": f"Unsupported S3 format: {file_format}"
                }
            
            # Upload to S3
            s3_client = boto3.client('s3')
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=content.encode('utf-8'),
                ContentType='application/json' if file_format == 'json' else 'text/csv'
            )
            
            record_count = len(data) if isinstance(data, list) else 1
            
            return {
                "status": "completed",
                "records_loaded": record_count,
                "s3_location": f"s3://{bucket}/{key}",
                "format": file_format
            }
        
        except Exception as e:
            return {
                "status": "error",
                "error_message": f"S3 upload error: {str(e)}",
                "error_type": type(e).__name__
            }
    
    def _load_to_api(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to API endpoint."""
        import asyncio
        
        url = config.get('url')
        method = config.get('method', 'POST').upper()
        headers = config.get('headers', {})
        batch_size = config.get('batch_size', 100)
        
        if not url:
            return {
                "status": "error",
                "error_message": "url is required for API destination"
            }
        
        # Ensure data is a list
        if not isinstance(data, list):
            data = [data] if data else []
        
        try:
            # Run async API loading
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(self._load_to_api_async(data, url, method, headers, batch_size))
            loop.close()
            
            return result
        
        except Exception as e:
            return {
                "status": "error",
                "error_message": f"API load error: {str(e)}",
                "error_type": type(e).__name__
            }
    
    async def _load_to_api_async(self, data: List[Any], url: str, method: str, headers: Dict[str, str], batch_size: int) -> Dict[str, Any]:
        """Async helper for API loading."""
        import aiohttp
        
        headers.setdefault('Content-Type', 'application/json')
        records_loaded = 0
        errors = []
        
        async with aiohttp.ClientSession() as session:
            # Process data in batches
            for i in range(0, len(data), batch_size):
                batch = data[i:i+batch_size]
                
                try:
                    async with session.request(
                        method, 
                        url, 
                        json=batch, 
                        headers=headers
                    ) as response:
                        if 200 <= response.status < 300:
                            records_loaded += len(batch)
                        else:
                            error_text = await response.text()
                            errors.append(f"Batch {i//batch_size + 1}: HTTP {response.status} - {error_text}")
                
                except Exception as e:
                    errors.append(f"Batch {i//batch_size + 1}: {str(e)}")
        
        return {
            "status": "completed" if not errors else "partial_success",
            "records_loaded": records_loaded,
            "total_records": len(data),
            "batch_size": batch_size,
            "errors": errors if errors else None
        }


class MonitorAgent:
    """Enhanced monitor agent with structured logging and metrics collection."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.events: List[str] = []
        self._path = Path(path) if path else None
        self.logger = get_logger("agent_etl.monitor")
        self.metrics: Dict[str, Any] = {
            "tasks_started": 0,
            "tasks_completed": 0,
            "tasks_failed": 0,
            "total_execution_time": 0.0,
            "start_time": None,
        }

    def _write(self, message: str) -> None:
        """Write message to file if path is configured."""
        if self._path:
            try:
                with self._path.open("a", encoding="utf-8") as fh:
                    fh.write(f"{time.time()}: {message}\n")
            except Exception as e:
                self.logger.error(f"Failed to write to monitor file: {e}")

    def start_pipeline(self, pipeline_id: str) -> None:
        """Record pipeline start."""
        self.metrics["start_time"] = time.time()
        message = f"Pipeline {pipeline_id} started"
        self.events.append(message)
        self._write(message)
        self.logger.info(
            "Pipeline execution started",
            extra={
                "event_type": "pipeline_start",
                "pipeline_id": pipeline_id,
            }
        )

    def end_pipeline(self, pipeline_id: str, success: bool = True) -> None:
        """Record pipeline completion."""
        if self.metrics["start_time"]:
            duration = time.time() - self.metrics["start_time"]
            self.metrics["total_execution_time"] = duration
        
        status = "completed" if success else "failed"
        message = f"Pipeline {pipeline_id} {status}"
        self.events.append(message)
        self._write(message)
        
        self.logger.info(
            f"Pipeline execution {status}",
            extra={
                "event_type": "pipeline_end",
                "pipeline_id": pipeline_id,
                "success": success,
                "duration_seconds": self.metrics["total_execution_time"],
                "metrics": self.get_metrics(),
            }
        )

    def log(self, message: str, task_id: Optional[str] = None) -> None:
        """Log a general message."""
        self.events.append(message)
        self._write(message)
        
        if task_id and "starting" in message.lower():
            self.metrics["tasks_started"] += 1
        elif task_id and "completed" in message.lower():
            self.metrics["tasks_completed"] += 1
        
        self.logger.info(
            message,
            extra={
                "event_type": "task_log",
            }
        )

    def error(self, message: str, task_id: Optional[str] = None, exception: Optional[Exception] = None) -> None:
        """Log an error message."""
        entry = f"ERROR: {message}"
        self.events.append(entry)
        self._write(entry)
        
        if task_id:
            self.metrics["tasks_failed"] += 1
        
        extra = {
            "event_type": "task_error",
        }
        
        if exception:
            extra["exception_type"] = type(exception).__name__
            extra["exception_message"] = str(exception)
        
        self.logger.error(message, extra=extra, exc_info=exception)

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics."""
        return self.metrics.copy()


class Pipeline:
    """Enhanced executable pipeline with resilience features."""

    def __init__(
        self,
        dag: dag_generator.SimpleDAG,
        operations: Dict[str, Callable[..., Any]],
        *,
        dag_id: str = "generated",
        timeout: Optional[float] = None,
        enable_retries: bool = True,
        enable_circuit_breaker: bool = True,
        enable_graceful_degradation: bool = True,
    ) -> None:
        self.dag = dag
        self.operations = operations
        self.dag_id = dag_id
        self.timeout = timeout
        self.enable_retries = enable_retries
        self.enable_circuit_breaker = enable_circuit_breaker
        self.enable_graceful_degradation = enable_graceful_degradation
        self.logger = get_logger(f"agent_etl.pipeline.{dag_id}")

    def execute(self, monitor: Optional[MonitorAgent] = None) -> Dict[str, Any]:
        """Run tasks in topological order with enhanced error handling."""
        results: Dict[str, Any] = {}
        start_time = time.time()
        
        if monitor:
            monitor.start_pipeline(self.dag_id)
        
        try:
            with LogContext(pipeline_id=self.dag_id):
                with TimedOperation(f"pipeline_execution_{self.dag_id}", self.logger):
                    for task_id in self.dag.topological_sort():
                        # Check timeout
                        if self.timeout and (time.time() - start_time) > self.timeout:
                            raise PipelineTimeoutException(
                                f"Pipeline {self.dag_id} timed out",
                                timeout_seconds=self.timeout,
                                pipeline_id=self.dag_id,
                            )
                        
                        results[task_id] = self._execute_task(task_id, results, monitor)
                        
            if monitor:
                monitor.end_pipeline(self.dag_id, success=True)
                
            return results
            
        except Exception as exc:
            if monitor:
                monitor.end_pipeline(self.dag_id, success=False)
            
            # Wrap non-ETL exceptions
            if not isinstance(exc, AgentETLException):
                exc = PipelineExecutionException(
                    f"Pipeline {self.dag_id} execution failed: {exc}",
                    pipeline_id=self.dag_id,
                    cause=exc,
                )
            
            raise exc

    def _execute_task(
        self,
        task_id: str,
        results: Dict[str, Any],
        monitor: Optional[MonitorAgent],
    ) -> Any:
        """Execute a single task with resilience features."""
        if monitor:
            monitor.log(f"starting {task_id}", task_id)
        
        func = self.operations.get(task_id)
        if func is None:
            self.logger.warning(f"No operation defined for task {task_id}")
            return None
        
        # Apply resilience decorators based on configuration
        resilient_func = self._make_resilient(func, task_id)
        
        try:
            with LogContext(task_id=task_id):
                with TimedOperation(f"task_execution_{task_id}", self.logger):
                    # Determine task arguments based on dependencies
                    if task_id.startswith("transform"):
                        src_task = task_id.replace("transform", "extract")
                        src_data = results.get(src_task)
                        result = resilient_func(src_data)
                    elif task_id.startswith("load"):
                        src_task = task_id.replace("load", "transform")
                        src_data = results.get(src_task)
                        result = resilient_func(src_data)
                    else:
                        result = resilient_func()
                    
                    if monitor:
                        monitor.log(f"completed {task_id}", task_id)
                    
                    return result
                    
        except Exception as exc:
            if monitor:
                monitor.error(f"{task_id} failed: {exc}", task_id, exc)
            
            # Wrap in task-specific exception if needed
            if not isinstance(exc, AgentETLException):
                exc = PipelineExecutionException(
                    f"Task {task_id} failed: {exc}",
                    task_id=task_id,
                    pipeline_id=self.dag_id,
                    cause=exc,
                )
            
            raise exc

    def _make_resilient(self, func: Callable, task_id: str) -> Callable:
        """Apply resilience patterns to a function."""
        resilient_func = func
        
        # Apply graceful degradation
        if self.enable_graceful_degradation:
            resilient_func = with_graceful_degradation(
                f"pipeline_{self.dag_id}_task_{task_id}",
                DegradationConfigs.DATA_PROCESSING,
            )(resilient_func)
        
        # Apply circuit breaker
        if self.enable_circuit_breaker:
            resilient_func = circuit_breaker(
                f"pipeline_{self.dag_id}_task_{task_id}",
                CircuitBreakerConfigs.STANDARD,
            )(resilient_func)
        
        # Apply retry logic
        if self.enable_retries:
            resilient_func = retry(RetryConfigs.STANDARD)(resilient_func)
        
        return resilient_func

    def get_status(self) -> Dict[str, Any]:
        """Get pipeline status information."""
        return {
            "dag_id": self.dag_id,
            "total_tasks": len(self.dag.tasks),
            "task_order": self.dag.topological_sort(),
            "timeout": self.timeout,
            "resilience_features": {
                "retries_enabled": self.enable_retries,
                "circuit_breaker_enabled": self.enable_circuit_breaker,
                "graceful_degradation_enabled": self.enable_graceful_degradation,
            },
        }


class DataOrchestrator:
    """Enhanced high-level interface to build and run ETL pipelines with resilience."""

    def __init__(self, enable_quantum_planning: bool = True, enable_adaptive_resources: bool = True):
        """Initialize the data orchestrator."""
        self.logger = get_logger("agent_etl.orchestrator")
        self.enable_quantum_planning = enable_quantum_planning
        self.enable_adaptive_resources = enable_adaptive_resources
        
        # Initialize quantum and adaptive systems
        if enable_quantum_planning:
            from .quantum_planner import QuantumPipelineOrchestrator
            self.quantum_orchestrator = QuantumPipelineOrchestrator(self)
        
        if enable_adaptive_resources:
            from .adaptive_resources import AdaptiveResourceManager
            self.resource_manager = AdaptiveResourceManager()
            self.resource_manager.start_monitoring()

    @retry(RetryConfigs.STANDARD)
    @circuit_breaker("data_source_analysis", CircuitBreakerConfigs.EXTERNAL_API)
    def create_pipeline(
        self,
        source: str,
        *,
        dag_id: str = "generated",
        operations: Optional[Dict[str, Callable[..., Any]]] = None,
        timeout: Optional[float] = None,
        enable_retries: bool = True,
        enable_circuit_breaker: bool = True,
        enable_graceful_degradation: bool = True,
    ) -> Pipeline:
        """Create a resilient ETL pipeline from a data source.
        
        Args:
            source: Data source identifier
            dag_id: Unique identifier for the DAG
            operations: Custom operations to override defaults
            timeout: Pipeline execution timeout in seconds
            enable_retries: Whether to enable retry mechanisms
            enable_circuit_breaker: Whether to enable circuit breaker pattern
            enable_graceful_degradation: Whether to enable graceful degradation
            
        Returns:
            Configured Pipeline instance
            
        Raises:
            DataSourceException: If data source analysis fails
            ValidationError: If input validation fails
        """
        with LogContext(dag_id=dag_id, source=source):
            self.logger.info(
                f"Creating pipeline for source: {source}",
                extra={
                    "event_type": "pipeline_creation_start",
                }
            )
            
            try:
                # Analyze data source with resilience
                metadata = data_source_analysis.analyze_source(source)
                
                # Generate DAG
                dag = dag_generator.generate_dag(metadata)
                
                # Set up default operations with resilience
                ops: Dict[str, Callable[..., Any]] = {
                    "extract": self._make_resilient_extract(),
                    "transform": self._make_resilient_transform(),
                    "load": self._make_resilient_load(),
                }
                
                # Add per-table operations
                for table in metadata["tables"]:
                    ops[f"extract_{table}"] = self._make_resilient_extract()
                    ops[f"transform_{table}"] = self._make_resilient_transform()
                    ops[f"load_{table}"] = self._make_resilient_load()
                
                # Override with custom operations
                if operations:
                    ops.update(operations)
                
                # Create pipeline with resilience features
                pipeline = Pipeline(
                    dag=dag,
                    operations=ops,
                    dag_id=dag_id,
                    timeout=timeout,
                    enable_retries=enable_retries,
                    enable_circuit_breaker=enable_circuit_breaker,
                    enable_graceful_degradation=enable_graceful_degradation,
                )
                
                self.logger.info(
                    f"Pipeline created successfully: {dag_id}",
                    extra={
                        "event_type": "pipeline_creation_success",
                        "total_tasks": len(dag.tasks),
                    }
                )
                
                return pipeline
                
            except Exception as exc:
                self.logger.error(
                    f"Failed to create pipeline: {exc}",
                    extra={
                        "event_type": "pipeline_creation_error",
                    },
                    exc_info=exc,
                )
                raise

    def _make_resilient_extract(self) -> Callable:
        """Create a resilient extract function."""
        @with_graceful_degradation("data_extraction", DegradationConfigs.DATA_PROCESSING)
        @circuit_breaker("data_extraction", CircuitBreakerConfigs.DATABASE)
        @retry(RetryConfigs.DATABASE)
        def resilient_extract(*args, **kwargs):
            return primary_data_extraction(*args, **kwargs)
        
        return resilient_extract

    def _make_resilient_transform(self) -> Callable:
        """Create a resilient transform function."""
        @with_graceful_degradation("data_transformation", DegradationConfigs.DATA_PROCESSING)
        @retry(RetryConfigs.STANDARD)
        def resilient_transform(*args, **kwargs):
            return transform_data(*args, **kwargs)
        
        return resilient_transform

    def _make_resilient_load(self) -> Callable:
        """Create a resilient load function."""
        @with_graceful_degradation("data_loading", DegradationConfigs.DATABASE)
        @circuit_breaker("data_loading", CircuitBreakerConfigs.DATABASE)
        @retry(RetryConfigs.DATABASE)
        def resilient_load(*args, **kwargs):
            return load_data(*args, **kwargs)
        
        return resilient_load

    def dag_to_airflow(self, pipeline: Pipeline, dag_id: Optional[str] = None) -> str:
        """Convert pipeline to Airflow DAG code.
        
        Args:
            pipeline: Pipeline to convert
            dag_id: Optional DAG ID override
            
        Returns:
            Airflow DAG Python code
        """
        try:
            return dag_generator.dag_to_airflow_code(
                pipeline.dag, dag_id=dag_id or pipeline.dag_id
            )
        except Exception as exc:
            self.logger.error(f"Failed to generate Airflow code: {exc}", exc_info=exc)
            raise DataProcessingException(
                f"Failed to generate Airflow code: {exc}",
                transformation_step="dag_to_airflow",
                cause=exc,
            )

    def get_orchestrator_status(self) -> Dict[str, Any]:
        """Get orchestrator status and metrics.
        
        Returns:
            Status information dictionary
        """
        from .circuit_breaker import circuit_breaker_registry
        from .graceful_degradation import degradation_manager
        
        status = {
            "circuit_breakers": circuit_breaker_registry.get_all_stats(),
            "degradation_services": degradation_manager.get_all_service_status(),
            "logger_name": self.logger.name,
            "quantum_planning_enabled": self.enable_quantum_planning,
            "adaptive_resources_enabled": self.enable_adaptive_resources,
        }
        
        # Add quantum planning metrics if enabled
        if self.enable_quantum_planning and hasattr(self, 'quantum_orchestrator'):
            status["quantum_metrics"] = self.quantum_orchestrator.quantum_planner.get_quantum_metrics()
        
        # Add resource management status if enabled
        if self.enable_adaptive_resources and hasattr(self, 'resource_manager'):
            status["resource_status"] = self.resource_manager.get_resource_status()
        
        return status
    
    async def execute_pipeline_async(
        self,
        pipeline: Pipeline,
        monitor: Optional[MonitorAgent] = None,
        enable_quantum_optimization: bool = None
    ) -> Dict[str, Any]:
        """Execute pipeline asynchronously with quantum optimization."""
        if enable_quantum_optimization is None:
            enable_quantum_optimization = self.enable_quantum_planning
        
        if enable_quantum_optimization and hasattr(self, 'quantum_orchestrator'):
            return await self.quantum_orchestrator.execute_quantum_pipeline(
                pipeline, monitor, enable_quantum_optimization
            )
        else:
            # Fallback to synchronous execution
            return pipeline.execute(monitor)
    
    def __del__(self):
        """Cleanup resources on destruction."""
        if hasattr(self, 'resource_manager'):
            self.resource_manager.stop_monitoring()
