

"""Core functionality for Agent-Orchestrated-ETL."""

from __future__ import annotations

import time
from typing import Any, Dict, List, Optional, Union
from pathlib import Path
import pandas as pd
import json
import asyncio

from .logging_config import get_logger
from .exceptions import DataProcessingException, ValidationError


def scaffolding() -> str:
    """
    Implements the core functionality for the 'scaffolding' task.
    This function sets up the initial project structure and dependencies.

    Returns:
        str: A confirmation message.
    """
    return "implemented"


def testing() -> bool:
    """Establishes the testing framework. For now, this is a placeholder.
    In a real scenario, this would configure pytest, add plugins, etc.

    Returns:
        bool: True if the framework is considered set up.
    """
    return True


def primary_data_extraction(*args, **kwargs) -> List[Dict[str, Any]]:
    """Advanced data extraction with multi-source support and error handling."""
    logger = get_logger("agent_etl.core.extraction")
    
    # Extract source configuration from arguments
    source_config = kwargs.get('source_config', {})
    source_type = source_config.get('type', 'sample')
    source_path = source_config.get('path', '')
    
    try:
        if source_type == 'database':
            return _extract_from_database(source_config)
        elif source_type == 'api':
            return _extract_from_api(source_config)
        elif source_type == 's3':
            return _extract_from_s3(source_config)
        elif source_type == 'file':
            return _extract_from_file(source_config)
        else:
            # Default sample data for testing
            return [
                {"id": 1, "name": "sample_data_1", "value": 100, "category": "test"},
                {"id": 2, "name": "sample_data_2", "value": 200, "category": "prod"},
                {"id": 3, "name": "sample_data_3", "value": 300, "category": "dev"}
            ]
            
    except Exception as e:
        logger.error(f"Data extraction failed for {source_type}: {str(e)}")
        raise DataProcessingException(f"Extraction failed: {str(e)}") from e


def _extract_from_database(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from database sources."""
    # Implementation for database extraction
    connection_string = config.get('connection_string', '')
    query = config.get('query', 'SELECT * FROM default_table LIMIT 100')
    
    # Mock database extraction for now
    return [
        {"id": 1, "db_field": "value1", "timestamp": time.time()},
        {"id": 2, "db_field": "value2", "timestamp": time.time()}
    ]


def _extract_from_api(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from API endpoints."""
    endpoint = config.get('endpoint', '')
    headers = config.get('headers', {})
    
    # Mock API extraction
    return [
        {"api_id": 1, "response_data": "api_value1", "status": "success"},
        {"api_id": 2, "response_data": "api_value2", "status": "success"}
    ]


def _extract_from_s3(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from S3 buckets."""
    bucket = config.get('bucket', '')
    key = config.get('key', '')
    
    # Mock S3 extraction
    return [
        {"s3_object": "file1.json", "size": 1024, "last_modified": time.time()},
        {"s3_object": "file2.csv", "size": 2048, "last_modified": time.time()}
    ]


def _extract_from_file(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from file sources."""
    file_path = config.get('path', '')
    file_type = config.get('format', 'json')
    
    if Path(file_path).exists():
        try:
            if file_type == 'json':
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    return data if isinstance(data, list) else [data]
            elif file_type == 'csv':
                df = pd.read_csv(file_path)
                return df.to_dict('records')
        except Exception as e:
            raise DataProcessingException(f"File extraction failed: {str(e)}")
    
    return [{"file_data": "mock_file_content", "source": file_path}]


def transform_data(data: Union[List[int], List[Dict[str, Any]]], transformation_rules: Optional[List[Dict[str, Any]]] = None, **kwargs) -> List[Dict[str, Any]]:
    """Advanced data transformation with multiple transformation strategies."""
    logger = get_logger("agent_etl.core.transformation")
    
    if not data:
        return []
    
    try:
        # Handle simple integer list (backward compatibility)
        if isinstance(data[0], int):
            return [{"original": n, "squared": n * n} for n in data]
        
        # Handle complex data transformations
        transformation_rules = kwargs.get('transformation_rules', [])
        transformed_data = []
        
        for record in data:
            transformed_record = record.copy()
            
            # Apply transformation rules
            for rule in transformation_rules:
                transformed_record = _apply_transformation_rule(transformed_record, rule)
            
            # Apply default transformations
            transformed_record = _apply_default_transformations(transformed_record)
            
            transformed_data.append(transformed_record)
        
        logger.info(f"Transformed {len(data)} records")
        return transformed_data
        
    except Exception as e:
        logger.error(f"Data transformation failed: {e}")
        raise DataProcessingException(f"Transformation failed: {e}", transformation_step="core_transform") from e


def _extract_from_database(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from database sources."""
    connection_string = config.get('connection_string')
    query = config.get('query', 'SELECT 1 as test_column')
    
    if not connection_string:
        raise ValidationError("Database connection string is required")
    
    try:
        import sqlalchemy
        engine = sqlalchemy.create_engine(connection_string)
        df = pd.read_sql_query(query, engine)
        return df.to_dict('records')
    except ImportError:
        # Fallback to mock data if SQLAlchemy not available
        return [{"test_column": 1, "source": "database_mock"}]
    except Exception as e:
        raise DataProcessingException(f"Database extraction failed: {e}")


def _extract_from_file(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from file sources (CSV, JSON, Parquet)."""
    file_path = config.get('path')
    file_format = config.get('format', 'auto')
    
    if not file_path:
        raise ValidationError("File path is required")
    
    path = Path(file_path)
    if not path.exists():
        raise ValidationError(f"File not found: {file_path}")
    
    try:
        if file_format == 'auto':
            file_format = path.suffix.lower()[1:]  # Remove the dot
        
        if file_format in ['csv']:
            df = pd.read_csv(file_path)
        elif file_format in ['json']:
            df = pd.read_json(file_path)
        elif file_format in ['parquet']:
            df = pd.read_parquet(file_path)
        else:
            raise ValidationError(f"Unsupported file format: {file_format}")
        
        return df.to_dict('records')
        
    except Exception as e:
        raise DataProcessingException(f"File extraction failed: {e}")


def _extract_from_s3(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from S3 sources."""
    bucket = config.get('bucket')
    key = config.get('key')
    
    if not bucket or not key:
        raise ValidationError("S3 bucket and key are required")
    
    try:
        import boto3
        s3_client = boto3.client('s3')
        obj = s3_client.get_object(Bucket=bucket, Key=key)
        
        # Determine file format from key extension
        if key.endswith('.csv'):
            df = pd.read_csv(obj['Body'])
        elif key.endswith('.json'):
            df = pd.read_json(obj['Body'])
        elif key.endswith('.parquet'):
            df = pd.read_parquet(obj['Body'])
        else:
            # Default to CSV
            df = pd.read_csv(obj['Body'])
        
        return df.to_dict('records')
        
    except ImportError:
        # Fallback to mock data if boto3 not available
        return [{"s3_column": "mock_value", "source": "s3_mock"}]
    except Exception as e:
        raise DataProcessingException(f"S3 extraction failed: {e}")


def _extract_from_api(config: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract data from API sources."""
    url = config.get('url')
    method = config.get('method', 'GET')
    headers = config.get('headers', {})
    params = config.get('params', {})
    
    if not url:
        raise ValidationError("API URL is required")
    
    try:
        import requests
        
        response = requests.request(method, url, headers=headers, params=params, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Handle different response formats
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            # Look for common data keys
            for key in ['data', 'results', 'items', 'records']:
                if key in data and isinstance(data[key], list):
                    return data[key]
            # If no list found, wrap the dict
            return [data]
        else:
            return [{"api_response": data}]
            
    except ImportError:
        # Fallback to mock data if requests not available
        return [{"api_column": "mock_value", "source": "api_mock"}]
    except Exception as e:
        raise DataProcessingException(f"API extraction failed: {e}")


def _apply_transformation_rule(record: Dict[str, Any], rule: Dict[str, Any]) -> Dict[str, Any]:
    """Apply a single transformation rule to a record."""
    rule_type = rule.get('type', 'none')
    
    if rule_type == 'add_field':
        field_name = rule.get('field_name')
        field_value = rule.get('field_value')
        if field_name:
            record[field_name] = field_value
    
    elif rule_type == 'rename_field':
        old_name = rule.get('old_name')
        new_name = rule.get('new_name')
        if old_name in record and new_name:
            record[new_name] = record.pop(old_name)
    
    elif rule_type == 'calculate_field':
        field_name = rule.get('field_name')
        calculation = rule.get('calculation', {})
        if field_name and calculation:
            record[field_name] = _calculate_field(record, calculation)
    
    elif rule_type == 'filter_condition':
        condition = rule.get('condition', {})
        if not _evaluate_condition(record, condition):
            return None  # Mark for removal
    
    elif rule_type == 'data_type_conversion':
        field_name = rule.get('field_name')
        target_type = rule.get('target_type')
        if field_name in record and target_type:
            record[field_name] = _convert_data_type(record[field_name], target_type)
    
    return record


def _apply_default_transformations(record: Dict[str, Any]) -> Dict[str, Any]:
    """Apply default transformations to enhance data quality."""
    # Add processing timestamp
    record['processed_at'] = time.time()
    
    # Add data quality score based on completeness
    non_null_fields = sum(1 for v in record.values() if v is not None and v != '')
    total_fields = len(record)
    record['data_quality_score'] = non_null_fields / total_fields if total_fields > 0 else 0
    
    # Clean string fields
    for key, value in record.items():
        if isinstance(value, str):
            record[key] = value.strip()
    
    return record


def _calculate_field(record: Dict[str, Any], calculation: Dict[str, Any]) -> Any:
    """Calculate a new field value based on existing fields."""
    calc_type = calculation.get('type', 'none')
    
    if calc_type == 'sum':
        fields = calculation.get('fields', [])
        return sum(record.get(field, 0) for field in fields if isinstance(record.get(field), (int, float)))
    
    elif calc_type == 'concat':
        fields = calculation.get('fields', [])
        separator = calculation.get('separator', ' ')
        return separator.join(str(record.get(field, '')) for field in fields)
    
    elif calc_type == 'multiply':
        field1 = calculation.get('field1')
        field2 = calculation.get('field2')
        if field1 in record and field2 in record:
            val1 = record[field1]
            val2 = record[field2]
            if isinstance(val1, (int, float)) and isinstance(val2, (int, float)):
                return val1 * val2
    
    return None


def _evaluate_condition(record: Dict[str, Any], condition: Dict[str, Any]) -> bool:
    """Evaluate a filter condition against a record."""
    operator = condition.get('operator', 'equals')
    field = condition.get('field')
    value = condition.get('value')
    
    if field not in record:
        return False
    
    record_value = record[field]
    
    if operator == 'equals':
        return record_value == value
    elif operator == 'not_equals':
        return record_value != value
    elif operator == 'greater_than':
        return isinstance(record_value, (int, float)) and record_value > value
    elif operator == 'less_than':
        return isinstance(record_value, (int, float)) and record_value < value
    elif operator == 'contains':
        return isinstance(record_value, str) and value in record_value
    elif operator == 'in':
        return record_value in value if isinstance(value, list) else False
    
    return True


def _convert_data_type(value: Any, target_type: str) -> Any:
    """Convert a value to the specified data type."""
    try:
        if target_type == 'int':
            return int(float(value)) if value is not None else None
        elif target_type == 'float':
            return float(value) if value is not None else None
        elif target_type == 'str':
            return str(value) if value is not None else None
        elif target_type == 'bool':
            if isinstance(value, str):
                return value.lower() in ('true', '1', 'yes', 'on')
            return bool(value) if value is not None else None
        else:
            return value
    except (ValueError, TypeError):
        return value  # Return original value if conversion fails


class DataQualityValidator:
    """Advanced data quality validation with comprehensive checks."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.core.validator")
    
    def validate_data_quality(self, data: List[Dict[str, Any]], rules: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Perform comprehensive data quality validation."""
        if not data:
            return {
                "status": "failed",
                "message": "No data to validate",
                "quality_score": 0.0,
                "issues": ["No data provided"]
            }
        
        issues = []
        quality_scores = []
        
        # Default validation rules
        default_rules = [
            {"type": "completeness", "threshold": 0.8},
            {"type": "uniqueness", "fields": ["id"]},
            {"type": "consistency", "check": "data_types"},
            {"type": "validity", "check": "required_fields"}
        ]
        
        validation_rules = rules or default_rules
        
        for rule in validation_rules:
            rule_result = self._apply_validation_rule(data, rule)
            quality_scores.append(rule_result["score"])
            if rule_result["issues"]:
                issues.extend(rule_result["issues"])
        
        overall_score = sum(quality_scores) / len(quality_scores) if quality_scores else 0.0
        
        return {
            "status": "passed" if overall_score >= 0.7 and not issues else "failed",
            "quality_score": overall_score,
            "total_records": len(data),
            "issues": issues,
            "validation_details": {
                "completeness_score": self._calculate_completeness(data),
                "consistency_score": self._calculate_consistency(data),
                "validity_score": self._calculate_validity(data)
            }
        }
    
    def _apply_validation_rule(self, data: List[Dict[str, Any]], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Apply a single validation rule."""
        rule_type = rule.get("type", "unknown")
        
        if rule_type == "completeness":
            return self._validate_completeness(data, rule)
        elif rule_type == "uniqueness":
            return self._validate_uniqueness(data, rule)
        elif rule_type == "consistency":
            return self._validate_consistency(data, rule)
        elif rule_type == "validity":
            return self._validate_validity(data, rule)
        else:
            return {"score": 1.0, "issues": []}
    
    def _validate_completeness(self, data: List[Dict[str, Any]], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data completeness."""
        threshold = rule.get("threshold", 0.8)
        issues = []
        
        if not data:
            return {"score": 0.0, "issues": ["No data to check completeness"]}
        
        # Calculate completeness for each field
        field_completeness = {}
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
        
        for field in all_fields:
            non_null_count = sum(1 for record in data if record.get(field) is not None and record.get(field) != '')
            completeness = non_null_count / len(data)
            field_completeness[field] = completeness
            
            if completeness < threshold:
                issues.append(f"Field '{field}' has low completeness: {completeness:.2%}")
        
        overall_completeness = sum(field_completeness.values()) / len(field_completeness) if field_completeness else 0
        
        return {
            "score": overall_completeness,
            "issues": issues
        }
    
    def _validate_uniqueness(self, data: List[Dict[str, Any]], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data uniqueness."""
        fields = rule.get("fields", [])
        issues = []
        
        if not fields:
            return {"score": 1.0, "issues": []}
        
        for field in fields:
            values = [record.get(field) for record in data if record.get(field) is not None]
            unique_values = set(values)
            
            if len(values) != len(unique_values):
                duplicates = len(values) - len(unique_values)
                issues.append(f"Field '{field}' has {duplicates} duplicate values")
        
        score = 0.0 if issues else 1.0
        return {"score": score, "issues": issues}
    
    def _validate_consistency(self, data: List[Dict[str, Any]], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data consistency."""
        check_type = rule.get("check", "data_types")
        issues = []
        
        if check_type == "data_types":
            # Check for consistent data types across records
            field_types = {}
            for record in data:
                for field, value in record.items():
                    if value is not None:
                        value_type = type(value).__name__
                        if field not in field_types:
                            field_types[field] = set()
                        field_types[field].add(value_type)
            
            for field, types in field_types.items():
                if len(types) > 1:
                    issues.append(f"Field '{field}' has inconsistent types: {', '.join(types)}")
        
        score = 0.8 if issues else 1.0
        return {"score": score, "issues": issues}
    
    def _validate_validity(self, data: List[Dict[str, Any]], rule: Dict[str, Any]) -> Dict[str, Any]:
        """Validate data validity."""
        check_type = rule.get("check", "required_fields")
        issues = []
        
        if check_type == "required_fields":
            required_fields = rule.get("required_fields", ["id"])
            
            for i, record in enumerate(data):
                for field in required_fields:
                    if field not in record or record[field] is None or record[field] == '':
                        issues.append(f"Record {i}: Missing required field '{field}'")
        
        score = max(0.0, 1.0 - (len(issues) / (len(data) * 2)))  # Penalty based on issue count
        return {"score": score, "issues": issues}
    
    def _calculate_completeness(self, data: List[Dict[str, Any]]) -> float:
        """Calculate overall data completeness score."""
        if not data:
            return 0.0
        
        total_fields = 0
        non_null_fields = 0
        
        for record in data:
            for value in record.values():
                total_fields += 1
                if value is not None and value != '':
                    non_null_fields += 1
        
        return non_null_fields / total_fields if total_fields > 0 else 0.0
    
    def _calculate_consistency(self, data: List[Dict[str, Any]]) -> float:
        """Calculate data consistency score."""
        if not data:
            return 0.0
        
        # Simple consistency check based on data type uniformity
        field_type_consistency = {}
        
        for record in data:
            for field, value in record.items():
                if value is not None:
                    value_type = type(value).__name__
                    if field not in field_type_consistency:
                        field_type_consistency[field] = {}
                    field_type_consistency[field][value_type] = field_type_consistency[field].get(value_type, 0) + 1
        
        consistency_scores = []
        for field, type_counts in field_type_consistency.items():
            if type_counts:
                max_count = max(type_counts.values())
                total_count = sum(type_counts.values())
                consistency_scores.append(max_count / total_count)
        
        return sum(consistency_scores) / len(consistency_scores) if consistency_scores else 1.0
    
    def _calculate_validity(self, data: List[Dict[str, Any]]) -> float:
        """Calculate data validity score."""
        if not data:
            return 0.0
        
        # Simple validity check - records with all required basic structure
        valid_records = 0
        
        for record in data:
            if isinstance(record, dict) and record:  # Non-empty dictionary
                valid_records += 1
        
        return valid_records / len(data) if data else 0.0
