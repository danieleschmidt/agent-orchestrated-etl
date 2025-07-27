"""ETL profiling operations module for comprehensive data profiling and quality analysis."""

from __future__ import annotations

import json
import time
import statistics
import csv
import os
import asyncio
import re
import random
import logging
from typing import Any, Dict, List, Optional, Union

try:
    import boto3
except ImportError:
    boto3 = None

try:
    from sqlalchemy import create_engine, text
except ImportError:
    create_engine = None
    text = None

try:
    from .etl_config import ProfilingConfig, ColumnProfile
    from ..exceptions import DataProcessingException
except ImportError:
    # Fallback for standalone usage
    from etl_config import ProfilingConfig, ColumnProfile
    class DataProcessingException(Exception):
        pass


class ETLProfilingOperations:
    """
    Comprehensive ETL profiling operations for data quality analysis,
    statistical profiling, sampling, and column analysis.
    """
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        """Initialize ETL profiling operations."""
        self.logger = logger or logging.getLogger(__name__)
    
    async def profile_data(self, data_source: str, profiling_config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Profile data to understand structure and quality using advanced algorithms.
        
        Args:
            data_source: Data source identifier or configuration
            profiling_config_dict: Profiling configuration parameters
            
        Returns:
            Comprehensive profiling results
        """
        self.logger.info("Starting advanced data profiling")
        
        try:
            # Create profiling configuration
            profiling_config = ProfilingConfig(**profiling_config_dict)
            
            # Perform comprehensive data profiling
            profile_result = await self.perform_comprehensive_profiling(data_source, profiling_config)
            
            return profile_result
            
        except Exception as e:
            self.logger.error(f"Data profiling failed: {e}", exc_info=e)
            raise DataProcessingException(f"Data profiling failed: {e}") from e
    
    async def perform_comprehensive_profiling(self, data_source: str, config: ProfilingConfig) -> Dict[str, Any]:
        """Perform comprehensive data profiling with advanced statistical analysis."""
        start_time = time.time()
        
        # Load sample data - try to determine if it's a config dict or simple string
        if isinstance(data_source, dict):
            sample_data = await self.load_real_sample_data(data_source, config)
        else:
            # Use mock data for simple string data sources
            sample_data = await self.load_sample_data(data_source, config)
        
        # Analyze dataset structure
        dataset_stats = self.analyze_dataset_structure(sample_data)
        
        # Profile each column
        column_profiles = []
        for column_name in sample_data.get("columns", []):
            column_data = sample_data.get("data", {}).get(column_name, [])
            if column_data:  # Only profile columns with data
                profile = self.profile_column(column_name, column_data, config)
                column_profiles.append(profile)
        
        # Calculate overall data quality score
        overall_quality = self.calculate_overall_quality_score(column_profiles)
        
        # Generate actionable recommendations
        recommendations = self.generate_profiling_recommendations(column_profiles, overall_quality)
        
        # Detect cross-column patterns and correlations
        correlations = {}
        if config.correlation_analysis and len(column_profiles) > 1:
            correlations = self.analyze_correlations(sample_data, column_profiles)
        
        execution_time = time.time() - start_time
        
        return {
            "data_source": data_source,
            "profiling_timestamp": time.time(),
            "execution_time_seconds": execution_time,
            "profiling_config": {
                "statistical_analysis": config.statistical_analysis,
                "anomaly_detection": config.anomaly_detection,
                "data_quality_scoring": config.data_quality_scoring,
                "sample_size": config.sample_size,
                "outlier_method": config.outlier_method
            },
            "dataset_statistics": dataset_stats,
            "column_profiles": [self.column_profile_to_dict(cp) for cp in column_profiles],
            "correlations": correlations,
            "overall_quality_score": overall_quality,
            "quality_breakdown": {
                "completeness": statistics.mean([cp.completeness_score for cp in column_profiles]) if column_profiles else 1.0,
                "validity": statistics.mean([cp.validity_score for cp in column_profiles]) if column_profiles else 1.0,
                "consistency": statistics.mean([cp.consistency_score for cp in column_profiles]) if column_profiles else 1.0,
                "outlier_impact": 1.0 - (sum([cp.outlier_percentage for cp in column_profiles]) / len(column_profiles) / 100) if column_profiles else 1.0
            },
            "recommendations": recommendations,
            "anomaly_summary": {
                "total_outliers": sum([cp.outlier_count for cp in column_profiles]),
                "columns_with_outliers": len([cp for cp in column_profiles if cp.outlier_count > 0]),
                "avg_outlier_percentage": statistics.mean([cp.outlier_percentage for cp in column_profiles]) if column_profiles else 0.0
            }
        }
    
    async def load_real_sample_data(self, data_source_config: Dict[str, Any], config: ProfilingConfig) -> Dict[str, Any]:
        """Load real sample data from actual data sources using intelligent sampling strategies."""
        
        source_type = data_source_config.get('type', '').lower()
        sampling_strategy = data_source_config.get('sampling_strategy', config.sampling_strategy or 'random')
        sample_size = data_source_config.get('sample_size', config.sample_size or 1000)
        
        if source_type == 'database':
            return await self.sample_database_data(data_source_config, sampling_strategy, sample_size)
        elif source_type == 'file':
            return await self.sample_file_data(data_source_config, sampling_strategy, sample_size)
        elif source_type == 'api':
            return await self.sample_api_data(data_source_config, sampling_strategy, sample_size)
        elif source_type == 's3':
            return await self.sample_s3_data(data_source_config, sampling_strategy, sample_size)
        else:
            raise ValueError(f"Unsupported data source type for sampling: {source_type}")
    
    async def sample_database_data(self, config: Dict[str, Any], strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from database using various sampling strategies."""
        if create_engine is None:
            raise ImportError("SQLAlchemy is required for database sampling. Install with: pip install sqlalchemy")
        
        connection_string = config.get('connection_string')
        table_name = config.get('table_name')
        query = config.get('query')
        
        if not connection_string:
            raise ValueError("Database connection_string required for sampling")
        
        # Determine base query
        if query:
            base_query = f"({query}) as sample_query"
        elif table_name:
            base_query = table_name
        else:
            raise ValueError("Either 'query' or 'table_name' required for database sampling")
        
        engine = create_engine(connection_string)
        
        try:
            with engine.connect() as conn:
                # Get total record count for percentage-based sampling
                if not query:
                    count_result = conn.execute(text(f"SELECT COUNT(*) FROM {base_query}"))
                    total_records = count_result.scalar()
                else:
                    # For queries, we'll use the sample size directly
                    total_records = sample_size * 2  # Estimate
                
                # Apply sampling strategy
                if strategy == 'random':
                    if connection_string.startswith('sqlite://'):
                        sample_query = f"SELECT * FROM {base_query} ORDER BY RANDOM() LIMIT {sample_size}"
                    elif 'mysql' in connection_string.lower():
                        sample_query = f"SELECT * FROM {base_query} ORDER BY RAND() LIMIT {sample_size}"
                    else:  # PostgreSQL and others
                        sample_query = f"SELECT * FROM {base_query} ORDER BY RANDOM() LIMIT {sample_size}"
                
                elif strategy == 'systematic':
                    # Systematic sampling: every nth record
                    if total_records > sample_size:
                        step = max(1, total_records // sample_size)
                        if connection_string.startswith('sqlite://'):
                            sample_query = f"""
                                SELECT * FROM (
                                    SELECT *, ROW_NUMBER() OVER (ORDER BY ROWID) as rn 
                                    FROM {base_query}
                                ) WHERE rn % {step} = 0 LIMIT {sample_size}
                            """
                        else:
                            sample_query = f"""
                                SELECT * FROM (
                                    SELECT *, ROW_NUMBER() OVER (ORDER BY 1) as rn 
                                    FROM {base_query}
                                ) ranked WHERE rn % {step} = 0 LIMIT {sample_size}
                            """
                    else:
                        sample_query = f"SELECT * FROM {base_query} LIMIT {sample_size}"
                
                elif strategy == 'stratified':
                    # Stratified sampling by a column (if specified)
                    strata_column = config.get('strata_column')
                    if strata_column:
                        sample_query = f"""
                            SELECT * FROM (
                                SELECT *, ROW_NUMBER() OVER (PARTITION BY {strata_column} ORDER BY RANDOM()) as rn
                                FROM {base_query}
                            ) WHERE rn <= {sample_size // 3} LIMIT {sample_size}
                        """
                    else:
                        # Fall back to random sampling
                        sample_query = f"SELECT * FROM {base_query} ORDER BY RANDOM() LIMIT {sample_size}"
                
                else:
                    # Default to simple LIMIT
                    sample_query = f"SELECT * FROM {base_query} LIMIT {sample_size}"
                
                # Execute sampling query
                result = conn.execute(text(sample_query))
                columns = list(result.keys())
                rows = result.fetchall()
                
                # Convert to dictionary format
                data = {}
                for col in columns:
                    data[col] = []
                
                for row in rows:
                    for i, col in enumerate(columns):
                        data[col].append(row[i])
                
                return {
                    "columns": columns,
                    "total_records": len(rows),
                    "data": data,
                    "sampling_metadata": {
                        "strategy": strategy,
                        "requested_size": sample_size,
                        "actual_size": len(rows),
                        "source_type": "database",
                        "table_name": table_name,
                        "connection_string": connection_string.split('@')[0] + '@***'  # Hide credentials
                    }
                }
                
        except Exception as e:
            raise RuntimeError(f"Database sampling failed: {str(e)}")
        finally:
            engine.dispose()
    
    async def sample_file_data(self, config: Dict[str, Any], strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from files using various sampling strategies."""
        
        file_path = config.get('file_path')
        file_format = config.get('file_format', 'csv').lower()
        
        if not file_path:
            raise ValueError("file_path required for file sampling")
        
        if file_format == 'csv':
            return await self.sample_csv_file(file_path, strategy, sample_size)
        elif file_format == 'json':
            return await self.sample_json_file(file_path, strategy, sample_size)
        elif file_format == 'jsonl':
            return await self.sample_jsonl_file(file_path, strategy, sample_size)
        else:
            raise ValueError(f"Unsupported file format for sampling: {file_format}")
    
    async def sample_csv_file(self, file_path: str, strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from CSV file."""
        # First pass: count total rows and get headers
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            headers = next(reader)
            total_rows = sum(1 for _ in reader)
        
        # Second pass: apply sampling strategy
        selected_rows = []
        
        if strategy == 'random':
            # Random sampling: collect all data then randomly select
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)
                selected_rows = random.sample(all_rows, min(sample_size, len(all_rows)))
        
        elif strategy == 'systematic':
            # Systematic sampling: every nth row
            step = max(1, total_rows // sample_size) if total_rows > sample_size else 1
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for i, row in enumerate(reader):
                    if i % step == 0 and len(selected_rows) < sample_size:
                        selected_rows.append(row)
        
        elif strategy == 'reservoir':
            # Reservoir sampling: memory-efficient for large files
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                reservoir = []
                for i, row in enumerate(reader):
                    if i < sample_size:
                        reservoir.append(row)
                    else:
                        # Replace with decreasing probability
                        j = random.randint(0, i)
                        if j < sample_size:
                            reservoir[j] = row
                selected_rows = reservoir
        
        else:
            # Default: take first n rows
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                selected_rows = [row for i, row in enumerate(reader) if i < sample_size]
        
        # Convert to column-based format
        data = {}
        for header in headers:
            data[header] = []
        
        for row in selected_rows:
            for header in headers:
                data[header].append(row.get(header))
        
        return {
            "columns": headers,
            "total_records": len(selected_rows),
            "data": data,
            "sampling_metadata": {
                "strategy": strategy,
                "requested_size": sample_size,
                "actual_size": len(selected_rows),
                "source_type": "csv_file",
                "file_path": file_path,
                "total_file_rows": total_rows
            }
        }
    
    async def sample_json_file(self, file_path: str, strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from JSON file."""
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and 'data' in data:
            records = data['data']
        else:
            records = [data]
        
        # Apply sampling strategy
        if strategy == 'random':
            selected_records = random.sample(records, min(sample_size, len(records)))
        elif strategy == 'systematic':
            step = max(1, len(records) // sample_size) if len(records) > sample_size else 1
            selected_records = [records[i] for i in range(0, len(records), step)][:sample_size]
        else:
            selected_records = records[:sample_size]
        
        # Extract columns from first record
        if selected_records and isinstance(selected_records[0], dict):
            columns = list(selected_records[0].keys())
        else:
            columns = ['value']
            selected_records = [{'value': record} for record in selected_records]
        
        # Convert to column-based format
        column_data = {}
        for col in columns:
            column_data[col] = []
        
        for record in selected_records:
            for col in columns:
                column_data[col].append(record.get(col))
        
        return {
            "columns": columns,
            "total_records": len(selected_records),
            "data": column_data,
            "sampling_metadata": {
                "strategy": strategy,
                "requested_size": sample_size,
                "actual_size": len(selected_records),
                "source_type": "json_file",
                "file_path": file_path,
                "total_file_records": len(records)
            }
        }
    
    async def sample_jsonl_file(self, file_path: str, strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from JSON Lines file."""
        # First pass: count lines
        with open(file_path, 'r', encoding='utf-8') as f:
            total_lines = sum(1 for line in f if line.strip())
        
        selected_records = []
        
        if strategy == 'random':
            # Random line selection
            line_indices = random.sample(range(total_lines), min(sample_size, total_lines))
            line_indices.sort()
            
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i in line_indices:
                        line = line.strip()
                        if line:
                            selected_records.append(json.loads(line))
        
        elif strategy == 'systematic':
            step = max(1, total_lines // sample_size) if total_lines > sample_size else 1
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i % step == 0 and len(selected_records) < sample_size:
                        line = line.strip()
                        if line:
                            selected_records.append(json.loads(line))
        
        else:
            # Default: first n lines
            with open(file_path, 'r', encoding='utf-8') as f:
                for i, line in enumerate(f):
                    if i >= sample_size:
                        break
                    line = line.strip()
                    if line:
                        selected_records.append(json.loads(line))
        
        # Extract columns
        all_columns = set()
        for record in selected_records:
            if isinstance(record, dict):
                all_columns.update(record.keys())
        
        columns = list(all_columns)
        
        # Convert to column-based format
        column_data = {}
        for col in columns:
            column_data[col] = []
        
        for record in selected_records:
            for col in columns:
                column_data[col].append(record.get(col))
        
        return {
            "columns": columns,
            "total_records": len(selected_records),
            "data": column_data,
            "sampling_metadata": {
                "strategy": strategy,
                "requested_size": sample_size,
                "actual_size": len(selected_records),
                "source_type": "jsonl_file",
                "file_path": file_path,
                "total_file_lines": total_lines
            }
        }
    
    async def sample_api_data(self, config: Dict[str, Any], strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from API endpoints."""
        # This is a placeholder implementation - would need actual API extraction logic
        # For now, return mock data structure
        return {
            "columns": ["id", "name", "value"],
            "total_records": sample_size,
            "data": {
                "id": list(range(sample_size)),
                "name": [f"item_{i}" for i in range(sample_size)],
                "value": [random.randint(1, 100) for _ in range(sample_size)]
            },
            "sampling_metadata": {
                "strategy": strategy,
                "requested_size": sample_size,
                "actual_size": sample_size,
                "source_type": "api",
                "url": config.get('url', ''),
                "api_type": config.get('api_type', 'rest')
            }
        }
    
    async def sample_s3_data(self, config: Dict[str, Any], strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from S3 objects."""
        if boto3 is None:
            raise ImportError("boto3 is required for S3 sampling. Install with: pip install boto3")
        
        bucket_name = config.get('bucket_name')
        prefix = config.get('prefix', '')
        file_format = config.get('file_format', 'csv').lower()
        
        if not bucket_name:
            raise ValueError("bucket_name required for S3 sampling")
        
        s3_client = boto3.client('s3')
        
        # List objects to sample from
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        objects = [obj for obj in response.get('Contents', []) if obj['Key'].endswith(f'.{file_format}')]
        
        if not objects:
            raise ValueError(f"No {file_format} files found in s3://{bucket_name}/{prefix}")
        
        # Sample from multiple files if needed
        sample_data = {"columns": [], "total_records": 0, "data": {}}
        records_per_file = max(1, sample_size // len(objects))
        
        for obj in objects[:min(len(objects), sample_size // records_per_file + 1)]:
            # Download file content
            response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            content = response['Body'].read().decode('utf-8')
            
            # Sample from this file
            if file_format == 'csv':
                file_sample = await self.sample_csv_content(content, strategy, records_per_file)
            elif file_format == 'json':
                file_sample = await self.sample_json_content(content, strategy, records_per_file)
            else:
                continue
            
            # Merge with existing sample
            if not sample_data["columns"]:
                sample_data["columns"] = file_sample["columns"]
                sample_data["data"] = {col: [] for col in file_sample["columns"]}
            
            for col in file_sample["columns"]:
                if col in sample_data["data"]:
                    sample_data["data"][col].extend(file_sample["data"][col])
            
            sample_data["total_records"] += file_sample["total_records"]
            
            if sample_data["total_records"] >= sample_size:
                break
        
        # Trim to requested sample size
        if sample_data["total_records"] > sample_size:
            for col in sample_data["data"]:
                sample_data["data"][col] = sample_data["data"][col][:sample_size]
            sample_data["total_records"] = sample_size
        
        sample_data["sampling_metadata"] = {
            "strategy": strategy,
            "requested_size": sample_size,
            "actual_size": sample_data["total_records"],
            "source_type": "s3",
            "bucket_name": bucket_name,
            "prefix": prefix,
            "file_format": file_format,
            "files_sampled": len([obj for obj in objects if sample_data["total_records"] > 0])
        }
        
        return sample_data
    
    async def sample_csv_content(self, content: str, strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from CSV content string."""
        lines = content.strip().split('\n')
        if not lines:
            return {"columns": [], "total_records": 0, "data": {}}
        
        # Parse headers
        reader = csv.reader([lines[0]])
        headers = next(reader)
        
        # Parse data rows
        data_lines = lines[1:]
        selected_rows = []
        
        if strategy == 'random':
            selected_indices = random.sample(range(len(data_lines)), min(sample_size, len(data_lines)))
            selected_rows = [list(csv.reader([data_lines[i]]))[0] for i in selected_indices]
        elif strategy == 'systematic':
            step = max(1, len(data_lines) // sample_size) if len(data_lines) > sample_size else 1
            selected_rows = [list(csv.reader([data_lines[i]]))[0] for i in range(0, len(data_lines), step)][:sample_size]
        else:
            selected_rows = [list(csv.reader([line]))[0] for line in data_lines[:sample_size]]
        
        # Convert to column format
        data = {}
        for i, header in enumerate(headers):
            data[header] = []
            for row in selected_rows:
                data[header].append(row[i] if i < len(row) else None)
        
        return {
            "columns": headers,
            "total_records": len(selected_rows),
            "data": data
        }
    
    async def sample_json_content(self, content: str, strategy: str, sample_size: int) -> Dict[str, Any]:
        """Sample data from JSON content string."""
        data = json.loads(content)
        
        if isinstance(data, list):
            records = data
        elif isinstance(data, dict) and 'data' in data:
            records = data['data']
        else:
            records = [data]
        
        # Apply sampling
        if strategy == 'random':
            selected_records = random.sample(records, min(sample_size, len(records)))
        elif strategy == 'systematic':
            step = max(1, len(records) // sample_size) if len(records) > sample_size else 1
            selected_records = [records[i] for i in range(0, len(records), step)][:sample_size]
        else:
            selected_records = records[:sample_size]
        
        # Extract columns and convert to column format
        if selected_records and isinstance(selected_records[0], dict):
            columns = list(selected_records[0].keys())
        else:
            columns = ['value']
            selected_records = [{'value': record} for record in selected_records]
        
        column_data = {}
        for col in columns:
            column_data[col] = []
        
        for record in selected_records:
            for col in columns:
                column_data[col].append(record.get(col))
        
        return {
            "columns": columns,
            "total_records": len(selected_records),
            "data": column_data
        }

    async def load_sample_data(self, data_source: str, config: ProfilingConfig) -> Dict[str, Any]:
        """Load sample data for profiling (mock implementation)."""
        # In a real implementation, this would:
        # 1. Connect to the actual data source
        # 2. Apply sampling strategy based on config
        # 3. Load data in efficient chunks
        
        # Mock data generation for demonstration
        random.seed(config.random_seed)
        
        # Generate realistic mock data
        columns = ["user_id", "email", "age", "salary", "registration_date", "is_premium", "last_login"]
        num_records = min(config.sample_size or 5000, 5000)
        
        mock_data = {
            "columns": columns,
            "total_records": num_records,
            "data": {
                "user_id": [f"user_{i:06d}" for i in range(num_records)],
                "email": [f"user{i}@{'company' if i % 10 == 0 else 'example'}.com" for i in range(num_records)],
                "age": [random.randint(18, 80) if random.random() > 0.05 else None for _ in range(num_records)],
                "salary": [random.normalvariate(75000, 25000) if random.random() > 0.03 else None for _ in range(num_records)],
                "registration_date": [f"2023-{random.randint(1,12):02d}-{random.randint(1,28):02d}" for _ in range(num_records)],
                "is_premium": [random.choice([True, False]) for _ in range(num_records)],
                "last_login": [f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if random.random() > 0.1 else None for _ in range(num_records)]
            }
        }
        
        # Add some intentional outliers for testing
        if num_records > 100:
            # Add salary outliers
            for i in range(5):
                mock_data["data"]["salary"][i] = random.choice([500000, -1000, 0])  # Outliers
            # Add age outliers  
            for i in range(5, 10):
                mock_data["data"]["age"][i] = random.choice([150, -5, 200])  # Outliers
        
        return mock_data
    
    def analyze_dataset_structure(self, sample_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze basic dataset structure and characteristics."""
        columns = sample_data.get("columns", [])
        data = sample_data.get("data", {})
        total_records = sample_data.get("total_records", 0)
        
        # Analyze data types
        type_distribution = {}
        for column in columns:
            column_data = data.get(column, [])
            inferred_type = self.infer_data_type(column_data)
            type_distribution[inferred_type] = type_distribution.get(inferred_type, 0) + 1
        
        # Calculate memory usage estimate
        estimated_memory_mb = total_records * len(columns) * 8 / (1024 * 1024)  # Rough estimate
        
        return {
            "total_records": total_records,
            "total_columns": len(columns),
            "data_type_distribution": type_distribution,
            "estimated_memory_mb": round(estimated_memory_mb, 2),
            "columns": columns,
            "has_missing_data": any(None in data.get(col, []) for col in columns),
            "sparsity": self.calculate_sparsity(data, columns, total_records)
        }
    
    def profile_column(self, column_name: str, column_data: List[Any], config: ProfilingConfig) -> ColumnProfile:
        """Create comprehensive profile for a single column."""
        # Basic statistics
        total_count = len(column_data)
        null_values = [x for x in column_data if x is None]
        non_null_data = [x for x in column_data if x is not None]
        
        null_count = len(null_values)
        null_percentage = (null_count / total_count * 100) if total_count > 0 else 0
        
        unique_values = list(set(non_null_data))
        unique_count = len(unique_values)
        unique_percentage = (unique_count / total_count * 100) if total_count > 0 else 0
        
        # Infer data type
        data_type = self.infer_data_type(non_null_data)
        
        # Initialize profile
        profile = ColumnProfile(
            name=column_name,
            data_type=data_type,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage
        )
        
        # Statistical analysis for numeric data
        if config.statistical_analysis and data_type in ['integer', 'float'] and non_null_data:
            try:
                numeric_data = [float(x) for x in non_null_data if isinstance(x, (int, float))]
                if numeric_data:
                    profile.mean = statistics.mean(numeric_data)
                    profile.median = statistics.median(numeric_data)
                    if len(numeric_data) > 1:
                        profile.std_dev = statistics.stdev(numeric_data)
                        profile.variance = statistics.variance(numeric_data)
                    profile.min_value = min(numeric_data)
                    profile.max_value = max(numeric_data)
                    
                    # Calculate percentiles
                    if len(numeric_data) >= 4:  # Need at least 4 values for meaningful percentiles
                        profile.percentiles = {}
                        for p in config.percentiles:
                            try:
                                profile.percentiles[f"p{int(p)}"] = self.calculate_percentile(numeric_data, p)
                            except Exception:
                                pass
                    
                    # Mode calculation
                    try:
                        profile.mode = statistics.mode(numeric_data)
                    except statistics.StatisticsError:
                        # No unique mode
                        pass
            except Exception as e:
                self.logger.warning(f"Statistical analysis failed for column {column_name}: {e}")
        
        # String analysis
        if data_type == 'string' and non_null_data:
            str_data = [str(x) for x in non_null_data if x is not None]
            if str_data:
                lengths = [len(s) for s in str_data]
                profile.avg_length = statistics.mean(lengths)
                profile.min_length = min(lengths)
                profile.max_length = max(lengths)
        
        # Pattern detection
        if config.pattern_detection:
            profile.detected_patterns = self.detect_patterns(non_null_data, config)
            profile.format_consistency = self.calculate_format_consistency(non_null_data, profile.detected_patterns)
        
        # Data quality scoring
        if config.data_quality_scoring:
            profile.completeness_score = max(0, (100 - null_percentage) / 100)
            profile.validity_score = self.calculate_validity_score(non_null_data, config)
            profile.consistency_score = profile.format_consistency
        
        # Anomaly detection
        if config.anomaly_detection and data_type in ['integer', 'float'] and non_null_data:
            outliers = self.detect_outliers(non_null_data, config)
            profile.outlier_count = len(outliers)
            profile.outlier_percentage = (len(outliers) / len(non_null_data) * 100) if non_null_data else 0
            profile.outlier_values = outliers[:10]  # Store first 10 outliers
        
        return profile
    
    def infer_data_type(self, data: List[Any]) -> str:
        """Infer the data type of a column."""
        if not data:
            return 'unknown'
        
        # Remove None values for type checking
        non_null_data = [x for x in data if x is not None]
        if not non_null_data:
            return 'null'
        
        # Sample first few non-null values for type inference
        sample = non_null_data[:100]
        
        # Check for boolean
        if all(isinstance(x, bool) for x in sample):
            return 'boolean'
        
        # Check for integer
        try:
            int_count = sum(1 for x in sample if isinstance(x, int) or (isinstance(x, str) and x.isdigit()))
            if int_count / len(sample) > 0.8:
                return 'integer'
        except Exception:
            pass
        
        # Check for float
        try:
            float_count = 0
            for x in sample:
                if isinstance(x, float):
                    float_count += 1
                elif isinstance(x, str):
                    try:
                        float(x)
                        float_count += 1
                    except ValueError:
                        pass
            if float_count / len(sample) > 0.8:
                return 'float'
        except Exception:
            pass
        
        # Check for date
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',  # YYYY-MM-DD
            r'^\d{2}/\d{2}/\d{4}$',  # MM/DD/YYYY
            r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO datetime
        ]
        date_count = 0
        for x in sample:
            if isinstance(x, str):
                for pattern in date_patterns:
                    if re.match(pattern, x):
                        date_count += 1
                        break
        if date_count / len(sample) > 0.8:
            return 'date'
        
        # Default to string
        return 'string'
    
    def detect_patterns(self, data: List[Any], config: ProfilingConfig) -> List[str]:
        """Detect common patterns in the data."""
        if not data:
            return []
        
        patterns = []
        str_data = [str(x) for x in data if x is not None]
        sample = str_data[:min(1000, len(str_data))]  # Sample for performance
        
        # Check against known patterns
        for pattern_name, pattern_regex in config.validity_patterns.items():
            try:
                matches = sum(1 for x in sample if re.match(pattern_regex, x))
                if matches / len(sample) > 0.7:  # 70% match threshold
                    patterns.append(pattern_name)
            except Exception:
                pass
        
        return patterns
    
    def calculate_format_consistency(self, data: List[Any], patterns: List[str]) -> float:
        """Calculate format consistency score."""
        if not data or not patterns:
            return 1.0
        
        # If we detected patterns, check how consistently they appear
        str_data = [str(x) for x in data if x is not None]
        if not str_data:
            return 1.0
        
        # For simplicity, return high consistency if patterns were detected
        return 0.9 if patterns else 0.7
    
    def calculate_validity_score(self, data: List[Any], config: ProfilingConfig) -> float:
        """Calculate validity score based on pattern matching and data constraints."""
        if not data:
            return 1.0
        
        # Basic validity checks
        str_data = [str(x) for x in data if x is not None]
        sample = str_data[:min(1000, len(str_data))]
        
        # Check for obviously invalid values
        invalid_count = 0
        for value in sample:
            # Check for common invalid patterns
            if value.lower() in ['null', 'none', 'n/a', 'na', '', 'undefined', 'nil']:
                invalid_count += 1
            elif len(value) == 0:
                invalid_count += 1
        
        validity_score = max(0, 1.0 - (invalid_count / len(sample)))
        return validity_score
    
    def detect_outliers(self, data: List[Any], config: ProfilingConfig) -> List[Any]:
        """Detect outliers using the configured method."""
        numeric_data = []
        try:
            numeric_data = [float(x) for x in data if x is not None and isinstance(x, (int, float))]
        except Exception:
            return []
        
        if len(numeric_data) < 4:  # Need minimum data for outlier detection
            return []
        
        outliers = []
        
        if config.outlier_method == "iqr":
            outliers = self.detect_outliers_iqr(numeric_data, config.outlier_threshold)
        elif config.outlier_method == "zscore":
            outliers = self.detect_outliers_zscore(numeric_data, config.zscore_threshold)
        elif config.outlier_method == "isolation_forest":
            # Simplified isolation forest - in practice would use sklearn
            outliers = self.detect_outliers_simple_isolation(numeric_data)
        
        return outliers
    
    def detect_outliers_iqr(self, data: List[float], threshold: float) -> List[float]:
        """Detect outliers using Interquartile Range method."""
        if len(data) < 4:
            return []
        
        try:
            q1 = self.calculate_percentile(data, 25)
            q3 = self.calculate_percentile(data, 75)
            iqr = q3 - q1
            
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr
            
            outliers = [x for x in data if x < lower_bound or x > upper_bound]
            return outliers
        except Exception:
            return []
    
    def detect_outliers_zscore(self, data: List[float], threshold: float) -> List[float]:
        """Detect outliers using Z-score method."""
        if len(data) < 2:
            return []
        
        try:
            mean_val = statistics.mean(data)
            std_val = statistics.stdev(data)
            
            if std_val == 0:
                return []
            
            outliers = []
            for x in data:
                z_score = abs((x - mean_val) / std_val)
                if z_score > threshold:
                    outliers.append(x)
            
            return outliers
        except Exception:
            return []
    
    def detect_outliers_simple_isolation(self, data: List[float]) -> List[float]:
        """Simple isolation forest approximation."""
        if len(data) < 10:
            return []
        
        # Simple approximation: values more than 2.5 std devs from median
        try:
            median_val = statistics.median(data)
            std_val = statistics.stdev(data)
            threshold = 2.5 * std_val
            
            outliers = [x for x in data if abs(x - median_val) > threshold]
            return outliers
        except Exception:
            return []
    
    def calculate_percentile(self, data: List[float], percentile: float) -> float:
        """Calculate percentile value."""
        if not data:
            return 0.0
        
        sorted_data = sorted(data)
        n = len(sorted_data)
        k = (percentile / 100) * (n - 1)
        
        if k == int(k):
            return sorted_data[int(k)]
        else:
            lower = sorted_data[int(k)]
            upper = sorted_data[int(k) + 1]
            fraction = k - int(k)
            return lower + fraction * (upper - lower)
    
    def calculate_sparsity(self, data: Dict[str, List], columns: List[str], total_records: int) -> float:
        """Calculate dataset sparsity (percentage of null values)."""
        if not columns or total_records == 0:
            return 0.0
        
        total_cells = len(columns) * total_records
        null_cells = 0
        
        for column in columns:
            column_data = data.get(column, [])
            null_cells += sum(1 for x in column_data if x is None)
        
        return (null_cells / total_cells * 100) if total_cells > 0 else 0.0
    
    def analyze_correlations(self, sample_data: Dict[str, Any], column_profiles: List[ColumnProfile]) -> Dict[str, Any]:
        """Analyze correlations between numeric columns."""
        numeric_columns = [cp.name for cp in column_profiles if cp.data_type in ['integer', 'float']]
        correlations = {}
        
        if len(numeric_columns) < 2:
            return correlations
        
        data = sample_data.get("data", {})
        
        # Calculate correlations between numeric columns
        for i, col1 in enumerate(numeric_columns):
            for col2 in numeric_columns[i+1:]:
                try:
                    col1_data = [float(x) for x in data.get(col1, []) if x is not None and isinstance(x, (int, float))]
                    col2_data = [float(x) for x in data.get(col2, []) if x is not None and isinstance(x, (int, float))]
                    
                    # Align data (same indices)
                    min_len = min(len(col1_data), len(col2_data))
                    if min_len > 10:  # Need sufficient data
                        correlation = self.calculate_correlation(col1_data[:min_len], col2_data[:min_len])
                        if abs(correlation) > 0.3:  # Only report significant correlations
                            correlations[f"{col1}_vs_{col2}"] = {
                                "correlation": correlation,
                                "strength": "strong" if abs(correlation) > 0.7 else "moderate",
                                "direction": "positive" if correlation > 0 else "negative"
                            }
                except Exception:
                    continue
        
        return correlations
    
    def calculate_correlation(self, x: List[float], y: List[float]) -> float:
        """Calculate Pearson correlation coefficient."""
        if len(x) != len(y) or len(x) < 2:
            return 0.0
        
        try:
            n = len(x)
            sum_x = sum(x)
            sum_y = sum(y)
            sum_x_sq = sum(xi**2 for xi in x)
            sum_y_sq = sum(yi**2 for yi in y)
            sum_xy = sum(xi * yi for xi, yi in zip(x, y))
            
            numerator = n * sum_xy - sum_x * sum_y
            denominator = ((n * sum_x_sq - sum_x**2) * (n * sum_y_sq - sum_y**2))**0.5
            
            if denominator == 0:
                return 0.0
            
            return numerator / denominator
        except Exception:
            return 0.0
    
    def calculate_overall_quality_score(self, column_profiles: List[ColumnProfile]) -> float:
        """Calculate overall data quality score."""
        if not column_profiles:
            return 1.0
        
        # Weighted average of quality dimensions
        completeness_scores = [cp.completeness_score for cp in column_profiles]
        validity_scores = [cp.validity_score for cp in column_profiles]
        consistency_scores = [cp.consistency_score for cp in column_profiles]
        
        # Calculate averages
        avg_completeness = statistics.mean(completeness_scores)
        avg_validity = statistics.mean(validity_scores)
        avg_consistency = statistics.mean(consistency_scores)
        
        # Weight the dimensions (completeness is most important)
        overall_score = (
            avg_completeness * 0.4 +
            avg_validity * 0.3 +
            avg_consistency * 0.3
        )
        
        return round(overall_score, 3)
    
    def generate_profiling_recommendations(self, column_profiles: List[ColumnProfile], overall_quality: float) -> List[str]:
        """Generate actionable recommendations based on profiling results."""
        recommendations = []
        
        # Check for data quality issues
        high_null_columns = [cp for cp in column_profiles if cp.null_percentage > 20]
        if high_null_columns:
            col_names = ", ".join([cp.name for cp in high_null_columns[:3]])
            recommendations.append(f"Address high null percentages in columns: {col_names}")
        
        # Check for outliers
        outlier_columns = [cp for cp in column_profiles if cp.outlier_percentage > 5]
        if outlier_columns:
            col_names = ", ".join([cp.name for cp in outlier_columns[:3]])
            recommendations.append(f"Investigate outliers in columns: {col_names}")
        
        # Check for low uniqueness (potential categorical columns)
        low_unique_columns = [cp for cp in column_profiles if cp.unique_percentage < 10 and cp.unique_count > 1]
        if low_unique_columns:
            col_names = ", ".join([cp.name for cp in low_unique_columns[:3]])
            recommendations.append(f"Consider indexing categorical columns: {col_names}")
        
        # Check for high cardinality
        high_unique_columns = [cp for cp in column_profiles if cp.unique_percentage > 95 and cp.data_type == 'string']
        if high_unique_columns:
            col_names = ", ".join([cp.name for cp in high_unique_columns[:3]])
            recommendations.append(f"High cardinality detected in: {col_names}. Consider normalization.")
        
        # Overall quality recommendations
        if overall_quality < 0.7:
            recommendations.append("Overall data quality is below recommended threshold. Implement data validation pipeline.")
        elif overall_quality < 0.9:
            recommendations.append("Data quality is good but has room for improvement. Focus on completeness and consistency.")
        
        # Performance recommendations
        total_columns = len(column_profiles)
        if total_columns > 50:
            recommendations.append("High column count detected. Consider dimensional modeling or column pruning.")
        
        return recommendations
    
    def column_profile_to_dict(self, profile: ColumnProfile) -> Dict[str, Any]:
        """Convert ColumnProfile to dictionary for JSON serialization."""
        return {
            "name": profile.name,
            "data_type": profile.data_type,
            "null_count": profile.null_count,
            "null_percentage": round(profile.null_percentage, 2),
            "unique_count": profile.unique_count,
            "unique_percentage": round(profile.unique_percentage, 2),
            "mean": round(profile.mean, 4) if profile.mean is not None else None,
            "median": round(profile.median, 4) if profile.median is not None else None,
            "mode": profile.mode,
            "std_dev": round(profile.std_dev, 4) if profile.std_dev is not None else None,
            "min_value": profile.min_value,
            "max_value": profile.max_value,
            "percentiles": {k: round(v, 4) for k, v in profile.percentiles.items()} if profile.percentiles else {},
            "avg_length": round(profile.avg_length, 2) if profile.avg_length is not None else None,
            "min_length": profile.min_length,
            "max_length": profile.max_length,
            "detected_patterns": profile.detected_patterns,
            "format_consistency": round(profile.format_consistency, 3),
            "completeness_score": round(profile.completeness_score, 3),
            "validity_score": round(profile.validity_score, 3),
            "consistency_score": round(profile.consistency_score, 3),
            "outlier_count": profile.outlier_count,
            "outlier_percentage": round(profile.outlier_percentage, 2),
            "outlier_values": profile.outlier_values[:5]  # Limit to first 5 for readability
        }