"""Intelligent Pipeline Auto-Detection System.

This module provides intelligent analysis and auto-detection capabilities to automatically
generate optimal ETL pipeline configurations based on data source analysis.
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from urllib.parse import urlparse

from .data_source_analysis import S3DataAnalyzer, analyze_source
from .logging_config import get_logger
from .models.pipeline_models import (
    OptimizationStrategy,
    PipelineConfig,
    PipelineType,
    TaskDefinition,
)
from .validation import ValidationError


class DataSourceType(str, Enum):
    """Data source type enumeration."""
    API = "api"
    S3 = "s3"
    DATABASE = "database"
    FILE = "file"
    STREAMING = "streaming"
    MESSAGE_QUEUE = "message_queue"
    UNKNOWN = "unknown"


class TransformationStrategy(str, Enum):
    """Transformation strategy enumeration."""
    BATCH_PROCESSING = "batch_processing"
    STREAMING_PROCESSING = "streaming_processing"
    INCREMENTAL_PROCESSING = "incremental_processing"
    AGGREGATION_FOCUSED = "aggregation_focused"
    CLEANSING_FOCUSED = "cleansing_focused"
    ENRICHMENT_FOCUSED = "enrichment_focused"
    NORMALIZATION_FOCUSED = "normalization_focused"
    REAL_TIME_PROCESSING = "real_time_processing"


class OutputDestinationType(str, Enum):
    """Output destination type enumeration."""
    DATA_WAREHOUSE = "data_warehouse"
    DATA_LAKE = "data_lake"
    DATABASE = "database"
    API = "api"
    FILE_SYSTEM = "file_system"
    STREAMING_PLATFORM = "streaming_platform"
    CACHE = "cache"
    SEARCH_ENGINE = "search_engine"


@dataclass
class DataPatterns:
    """Container for detected data patterns."""
    has_timestamps: bool = False
    has_incremental_keys: bool = False
    has_nested_structures: bool = False
    has_large_text_fields: bool = False
    has_numeric_data: bool = False
    has_categorical_data: bool = False
    has_geospatial_data: bool = False
    has_hierarchical_data: bool = False
    estimated_record_size: Optional[int] = None
    estimated_volume: Optional[str] = None  # "small", "medium", "large", "xlarge"
    data_quality_issues: List[str] = None
    schema_complexity: str = "simple"  # "simple", "moderate", "complex"
    update_frequency: str = "unknown"  # "batch", "hourly", "daily", "real_time"
    
    def __post_init__(self):
        if self.data_quality_issues is None:
            self.data_quality_issues = []


@dataclass
class PipelineRecommendation:
    """Container for pipeline recommendations."""
    recommended_type: PipelineType
    optimization_strategy: OptimizationStrategy
    transformation_strategy: TransformationStrategy
    output_destinations: List[OutputDestinationType]
    performance_optimizations: List[str]
    estimated_resources: Dict[str, Any]
    confidence_score: float
    reasoning: List[str]
    warnings: List[str] = None
    alternative_approaches: List[str] = None
    
    def __post_init__(self):
        if self.warnings is None:
            self.warnings = []
        if self.alternative_approaches is None:
            self.alternative_approaches = []


class IntelligentPipelineDetector:
    """Intelligent system for auto-detecting optimal ETL pipeline configurations."""
    
    def __init__(self):
        """Initialize the intelligent pipeline detector."""
        self.logger = get_logger("agent_etl.intelligent_detector")
        
        # Pattern detection configurations
        self.timestamp_patterns = [
            r'\b\d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}',  # ISO timestamp
            r'\btimestamp\b',
            r'\bcreated_at\b',
            r'\bupdated_at\b',
            r'\bdate\b',
            r'\btime\b'
        ]
        
        self.incremental_key_patterns = [
            r'\bid\b',
            r'\bkey\b',
            r'\bsequence\b',
            r'\bversion\b',
            r'\bmodified\b'
        ]
        
        self.geospatial_patterns = [
            r'\blat(itude)?\b',
            r'\blon(gitude)?\b',
            r'\bcoordinates?\b',
            r'\blocation\b',
            r'\baddress\b',
            r'\bzip_?code\b',
            r'\bpostal_?code\b'
        ]
        
        # Volume thresholds (in records)
        self.volume_thresholds = {
            "small": 10000,
            "medium": 1000000,
            "large": 100000000,
            "xlarge": float('inf')
        }
        
        # Resource estimation configs
        self.base_resources = {
            "cpu_cores": 2,
            "memory_gb": 4,
            "storage_gb": 10
        }
    
    def detect_data_source_type(self, source_identifier: str, metadata: Optional[Dict[str, Any]] = None) -> DataSourceType:
        """Detect the type of data source based on identifier and metadata.
        
        Args:
            source_identifier: Source identifier (URI, connection string, etc.)
            metadata: Optional metadata about the source
            
        Returns:
            Detected data source type
        """
        self.logger.info(f"Detecting data source type for: {source_identifier}")
        
        try:
            # Normalize identifier
            identifier_lower = source_identifier.lower().strip()
            
            # URL-based detection
            if identifier_lower.startswith(('http://', 'https://')):
                parsed = urlparse(source_identifier)
                
                # Check for API endpoints
                if any(api_pattern in parsed.path.lower() for api_pattern in ['/api/', '/v1/', '/v2/', '/rest/', '/graphql']):
                    return DataSourceType.API
                elif parsed.path.endswith(('.json', '.xml', '.csv')):
                    return DataSourceType.FILE
                else:
                    return DataSourceType.API  # Default for HTTP/HTTPS
            
            # S3 detection
            elif identifier_lower.startswith('s3://'):
                return DataSourceType.S3
            
            # Database connection string detection
            elif any(db_prefix in identifier_lower for db_prefix in [
                'postgresql://', 'mysql://', 'sqlite://', 'oracle://',
                'mssql://', 'mongodb://', 'redis://', 'jdbc:'
            ]):
                return DataSourceType.DATABASE
            
            # File path detection
            elif any(identifier_lower.endswith(ext) for ext in [
                '.csv', '.json', '.xml', '.parquet', '.avro', '.orc', '.txt'
            ]) or '/' in identifier_lower or '\\' in identifier_lower:
                return DataSourceType.FILE
            
            # Streaming platform detection
            elif any(streaming_pattern in identifier_lower for streaming_pattern in [
                'kafka', 'kinesis', 'pubsub', 'eventbridge', 'rabbitmq', 'activemq'
            ]):
                return DataSourceType.STREAMING
            
            # Message queue detection
            elif any(mq_pattern in identifier_lower for mq_pattern in [
                'sqs', 'sns', 'servicebus', 'queue'
            ]):
                return DataSourceType.MESSAGE_QUEUE
            
            # Use metadata for additional hints
            if metadata:
                source_type = metadata.get('source_type', '').lower()
                if source_type in [member.value for member in DataSourceType]:
                    return DataSourceType(source_type)
            
            self.logger.warning(f"Could not determine data source type for: {source_identifier}")
            return DataSourceType.UNKNOWN
            
        except Exception as e:
            self.logger.error(f"Error detecting data source type: {e}")
            return DataSourceType.UNKNOWN
    
    def analyze_data_patterns(self, source_identifier: str, metadata: Dict[str, Any]) -> DataPatterns:
        """Analyze data patterns from source metadata.
        
        Args:
            source_identifier: Source identifier
            metadata: Source metadata from analysis
            
        Returns:
            Detected data patterns
        """
        self.logger.info(f"Analyzing data patterns for: {source_identifier}")
        
        patterns = DataPatterns()
        
        try:
            # Extract fields and analyze them
            fields = metadata.get('fields', [])
            analysis_metadata = metadata.get('analysis_metadata', {})
            
            # Analyze field names and types
            field_names_text = ' '.join(str(field).lower() for field in fields)
            
            # Check for timestamps
            patterns.has_timestamps = any(
                re.search(pattern, field_names_text, re.IGNORECASE)
                for pattern in self.timestamp_patterns
            )
            
            # Check for incremental keys
            patterns.has_incremental_keys = any(
                re.search(pattern, field_names_text, re.IGNORECASE)
                for pattern in self.incremental_key_patterns
            )
            
            # Check for geospatial data
            patterns.has_geospatial_data = any(
                re.search(pattern, field_names_text, re.IGNORECASE)
                for pattern in self.geospatial_patterns
            )
            
            # Analyze data types from quality information
            data_quality = analysis_metadata.get('data_quality', {})
            data_types = data_quality.get('data_types', {})
            
            if data_types:
                type_counts = {}
                for field, dtype in data_types.items():
                    type_counts[dtype] = type_counts.get(dtype, 0) + 1
                
                patterns.has_numeric_data = type_counts.get('numeric', 0) > 0
                patterns.has_categorical_data = type_counts.get('string', 0) > 0
                patterns.has_large_text_fields = any(
                    'text' in str(dtype).lower() or 'varchar' in str(dtype).lower()
                    for dtype in data_types.values()
                )
            
            # Analyze schema complexity
            schema_info = analysis_metadata.get('schema_info', {})
            if schema_info:
                total_columns = sum(
                    len(schema.get('columns', []))
                    for schema in schema_info.values()
                    if isinstance(schema, dict)
                )
                
                if total_columns <= 10:
                    patterns.schema_complexity = "simple"
                elif total_columns <= 50:
                    patterns.schema_complexity = "moderate"
                else:
                    patterns.schema_complexity = "complex"
                
                # Check for nested structures (JSON format indicates complexity)
                patterns.has_nested_structures = any(
                    'json' in str(format_type).lower()
                    for format_type in schema_info.keys()
                )
            
            # Estimate volume
            total_files = analysis_metadata.get('total_files', 0)
            file_formats = analysis_metadata.get('file_formats', {})
            
            if total_files > 0:
                # Rough estimation based on file count and formats
                estimated_records = self._estimate_record_count(total_files, file_formats, analysis_metadata)
                patterns.estimated_volume = self._categorize_volume(estimated_records)
                
                # Estimate record size
                total_size_mb = analysis_metadata.get('total_size_mb', 0)
                if total_size_mb > 0 and estimated_records > 0:
                    patterns.estimated_record_size = int((total_size_mb * 1024 * 1024) / estimated_records)
            
            # Detect hierarchical data patterns
            if patterns.has_nested_structures or any(
                '.' in str(field) for field in fields
            ):
                patterns.has_hierarchical_data = True
            
            # Detect data quality issues
            potential_pii = data_quality.get('potential_pii', [])
            if potential_pii:
                patterns.data_quality_issues.append(f"Contains potential PII fields: {', '.join(potential_pii)}")
            
            # Estimate update frequency based on timestamps and incremental keys
            if patterns.has_timestamps and patterns.has_incremental_keys:
                patterns.update_frequency = "real_time"
            elif patterns.has_timestamps:
                patterns.update_frequency = "batch"
            else:
                patterns.update_frequency = "unknown"
            
            self.logger.info(f"Data pattern analysis complete: {patterns.schema_complexity} schema, {patterns.estimated_volume} volume")
            
        except Exception as e:
            self.logger.error(f"Error analyzing data patterns: {e}")
            patterns.data_quality_issues.append(f"Analysis error: {str(e)}")
        
        return patterns
    
    def _estimate_record_count(self, total_files: int, file_formats: Dict[str, int], analysis_metadata: Dict[str, Any]) -> int:
        """Estimate total record count based on file information."""
        # Use rough estimates per file type
        format_multipliers = {
            'csv': 1000,      # ~1K records per CSV file on average
            'json': 500,      # ~500 records per JSON file
            'parquet': 10000, # ~10K records per Parquet file (compressed)
            'avro': 5000,     # ~5K records per Avro file
            'xml': 100,       # ~100 records per XML file
            'unknown': 100    # Conservative estimate for unknown formats
        }
        
        estimated_records = 0
        for format_type, file_count in file_formats.items():
            multiplier = format_multipliers.get(format_type, format_multipliers['unknown'])
            estimated_records += file_count * multiplier
        
        return max(estimated_records, total_files * 100)  # Minimum 100 records per file
    
    def _categorize_volume(self, estimated_records: int) -> str:
        """Categorize data volume based on record count."""
        for volume_category, threshold in self.volume_thresholds.items():
            if estimated_records < threshold:
                return volume_category
        return "xlarge"
    
    def recommend_transformation_strategy(self, patterns: DataPatterns, source_type: DataSourceType) -> TransformationStrategy:
        """Recommend transformation strategy based on data patterns.
        
        Args:
            patterns: Detected data patterns
            source_type: Detected source type
            
        Returns:
            Recommended transformation strategy
        """
        self.logger.info("Analyzing transformation strategy requirements")
        
        # Real-time processing for streaming sources
        if source_type in [DataSourceType.STREAMING, DataSourceType.MESSAGE_QUEUE]:
            if patterns.update_frequency == "real_time":
                return TransformationStrategy.REAL_TIME_PROCESSING
            else:
                return TransformationStrategy.STREAMING_PROCESSING
        
        # Incremental processing for sources with incremental keys
        if patterns.has_incremental_keys and patterns.has_timestamps:
            return TransformationStrategy.INCREMENTAL_PROCESSING
        
        # Strategy based on data characteristics
        if patterns.estimated_volume in ["large", "xlarge"]:
            # For large volumes, prefer batch processing
            if patterns.has_numeric_data and patterns.schema_complexity == "simple":
                return TransformationStrategy.AGGREGATION_FOCUSED
            else:
                return TransformationStrategy.BATCH_PROCESSING
        
        # Data quality focused strategies
        if patterns.data_quality_issues:
            return TransformationStrategy.CLEANSING_FOCUSED
        
        # Enrichment for geospatial or hierarchical data
        if patterns.has_geospatial_data or patterns.has_hierarchical_data:
            return TransformationStrategy.ENRICHMENT_FOCUSED
        
        # Normalization for complex schemas
        if patterns.schema_complexity == "complex":
            return TransformationStrategy.NORMALIZATION_FOCUSED
        
        # Default to batch processing
        return TransformationStrategy.BATCH_PROCESSING
    
    def recommend_output_destinations(self, patterns: DataPatterns, transformation_strategy: TransformationStrategy) -> List[OutputDestinationType]:
        """Recommend output destinations based on patterns and transformation strategy.
        
        Args:
            patterns: Detected data patterns
            transformation_strategy: Chosen transformation strategy
            
        Returns:
            List of recommended output destinations
        """
        destinations = []
        
        # Real-time and streaming destinations
        if transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            destinations.extend([
                OutputDestinationType.STREAMING_PLATFORM,
                OutputDestinationType.CACHE
            ])
        
        # Analytics-focused destinations
        if patterns.has_numeric_data and patterns.estimated_volume in ["medium", "large", "xlarge"]:
            destinations.extend([
                OutputDestinationType.DATA_WAREHOUSE,
                OutputDestinationType.DATA_LAKE
            ])
        
        # Operational destinations
        if patterns.schema_complexity in ["simple", "moderate"]:
            destinations.append(OutputDestinationType.DATABASE)
        
        # Search and exploration destinations
        if patterns.has_large_text_fields or patterns.has_categorical_data:
            destinations.append(OutputDestinationType.SEARCH_ENGINE)
        
        # File-based destinations for archival or further processing
        if patterns.estimated_volume == "small":
            destinations.append(OutputDestinationType.FILE_SYSTEM)
        
        # API destinations for real-time access
        if transformation_strategy == TransformationStrategy.REAL_TIME_PROCESSING:
            destinations.append(OutputDestinationType.API)
        
        # Remove duplicates and ensure at least one destination
        destinations = list(dict.fromkeys(destinations))
        if not destinations:
            destinations = [OutputDestinationType.DATABASE]  # Safe default
        
        return destinations
    
    def generate_performance_optimizations(self, patterns: DataPatterns, source_type: DataSourceType, transformation_strategy: TransformationStrategy) -> List[str]:
        """Generate performance optimization recommendations.
        
        Args:
            patterns: Detected data patterns
            source_type: Source type
            transformation_strategy: Transformation strategy
            
        Returns:
            List of optimization recommendations
        """
        optimizations = []
        
        # Volume-based optimizations
        if patterns.estimated_volume in ["large", "xlarge"]:
            optimizations.extend([
                "Enable parallel processing with multiple worker threads",
                "Implement data partitioning for better I/O performance",
                "Use columnar storage formats (Parquet, ORC) for analytical workloads",
                "Consider implementing incremental processing to reduce full data scans"
            ])
        
        # Memory optimizations
        if patterns.estimated_record_size and patterns.estimated_record_size > 1024:  # Large records
            optimizations.extend([
                "Implement streaming data processing to reduce memory footprint",
                "Use data chunking for processing large records",
                "Consider data compression during processing"
            ])
        
        # Schema complexity optimizations
        if patterns.schema_complexity == "complex":
            optimizations.extend([
                "Implement schema evolution handling for future-proofing",
                "Use nested data processing techniques for hierarchical data",
                "Consider schema normalization to reduce complexity"
            ])
        
        # Source-specific optimizations
        if source_type == DataSourceType.S3:
            optimizations.extend([
                "Use S3 multipart downloads for large files",
                "Implement S3 prefix-based parallel processing",
                "Consider S3 Select for server-side filtering"
            ])
        elif source_type == DataSourceType.API:
            optimizations.extend([
                "Implement rate limiting and backoff strategies",
                "Use connection pooling for better throughput",
                "Cache frequent API responses to reduce load"
            ])
        elif source_type == DataSourceType.DATABASE:
            optimizations.extend([
                "Use connection pooling for database connections",
                "Implement query optimization and indexing strategies",
                "Consider read replicas for large read workloads"
            ])
        
        # Transformation-specific optimizations
        if transformation_strategy == TransformationStrategy.AGGREGATION_FOCUSED:
            optimizations.extend([
                "Pre-aggregate data at source when possible",
                "Use materialized views for frequent aggregations",
                "Implement incremental aggregation updates"
            ])
        elif transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            optimizations.extend([
                "Use micro-batching for balanced latency and throughput",
                "Implement event-driven processing architecture",
                "Use in-memory caching for frequently accessed data"
            ])
        
        # Data quality optimizations
        if patterns.data_quality_issues:
            optimizations.extend([
                "Implement early data validation to fail fast",
                "Use data profiling to understand quality patterns",
                "Set up data quality monitoring and alerting"
            ])
        
        # Generic performance optimizations
        optimizations.extend([
            "Monitor and optimize resource utilization (CPU, memory, I/O)",
            "Implement comprehensive error handling and retry mechanisms",
            "Use appropriate data serialization formats for your use case"
        ])
        
        return optimizations
    
    def estimate_resources(self, patterns: DataPatterns, transformation_strategy: TransformationStrategy) -> Dict[str, Any]:
        """Estimate resource requirements for the pipeline.
        
        Args:
            patterns: Detected data patterns
            transformation_strategy: Transformation strategy
            
        Returns:
            Estimated resource requirements
        """
        resources = self.base_resources.copy()
        
        # Scale based on data volume
        volume_multipliers = {
            "small": 1.0,
            "medium": 2.0,
            "large": 4.0,
            "xlarge": 8.0
        }
        
        multiplier = volume_multipliers.get(patterns.estimated_volume, 1.0)
        
        resources["cpu_cores"] = max(2, int(resources["cpu_cores"] * multiplier))
        resources["memory_gb"] = max(4, int(resources["memory_gb"] * multiplier))
        resources["storage_gb"] = max(10, int(resources["storage_gb"] * multiplier * 2))  # More storage for large data
        
        # Adjust based on transformation strategy
        if transformation_strategy == TransformationStrategy.AGGREGATION_FOCUSED:
            resources["memory_gb"] *= 2  # Aggregations need more memory
        elif transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            resources["cpu_cores"] *= 2  # Real-time needs more CPU
            resources["memory_gb"] = max(resources["memory_gb"], 8)  # Minimum memory for streaming
        
        # Adjust based on schema complexity
        if patterns.schema_complexity == "complex":
            resources["cpu_cores"] = max(resources["cpu_cores"], 4)
            resources["memory_gb"] += 2
        
        # Add additional resource estimates
        resources.update({
            "estimated_execution_time_minutes": self._estimate_execution_time(patterns, transformation_strategy),
            "recommended_worker_count": min(max(2, resources["cpu_cores"] // 2), 10),
            "estimated_cost_per_run": self._estimate_cost(resources),
            "scaling_recommendations": self._generate_scaling_recommendations(patterns, resources)
        })
        
        return resources
    
    def _estimate_execution_time(self, patterns: DataPatterns, transformation_strategy: TransformationStrategy) -> int:
        """Estimate pipeline execution time in minutes."""
        base_time = {
            "small": 5,
            "medium": 15,
            "large": 60,
            "xlarge": 240
        }
        
        time_estimate = base_time.get(patterns.estimated_volume, 15)
        
        # Adjust based on transformation strategy
        if transformation_strategy == TransformationStrategy.CLEANSING_FOCUSED:
            time_estimate *= 1.5  # Cleansing takes more time
        elif transformation_strategy == TransformationStrategy.AGGREGATION_FOCUSED:
            time_estimate *= 1.3  # Aggregations take more time
        elif transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            time_estimate = 1  # Continuous processing
        
        return int(time_estimate)
    
    def _estimate_cost(self, resources: Dict[str, Any]) -> str:
        """Estimate cost category based on resources."""
        cpu_cost = resources["cpu_cores"] * 0.10  # $0.10 per core-hour
        memory_cost = resources["memory_gb"] * 0.05  # $0.05 per GB-hour
        storage_cost = resources["storage_gb"] * 0.01  # $0.01 per GB
        
        total_hourly_cost = cpu_cost + memory_cost + storage_cost
        
        if total_hourly_cost < 1.0:
            return "low ($0.50-$1.00/hour)"
        elif total_hourly_cost < 5.0:
            return "medium ($1.00-$5.00/hour)"
        elif total_hourly_cost < 20.0:
            return "high ($5.00-$20.00/hour)"
        else:
            return "very high ($20.00+/hour)"
    
    def _generate_scaling_recommendations(self, patterns: DataPatterns, resources: Dict[str, Any]) -> List[str]:
        """Generate scaling recommendations."""
        recommendations = []
        
        if patterns.estimated_volume in ["large", "xlarge"]:
            recommendations.extend([
                "Consider horizontal scaling with multiple processing nodes",
                "Implement auto-scaling based on queue depth or processing time",
                "Use distributed processing frameworks (Spark, Dask) for very large datasets"
            ])
        
        if resources["memory_gb"] > 16:
            recommendations.append("Consider using memory-optimized instance types")
        
        if resources["cpu_cores"] > 8:
            recommendations.append("Consider using compute-optimized instance types")
        
        return recommendations
    
    def generate_pipeline_recommendation(self, source_identifier: str, metadata: Optional[Dict[str, Any]] = None) -> PipelineRecommendation:
        """Generate comprehensive pipeline recommendation.
        
        Args:
            source_identifier: Source identifier
            metadata: Optional source metadata
            
        Returns:
            Complete pipeline recommendation
        """
        start_time = time.time()
        self.logger.info(f"Generating pipeline recommendation for: {source_identifier}")
        
        try:
            # If metadata not provided, analyze the source
            if metadata is None:
                try:
                    metadata = analyze_source(source_identifier)
                except Exception as e:
                    self.logger.warning(f"Could not analyze source directly: {e}")
                    metadata = {"tables": [], "fields": [], "analysis_metadata": {}}
            
            # Step 1: Detect data source type
            source_type = self.detect_data_source_type(source_identifier, metadata)
            
            # Step 2: Analyze data patterns
            patterns = self.analyze_data_patterns(source_identifier, metadata)
            
            # Step 3: Recommend transformation strategy
            transformation_strategy = self.recommend_transformation_strategy(patterns, source_type)
            
            # Step 4: Recommend output destinations
            output_destinations = self.recommend_output_destinations(patterns, transformation_strategy)
            
            # Step 5: Generate performance optimizations
            performance_optimizations = self.generate_performance_optimizations(patterns, source_type, transformation_strategy)
            
            # Step 6: Estimate resources
            estimated_resources = self.estimate_resources(patterns, transformation_strategy)
            
            # Step 7: Determine pipeline type and optimization strategy
            pipeline_type = self._determine_pipeline_type(patterns, source_type, transformation_strategy)
            optimization_strategy = self._determine_optimization_strategy(patterns, transformation_strategy)
            
            # Step 8: Calculate confidence score
            confidence_score = self._calculate_confidence_score(source_type, patterns, metadata)
            
            # Step 9: Generate reasoning
            reasoning = self._generate_reasoning(source_type, patterns, transformation_strategy, output_destinations)
            
            # Step 10: Generate warnings and alternatives
            warnings = self._generate_warnings(patterns, source_type)
            alternatives = self._generate_alternatives(transformation_strategy, source_type)
            
            recommendation = PipelineRecommendation(
                recommended_type=pipeline_type,
                optimization_strategy=optimization_strategy,
                transformation_strategy=transformation_strategy,
                output_destinations=output_destinations,
                performance_optimizations=performance_optimizations,
                estimated_resources=estimated_resources,
                confidence_score=confidence_score,
                reasoning=reasoning,
                warnings=warnings,
                alternative_approaches=alternatives
            )
            
            analysis_time = time.time() - start_time
            self.logger.info(f"Pipeline recommendation generated in {analysis_time:.2f}s with confidence {confidence_score:.2f}")
            
            return recommendation
            
        except Exception as e:
            self.logger.error(f"Error generating pipeline recommendation: {e}")
            # Return a safe fallback recommendation
            return self._generate_fallback_recommendation(source_identifier, str(e))
    
    def _determine_pipeline_type(self, patterns: DataPatterns, source_type: DataSourceType, transformation_strategy: TransformationStrategy) -> PipelineType:
        """Determine appropriate pipeline type."""
        if transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            return PipelineType.STREAMING
        elif patterns.has_numeric_data and patterns.estimated_volume in ["medium", "large", "xlarge"]:
            return PipelineType.DATA_WAREHOUSE
        elif patterns.estimated_volume == "xlarge":
            return PipelineType.DATA_LAKE
        elif patterns.has_hierarchical_data or patterns.has_geospatial_data:
            return PipelineType.MACHINE_LEARNING
        else:
            return PipelineType.BATCH_PROCESSING
    
    def _determine_optimization_strategy(self, patterns: DataPatterns, transformation_strategy: TransformationStrategy) -> OptimizationStrategy:
        """Determine optimization strategy."""
        if transformation_strategy in [TransformationStrategy.REAL_TIME_PROCESSING, TransformationStrategy.STREAMING_PROCESSING]:
            return OptimizationStrategy.LATENCY_OPTIMIZED
        elif transformation_strategy == TransformationStrategy.AGGREGATION_FOCUSED:
            return OptimizationStrategy.ANALYTICS_OPTIMIZED
        elif patterns.estimated_volume in ["large", "xlarge"]:
            return OptimizationStrategy.THROUGHPUT_OPTIMIZED
        elif patterns.has_hierarchical_data or patterns.has_geospatial_data:
            return OptimizationStrategy.ML_OPTIMIZED
        elif patterns.estimated_volume == "small":
            return OptimizationStrategy.STORAGE_OPTIMIZED
        else:
            return OptimizationStrategy.BALANCED
    
    def _calculate_confidence_score(self, source_type: DataSourceType, patterns: DataPatterns, metadata: Dict[str, Any]) -> float:
        """Calculate confidence score for recommendations."""
        confidence = 0.5  # Base confidence
        
        # Boost confidence based on successful detections
        if source_type != DataSourceType.UNKNOWN:
            confidence += 0.2
        
        if patterns.schema_complexity != "unknown":
            confidence += 0.1
        
        if patterns.estimated_volume != "unknown":
            confidence += 0.1
        
        if patterns.update_frequency != "unknown":
            confidence += 0.1
        
        # Reduce confidence for issues
        if patterns.data_quality_issues:
            confidence -= 0.1
        
        if not metadata.get('fields'):
            confidence -= 0.2
        
        # Cap confidence between 0.1 and 1.0
        return max(0.1, min(1.0, confidence))
    
    def _generate_reasoning(self, source_type: DataSourceType, patterns: DataPatterns, transformation_strategy: TransformationStrategy, output_destinations: List[OutputDestinationType]) -> List[str]:
        """Generate reasoning for recommendations."""
        reasoning = []
        
        reasoning.append(f"Detected {source_type.value} data source with {patterns.schema_complexity} schema complexity")
        reasoning.append(f"Estimated data volume: {patterns.estimated_volume}")
        reasoning.append(f"Recommended {transformation_strategy.value} based on data characteristics")
        
        if patterns.has_timestamps:
            reasoning.append("Timestamps detected - enabling time-based processing optimizations")
        
        if patterns.has_incremental_keys:
            reasoning.append("Incremental keys found - enabling incremental processing capabilities")
        
        if patterns.has_geospatial_data:
            reasoning.append("Geospatial data detected - considering location-aware processing")
        
        if patterns.data_quality_issues:
            reasoning.append(f"Data quality issues identified - implementing cleansing strategies")
        
        reasoning.append(f"Output destinations selected based on access patterns and data characteristics")
        
        return reasoning
    
    def _generate_warnings(self, patterns: DataPatterns, source_type: DataSourceType) -> List[str]:
        """Generate warnings for potential issues."""
        warnings = []
        
        if patterns.data_quality_issues:
            warnings.append("Data quality issues detected - additional cleansing steps recommended")
        
        if patterns.estimated_volume == "xlarge":
            warnings.append("Very large dataset detected - consider distributed processing")
        
        if source_type == DataSourceType.UNKNOWN:
            warnings.append("Could not reliably detect source type - recommendations may be less accurate")
        
        if patterns.schema_complexity == "complex":
            warnings.append("Complex schema detected - additional transformation logic may be required")
        
        if patterns.has_large_text_fields:
            warnings.append("Large text fields detected - consider text processing and storage optimizations")
        
        return warnings
    
    def _generate_alternatives(self, transformation_strategy: TransformationStrategy, source_type: DataSourceType) -> List[str]:
        """Generate alternative approaches."""
        alternatives = []
        
        if transformation_strategy == TransformationStrategy.BATCH_PROCESSING:
            alternatives.extend([
                "Consider incremental processing for better performance",
                "Evaluate streaming processing for real-time requirements"
            ])
        elif transformation_strategy == TransformationStrategy.REAL_TIME_PROCESSING:
            alternatives.append("Consider micro-batching for better resource utilization")
        
        if source_type == DataSourceType.API:
            alternatives.append("Consider caching frequently accessed API data")
        elif source_type == DataSourceType.S3:
            alternatives.append("Consider using AWS Glue for managed ETL processing")
        
        alternatives.append("Evaluate using managed cloud services for reduced operational overhead")
        
        return alternatives
    
    def _generate_fallback_recommendation(self, source_identifier: str, error_message: str) -> PipelineRecommendation:
        """Generate a safe fallback recommendation when analysis fails."""
        self.logger.warning(f"Generating fallback recommendation due to error: {error_message}")
        
        return PipelineRecommendation(
            recommended_type=PipelineType.GENERAL_PURPOSE,
            optimization_strategy=OptimizationStrategy.BALANCED,
            transformation_strategy=TransformationStrategy.BATCH_PROCESSING,
            output_destinations=[OutputDestinationType.DATABASE],
            performance_optimizations=[
                "Implement basic error handling and retry mechanisms",
                "Monitor pipeline execution and performance",
                "Start with conservative resource allocation"
            ],
            estimated_resources={
                "cpu_cores": 2,
                "memory_gb": 4,
                "storage_gb": 10,
                "estimated_execution_time_minutes": 30,
                "recommended_worker_count": 2,
                "estimated_cost_per_run": "low ($0.50-$1.00/hour)",
                "scaling_recommendations": ["Monitor performance and scale as needed"]
            },
            confidence_score=0.3,
            reasoning=[
                f"Using fallback recommendations due to analysis error: {error_message}",
                "Conservative approach with basic batch processing",
                "Recommendations should be refined after manual analysis"
            ],
            warnings=[
                "Auto-detection failed - manual configuration strongly recommended",
                f"Error encountered: {error_message}",
                "These are generic recommendations and may not be optimal"
            ],
            alternative_approaches=[
                "Perform manual source analysis and configuration",
                "Use existing pipeline templates as starting points",
                "Consult with data engineering team for complex sources"
            ]
        )
    
    def create_pipeline_config(self, source_identifier: str, recommendation: PipelineRecommendation, pipeline_name: Optional[str] = None) -> PipelineConfig:
        """Create a PipelineConfig based on recommendations.
        
        Args:
            source_identifier: Source identifier
            recommendation: Pipeline recommendation
            pipeline_name: Optional custom pipeline name
            
        Returns:
            Configured PipelineConfig
        """
        if pipeline_name is None:
            pipeline_name = f"auto_detected_pipeline_{int(time.time())}"
        
        pipeline_id = f"pipeline_{pipeline_name.lower().replace(' ', '_')}"
        
        # Create basic tasks based on transformation strategy
        tasks = self._generate_tasks(source_identifier, recommendation)
        
        # Build task dependencies
        dependencies = self._build_task_dependencies(tasks)
        
        # Configure data source
        data_source = {
            "type": self.detect_data_source_type(source_identifier).value,
            "source_identifier": source_identifier,
            "configuration": {}
        }
        
        # Configure target based on recommended output destinations
        target_config = {
            "destinations": [dest.value for dest in recommendation.output_destinations],
            "primary_destination": recommendation.output_destinations[0].value if recommendation.output_destinations else "database"
        }
        
        # Create validation rules based on recommendations
        validation_rules = []
        if "data quality issues" in ' '.join(recommendation.warnings).lower():
            validation_rules.extend([
                {"type": "not_null", "fields": ["id"]},
                {"type": "data_type", "field": "id", "expected_type": "string"}
            ])
        
        return PipelineConfig(
            pipeline_id=pipeline_id,
            name=pipeline_name,
            description=f"Auto-generated pipeline for {source_identifier}",
            pipeline_type=recommendation.recommended_type,
            optimization_strategy=recommendation.optimization_strategy,
            data_source=data_source,
            target_configuration=target_config,
            tasks=tasks,
            dependencies=dependencies,
            max_parallel_tasks=recommendation.estimated_resources.get("recommended_worker_count", 5),
            timeout_minutes=recommendation.estimated_resources.get("estimated_execution_time_minutes", 30) + 10,
            resource_allocation={
                "cpu_cores": recommendation.estimated_resources.get("cpu_cores", 2),
                "memory_gb": recommendation.estimated_resources.get("memory_gb", 4),
                "storage_gb": recommendation.estimated_resources.get("storage_gb", 10)
            },
            validation_rules=validation_rules,
            monitoring_config={
                "enable_metrics": True,
                "enable_logging": True,
                "alert_on_failure": True
            },
            tags=["auto_generated", recommendation.transformation_strategy.value, recommendation.recommended_type.value]
        )
    
    def _generate_tasks(self, source_identifier: str, recommendation: PipelineRecommendation) -> List[TaskDefinition]:
        """Generate pipeline tasks based on recommendations."""
        tasks = []
        
        # Always start with extraction
        extract_task = TaskDefinition(
            task_id="extract_data",
            name="Extract Data",
            task_type="extraction",
            description=f"Extract data from {source_identifier}",
            tool_name="primary_data_extraction",
            inputs={"source": source_identifier},
            timeout_seconds=300,
            parallelizable=True,
            tags=["extraction"]
        )
        tasks.append(extract_task)
        
        # Add transformation tasks based on strategy
        if recommendation.transformation_strategy == TransformationStrategy.CLEANSING_FOCUSED:
            cleanse_task = TaskDefinition(
                task_id="cleanse_data",
                name="Cleanse Data",
                task_type="transformation",
                description="Clean and validate data quality",
                tool_name="data_cleansing",
                dependencies=["extract_data"],
                timeout_seconds=600,
                tags=["transformation", "cleansing"]
            )
            tasks.append(cleanse_task)
        
        elif recommendation.transformation_strategy == TransformationStrategy.AGGREGATION_FOCUSED:
            aggregate_task = TaskDefinition(
                task_id="aggregate_data",
                name="Aggregate Data",
                task_type="transformation",
                description="Perform data aggregations",
                tool_name="data_aggregation",
                dependencies=["extract_data"],
                timeout_seconds=900,
                tags=["transformation", "aggregation"]
            )
            tasks.append(aggregate_task)
        
        elif recommendation.transformation_strategy == TransformationStrategy.NORMALIZATION_FOCUSED:
            normalize_task = TaskDefinition(
                task_id="normalize_data",
                name="Normalize Data",
                task_type="transformation",
                description="Normalize data structure",
                tool_name="data_normalization",
                dependencies=["extract_data"],
                timeout_seconds=600,
                tags=["transformation", "normalization"]
            )
            tasks.append(normalize_task)
        
        else:
            # Generic transformation
            transform_task = TaskDefinition(
                task_id="transform_data",
                name="Transform Data",
                task_type="transformation",
                description="Apply data transformations",
                tool_name="transform_data",
                dependencies=["extract_data"],
                timeout_seconds=600,
                tags=["transformation"]
            )
            tasks.append(transform_task)
        
        # Add loading tasks for each destination
        last_transform_task = [task for task in tasks if task.task_type == "transformation"][-1]
        
        for i, destination in enumerate(recommendation.output_destinations):
            load_task = TaskDefinition(
                task_id=f"load_to_{destination.value}",
                name=f"Load to {destination.value.title()}",
                task_type="loading",
                description=f"Load data to {destination.value}",
                tool_name="load_data",
                dependencies=[last_transform_task.task_id],
                inputs={"destination_type": destination.value},
                timeout_seconds=300,
                parallelizable=True,
                tags=["loading", destination.value]
            )
            tasks.append(load_task)
        
        return tasks
    
    def _build_task_dependencies(self, tasks: List[TaskDefinition]) -> Dict[str, List[str]]:
        """Build task dependencies dictionary."""
        dependencies = {}
        
        for task in tasks:
            if task.dependencies:
                dependencies[task.task_id] = task.dependencies
        
        return dependencies