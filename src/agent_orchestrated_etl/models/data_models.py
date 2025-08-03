"""Data-related models with schemas and validation."""

from __future__ import annotations

import time
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, validator


class DataSourceType(str, Enum):
    """Data source type enumeration."""
    FILE = "file"
    DATABASE = "database"
    API = "api"
    S3 = "s3"
    STREAM = "stream"
    WAREHOUSE = "warehouse"
    LAKE = "lake"


class DataFormat(str, Enum):
    """Data format enumeration."""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    XML = "xml"
    YAML = "yaml"
    EXCEL = "excel"
    SQL = "sql"


class DataType(str, Enum):
    """Data type enumeration."""
    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    TIMESTAMP = "timestamp"
    ARRAY = "array"
    OBJECT = "object"
    BINARY = "binary"


class CompressionType(str, Enum):
    """Compression type enumeration."""
    NONE = "none"
    GZIP = "gzip"
    BZIP2 = "bzip2"
    SNAPPY = "snappy"
    LZ4 = "lz4"
    ZSTD = "zstd"


class QualityDimension(str, Enum):
    """Data quality dimension enumeration."""
    COMPLETENESS = "completeness"
    ACCURACY = "accuracy"
    CONSISTENCY = "consistency"
    VALIDITY = "validity"
    UNIQUENESS = "uniqueness"
    TIMELINESS = "timeliness"
    INTEGRITY = "integrity"


class TransformationType(str, Enum):
    """Transformation type enumeration."""
    FILTER = "filter"
    MAP = "map"
    AGGREGATE = "aggregate"
    JOIN = "join"
    SORT = "sort"
    GROUP = "group"
    PIVOT = "pivot"
    UNPIVOT = "unpivot"
    NORMALIZE = "normalize"
    DENORMALIZE = "denormalize"
    CLEAN = "clean"
    VALIDATE = "validate"
    ENRICH = "enrich"


class FieldSchema(BaseModel):
    """Schema definition for a data field."""
    
    name: str = Field(..., description="Field name")
    data_type: DataType = Field(..., description="Field data type")
    description: Optional[str] = Field(None, description="Field description")
    
    # Constraints
    required: bool = Field(True, description="Whether field is required")
    nullable: bool = Field(False, description="Whether field can be null")
    unique: bool = Field(False, description="Whether field values must be unique")
    
    # Validation rules
    min_length: Optional[int] = Field(None, description="Minimum string length")
    max_length: Optional[int] = Field(None, description="Maximum string length")
    min_value: Optional[Union[int, float]] = Field(None, description="Minimum numeric value")
    max_value: Optional[Union[int, float]] = Field(None, description="Maximum numeric value")
    pattern: Optional[str] = Field(None, description="Regex pattern for validation")
    allowed_values: Optional[List[Any]] = Field(None, description="List of allowed values")
    
    # Format information
    format_string: Optional[str] = Field(None, description="Format string for dates/times")
    encoding: Optional[str] = Field(None, description="Character encoding")
    
    # Metadata
    tags: List[str] = Field(default_factory=list, description="Field tags")
    sensitive: bool = Field(False, description="Whether field contains sensitive data")
    pii: bool = Field(False, description="Whether field contains PII")
    
    @validator('name')
    def validate_name(cls, v):
        if not v or not v.strip():
            raise ValueError('Field name cannot be empty')
        return v.strip()


class DataSchema(BaseModel):
    """Schema definition for structured data."""
    
    schema_id: str = Field(..., description="Unique schema identifier")
    name: str = Field(..., description="Schema name")
    version: str = Field("1.0", description="Schema version")
    description: Optional[str] = Field(None, description="Schema description")
    
    # Field definitions
    fields: List[FieldSchema] = Field(..., description="Field schemas")
    primary_key: Optional[List[str]] = Field(None, description="Primary key field names")
    indexes: List[Dict[str, Any]] = Field(default_factory=list, description="Index definitions")
    
    # Schema properties
    strict_mode: bool = Field(True, description="Whether to enforce strict validation")
    allow_extra_fields: bool = Field(False, description="Whether to allow undefined fields")
    
    # Evolution settings
    backward_compatible: bool = Field(True, description="Whether changes are backward compatible")
    evolution_strategy: str = Field("strict", description="Schema evolution strategy")
    
    # Metadata
    created_at: float = Field(default_factory=time.time, description="Creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Schema tags")
    
    @validator('fields')
    def validate_fields(cls, v):
        if not v:
            raise ValueError('Schema must have at least one field')
        
        field_names = [field.name for field in v]
        if len(field_names) != len(set(field_names)):
            raise ValueError('Duplicate field names found in schema')
        
        return v
    
    @validator('primary_key')
    def validate_primary_key(cls, v, values):
        if v and 'fields' in values:
            field_names = {field.name for field in values['fields']}
            for key_field in v:
                if key_field not in field_names:
                    raise ValueError(f'Primary key field {key_field} not found in schema')
        return v
    
    def get_field_by_name(self, name: str) -> Optional[FieldSchema]:
        """Get field schema by name."""
        for field in self.fields:
            if field.name == name:
                return field
        return None
    
    def validate_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Validate a record against this schema."""
        validation_result = {
            "valid": True,
            "errors": [],
            "warnings": []
        }
        
        # Check required fields
        for field in self.fields:
            if field.required and field.name not in record:
                validation_result["valid"] = False
                validation_result["errors"].append(f"Required field '{field.name}' is missing")
        
        # Check field types and constraints
        for field_name, value in record.items():
            field_schema = self.get_field_by_name(field_name)
            
            if not field_schema:
                if not self.allow_extra_fields:
                    validation_result["valid"] = False
                    validation_result["errors"].append(f"Unknown field '{field_name}'")
                continue
            
            # Validate field value
            field_validation = self._validate_field_value(field_schema, value)
            if not field_validation["valid"]:
                validation_result["valid"] = False
                validation_result["errors"].extend(field_validation["errors"])
        
        return validation_result
    
    def _validate_field_value(self, field: FieldSchema, value: Any) -> Dict[str, Any]:
        """Validate a single field value."""
        result = {"valid": True, "errors": []}
        
        # Check null values
        if value is None:
            if not field.nullable:
                result["valid"] = False
                result["errors"].append(f"Field '{field.name}' cannot be null")
            return result
        
        # Type validation would go here
        # This is a simplified implementation
        return result


class DataSource(BaseModel):
    """Data source configuration and metadata."""
    
    source_id: str = Field(..., description="Unique source identifier")
    name: str = Field(..., description="Source name")
    source_type: DataSourceType = Field(..., description="Source type")
    
    # Connection configuration
    connection_config: Dict[str, Any] = Field(..., description="Connection configuration")
    authentication: Optional[Dict[str, Any]] = Field(None, description="Authentication configuration")
    
    # Data format and structure
    data_format: DataFormat = Field(..., description="Data format")
    compression: CompressionType = Field(CompressionType.NONE, description="Compression type")
    schema: Optional[DataSchema] = Field(None, description="Data schema")
    
    # Access patterns
    access_pattern: str = Field("batch", description="Access pattern (batch, streaming, etc.)")
    refresh_frequency: Optional[str] = Field(None, description="Data refresh frequency")
    partitioning: Optional[Dict[str, Any]] = Field(None, description="Partitioning configuration")
    
    # Quality and monitoring
    quality_profile: Optional[Dict[str, Any]] = Field(None, description="Data quality profile")
    monitoring_config: Dict[str, Any] = Field(default_factory=dict, description="Monitoring configuration")
    
    # Metadata
    description: Optional[str] = Field(None, description="Source description")
    owner: Optional[str] = Field(None, description="Data owner")
    steward: Optional[str] = Field(None, description="Data steward")
    tags: List[str] = Field(default_factory=list, description="Source tags")
    
    # Operational information
    last_updated: Optional[float] = Field(None, description="Last update timestamp")
    data_size_estimate: Optional[str] = Field(None, description="Estimated data size")
    record_count_estimate: Optional[int] = Field(None, description="Estimated record count")
    
    # Security and compliance
    sensitivity_level: str = Field("public", description="Data sensitivity level")
    compliance_tags: List[str] = Field(default_factory=list, description="Compliance requirements")
    retention_policy: Optional[Dict[str, Any]] = Field(None, description="Data retention policy")
    
    def get_connection_string(self) -> str:
        """Generate connection string based on source type and config."""
        if self.source_type == DataSourceType.DATABASE:
            return self._build_database_connection_string()
        elif self.source_type == DataSourceType.S3:
            return self._build_s3_connection_string()
        elif self.source_type == DataSourceType.API:
            return self._build_api_connection_string()
        else:
            return self.connection_config.get("url", "")
    
    def _build_database_connection_string(self) -> str:
        """Build database connection string."""
        config = self.connection_config
        driver = config.get("driver", "postgresql")
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        database = config.get("database", "")
        
        return f"{driver}://{host}:{port}/{database}"
    
    def _build_s3_connection_string(self) -> str:
        """Build S3 connection string."""
        config = self.connection_config
        bucket = config.get("bucket", "")
        prefix = config.get("prefix", "")
        
        return f"s3://{bucket}/{prefix}".rstrip("/")
    
    def _build_api_connection_string(self) -> str:
        """Build API connection string."""
        return self.connection_config.get("base_url", "")


class TransformationRule(BaseModel):
    """Rule for data transformation."""
    
    rule_id: str = Field(..., description="Unique rule identifier")
    name: str = Field(..., description="Rule name")
    transformation_type: TransformationType = Field(..., description="Type of transformation")
    description: Optional[str] = Field(None, description="Rule description")
    
    # Rule configuration
    source_fields: List[str] = Field(default_factory=list, description="Source field names")
    target_field: Optional[str] = Field(None, description="Target field name")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Transformation parameters")
    
    # Conditions
    conditions: List[Dict[str, Any]] = Field(default_factory=list, description="Application conditions")
    apply_when: Optional[str] = Field(None, description="When to apply the rule")
    
    # Execution configuration
    order: int = Field(100, description="Execution order")
    enabled: bool = Field(True, description="Whether rule is enabled")
    continue_on_error: bool = Field(False, description="Whether to continue on error")
    
    # Quality impact
    affects_quality: bool = Field(True, description="Whether rule affects data quality")
    quality_impact: Optional[str] = Field(None, description="Quality impact description")
    
    # Metadata
    created_at: float = Field(default_factory=time.time, description="Creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Rule tags")


class ValidationRule(BaseModel):
    """Rule for data validation."""
    
    rule_id: str = Field(..., description="Unique rule identifier")
    name: str = Field(..., description="Rule name")
    quality_dimension: QualityDimension = Field(..., description="Quality dimension")
    description: Optional[str] = Field(None, description="Rule description")
    
    # Rule definition
    field_name: Optional[str] = Field(None, description="Target field name")
    rule_expression: str = Field(..., description="Validation expression")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Rule parameters")
    
    # Thresholds and severity
    error_threshold: Optional[float] = Field(None, description="Error threshold (0-1)")
    warning_threshold: Optional[float] = Field(None, description="Warning threshold (0-1)")
    severity: str = Field("medium", description="Rule severity")
    
    # Execution configuration
    enabled: bool = Field(True, description="Whether rule is enabled")
    fail_pipeline_on_error: bool = Field(False, description="Whether to fail pipeline on error")
    
    # Metadata
    created_at: float = Field(default_factory=time.time, description="Creation timestamp")
    updated_at: Optional[float] = Field(None, description="Last update timestamp")
    tags: List[str] = Field(default_factory=list, description="Rule tags")


class DataQualityMetrics(BaseModel):
    """Data quality metrics and scores."""
    
    metrics_id: str = Field(..., description="Unique metrics identifier")
    data_source_id: str = Field(..., description="Associated data source ID")
    measurement_timestamp: float = Field(default_factory=time.time, description="Measurement timestamp")
    
    # Overall scores
    overall_quality_score: float = Field(..., description="Overall quality score (0-1)")
    dimension_scores: Dict[QualityDimension, float] = Field(..., description="Dimension-specific scores")
    
    # Detailed metrics
    completeness_metrics: Dict[str, Any] = Field(default_factory=dict, description="Completeness metrics")
    accuracy_metrics: Dict[str, Any] = Field(default_factory=dict, description="Accuracy metrics")
    consistency_metrics: Dict[str, Any] = Field(default_factory=dict, description="Consistency metrics")
    validity_metrics: Dict[str, Any] = Field(default_factory=dict, description="Validity metrics")
    uniqueness_metrics: Dict[str, Any] = Field(default_factory=dict, description="Uniqueness metrics")
    timeliness_metrics: Dict[str, Any] = Field(default_factory=dict, description="Timeliness metrics")
    integrity_metrics: Dict[str, Any] = Field(default_factory=dict, description="Integrity metrics")
    
    # Data profiling results
    record_count: int = Field(0, description="Total record count")
    field_profiles: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Field-level profiles")
    
    # Issues and anomalies
    critical_issues: List[Dict[str, Any]] = Field(default_factory=list, description="Critical quality issues")
    warnings: List[Dict[str, Any]] = Field(default_factory=list, description="Quality warnings")
    anomalies: List[Dict[str, Any]] = Field(default_factory=list, description="Detected anomalies")
    
    # Trends and history
    trend_direction: Optional[str] = Field(None, description="Quality trend direction")
    previous_score: Optional[float] = Field(None, description="Previous quality score")
    score_change: Optional[float] = Field(None, description="Score change from previous measurement")
    
    # Recommendations
    improvement_recommendations: List[str] = Field(default_factory=list, description="Quality improvement recommendations")
    
    def calculate_dimension_score(self, dimension: QualityDimension) -> float:
        """Calculate score for a specific quality dimension."""
        if dimension in self.dimension_scores:
            return self.dimension_scores[dimension]
        
        # Fallback calculation based on available metrics
        if dimension == QualityDimension.COMPLETENESS:
            return self._calculate_completeness_score()
        elif dimension == QualityDimension.ACCURACY:
            return self._calculate_accuracy_score()
        elif dimension == QualityDimension.CONSISTENCY:
            return self._calculate_consistency_score()
        elif dimension == QualityDimension.VALIDITY:
            return self._calculate_validity_score()
        elif dimension == QualityDimension.UNIQUENESS:
            return self._calculate_uniqueness_score()
        else:
            return 0.5  # Default neutral score
    
    def _calculate_completeness_score(self) -> float:
        """Calculate completeness score from metrics."""
        metrics = self.completeness_metrics
        if not metrics:
            return 0.5
        
        total_fields = metrics.get("total_fields", 1)
        complete_fields = metrics.get("complete_fields", 0)
        
        return complete_fields / total_fields if total_fields > 0 else 0.0
    
    def _calculate_accuracy_score(self) -> float:
        """Calculate accuracy score from metrics."""
        metrics = self.accuracy_metrics
        if not metrics:
            return 0.5
        
        total_records = metrics.get("total_records", 1)
        accurate_records = metrics.get("accurate_records", 0)
        
        return accurate_records / total_records if total_records > 0 else 0.0
    
    def _calculate_consistency_score(self) -> float:
        """Calculate consistency score from metrics."""
        metrics = self.consistency_metrics
        if not metrics:
            return 0.5
        
        total_checks = metrics.get("total_checks", 1)
        consistent_checks = metrics.get("consistent_checks", 0)
        
        return consistent_checks / total_checks if total_checks > 0 else 0.0
    
    def _calculate_validity_score(self) -> float:
        """Calculate validity score from metrics."""
        metrics = self.validity_metrics
        if not metrics:
            return 0.5
        
        total_values = metrics.get("total_values", 1)
        valid_values = metrics.get("valid_values", 0)
        
        return valid_values / total_values if total_values > 0 else 0.0
    
    def _calculate_uniqueness_score(self) -> float:
        """Calculate uniqueness score from metrics."""
        metrics = self.uniqueness_metrics
        if not metrics:
            return 0.5
        
        total_values = metrics.get("total_values", 1)
        unique_values = metrics.get("unique_values", 0)
        
        return unique_values / total_values if total_values > 0 else 0.0
    
    def get_quality_grade(self) -> str:
        """Get letter grade based on overall quality score."""
        score = self.overall_quality_score
        
        if score >= 0.9:
            return "A"
        elif score >= 0.8:
            return "B"
        elif score >= 0.7:
            return "C"
        elif score >= 0.6:
            return "D"
        else:
            return "F"
    
    def is_acceptable_quality(self, threshold: float = 0.7) -> bool:
        """Check if quality meets acceptable threshold."""
        return self.overall_quality_score >= threshold