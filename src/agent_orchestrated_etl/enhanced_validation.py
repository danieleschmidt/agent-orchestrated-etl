"""Enhanced validation module with comprehensive data quality checks."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Union, Callable
from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from .logging_config import get_logger
from .exceptions import ValidationError, DataProcessingException


@dataclass
class ValidationRule:
    """Defines a validation rule for data quality checks."""
    name: str
    check_function: Callable[[Any], bool]
    error_message: str
    severity: str = "error"  # error, warning, info
    apply_to_fields: Optional[List[str]] = None


@dataclass
class DataQualityReport:
    """Report containing data quality assessment results."""
    total_records: int
    valid_records: int
    invalid_records: int
    validation_errors: List[Dict[str, Any]]
    warnings: List[Dict[str, Any]]
    quality_score: float
    field_statistics: Dict[str, Any]


class EnhancedDataValidator:
    """Enhanced data validator with comprehensive quality checks."""
    
    def __init__(self):
        self.logger = get_logger("enhanced_validator")
        self.validation_rules: List[ValidationRule] = []
        self.setup_default_rules()
    
    def setup_default_rules(self):
        """Setup default validation rules."""
        # Email validation
        self.add_rule(
            "email_format",
            lambda x: bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', str(x))),
            "Invalid email format",
            apply_to_fields=["email", "contact_email", "user_email"]
        )
        
        # Phone number validation
        self.add_rule(
            "phone_format",
            lambda x: bool(re.match(r'^\+?1?\d{9,15}$', re.sub(r'[\s\-\(\)]', '', str(x)))),
            "Invalid phone number format",
            apply_to_fields=["phone", "telephone", "mobile"]
        )
        
        # Date format validation
        self.add_rule(
            "date_format",
            self._validate_date_format,
            "Invalid date format",
            apply_to_fields=["date", "created_at", "updated_at", "timestamp"]
        )
        
        # Non-empty validation
        self.add_rule(
            "non_empty",
            lambda x: x is not None and str(x).strip() != "",
            "Field cannot be empty",
            severity="warning",
            apply_to_fields=["name", "title", "description"]
        )
        
        # Numeric range validation
        self.add_rule(
            "positive_number",
            lambda x: isinstance(x, (int, float)) and x >= 0,
            "Value must be a positive number",
            apply_to_fields=["amount", "price", "quantity", "count"]
        )
    
    def add_rule(
        self, 
        name: str, 
        check_function: Callable[[Any], bool], 
        error_message: str,
        severity: str = "error",
        apply_to_fields: Optional[List[str]] = None
    ):
        """Add a custom validation rule."""
        rule = ValidationRule(
            name=name,
            check_function=check_function,
            error_message=error_message,
            severity=severity,
            apply_to_fields=apply_to_fields
        )
        self.validation_rules.append(rule)
    
    def validate_dataset(self, data: List[Dict[str, Any]]) -> DataQualityReport:
        """Validate an entire dataset and generate quality report."""
        if not data:
            return DataQualityReport(
                total_records=0,
                valid_records=0,
                invalid_records=0,
                validation_errors=[],
                warnings=[],
                quality_score=0.0,
                field_statistics={}
            )
        
        validation_errors = []
        warnings = []
        valid_record_count = 0
        field_stats = self._calculate_field_statistics(data)
        
        for record_idx, record in enumerate(data):
            record_valid = True
            
            for field_name, field_value in record.items():
                applicable_rules = self._get_applicable_rules(field_name)
                
                for rule in applicable_rules:
                    try:
                        if not rule.check_function(field_value):
                            error_detail = {
                                "record_index": record_idx,
                                "field": field_name,
                                "value": field_value,
                                "rule": rule.name,
                                "message": rule.error_message,
                                "severity": rule.severity
                            }
                            
                            if rule.severity == "error":
                                validation_errors.append(error_detail)
                                record_valid = False
                            else:
                                warnings.append(error_detail)
                                
                    except Exception as e:
                        self.logger.error(f"Validation rule '{rule.name}' failed: {str(e)}")
                        validation_errors.append({
                            "record_index": record_idx,
                            "field": field_name,
                            "value": field_value,
                            "rule": rule.name,
                            "message": f"Validation rule error: {str(e)}",
                            "severity": "error"
                        })
                        record_valid = False
            
            if record_valid:
                valid_record_count += 1
        
        # Calculate quality score
        total_records = len(data)
        quality_score = (valid_record_count / total_records) * 100 if total_records > 0 else 0
        
        report = DataQualityReport(
            total_records=total_records,
            valid_records=valid_record_count,
            invalid_records=total_records - valid_record_count,
            validation_errors=validation_errors,
            warnings=warnings,
            quality_score=quality_score,
            field_statistics=field_stats
        )
        
        self.logger.info(f"Data quality validation completed. Score: {quality_score:.1f}%")
        return report
    
    def _get_applicable_rules(self, field_name: str) -> List[ValidationRule]:
        """Get validation rules applicable to a specific field."""
        applicable_rules = []
        
        for rule in self.validation_rules:
            if rule.apply_to_fields is None:
                # Rule applies to all fields
                applicable_rules.append(rule)
            elif field_name.lower() in [f.lower() for f in rule.apply_to_fields]:
                # Field name matches
                applicable_rules.append(rule)
            elif any(pattern in field_name.lower() for pattern in rule.apply_to_fields):
                # Partial field name match
                applicable_rules.append(rule)
        
        return applicable_rules
    
    def _validate_date_format(self, value: Any) -> bool:
        """Validate date format."""
        if value is None:
            return False
        
        date_patterns = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ"
        ]
        
        for pattern in date_patterns:
            try:
                datetime.strptime(str(value), pattern)
                return True
            except ValueError:
                continue
        
        return False
    
    def _calculate_field_statistics(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Calculate statistics for each field in the dataset."""
        if not data:
            return {}
        
        # Convert to pandas DataFrame for easier statistics
        try:
            df = pd.DataFrame(data)
            stats = {}
            
            for column in df.columns:
                col_stats = {
                    "count": len(df[column]),
                    "non_null_count": df[column].count(),
                    "null_count": df[column].isnull().sum(),
                    "null_percentage": (df[column].isnull().sum() / len(df[column])) * 100,
                    "unique_count": df[column].nunique(),
                    "data_type": str(df[column].dtype)
                }
                
                # Add numeric statistics if applicable
                if df[column].dtype in ['int64', 'float64']:
                    col_stats.update({
                        "mean": df[column].mean(),
                        "median": df[column].median(),
                        "std": df[column].std(),
                        "min": df[column].min(),
                        "max": df[column].max()
                    })
                
                # Add string statistics if applicable
                elif df[column].dtype == 'object':
                    col_stats.update({
                        "avg_length": df[column].astype(str).str.len().mean(),
                        "max_length": df[column].astype(str).str.len().max(),
                        "min_length": df[column].astype(str).str.len().min()
                    })
                
                stats[column] = col_stats
            
            return stats
            
        except Exception as e:
            self.logger.warning(f"Failed to calculate field statistics: {str(e)}")
            return {}
    
    def generate_validation_report(self, report: DataQualityReport) -> str:
        """Generate a human-readable validation report."""
        report_lines = [
            "=== DATA QUALITY VALIDATION REPORT ===",
            f"Total Records: {report.total_records}",
            f"Valid Records: {report.valid_records}",
            f"Invalid Records: {report.invalid_records}",
            f"Quality Score: {report.quality_score:.1f}%",
            ""
        ]
        
        if report.validation_errors:
            report_lines.append("=== VALIDATION ERRORS ===")
            for error in report.validation_errors[:10]:  # Show first 10 errors
                report_lines.append(
                    f"Record {error['record_index']}, Field '{error['field']}': "
                    f"{error['message']} (Value: {error['value']})"
                )
            
            if len(report.validation_errors) > 10:
                report_lines.append(f"... and {len(report.validation_errors) - 10} more errors")
            report_lines.append("")
        
        if report.warnings:
            report_lines.append("=== WARNINGS ===")
            for warning in report.warnings[:5]:  # Show first 5 warnings
                report_lines.append(
                    f"Record {warning['record_index']}, Field '{warning['field']}': "
                    f"{warning['message']}"
                )
            
            if len(report.warnings) > 5:
                report_lines.append(f"... and {len(report.warnings) - 5} more warnings")
            report_lines.append("")
        
        if report.field_statistics:
            report_lines.append("=== FIELD STATISTICS ===")
            for field, stats in report.field_statistics.items():
                report_lines.append(f"{field}: {stats['non_null_count']}/{stats['count']} non-null "
                                  f"({stats['null_percentage']:.1f}% null)")
        
        return "\n".join(report_lines)


def validate_pipeline_config(config: Dict[str, Any]) -> bool:
    """Validate pipeline configuration."""
    validator = EnhancedDataValidator()
    
    required_fields = ["source_config", "transformation_rules"]
    
    for field in required_fields:
        if field not in config:
            raise ValidationError(f"Required configuration field missing: {field}")
    
    # Validate source configuration
    source_config = config.get("source_config", {})
    if "type" not in source_config:
        raise ValidationError("Source configuration must specify 'type'")
    
    return True