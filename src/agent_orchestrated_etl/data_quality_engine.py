"""Intelligent data quality engine with ML-powered anomaly detection."""

from __future__ import annotations

import json
import statistics
import time
from typing import Dict, Any, List, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import re
from datetime import datetime, timedelta

from .logging_config import get_logger
from .exceptions import DataValidationException, DataCorruptionException


class QualityLevel(Enum):
    """Data quality assessment levels."""
    EXCELLENT = "excellent"  # 95-100%
    GOOD = "good"           # 85-94%
    ACCEPTABLE = "acceptable"  # 70-84%
    POOR = "poor"           # 50-69%
    CRITICAL = "critical"   # 0-49%


class AnomalyType(Enum):
    """Types of data anomalies."""
    OUTLIER = "outlier"
    PATTERN_CHANGE = "pattern_change"
    MISSING_DATA = "missing_data"
    DUPLICATE = "duplicate"
    FORMAT_VIOLATION = "format_violation"
    RANGE_VIOLATION = "range_violation"
    CORRELATION_ANOMALY = "correlation_anomaly"


@dataclass
class QualityRule:
    """Data quality rule configuration."""
    name: str
    field_name: Optional[str] = None
    rule_type: str = "custom"
    parameters: Dict[str, Any] = field(default_factory=dict)
    severity: str = "warning"  # warning, error, critical
    enabled: bool = True
    description: str = ""


@dataclass
class QualityIssue:
    """Data quality issue detected."""
    rule_name: str
    field_name: Optional[str]
    issue_type: str
    severity: str
    message: str
    record_index: Optional[int] = None
    record_count: int = 1
    suggestions: List[str] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)


@dataclass
class DataProfile:
    """Statistical profile of data."""
    field_name: str
    data_type: str
    total_count: int = 0
    null_count: int = 0
    unique_count: int = 0
    min_value: Optional[Union[int, float, str]] = None
    max_value: Optional[Union[int, float, str]] = None
    mean_value: Optional[float] = None
    median_value: Optional[float] = None
    std_deviation: Optional[float] = None
    common_patterns: List[str] = field(default_factory=list)
    sample_values: List[Any] = field(default_factory=list)


class IntelligentDataQualityEngine:
    """Advanced data quality engine with ML-powered analysis."""
    
    def __init__(self):
        self.logger = get_logger("agent_etl.data_quality")
        self.quality_rules: Dict[str, QualityRule] = {}
        self.data_profiles: Dict[str, DataProfile] = {}
        self.anomaly_baselines: Dict[str, Dict[str, Any]] = {}
        self.quality_history: List[Dict[str, Any]] = []
        
        # Initialize built-in quality rules
        self._initialize_builtin_rules()
    
    def _initialize_builtin_rules(self):
        """Initialize built-in data quality rules."""
        # Completeness rules
        self.add_quality_rule(QualityRule(
            name="completeness_check",
            rule_type="completeness",
            parameters={"min_completeness": 0.9},
            severity="warning",
            description="Check if data fields have sufficient completeness"
        ))
        
        # Uniqueness rules
        self.add_quality_rule(QualityRule(
            name="uniqueness_check",
            rule_type="uniqueness",
            parameters={"expected_unique_fields": ["id", "email", "ssn"]},
            severity="error",
            description="Check for duplicate values in unique fields"
        ))
        
        # Format validation rules
        self.add_quality_rule(QualityRule(
            name="email_format",
            field_name="email",
            rule_type="format",
            parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
            severity="error",
            description="Validate email address format"
        ))
        
        self.add_quality_rule(QualityRule(
            name="phone_format",
            field_name="phone",
            rule_type="format",
            parameters={"pattern": r"^\+?[\d\s\-\(\)]{10,}$"},
            severity="warning",
            description="Validate phone number format"
        ))
        
        # Range validation rules
        self.add_quality_rule(QualityRule(
            name="age_range",
            field_name="age",
            rule_type="range",
            parameters={"min_value": 0, "max_value": 150},
            severity="error",
            description="Validate age is within reasonable range"
        ))
        
        # Data type consistency rules
        self.add_quality_rule(QualityRule(
            name="data_type_consistency",
            rule_type="consistency",
            parameters={},
            severity="warning",
            description="Check for consistent data types across records"
        ))
        
        # Anomaly detection rules
        self.add_quality_rule(QualityRule(
            name="statistical_outliers",
            rule_type="anomaly",
            parameters={"std_threshold": 3.0, "min_samples": 10},
            severity="warning",
            description="Detect statistical outliers using z-score"
        ))
    
    def add_quality_rule(self, rule: QualityRule):
        """Add a custom quality rule."""
        self.quality_rules[rule.name] = rule
        self.logger.info(f"Added quality rule: {rule.name}")
    
    def remove_quality_rule(self, rule_name: str):
        """Remove a quality rule."""
        if rule_name in self.quality_rules:
            del self.quality_rules[rule_name]
            self.logger.info(f"Removed quality rule: {rule_name}")
    
    def analyze_data_quality(
        self,
        data: List[Dict[str, Any]],
        create_profile: bool = True,
        detect_anomalies: bool = True
    ) -> Dict[str, Any]:
        """Comprehensive data quality analysis."""
        start_time = time.time()
        
        if not data:
            return {
                "status": "error",
                "message": "No data provided for analysis",
                "quality_score": 0.0,
                "issues": []
            }
        
        self.logger.info(f"Starting data quality analysis for {len(data)} records")
        
        issues = []
        profiles = {}
        
        try:
            # Create data profiles
            if create_profile:
                profiles = self._create_data_profiles(data)
                self.data_profiles.update(profiles)
            
            # Apply quality rules
            for rule_name, rule in self.quality_rules.items():
                if not rule.enabled:
                    continue
                
                rule_issues = self._apply_quality_rule(rule, data, profiles)
                issues.extend(rule_issues)
            
            # Detect anomalies using ML techniques
            if detect_anomalies:
                anomaly_issues = self._detect_anomalies(data, profiles)
                issues.extend(anomaly_issues)
            
            # Calculate overall quality score
            quality_score = self._calculate_quality_score(data, issues)
            quality_level = self._determine_quality_level(quality_score)
            
            # Generate improvement suggestions
            suggestions = self._generate_improvement_suggestions(issues, profiles)
            
            analysis_time = time.time() - start_time
            
            result = {
                "status": "completed",
                "timestamp": time.time(),
                "analysis_time_seconds": analysis_time,
                "record_count": len(data),
                "quality_score": quality_score,
                "quality_level": quality_level.value,
                "issues_found": len(issues),
                "issues": [self._issue_to_dict(issue) for issue in issues],
                "data_profiles": {name: self._profile_to_dict(profile) 
                                for name, profile in profiles.items()},
                "improvement_suggestions": suggestions,
                "quality_breakdown": self._analyze_quality_breakdown(issues)
            }
            
            # Store in quality history
            self.quality_history.append({
                "timestamp": time.time(),
                "quality_score": quality_score,
                "quality_level": quality_level.value,
                "issues_count": len(issues),
                "record_count": len(data)
            })
            
            # Clean up old history
            if len(self.quality_history) > 100:
                self.quality_history = self.quality_history[-100:]
            
            self.logger.info(
                f"Data quality analysis completed",
                extra={
                    "quality_score": quality_score,
                    "quality_level": quality_level.value,
                    "issues_found": len(issues),
                    "analysis_time": analysis_time
                }
            )
            
            return result
            
        except Exception as e:
            self.logger.error(f"Data quality analysis failed: {e}", exc_info=True)
            return {
                "status": "error",
                "message": f"Analysis failed: {str(e)}",
                "quality_score": 0.0,
                "issues": []
            }
    
    def _create_data_profiles(self, data: List[Dict[str, Any]]) -> Dict[str, DataProfile]:
        """Create statistical profiles for each field in the data."""
        profiles = {}
        
        # Get all unique field names
        all_fields = set()
        for record in data:
            all_fields.update(record.keys())
        
        for field_name in all_fields:
            profile = self._profile_field(field_name, data)
            profiles[field_name] = profile
        
        return profiles
    
    def _profile_field(self, field_name: str, data: List[Dict[str, Any]]) -> DataProfile:
        """Create a statistical profile for a single field."""
        values = []
        null_count = 0
        
        for record in data:
            value = record.get(field_name)
            if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
                null_count += 1
            else:
                values.append(value)
        
        if not values:
            return DataProfile(
                field_name=field_name,
                data_type="unknown",
                total_count=len(data),
                null_count=null_count
            )
        
        # Determine data type
        data_type = self._infer_data_type(values)
        
        # Calculate statistics
        profile = DataProfile(
            field_name=field_name,
            data_type=data_type,
            total_count=len(data),
            null_count=null_count,
            unique_count=len(set(str(v) for v in values))
        )
        
        # Type-specific statistics
        if data_type in ["integer", "float"]:
            numeric_values = [float(v) for v in values if self._is_numeric(v)]
            if numeric_values:
                profile.min_value = min(numeric_values)
                profile.max_value = max(numeric_values)
                profile.mean_value = statistics.mean(numeric_values)
                profile.median_value = statistics.median(numeric_values)
                if len(numeric_values) > 1:
                    profile.std_deviation = statistics.stdev(numeric_values)
        
        elif data_type == "string":
            str_values = [str(v) for v in values]
            profile.min_value = min(str_values, key=len)
            profile.max_value = max(str_values, key=len)
            profile.common_patterns = self._extract_patterns(str_values)
        
        # Sample values (up to 5)
        profile.sample_values = values[:5]
        
        return profile
    
    def _infer_data_type(self, values: List[Any]) -> str:
        """Infer the primary data type of a list of values."""
        if not values:
            return "unknown"
        
        type_counts = {}
        
        for value in values[:100]:  # Check first 100 values for efficiency
            if isinstance(value, int):
                type_counts["integer"] = type_counts.get("integer", 0) + 1
            elif isinstance(value, float):
                type_counts["float"] = type_counts.get("float", 0) + 1
            elif isinstance(value, bool):
                type_counts["boolean"] = type_counts.get("boolean", 0) + 1
            elif isinstance(value, str):
                if self._is_numeric(value):
                    if '.' in value:
                        type_counts["float"] = type_counts.get("float", 0) + 1
                    else:
                        type_counts["integer"] = type_counts.get("integer", 0) + 1
                elif self._is_date(value):
                    type_counts["date"] = type_counts.get("date", 0) + 1
                else:
                    type_counts["string"] = type_counts.get("string", 0) + 1
            else:
                type_counts["unknown"] = type_counts.get("unknown", 0) + 1
        
        # Return the most common type
        if type_counts:
            return max(type_counts, key=type_counts.get)
        return "unknown"
    
    def _is_numeric(self, value: Any) -> bool:
        """Check if a value is numeric."""
        try:
            float(value)
            return True
        except (ValueError, TypeError):
            return False
    
    def _is_date(self, value: str) -> bool:
        """Check if a string value represents a date."""
        date_patterns = [
            r'^\d{4}-\d{2}-\d{2}$',
            r'^\d{2}/\d{2}/\d{4}$',
            r'^\d{2}-\d{2}-\d{4}$'
        ]
        
        for pattern in date_patterns:
            if re.match(pattern, str(value)):
                return True
        return False
    
    def _extract_patterns(self, str_values: List[str]) -> List[str]:
        """Extract common patterns from string values."""
        patterns = {}
        
        for value in str_values[:50]:  # Analyze first 50 for patterns
            # Replace digits with 'D', letters with 'A', special chars with 'S'
            pattern = re.sub(r'\d', 'D', value)
            pattern = re.sub(r'[a-zA-Z]', 'A', pattern)
            pattern = re.sub(r'[^DA]', 'S', pattern)
            
            patterns[pattern] = patterns.get(pattern, 0) + 1
        
        # Return top 3 patterns
        sorted_patterns = sorted(patterns.items(), key=lambda x: x[1], reverse=True)
        return [pattern for pattern, count in sorted_patterns[:3]]
    
    def _apply_quality_rule(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Apply a single quality rule to the data."""
        issues = []
        
        try:
            if rule.rule_type == "completeness":
                issues.extend(self._check_completeness(rule, data, profiles))
            elif rule.rule_type == "uniqueness":
                issues.extend(self._check_uniqueness(rule, data))
            elif rule.rule_type == "format":
                issues.extend(self._check_format(rule, data))
            elif rule.rule_type == "range":
                issues.extend(self._check_range(rule, data))
            elif rule.rule_type == "consistency":
                issues.extend(self._check_consistency(rule, data, profiles))
            elif rule.rule_type == "anomaly":
                issues.extend(self._check_statistical_anomalies(rule, data, profiles))
            
        except Exception as e:
            self.logger.error(f"Error applying rule {rule.name}: {e}")
            issues.append(QualityIssue(
                rule_name=rule.name,
                field_name=rule.field_name,
                issue_type="rule_error",
                severity="warning",
                message=f"Failed to apply rule: {str(e)}"
            ))
        
        return issues
    
    def _check_completeness(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Check completeness of data fields."""
        issues = []
        min_completeness = rule.parameters.get("min_completeness", 0.9)
        
        for field_name, profile in profiles.items():
            if profile.total_count == 0:
                continue
            
            completeness = 1 - (profile.null_count / profile.total_count)
            
            if completeness < min_completeness:
                issues.append(QualityIssue(
                    rule_name=rule.name,
                    field_name=field_name,
                    issue_type="completeness",
                    severity=rule.severity,
                    message=f"Field '{field_name}' has low completeness: {completeness:.1%} (expected: {min_completeness:.1%})",
                    record_count=profile.null_count,
                    suggestions=[
                        f"Investigate source of missing values in '{field_name}'",
                        "Consider data imputation strategies",
                        "Update data collection process"
                    ]
                ))
        
        return issues
    
    def _check_uniqueness(self, rule: QualityRule, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check for duplicate values in fields that should be unique."""
        issues = []
        unique_fields = rule.parameters.get("expected_unique_fields", [])
        
        for field_name in unique_fields:
            values = []
            for i, record in enumerate(data):
                value = record.get(field_name)
                if value is not None:
                    values.append((value, i))
            
            # Find duplicates
            seen = set()
            duplicates = []
            for value, index in values:
                if value in seen:
                    duplicates.append((value, index))
                else:
                    seen.add(value)
            
            if duplicates:
                issues.append(QualityIssue(
                    rule_name=rule.name,
                    field_name=field_name,
                    issue_type="uniqueness",
                    severity=rule.severity,
                    message=f"Field '{field_name}' has {len(duplicates)} duplicate values",
                    record_count=len(duplicates),
                    suggestions=[
                        f"Remove duplicate records with same '{field_name}' value",
                        "Investigate data source for duplicate generation",
                        "Add unique constraints to prevent future duplicates"
                    ]
                ))
        
        return issues
    
    def _check_format(self, rule: QualityRule, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check format validation using regex patterns."""
        issues = []
        
        if not rule.field_name:
            return issues
        
        pattern = rule.parameters.get("pattern", "")
        if not pattern:
            return issues
        
        invalid_records = []
        
        for i, record in enumerate(data):
            value = record.get(rule.field_name)
            if value is not None and not re.match(pattern, str(value)):
                invalid_records.append(i)
        
        if invalid_records:
            issues.append(QualityIssue(
                rule_name=rule.name,
                field_name=rule.field_name,
                issue_type="format",
                severity=rule.severity,
                message=f"Field '{rule.field_name}' has {len(invalid_records)} records with invalid format",
                record_count=len(invalid_records),
                suggestions=[
                    f"Standardize format for '{rule.field_name}' field",
                    "Add format validation at data entry point",
                    "Clean and reformat existing invalid records"
                ]
            ))
        
        return issues
    
    def _check_range(self, rule: QualityRule, data: List[Dict[str, Any]]) -> List[QualityIssue]:
        """Check if numeric values are within expected ranges."""
        issues = []
        
        if not rule.field_name:
            return issues
        
        min_value = rule.parameters.get("min_value")
        max_value = rule.parameters.get("max_value")
        
        if min_value is None and max_value is None:
            return issues
        
        out_of_range = []
        
        for i, record in enumerate(data):
            value = record.get(rule.field_name)
            if value is not None and self._is_numeric(value):
                numeric_value = float(value)
                
                if min_value is not None and numeric_value < min_value:
                    out_of_range.append(i)
                elif max_value is not None and numeric_value > max_value:
                    out_of_range.append(i)
        
        if out_of_range:
            issues.append(QualityIssue(
                rule_name=rule.name,
                field_name=rule.field_name,
                issue_type="range",
                severity=rule.severity,
                message=f"Field '{rule.field_name}' has {len(out_of_range)} values outside expected range",
                record_count=len(out_of_range),
                suggestions=[
                    f"Review out-of-range values in '{rule.field_name}'",
                    "Investigate data collection or processing errors",
                    "Consider if range constraints need adjustment"
                ]
            ))
        
        return issues
    
    def _check_consistency(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Check for data type consistency across records."""
        issues = []
        
        for field_name, profile in profiles.items():
            # Check if field has mixed data types
            type_variations = self._check_type_consistency(field_name, data)
            
            if len(type_variations) > 1:
                issues.append(QualityIssue(
                    rule_name=rule.name,
                    field_name=field_name,
                    issue_type="consistency",
                    severity=rule.severity,
                    message=f"Field '{field_name}' has inconsistent data types: {list(type_variations.keys())}",
                    record_count=sum(type_variations.values()),
                    suggestions=[
                        f"Standardize data type for '{field_name}' field",
                        "Convert values to consistent format",
                        "Add type validation in data processing"
                    ]
                ))
        
        return issues
    
    def _check_type_consistency(self, field_name: str, data: List[Dict[str, Any]]) -> Dict[str, int]:
        """Check type consistency for a field across all records."""
        type_counts = {}
        
        for record in data:
            value = record.get(field_name)
            if value is not None:
                value_type = type(value).__name__
                if isinstance(value, str) and self._is_numeric(value):
                    value_type = "numeric_string"
                
                type_counts[value_type] = type_counts.get(value_type, 0) + 1
        
        return type_counts
    
    def _check_statistical_anomalies(
        self,
        rule: QualityRule,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Detect statistical outliers using z-score method."""
        issues = []
        std_threshold = rule.parameters.get("std_threshold", 3.0)
        min_samples = rule.parameters.get("min_samples", 10)
        
        for field_name, profile in profiles.items():
            if profile.data_type not in ["integer", "float"] or profile.std_deviation is None:
                continue
            
            if profile.unique_count < min_samples:
                continue
            
            # Get numeric values for this field
            numeric_values = []
            for record in data:
                value = record.get(field_name)
                if value is not None and self._is_numeric(value):
                    numeric_values.append(float(value))
            
            if len(numeric_values) < min_samples:
                continue
            
            # Calculate z-scores and find outliers
            mean_val = statistics.mean(numeric_values)
            std_val = statistics.stdev(numeric_values)
            
            if std_val == 0:  # No variation
                continue
            
            outliers = []
            for value in numeric_values:
                z_score = abs((value - mean_val) / std_val)
                if z_score > std_threshold:
                    outliers.append(value)
            
            if outliers:
                issues.append(QualityIssue(
                    rule_name=rule.name,
                    field_name=field_name,
                    issue_type="outlier",
                    severity=rule.severity,
                    message=f"Field '{field_name}' has {len(outliers)} statistical outliers (z-score > {std_threshold})",
                    record_count=len(outliers),
                    suggestions=[
                        f"Review outlier values in '{field_name}': {outliers[:5]}",
                        "Investigate if outliers are data errors or valid extreme values",
                        "Consider data transformation or outlier handling strategy"
                    ]
                ))
        
        return issues
    
    def _detect_anomalies(
        self,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Advanced anomaly detection using ML techniques."""
        issues = []
        
        # This would implement more sophisticated anomaly detection
        # For now, we'll implement a simple correlation-based check
        issues.extend(self._detect_correlation_anomalies(data, profiles))
        
        return issues
    
    def _detect_correlation_anomalies(
        self,
        data: List[Dict[str, Any]],
        profiles: Dict[str, DataProfile]
    ) -> List[QualityIssue]:
        """Detect anomalies based on expected correlations between fields."""
        issues = []
        
        # Simple example: detect records where age and salary don't correlate reasonably
        age_field = None
        salary_field = None
        
        for field_name, profile in profiles.items():
            if 'age' in field_name.lower() and profile.data_type in ['integer', 'float']:
                age_field = field_name
            elif 'salary' in field_name.lower() and profile.data_type in ['integer', 'float']:
                salary_field = field_name
        
        if age_field and salary_field:
            anomalous_records = []
            
            for i, record in enumerate(data):
                age = record.get(age_field)
                salary = record.get(salary_field)
                
                if age and salary and self._is_numeric(age) and self._is_numeric(salary):
                    age_val = float(age)
                    salary_val = float(salary)
                    
                    # Simple heuristic: very young people with very high salaries
                    if age_val < 25 and salary_val > 200000:
                        anomalous_records.append(i)
                    # Very old people with very low salaries (might be data error)
                    elif age_val > 70 and salary_val < 20000:
                        anomalous_records.append(i)
            
            if anomalous_records:
                issues.append(QualityIssue(
                    rule_name="correlation_anomaly",
                    field_name=f"{age_field},{salary_field}",
                    issue_type="correlation",
                    severity="warning",
                    message=f"Found {len(anomalous_records)} records with unusual age-salary correlation",
                    record_count=len(anomalous_records),
                    suggestions=[
                        "Review records with unusual age-salary combinations",
                        "Verify data entry accuracy for these fields",
                        "Consider if these represent valid edge cases"
                    ]
                ))
        
        return issues
    
    def _calculate_quality_score(self, data: List[Dict[str, Any]], issues: List[QualityIssue]) -> float:
        """Calculate overall data quality score (0-100)."""
        if not data:
            return 0.0
        
        total_records = len(data)
        
        # Weight issues by severity
        severity_weights = {
            "critical": 10,
            "error": 5,
            "warning": 2
        }
        
        total_penalty = 0
        for issue in issues:
            weight = severity_weights.get(issue.severity, 1)
            penalty = (issue.record_count / total_records) * weight
            total_penalty += penalty
        
        # Calculate score (100 - penalty, minimum 0)
        score = max(0, 100 - (total_penalty * 10))
        return round(score, 2)
    
    def _determine_quality_level(self, score: float) -> QualityLevel:
        """Determine quality level based on score."""
        if score >= 95:
            return QualityLevel.EXCELLENT
        elif score >= 85:
            return QualityLevel.GOOD
        elif score >= 70:
            return QualityLevel.ACCEPTABLE
        elif score >= 50:
            return QualityLevel.POOR
        else:
            return QualityLevel.CRITICAL
    
    def _generate_improvement_suggestions(
        self,
        issues: List[QualityIssue],
        profiles: Dict[str, DataProfile]
    ) -> List[str]:
        """Generate actionable improvement suggestions."""
        suggestions = set()
        
        # Issue-based suggestions
        for issue in issues:
            suggestions.update(issue.suggestions)
        
        # Profile-based suggestions
        for field_name, profile in profiles.items():
            completeness = 1 - (profile.null_count / profile.total_count) if profile.total_count > 0 else 0
            
            if completeness < 0.8:
                suggestions.add(f"Improve data collection for '{field_name}' to reduce missing values")
            
            if profile.unique_count == 1 and profile.total_count > 1:
                suggestions.add(f"Consider if field '{field_name}' adds value (all values are identical)")
        
        return list(suggestions)[:10]  # Limit to top 10 suggestions
    
    def _analyze_quality_breakdown(self, issues: List[QualityIssue]) -> Dict[str, Any]:
        """Analyze quality issues by type and severity."""
        breakdown = {
            "by_type": {},
            "by_severity": {},
            "by_field": {}
        }
        
        for issue in issues:
            # By type
            breakdown["by_type"][issue.issue_type] = breakdown["by_type"].get(issue.issue_type, 0) + 1
            
            # By severity
            breakdown["by_severity"][issue.severity] = breakdown["by_severity"].get(issue.severity, 0) + 1
            
            # By field
            if issue.field_name:
                breakdown["by_field"][issue.field_name] = breakdown["by_field"].get(issue.field_name, 0) + 1
        
        return breakdown
    
    def _issue_to_dict(self, issue: QualityIssue) -> Dict[str, Any]:
        """Convert QualityIssue to dictionary."""
        return {
            "rule_name": issue.rule_name,
            "field_name": issue.field_name,
            "issue_type": issue.issue_type,
            "severity": issue.severity,
            "message": issue.message,
            "record_index": issue.record_index,
            "record_count": issue.record_count,
            "suggestions": issue.suggestions,
            "timestamp": issue.timestamp
        }
    
    def _profile_to_dict(self, profile: DataProfile) -> Dict[str, Any]:
        """Convert DataProfile to dictionary."""
        return {
            "field_name": profile.field_name,
            "data_type": profile.data_type,
            "total_count": profile.total_count,
            "null_count": profile.null_count,
            "unique_count": profile.unique_count,
            "completeness": 1 - (profile.null_count / profile.total_count) if profile.total_count > 0 else 0,
            "min_value": profile.min_value,
            "max_value": profile.max_value,
            "mean_value": profile.mean_value,
            "median_value": profile.median_value,
            "std_deviation": profile.std_deviation,
            "common_patterns": profile.common_patterns,
            "sample_values": profile.sample_values
        }
    
    def get_quality_trends(self, days: int = 30) -> Dict[str, Any]:
        """Get quality trends over time."""
        cutoff_time = time.time() - (days * 24 * 60 * 60)
        recent_history = [h for h in self.quality_history if h["timestamp"] > cutoff_time]
        
        if len(recent_history) < 2:
            return {"message": "Insufficient data for trend analysis"}
        
        # Calculate trends
        scores = [h["quality_score"] for h in recent_history]
        issue_counts = [h["issues_count"] for h in recent_history]
        
        score_trend = "stable"
        if len(scores) >= 2:
            if scores[-1] > scores[0]:
                score_trend = "improving"
            elif scores[-1] < scores[0]:
                score_trend = "declining"
        
        return {
            "period_days": days,
            "analysis_count": len(recent_history),
            "average_quality_score": statistics.mean(scores),
            "score_trend": score_trend,
            "latest_quality_score": scores[-1] if scores else 0,
            "average_issues_per_analysis": statistics.mean(issue_counts),
            "score_history": scores[-10:],  # Last 10 scores
            "timestamp": time.time()
        }


# Global data quality engine instance
_quality_engine = None


def get_quality_engine() -> IntelligentDataQualityEngine:
    """Get the global data quality engine instance."""
    global _quality_engine
    if _quality_engine is None:
        _quality_engine = IntelligentDataQualityEngine()
    return _quality_engine


def analyze_data_quality(data: List[Dict[str, Any]], **kwargs) -> Dict[str, Any]:
    """Analyze data quality using the global engine."""
    engine = get_quality_engine()
    return engine.analyze_data_quality(data, **kwargs)