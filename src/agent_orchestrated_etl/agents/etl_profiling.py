"""Data profiling functionality for ETL operations."""

from __future__ import annotations

import re
import statistics
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Union


@dataclass
class ProfilingConfig:
    """Configuration for data profiling operations."""

    # Profiling depth settings
    statistical_analysis: bool = True
    anomaly_detection: bool = True
    data_quality_scoring: bool = True
    pattern_detection: bool = True

    # Sampling settings
    sample_size: Optional[int] = 10000  # None for full dataset
    sample_percentage: float = 10.0  # Used if sample_size is None
    sampling_strategy: Optional[str] = 'random'  # random, systematic, stratified, reservoir
    random_seed: int = 42

    # Statistical analysis settings
    percentiles: List[float] = None
    correlation_analysis: bool = False
    distribution_analysis: bool = True

    # Anomaly detection settings
    outlier_method: str = "iqr"  # iqr, zscore, isolation_forest
    outlier_threshold: float = 1.5  # For IQR method
    zscore_threshold: float = 3.0  # For Z-score method

    # Data quality settings
    completeness_threshold: float = 0.95
    validity_patterns: Dict[str, str] = None
    consistency_checks: List[str] = None

    # Performance settings
    max_unique_values: int = 1000  # For categorical analysis
    timeout_seconds: int = 300

    def __post_init__(self):
        """Set default values for mutable fields."""
        if self.percentiles is None:
            self.percentiles = [25.0, 50.0, 75.0, 90.0, 95.0, 99.0]
        if self.validity_patterns is None:
            self.validity_patterns = {
                'email': r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
                'phone': r'^\+?[1-9]\d{1,14}$',
                'url': r'^https?://[^\s]+$',
                'ipv4': r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'
            }
        if self.consistency_checks is None:
            self.consistency_checks = ['format_consistency', 'range_consistency', 'pattern_consistency']


@dataclass
class ColumnProfile:
    """Detailed profile for a single column."""

    name: str
    data_type: str
    null_count: int
    null_percentage: float
    unique_count: int
    unique_percentage: float

    # Statistical measures (for numeric columns)
    mean: Optional[float] = None
    median: Optional[float] = None
    mode: Optional[Union[str, float]] = None
    std_dev: Optional[float] = None
    variance: Optional[float] = None
    min_value: Optional[Union[str, float]] = None
    max_value: Optional[Union[str, float]] = None
    percentiles: Optional[Dict[str, float]] = None

    # String-specific measures
    avg_length: Optional[float] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None

    # Pattern analysis
    detected_patterns: List[str] = None
    format_consistency: float = 1.0

    # Data quality metrics
    completeness_score: float = 1.0
    validity_score: float = 1.0
    consistency_score: float = 1.0

    # Anomaly detection
    outlier_count: int = 0
    outlier_percentage: float = 0.0
    outlier_values: List[Union[str, float]] = None

    def __post_init__(self):
        """Initialize mutable fields."""
        if self.detected_patterns is None:
            self.detected_patterns = []
        if self.outlier_values is None:
            self.outlier_values = []
        if self.percentiles is None:
            self.percentiles = {}


class DataProfiler:
    """Handles data profiling operations."""

    def __init__(self):
        """Initialize the data profiler."""
        pass

    def analyze_dataset_structure(self, sample_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze the overall structure of a dataset."""
        analysis = {
            "row_count": len(sample_data.get("rows", [])),
            "column_count": len(sample_data.get("columns", [])),
            "columns": sample_data.get("columns", []),
            "estimated_size_mb": 0,
            "data_types": {},
            "missing_data_summary": {},
            "uniqueness_summary": {},
            "data_quality_score": 0.0
        }

        if not sample_data.get("rows"):
            return analysis

        rows = sample_data["rows"]
        columns = sample_data.get("columns", [])

        # Estimate data size
        if rows:
            avg_row_size = len(str(rows[0])) if rows else 0
            analysis["estimated_size_mb"] = (avg_row_size * analysis["row_count"]) / (1024 * 1024)

        # Analyze each column
        for col in columns:
            col_data = [row.get(col) for row in rows if col in row]

            # Data type analysis
            analysis["data_types"][col] = self._infer_data_type(col_data)

            # Missing data
            null_count = sum(1 for x in col_data if x is None or str(x).strip() == "")
            analysis["missing_data_summary"][col] = {
                "null_count": null_count,
                "null_percentage": (null_count / len(col_data)) * 100 if col_data else 0
            }

            # Uniqueness
            unique_count = len(set(str(x) for x in col_data if x is not None))
            analysis["uniqueness_summary"][col] = {
                "unique_count": unique_count,
                "unique_percentage": (unique_count / len(col_data)) * 100 if col_data else 0
            }

        # Overall data quality score
        if analysis["missing_data_summary"]:
            avg_completeness = statistics.mean(
                100 - summary["null_percentage"]
                for summary in analysis["missing_data_summary"].values()
            )
            analysis["data_quality_score"] = avg_completeness / 100.0

        return analysis

    def profile_column(self, column_name: str, column_data: List[Any], config: ProfilingConfig) -> ColumnProfile:
        """Profile a single column of data."""
        # Filter out None values for most calculations
        non_null_data = [x for x in column_data if x is not None]

        # Basic statistics
        total_count = len(column_data)
        null_count = total_count - len(non_null_data)
        null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0

        unique_values = set(str(x) for x in non_null_data)
        unique_count = len(unique_values)
        unique_percentage = (unique_count / len(non_null_data)) * 100 if non_null_data else 0

        # Infer data type
        data_type = self._infer_data_type(non_null_data)

        # Create base profile
        profile = ColumnProfile(
            name=column_name,
            data_type=data_type,
            null_count=null_count,
            null_percentage=null_percentage,
            unique_count=unique_count,
            unique_percentage=unique_percentage
        )

        if not non_null_data:
            return profile

        # Statistical analysis for numeric data
        if data_type in ['integer', 'float'] and config.statistical_analysis:
            try:
                numeric_data = []
                for x in non_null_data:
                    try:
                        if isinstance(x, (int, float)):
                            numeric_data.append(float(x))
                        else:
                            numeric_data.append(float(str(x)))
                    except (ValueError, TypeError):
                        continue

                if numeric_data:
                    profile.mean = statistics.mean(numeric_data)
                    profile.median = statistics.median(numeric_data)
                    profile.min_value = min(numeric_data)
                    profile.max_value = max(numeric_data)

                    if len(numeric_data) > 1:
                        profile.std_dev = statistics.stdev(numeric_data)
                        profile.variance = statistics.variance(numeric_data)

                    # Calculate percentiles
                    if config.percentiles:
                        profile.percentiles = {}
                        sorted_data = sorted(numeric_data)
                        for p in config.percentiles:
                            idx = int((p / 100.0) * (len(sorted_data) - 1))
                            profile.percentiles[f"p{p}"] = sorted_data[idx]

                    # Mode for numeric data
                    if len(numeric_data) > 0:
                        try:
                            profile.mode = statistics.mode(numeric_data)
                        except statistics.StatisticsError:
                            # No unique mode
                            pass
            except Exception:
                # Skip statistical analysis if there are issues
                pass

        # String-specific analysis
        if data_type == 'string':
            string_data = [str(x) for x in non_null_data]
            if string_data:
                profile.avg_length = statistics.mean(len(s) for s in string_data)
                profile.min_length = min(len(s) for s in string_data)
                profile.max_length = max(len(s) for s in string_data)

                # Mode for string data
                if len(string_data) > 0:
                    try:
                        profile.mode = statistics.mode(string_data)
                    except statistics.StatisticsError:
                        # No unique mode
                        pass

        # Pattern detection
        if config.pattern_detection:
            profile.detected_patterns = self._detect_patterns(non_null_data, config)
            profile.format_consistency = self._calculate_format_consistency(non_null_data, profile.detected_patterns)

        # Data quality scoring
        if config.data_quality_scoring:
            profile.completeness_score = (len(non_null_data) / total_count) if total_count > 0 else 0
            profile.validity_score = self._calculate_validity_score(non_null_data, config)
            profile.consistency_score = profile.format_consistency

        # Anomaly detection
        if config.anomaly_detection and data_type in ['integer', 'float']:
            outliers = self._detect_outliers(non_null_data, config)
            profile.outlier_values = outliers
            profile.outlier_count = len(outliers)
            profile.outlier_percentage = (len(outliers) / len(non_null_data)) * 100 if non_null_data else 0

        return profile

    def _infer_data_type(self, data: List[Any]) -> str:
        """Infer the data type of a column."""
        if not data:
            return 'unknown'

        # Count occurrences of each type
        type_counts = {
            'integer': 0,
            'float': 0,
            'boolean': 0,
            'datetime': 0,
            'string': 0
        }

        sample_size = min(100, len(data))  # Sample for performance
        sample_data = data[:sample_size]

        for value in sample_data:
            if value is None:
                continue

            # Convert to string for pattern matching
            str_value = str(value).strip()

            # Check for integer
            if str_value.isdigit() or (str_value.startswith('-') and str_value[1:].isdigit()):
                type_counts['integer'] += 1
            # Check for float
            elif self._is_float(str_value):
                type_counts['float'] += 1
            # Check for boolean
            elif str_value.lower() in ['true', 'false', '1', '0', 'yes', 'no']:
                type_counts['boolean'] += 1
            # Check for datetime patterns
            elif self._is_datetime(str_value):
                type_counts['datetime'] += 1
            else:
                type_counts['string'] += 1

        # Return the most common type
        if not any(type_counts.values()):
            return 'string'

        return max(type_counts, key=type_counts.get)

    def _is_float(self, value: str) -> bool:
        """Check if a string represents a float."""
        try:
            float(value)
            return '.' in value or 'e' in value.lower()
        except ValueError:
            return False

    def _is_datetime(self, value: str) -> bool:
        """Check if a string represents a datetime."""
        # Common datetime patterns
        datetime_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
            r'\d{2}-\d{2}-\d{4}',  # MM-DD-YYYY
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # ISO format
        ]

        for pattern in datetime_patterns:
            if re.match(pattern, value):
                return True
        return False

    def _detect_patterns(self, data: List[Any], config: ProfilingConfig) -> List[str]:
        """Detect common patterns in the data."""
        if not data:
            return []

        patterns = []
        sample_data = [str(x) for x in data[:100]]  # Sample for performance

        # Check against predefined patterns
        for pattern_name, pattern_regex in config.validity_patterns.items():
            matches = sum(1 for x in sample_data if re.match(pattern_regex, x))
            if matches / len(sample_data) > 0.8:  # 80% match threshold
                patterns.append(pattern_name)

        return patterns

    def _calculate_format_consistency(self, data: List[Any], patterns: List[str]) -> float:
        """Calculate how consistent the data format is."""
        if not data:
            return 1.0

        if not patterns:
            # If no patterns detected, check for general consistency
            sample_data = [str(x) for x in data[:100]]
            if len(set(len(x) for x in sample_data)) == 1:
                return 1.0  # All same length
            else:
                return 0.8  # Decent consistency

        # Calculate consistency based on pattern matches
        sample_data = [str(x) for x in data[:100]]
        total_matches = 0

        for pattern_name in patterns:
            pattern_regex = ProfilingConfig().validity_patterns.get(pattern_name, '')
            if pattern_regex:
                matches = sum(1 for x in sample_data if re.match(pattern_regex, x))
                total_matches += matches

        return min(1.0, total_matches / len(sample_data))

    def _calculate_validity_score(self, data: List[Any], config: ProfilingConfig) -> float:
        """Calculate a validity score for the data."""
        if not data:
            return 1.0

        # Basic validity checks
        valid_count = 0
        sample_data = data[:100]  # Sample for performance

        for value in sample_data:
            str_value = str(value).strip()

            # Check if value is not empty or meaningless
            if str_value and str_value.lower() not in ['null', 'na', 'n/a', 'none', '']:
                valid_count += 1

        return valid_count / len(sample_data)

    def _detect_outliers(self, data: List[Any], config: ProfilingConfig) -> List[Any]:
        """Detect outliers in numeric data."""
        if not data:
            return []

        # Convert to numeric
        numeric_data = []
        for x in data:
            try:
                if isinstance(x, (int, float)):
                    numeric_data.append(float(x))
                else:
                    numeric_data.append(float(str(x)))
            except (ValueError, TypeError):
                continue

        if len(numeric_data) < 4:  # Need at least 4 points for outlier detection
            return []

        if config.outlier_method == "iqr":
            return self._detect_outliers_iqr(numeric_data, config.outlier_threshold)
        elif config.outlier_method == "zscore":
            return self._detect_outliers_zscore(numeric_data, config.zscore_threshold)
        else:
            return []

    def _detect_outliers_iqr(self, data: List[float], threshold: float) -> List[float]:
        """Detect outliers using the IQR method."""
        if len(data) < 4:
            return []

        sorted_data = sorted(data)
        n = len(sorted_data)

        q1_idx = n // 4
        q3_idx = 3 * n // 4

        q1 = sorted_data[q1_idx]
        q3 = sorted_data[q3_idx]
        iqr = q3 - q1

        lower_bound = q1 - threshold * iqr
        upper_bound = q3 + threshold * iqr

        outliers = [x for x in data if x < lower_bound or x > upper_bound]
        return outliers

    def _detect_outliers_zscore(self, data: List[float], threshold: float) -> List[float]:
        """Detect outliers using the Z-score method."""
        if len(data) < 2:
            return []

        mean = statistics.mean(data)
        std_dev = statistics.stdev(data)

        if std_dev == 0:
            return []

        outliers = []
        for x in data:
            z_score = abs(x - mean) / std_dev
            if z_score > threshold:
                outliers.append(x)

        return outliers
