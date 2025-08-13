"""Configuration classes for ETL operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Union


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
