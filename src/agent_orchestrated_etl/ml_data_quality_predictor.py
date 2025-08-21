"""Advanced Data Quality Prediction using Machine Learning for Proactive Quality Management."""

from __future__ import annotations

import pickle
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import pandas as pd

from .exceptions import DataProcessingException
from .logging_config import get_logger


class QualityIssueType(Enum):
    """Types of data quality issues that can be predicted."""
    MISSING_VALUES = "missing_values"
    DUPLICATE_RECORDS = "duplicate_records"
    SCHEMA_DRIFT = "schema_drift"
    OUTLIERS = "outliers"
    FORMAT_VIOLATIONS = "format_violations"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    FRESHNESS_DECAY = "freshness_decay"
    COMPLETENESS_DROP = "completeness_drop"
    ACCURACY_DEGRADATION = "accuracy_degradation"


@dataclass
class DataQualityMetrics:
    """Comprehensive data quality metrics."""
    dataset_id: str
    timestamp: datetime
    completeness: float  # 0.0 to 1.0
    accuracy: float  # 0.0 to 1.0
    consistency: float  # 0.0 to 1.0
    validity: float  # 0.0 to 1.0
    uniqueness: float  # 0.0 to 1.0
    timeliness: float  # 0.0 to 1.0
    overall_score: float  # 0.0 to 1.0

    # Detailed metrics
    null_percentage: float
    duplicate_percentage: float
    outlier_percentage: float
    schema_violations: int
    format_errors: int
    referential_integrity_errors: int
    freshness_hours: float

    # Statistical features
    row_count: int
    column_count: int
    mean_column_entropy: float
    data_volume_gb: float

    def to_feature_vector(self) -> np.ndarray:
        """Convert metrics to feature vector for ML models."""
        return np.array([
            self.completeness,
            self.accuracy,
            self.consistency,
            self.validity,
            self.uniqueness,
            self.timeliness,
            self.null_percentage,
            self.duplicate_percentage,
            self.outlier_percentage,
            min(self.schema_violations / 100.0, 1.0),  # Normalize
            min(self.format_errors / 1000.0, 1.0),  # Normalize
            min(self.referential_integrity_errors / 100.0, 1.0),  # Normalize
            min(self.freshness_hours / 24.0, 1.0),  # Normalize to days
            np.log10(max(self.row_count, 1)) / 8.0,  # Log normalize
            self.column_count / 100.0,  # Normalize
            self.mean_column_entropy,
            min(self.data_volume_gb / 1000.0, 1.0)  # Normalize to TB
        ])


@dataclass
class QualityPrediction:
    """Prediction of future data quality issues."""
    dataset_id: str
    prediction_horizon: timedelta
    predicted_issues: List[QualityIssueType]
    confidence_scores: Dict[QualityIssueType, float]
    predicted_overall_score: float
    risk_level: str  # low, medium, high, critical
    recommended_actions: List[str]
    prediction_timestamp: datetime


class DataQualityFeatureExtractor:
    """Extracts features from data for quality prediction."""

    def __init__(self):
        self.logger = get_logger("quality.feature_extractor")

    async def extract_features(
        self,
        data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
        dataset_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> DataQualityMetrics:
        """Extract comprehensive quality features from data."""
        try:
            # Convert to DataFrame if needed
            if isinstance(data, dict):
                df = pd.DataFrame([data])
            elif isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = data

            if df.empty:
                return self._create_empty_metrics(dataset_id)

            # Basic statistics
            row_count = len(df)
            column_count = len(df.columns)

            # Completeness analysis
            null_percentage = df.isnull().sum().sum() / (row_count * column_count)
            completeness = 1.0 - null_percentage

            # Uniqueness analysis
            unique_ratios = []
            for col in df.columns:
                if df[col].dtype in ['object', 'string']:
                    unique_ratio = df[col].nunique() / max(row_count, 1)
                    unique_ratios.append(unique_ratio)

            uniqueness = np.mean(unique_ratios) if unique_ratios else 1.0

            # Duplicate analysis
            duplicate_percentage = df.duplicated().sum() / max(row_count, 1)

            # Outlier detection using IQR method
            outlier_count = 0
            numeric_columns = df.select_dtypes(include=[np.number]).columns

            for col in numeric_columns:
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                outliers = ((df[col] < lower_bound) | (df[col] > upper_bound)).sum()
                outlier_count += outliers

            outlier_percentage = outlier_count / max(row_count * len(numeric_columns), 1)

            # Schema validation
            schema_violations = 0
            expected_schema = metadata.get('expected_schema', {}) if metadata else {}

            for col, expected_type in expected_schema.items():
                if col in df.columns:
                    if str(df[col].dtype) != expected_type:
                        schema_violations += 1

            # Format validation
            format_errors = 0

            # Check date formats
            for col in df.columns:
                if 'date' in col.lower() or 'time' in col.lower():
                    try:
                        pd.to_datetime(df[col], errors='coerce')
                        invalid_dates = df[col].isnull().sum()
                        format_errors += invalid_dates
                    except:
                        format_errors += row_count

            # Email format validation
            email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
            for col in df.columns:
                if 'email' in col.lower():
                    invalid_emails = ~df[col].str.match(email_pattern, na=False)
                    format_errors += invalid_emails.sum()

            # Calculate entropy for each column
            entropies = []
            for col in df.columns:
                if df[col].dtype == 'object':
                    value_counts = df[col].value_counts(normalize=True)
                    entropy = -np.sum(value_counts * np.log2(value_counts + 1e-10))
                    entropies.append(entropy)

            mean_column_entropy = np.mean(entropies) if entropies else 0.0

            # Data volume calculation
            data_volume_gb = df.memory_usage(deep=True).sum() / (1024**3)

            # Freshness calculation (if timestamp column exists)
            freshness_hours = 0.0
            timestamp_cols = [col for col in df.columns if 'timestamp' in col.lower() or 'created' in col.lower()]

            if timestamp_cols:
                try:
                    latest_timestamp = pd.to_datetime(df[timestamp_cols[0]]).max()
                    freshness_hours = (datetime.now() - latest_timestamp).total_seconds() / 3600
                except:
                    freshness_hours = 24.0  # Default to 24 hours if parsing fails

            # Calculate derived quality scores
            accuracy = 1.0 - (format_errors / max(row_count * column_count, 1))
            consistency = 1.0 - (schema_violations / max(column_count, 1))
            validity = 1.0 - (format_errors / max(row_count * column_count, 1))
            timeliness = max(0.0, 1.0 - (freshness_hours / 168.0))  # Normalize to week

            # Overall score calculation
            overall_score = np.mean([completeness, accuracy, consistency, validity, uniqueness, timeliness])

            metrics = DataQualityMetrics(
                dataset_id=dataset_id,
                timestamp=datetime.now(),
                completeness=completeness,
                accuracy=accuracy,
                consistency=consistency,
                validity=validity,
                uniqueness=uniqueness,
                timeliness=timeliness,
                overall_score=overall_score,
                null_percentage=null_percentage,
                duplicate_percentage=duplicate_percentage,
                outlier_percentage=outlier_percentage,
                schema_violations=schema_violations,
                format_errors=format_errors,
                referential_integrity_errors=0,  # Would require join analysis
                freshness_hours=freshness_hours,
                row_count=row_count,
                column_count=column_count,
                mean_column_entropy=mean_column_entropy,
                data_volume_gb=data_volume_gb
            )

            self.logger.debug(f"Extracted quality features for {dataset_id}: score {overall_score:.3f}")
            return metrics

        except Exception as e:
            self.logger.error(f"Feature extraction failed for {dataset_id}: {e}")
            return self._create_empty_metrics(dataset_id)

    def _create_empty_metrics(self, dataset_id: str) -> DataQualityMetrics:
        """Create empty metrics for failed extraction."""
        return DataQualityMetrics(
            dataset_id=dataset_id,
            timestamp=datetime.now(),
            completeness=0.0,
            accuracy=0.0,
            consistency=0.0,
            validity=0.0,
            uniqueness=0.0,
            timeliness=0.0,
            overall_score=0.0,
            null_percentage=1.0,
            duplicate_percentage=0.0,
            outlier_percentage=0.0,
            schema_violations=0,
            format_errors=0,
            referential_integrity_errors=0,
            freshness_hours=0.0,
            row_count=0,
            column_count=0,
            mean_column_entropy=0.0,
            data_volume_gb=0.0
        )


class QualityTimeSeriesAnalyzer:
    """Analyzes time series patterns in data quality metrics."""

    def __init__(self, window_size: int = 24):
        self.window_size = window_size  # Hours
        self.logger = get_logger("quality.timeseries")

    def analyze_trends(
        self,
        historical_metrics: List[DataQualityMetrics]
    ) -> Dict[str, Any]:
        """Analyze trends in quality metrics over time."""
        if not historical_metrics:
            return {"trend": "insufficient_data"}

        # Sort by timestamp
        sorted_metrics = sorted(historical_metrics, key=lambda x: x.timestamp)

        # Extract time series data
        timestamps = [m.timestamp for m in sorted_metrics]
        overall_scores = [m.overall_score for m in sorted_metrics]
        completeness_scores = [m.completeness for m in sorted_metrics]
        accuracy_scores = [m.accuracy for m in sorted_metrics]

        # Calculate trends
        trends = {}

        if len(overall_scores) >= 3:
            # Linear trend calculation
            x = np.arange(len(overall_scores))
            overall_trend = np.polyfit(x, overall_scores, 1)[0]
            completeness_trend = np.polyfit(x, completeness_scores, 1)[0]
            accuracy_trend = np.polyfit(x, accuracy_scores, 1)[0]

            trends = {
                "overall_trend": overall_trend,
                "completeness_trend": completeness_trend,
                "accuracy_trend": accuracy_trend,
                "trend_direction": "improving" if overall_trend > 0.01 else "degrading" if overall_trend < -0.01 else "stable",
                "volatility": np.std(overall_scores),
                "recent_score": overall_scores[-1],
                "score_change_24h": overall_scores[-1] - overall_scores[max(0, len(overall_scores)-24)]
            }

        # Seasonal pattern detection
        if len(overall_scores) >= 24:
            # Simple periodicity detection using autocorrelation
            hourly_scores = overall_scores[-24:]
            daily_pattern = self._detect_daily_pattern(hourly_scores)
            trends["daily_pattern"] = daily_pattern

        return trends

    def _detect_daily_pattern(self, hourly_scores: List[float]) -> Dict[str, Any]:
        """Detect daily patterns in quality scores."""
        try:
            # Calculate hour-of-day averages
            hour_averages = {}
            for i, score in enumerate(hourly_scores):
                hour = i % 24
                if hour not in hour_averages:
                    hour_averages[hour] = []
                hour_averages[hour].append(score)

            # Find peak and trough hours
            hour_means = {hour: np.mean(scores) for hour, scores in hour_averages.items()}
            peak_hour = max(hour_means, key=hour_means.get)
            trough_hour = min(hour_means, key=hour_means.get)

            return {
                "has_pattern": max(hour_means.values()) - min(hour_means.values()) > 0.1,
                "peak_hour": peak_hour,
                "trough_hour": trough_hour,
                "peak_score": hour_means[peak_hour],
                "trough_score": hour_means[trough_hour]
            }

        except Exception:
            return {"has_pattern": False}


class QualityPredictionModel:
    """Machine learning model for predicting data quality issues."""

    def __init__(self, model_type: str = "ensemble"):
        self.model_type = model_type
        self.logger = get_logger("quality.prediction_model")

        # Simple ensemble of statistical models
        self.models = {
            "trend_predictor": self._create_trend_model(),
            "anomaly_detector": self._create_anomaly_model(),
            "issue_classifier": self._create_issue_classifier()
        }

        # Model weights
        self.model_weights = {
            "trend_predictor": 0.4,
            "anomaly_detector": 0.3,
            "issue_classifier": 0.3
        }

    def _create_trend_model(self) -> Dict[str, Any]:
        """Create trend prediction model."""
        return {
            "type": "linear_regression",
            "parameters": {},
            "trained": False
        }

    def _create_anomaly_model(self) -> Dict[str, Any]:
        """Create anomaly detection model."""
        return {
            "type": "isolation_forest",
            "parameters": {"contamination": 0.1},
            "trained": False
        }

    def _create_issue_classifier(self) -> Dict[str, Any]:
        """Create issue classification model."""
        return {
            "type": "random_forest",
            "parameters": {"n_estimators": 100},
            "trained": False
        }

    async def train(
        self,
        historical_metrics: List[DataQualityMetrics],
        known_issues: List[QualityIssueType]
    ) -> None:
        """Train the prediction models on historical data."""
        try:
            if len(historical_metrics) < 10:
                self.logger.warning("Insufficient training data for quality prediction model")
                return

            # Extract features
            features = np.array([m.to_feature_vector() for m in historical_metrics])

            # Train trend predictor
            self._train_trend_predictor(features, [m.overall_score for m in historical_metrics])

            # Train anomaly detector
            self._train_anomaly_detector(features)

            # Train issue classifier
            if known_issues:
                self._train_issue_classifier(features, known_issues)

            self.logger.info("Quality prediction models trained successfully")

        except Exception as e:
            self.logger.error(f"Model training failed: {e}")

    def _train_trend_predictor(self, features: np.ndarray, scores: List[float]) -> None:
        """Train trend prediction model using simple linear regression."""
        try:
            # Simple linear trend on overall scores
            X = np.arange(len(scores)).reshape(-1, 1)
            y = np.array(scores)

            # Calculate slope and intercept
            slope = np.corrcoef(X.flatten(), y)[0, 1] * (np.std(y) / np.std(X.flatten()))
            intercept = np.mean(y) - slope * np.mean(X.flatten())

            self.models["trend_predictor"]["parameters"] = {
                "slope": slope,
                "intercept": intercept,
                "feature_means": np.mean(features, axis=0),
                "feature_stds": np.std(features, axis=0)
            }
            self.models["trend_predictor"]["trained"] = True

        except Exception as e:
            self.logger.error(f"Trend predictor training failed: {e}")

    def _train_anomaly_detector(self, features: np.ndarray) -> None:
        """Train anomaly detector using statistical methods."""
        try:
            # Calculate feature statistics for anomaly detection
            feature_means = np.mean(features, axis=0)
            feature_stds = np.std(features, axis=0)

            # Use 2-sigma threshold for anomaly detection
            self.models["anomaly_detector"]["parameters"] = {
                "feature_means": feature_means,
                "feature_stds": feature_stds,
                "threshold": 2.0
            }
            self.models["anomaly_detector"]["trained"] = True

        except Exception as e:
            self.logger.error(f"Anomaly detector training failed: {e}")

    def _train_issue_classifier(
        self,
        features: np.ndarray,
        known_issues: List[QualityIssueType]
    ) -> None:
        """Train issue classifier using statistical patterns."""
        try:
            # Simple rule-based classifier
            issue_patterns = {}

            for issue_type in QualityIssueType:
                # Define feature thresholds for each issue type
                if issue_type == QualityIssueType.MISSING_VALUES:
                    issue_patterns[issue_type] = {"feature_idx": 6, "threshold": 0.1}  # null_percentage
                elif issue_type == QualityIssueType.DUPLICATE_RECORDS:
                    issue_patterns[issue_type] = {"feature_idx": 7, "threshold": 0.05}  # duplicate_percentage
                elif issue_type == QualityIssueType.OUTLIERS:
                    issue_patterns[issue_type] = {"feature_idx": 8, "threshold": 0.1}  # outlier_percentage
                elif issue_type == QualityIssueType.SCHEMA_DRIFT:
                    issue_patterns[issue_type] = {"feature_idx": 9, "threshold": 0.01}  # schema_violations
                elif issue_type == QualityIssueType.FORMAT_VIOLATIONS:
                    issue_patterns[issue_type] = {"feature_idx": 10, "threshold": 0.01}  # format_errors
                elif issue_type == QualityIssueType.FRESHNESS_DECAY:
                    issue_patterns[issue_type] = {"feature_idx": 12, "threshold": 0.5}  # freshness_hours
                else:
                    # Default pattern
                    issue_patterns[issue_type] = {"feature_idx": 0, "threshold": 0.8}

            self.models["issue_classifier"]["parameters"] = {
                "issue_patterns": issue_patterns
            }
            self.models["issue_classifier"]["trained"] = True

        except Exception as e:
            self.logger.error(f"Issue classifier training failed: {e}")

    async def predict(
        self,
        current_metrics: DataQualityMetrics,
        prediction_horizon: timedelta
    ) -> QualityPrediction:
        """Predict future quality issues."""
        try:
            features = current_metrics.to_feature_vector()

            # Get predictions from each model
            trend_prediction = self._predict_trend(features, prediction_horizon)
            anomaly_score = self._predict_anomaly(features)
            issue_predictions = self._predict_issues(features)

            # Combine predictions
            predicted_overall_score = (
                trend_prediction * self.model_weights["trend_predictor"] +
                (1.0 - anomaly_score) * self.model_weights["anomaly_detector"] +
                current_metrics.overall_score * self.model_weights["issue_classifier"]
            )

            # Determine risk level
            if predicted_overall_score < 0.5:
                risk_level = "critical"
            elif predicted_overall_score < 0.7:
                risk_level = "high"
            elif predicted_overall_score < 0.85:
                risk_level = "medium"
            else:
                risk_level = "low"

            # Generate recommendations
            recommendations = self._generate_recommendations(
                current_metrics, issue_predictions, risk_level
            )

            prediction = QualityPrediction(
                dataset_id=current_metrics.dataset_id,
                prediction_horizon=prediction_horizon,
                predicted_issues=issue_predictions,
                confidence_scores=dict.fromkeys(issue_predictions, 0.7),  # Simplified
                predicted_overall_score=predicted_overall_score,
                risk_level=risk_level,
                recommended_actions=recommendations,
                prediction_timestamp=datetime.now()
            )

            self.logger.debug(f"Generated quality prediction for {current_metrics.dataset_id}: {risk_level} risk")
            return prediction

        except Exception as e:
            self.logger.error(f"Quality prediction failed: {e}")
            return self._create_default_prediction(current_metrics, prediction_horizon)

    def _predict_trend(self, features: np.ndarray, horizon: timedelta) -> float:
        """Predict quality trend."""
        if not self.models["trend_predictor"]["trained"]:
            return 0.8  # Default prediction

        params = self.models["trend_predictor"]["parameters"]

        # Simple linear extrapolation
        hours_ahead = horizon.total_seconds() / 3600
        trend = params["slope"] * hours_ahead

        return max(0.0, min(1.0, 0.8 + trend))  # Bounded prediction

    def _predict_anomaly(self, features: np.ndarray) -> float:
        """Predict anomaly score."""
        if not self.models["anomaly_detector"]["trained"]:
            return 0.1  # Default low anomaly score

        params = self.models["anomaly_detector"]["parameters"]

        # Calculate Z-scores
        z_scores = np.abs(features - params["feature_means"]) / (params["feature_stds"] + 1e-8)
        max_z_score = np.max(z_scores)

        # Convert to anomaly score
        anomaly_score = min(1.0, max_z_score / params["threshold"])

        return anomaly_score

    def _predict_issues(self, features: np.ndarray) -> List[QualityIssueType]:
        """Predict specific quality issues."""
        if not self.models["issue_classifier"]["trained"]:
            return []

        patterns = self.models["issue_classifier"]["parameters"]["issue_patterns"]
        predicted_issues = []

        for issue_type, pattern in patterns.items():
            feature_idx = pattern["feature_idx"]
            threshold = pattern["threshold"]

            if feature_idx < len(features) and features[feature_idx] > threshold:
                predicted_issues.append(issue_type)

        return predicted_issues

    def _generate_recommendations(
        self,
        metrics: DataQualityMetrics,
        predicted_issues: List[QualityIssueType],
        risk_level: str
    ) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []

        if QualityIssueType.MISSING_VALUES in predicted_issues:
            recommendations.append("Implement data validation at source to reduce missing values")

        if QualityIssueType.DUPLICATE_RECORDS in predicted_issues:
            recommendations.append("Add deduplication step in data pipeline")

        if QualityIssueType.OUTLIERS in predicted_issues:
            recommendations.append("Review outlier detection rules and data sources")

        if QualityIssueType.SCHEMA_DRIFT in predicted_issues:
            recommendations.append("Monitor schema changes and update validation rules")

        if QualityIssueType.FRESHNESS_DECAY in predicted_issues:
            recommendations.append("Increase data ingestion frequency or check source systems")

        if risk_level in ["high", "critical"]:
            recommendations.extend([
                "Implement additional data quality checks",
                "Set up real-time monitoring alerts",
                "Consider manual data review process"
            ])

        return recommendations

    def _create_default_prediction(
        self,
        metrics: DataQualityMetrics,
        horizon: timedelta
    ) -> QualityPrediction:
        """Create default prediction when models fail."""
        return QualityPrediction(
            dataset_id=metrics.dataset_id,
            prediction_horizon=horizon,
            predicted_issues=[],
            confidence_scores={},
            predicted_overall_score=metrics.overall_score,
            risk_level="medium",
            recommended_actions=["Monitor data quality closely"],
            prediction_timestamp=datetime.now()
        )


class MLDataQualityPredictor:
    """Main class for ML-based data quality prediction."""

    def __init__(self, model_save_path: Optional[str] = None):
        self.logger = get_logger("ml_quality_predictor")
        self.model_save_path = model_save_path or "quality_prediction_model.pkl"

        # Initialize components
        self.feature_extractor = DataQualityFeatureExtractor()
        self.timeseries_analyzer = QualityTimeSeriesAnalyzer()
        self.prediction_model = QualityPredictionModel()

        # Historical data storage
        self.quality_history: Dict[str, List[DataQualityMetrics]] = defaultdict(list)
        self.prediction_history: Dict[str, List[QualityPrediction]] = defaultdict(list)

        # Load existing model if available
        self._load_model()

    def _load_model(self) -> None:
        """Load existing ML model."""
        try:
            if Path(self.model_save_path).exists():
                with open(self.model_save_path, 'rb') as f:
                    model_data = pickle.load(f)

                self.prediction_model.models = model_data.get("models", self.prediction_model.models)
                self.quality_history = model_data.get("quality_history", self.quality_history)

                self.logger.info("Quality prediction model loaded successfully")

        except Exception as e:
            self.logger.warning(f"Failed to load existing model: {e}")

    async def analyze_data_quality(
        self,
        data: Union[pd.DataFrame, Dict[str, Any], List[Dict[str, Any]]],
        dataset_id: str,
        metadata: Optional[Dict[str, Any]] = None
    ) -> DataQualityMetrics:
        """Analyze current data quality and store metrics."""
        try:
            # Extract quality metrics
            metrics = await self.feature_extractor.extract_features(data, dataset_id, metadata)

            # Store in history
            self.quality_history[dataset_id].append(metrics)

            # Keep only recent history (last 1000 entries)
            if len(self.quality_history[dataset_id]) > 1000:
                self.quality_history[dataset_id] = self.quality_history[dataset_id][-1000:]

            self.logger.info(f"Quality analysis complete for {dataset_id}: score {metrics.overall_score:.3f}")
            return metrics

        except Exception as e:
            self.logger.error(f"Quality analysis failed for {dataset_id}: {e}")
            raise DataProcessingException(f"Quality analysis failed: {e}")

    async def predict_quality_issues(
        self,
        dataset_id: str,
        prediction_horizon: timedelta = timedelta(hours=24)
    ) -> QualityPrediction:
        """Predict future quality issues for a dataset."""
        try:
            if dataset_id not in self.quality_history or not self.quality_history[dataset_id]:
                raise ValueError(f"No quality history found for dataset {dataset_id}")

            # Get current metrics
            current_metrics = self.quality_history[dataset_id][-1]

            # Analyze trends
            trend_analysis = self.timeseries_analyzer.analyze_trends(
                self.quality_history[dataset_id]
            )

            # Generate prediction
            prediction = await self.prediction_model.predict(current_metrics, prediction_horizon)

            # Store prediction
            self.prediction_history[dataset_id].append(prediction)

            self.logger.info(f"Quality prediction generated for {dataset_id}: {prediction.risk_level} risk")
            return prediction

        except Exception as e:
            self.logger.error(f"Quality prediction failed for {dataset_id}: {e}")
            raise DataProcessingException(f"Quality prediction failed: {e}")

    async def train_model(
        self,
        dataset_id: Optional[str] = None,
        known_issues: Optional[List[QualityIssueType]] = None
    ) -> None:
        """Train the ML model on historical data."""
        try:
            # Collect training data
            all_metrics = []

            if dataset_id and dataset_id in self.quality_history:
                all_metrics = self.quality_history[dataset_id]
            else:
                # Use all available data
                for metrics_list in self.quality_history.values():
                    all_metrics.extend(metrics_list)

            if len(all_metrics) < 10:
                self.logger.warning("Insufficient data for model training")
                return

            # Train the model
            await self.prediction_model.train(all_metrics, known_issues or [])

            self.logger.info(f"Model training completed with {len(all_metrics)} samples")

        except Exception as e:
            self.logger.error(f"Model training failed: {e}")

    async def save_model(self) -> None:
        """Save the trained model and historical data."""
        try:
            model_data = {
                "models": self.prediction_model.models,
                "quality_history": dict(self.quality_history),
                "prediction_history": dict(self.prediction_history),
                "saved_at": datetime.now().isoformat()
            }

            with open(self.model_save_path, 'wb') as f:
                pickle.dump(model_data, f)

            self.logger.info("Quality prediction model saved successfully")

        except Exception as e:
            self.logger.error(f"Failed to save model: {e}")

    def get_quality_summary(self, dataset_id: str) -> Dict[str, Any]:
        """Get quality summary for a dataset."""
        if dataset_id not in self.quality_history:
            return {"error": "No quality data available"}

        metrics_list = self.quality_history[dataset_id]
        if not metrics_list:
            return {"error": "No quality metrics found"}

        current = metrics_list[-1]
        trend_analysis = self.timeseries_analyzer.analyze_trends(metrics_list)

        return {
            "dataset_id": dataset_id,
            "current_score": current.overall_score,
            "trend": trend_analysis.get("trend_direction", "unknown"),
            "volatility": trend_analysis.get("volatility", 0.0),
            "total_measurements": len(metrics_list),
            "last_updated": current.timestamp.isoformat(),
            "key_issues": {
                "high_null_percentage": current.null_percentage > 0.1,
                "high_duplicate_percentage": current.duplicate_percentage > 0.05,
                "high_outlier_percentage": current.outlier_percentage > 0.1,
                "schema_violations": current.schema_violations > 0,
                "freshness_issues": current.freshness_hours > 24
            }
        }


# Integration function
async def setup_quality_prediction(
    predictor: MLDataQualityPredictor,
    pipeline_monitor
) -> None:
    """Set up quality prediction integration with pipeline monitoring."""
    try:
        # This would integrate with the existing pipeline monitoring system
        get_logger("quality_integration").info("Quality prediction system integrated")

        # Example: Monitor data quality during pipeline execution
        # pipeline_monitor.add_quality_callback(predictor.analyze_data_quality)

    except Exception as e:
        get_logger("quality_integration").error(f"Quality prediction integration failed: {e}")
        raise DataProcessingException(f"Quality prediction integration failed: {e}")
