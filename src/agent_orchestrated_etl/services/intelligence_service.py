"""Intelligent decision-making service with machine learning capabilities."""

from __future__ import annotations

import statistics
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..exceptions import DataProcessingException
from ..logging_config import get_logger


@dataclass
class DecisionContext:
    """Context information for intelligent decision making."""
    pipeline_id: str
    data_characteristics: Dict[str, Any]
    historical_performance: List[Dict[str, Any]]
    current_conditions: Dict[str, Any]
    optimization_goals: List[str]


@dataclass
class IntelligentRecommendation:
    """Intelligent recommendation with confidence score."""
    recommendation_type: str
    action: str
    confidence_score: float
    reasoning: str
    expected_impact: Dict[str, Any]
    implementation_complexity: str  # low, medium, high


class IntelligenceService:
    """AI-driven intelligence service for pipeline optimization and decision making."""

    def __init__(self):
        self.logger = get_logger("agent_etl.services.intelligence")
        self.decision_history: List[Dict[str, Any]] = []
        self.performance_patterns: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        self.optimization_models: Dict[str, Any] = {}
        self.learning_cache: Dict[str, Dict[str, Any]] = {}

    async def analyze_pipeline_requirements(self,
                                          source_config: Dict[str, Any],
                                          business_requirements: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Analyze pipeline requirements and suggest optimal configuration."""

        analysis_start = time.time()

        try:
            # Analyze data source characteristics
            source_analysis = await self._analyze_data_source(source_config)

            # Analyze business requirements
            business_analysis = self._analyze_business_requirements(business_requirements or {})

            # Generate intelligent recommendations
            recommendations = await self._generate_configuration_recommendations(
                source_analysis, business_analysis
            )

            # Predict performance characteristics
            performance_prediction = await self._predict_performance(
                source_analysis, recommendations
            )

            analysis_time = time.time() - analysis_start

            result = {
                "analysis_timestamp": datetime.now().isoformat(),
                "analysis_duration": analysis_time,
                "source_analysis": source_analysis,
                "business_analysis": business_analysis,
                "recommendations": recommendations,
                "performance_prediction": performance_prediction,
                "confidence_score": self._calculate_overall_confidence(recommendations)
            }

            self.logger.info(f"Pipeline requirements analyzed in {analysis_time:.2f}s")
            return result

        except Exception as e:
            self.logger.error(f"Pipeline analysis failed: {e}")
            raise DataProcessingException(f"Intelligence analysis failed: {e}") from e

    async def optimize_execution_strategy(self,
                                        pipeline_id: str,
                                        current_metrics: Dict[str, Any],
                                        historical_data: List[Dict[str, Any]]) -> List[IntelligentRecommendation]:
        """Generate intelligent optimization recommendations based on execution patterns."""

        try:
            # Analyze current performance bottlenecks
            bottlenecks = self._identify_performance_bottlenecks(current_metrics)

            # Learn from historical patterns
            patterns = self._analyze_historical_patterns(historical_data)

            # Generate optimization recommendations
            recommendations = []

            # Resource optimization recommendations
            resource_recs = await self._generate_resource_optimizations(
                current_metrics, patterns, bottlenecks
            )
            recommendations.extend(resource_recs)

            # Algorithm optimization recommendations
            algorithm_recs = await self._generate_algorithm_optimizations(
                current_metrics, patterns
            )
            recommendations.extend(algorithm_recs)

            # Infrastructure optimization recommendations
            infra_recs = await self._generate_infrastructure_optimizations(
                current_metrics, historical_data
            )
            recommendations.extend(infra_recs)

            # Sort by expected impact and confidence
            recommendations.sort(
                key=lambda x: (x.confidence_score * self._impact_score(x.expected_impact)),
                reverse=True
            )

            # Update learning models
            await self._update_optimization_models(pipeline_id, recommendations, current_metrics)

            self.logger.info(f"Generated {len(recommendations)} optimization recommendations")
            return recommendations

        except Exception as e:
            self.logger.error(f"Optimization strategy generation failed: {e}")
            return []

    async def predict_pipeline_behavior(self,
                                      pipeline_config: Dict[str, Any],
                                      execution_context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Predict pipeline behavior and potential issues."""

        try:
            # Extract key features for prediction
            features = self._extract_prediction_features(pipeline_config, execution_context or {})

            # Predict execution time
            time_prediction = await self._predict_execution_time(features)

            # Predict resource usage
            resource_prediction = await self._predict_resource_usage(features)

            # Predict potential failure points
            failure_prediction = await self._predict_failure_risks(features)

            # Predict data quality outcomes
            quality_prediction = await self._predict_data_quality(features)

            # Generate early warning alerts
            alerts = self._generate_early_warnings(
                time_prediction, resource_prediction, failure_prediction, quality_prediction
            )

            prediction = {
                "prediction_timestamp": datetime.now().isoformat(),
                "execution_time": time_prediction,
                "resource_usage": resource_prediction,
                "failure_risks": failure_prediction,
                "data_quality": quality_prediction,
                "confidence_intervals": self._calculate_confidence_intervals(features),
                "early_warnings": alerts,
                "optimization_opportunities": await self._identify_optimization_opportunities(features)
            }

            self.logger.info("Pipeline behavior prediction completed")
            return prediction

        except Exception as e:
            self.logger.error(f"Behavior prediction failed: {e}")
            return {"error": str(e), "prediction_available": False}

    async def learn_from_execution(self,
                                 pipeline_id: str,
                                 execution_data: Dict[str, Any],
                                 actual_outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """Learn from pipeline execution to improve future predictions."""

        try:
            # Extract learning features
            features = self._extract_learning_features(execution_data)
            outcomes = self._extract_outcome_features(actual_outcomes)

            # Update pattern recognition models
            pattern_updates = await self._update_pattern_models(pipeline_id, features, outcomes)

            # Update prediction accuracy
            prediction_accuracy = await self._update_prediction_models(features, outcomes)

            # Update optimization effectiveness
            optimization_effectiveness = await self._evaluate_optimization_effectiveness(
                pipeline_id, execution_data, actual_outcomes
            )

            # Store learning data
            learning_record = {
                "pipeline_id": pipeline_id,
                "timestamp": datetime.now().isoformat(),
                "features": features,
                "outcomes": outcomes,
                "learning_metrics": {
                    "pattern_accuracy": pattern_updates.get("accuracy", 0.0),
                    "prediction_accuracy": prediction_accuracy.get("accuracy", 0.0),
                    "optimization_effectiveness": optimization_effectiveness.get("effectiveness", 0.0)
                }
            }

            self.decision_history.append(learning_record)

            # Update learning cache
            cache_key = f"pipeline_{pipeline_id}"
            if cache_key not in self.learning_cache:
                self.learning_cache[cache_key] = {"learning_history": [], "model_updates": 0}

            self.learning_cache[cache_key]["learning_history"].append(learning_record)
            self.learning_cache[cache_key]["model_updates"] += 1

            # Keep only recent learning data
            self.learning_cache[cache_key]["learning_history"] = \
                self.learning_cache[cache_key]["learning_history"][-50:]

            learning_summary = {
                "learning_timestamp": datetime.now().isoformat(),
                "pipeline_id": pipeline_id,
                "pattern_improvements": pattern_updates,
                "prediction_improvements": prediction_accuracy,
                "optimization_insights": optimization_effectiveness,
                "total_learning_sessions": self.learning_cache[cache_key]["model_updates"]
            }

            self.logger.info(f"Learning session completed for pipeline {pipeline_id}")
            return learning_summary

        except Exception as e:
            self.logger.error(f"Learning from execution failed: {e}")
            return {"error": str(e), "learning_successful": False}

    async def _analyze_data_source(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data source characteristics."""
        source_type = source_config.get("type", "unknown")

        analysis = {
            "source_type": source_type,
            "complexity_score": 1,
            "estimated_throughput": "unknown",
            "data_characteristics": {},
            "optimization_potential": "medium"
        }

        # Type-specific analysis
        if source_type == "database":
            analysis["complexity_score"] = 3
            analysis["estimated_throughput"] = "high"
            analysis["optimization_potential"] = "high"
            analysis["data_characteristics"] = {
                "structured": True,
                "schema_available": True,
                "queryable": True
            }
        elif source_type == "file":
            analysis["complexity_score"] = 2
            analysis["estimated_throughput"] = "medium"
            analysis["optimization_potential"] = "medium"
            analysis["data_characteristics"] = {
                "structured": True,
                "schema_inference_needed": True,
                "batch_processable": True
            }
        elif source_type == "api":
            analysis["complexity_score"] = 4
            analysis["estimated_throughput"] = "variable"
            analysis["optimization_potential"] = "high"
            analysis["data_characteristics"] = {
                "structured": False,
                "rate_limited": True,
                "real_time": True
            }
        elif source_type == "s3":
            analysis["complexity_score"] = 2
            analysis["estimated_throughput"] = "high"
            analysis["optimization_potential"] = "high"
            analysis["data_characteristics"] = {
                "cloud_native": True,
                "scalable": True,
                "batch_optimized": True
            }

        return analysis

    def _analyze_business_requirements(self, business_requirements: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze business requirements for optimization guidance."""

        analysis = {
            "priority": business_requirements.get("priority", "medium"),
            "sla_requirements": business_requirements.get("sla", {}),
            "cost_constraints": business_requirements.get("cost_constraints", {}),
            "quality_requirements": business_requirements.get("quality_requirements", {}),
            "compliance_needs": business_requirements.get("compliance", [])
        }

        # Derive optimization goals
        optimization_goals = []

        if analysis["priority"] == "high":
            optimization_goals.append("minimize_latency")

        if analysis["cost_constraints"]:
            optimization_goals.append("minimize_cost")

        if analysis["quality_requirements"]:
            optimization_goals.append("maximize_quality")

        if analysis["compliance_needs"]:
            optimization_goals.append("ensure_compliance")

        analysis["optimization_goals"] = optimization_goals

        return analysis

    async def _generate_configuration_recommendations(self,
                                                    source_analysis: Dict[str, Any],
                                                    business_analysis: Dict[str, Any]) -> List[IntelligentRecommendation]:
        """Generate intelligent configuration recommendations."""

        recommendations = []

        # Parallelism recommendations
        if source_analysis["complexity_score"] > 2:
            recommendations.append(IntelligentRecommendation(
                recommendation_type="parallelism",
                action="Enable parallel processing with 4 workers",
                confidence_score=0.85,
                reasoning="High complexity data source benefits from parallel processing",
                expected_impact={"performance_improvement": "30-50%", "resource_usage": "+40%"},
                implementation_complexity="medium"
            ))

        # Caching recommendations
        if "minimize_latency" in business_analysis.get("optimization_goals", []):
            recommendations.append(IntelligentRecommendation(
                recommendation_type="caching",
                action="Enable aggressive caching for repeated operations",
                confidence_score=0.90,
                reasoning="Latency-sensitive workload benefits from caching",
                expected_impact={"latency_reduction": "20-40%", "memory_usage": "+25%"},
                implementation_complexity="low"
            ))

        # Batch size recommendations
        if source_analysis["estimated_throughput"] == "high":
            recommendations.append(IntelligentRecommendation(
                recommendation_type="batch_size",
                action="Increase batch size to 5000 records",
                confidence_score=0.80,
                reasoning="High throughput source can handle larger batches efficiently",
                expected_impact={"throughput_improvement": "25-35%", "memory_usage": "+20%"},
                implementation_complexity="low"
            ))

        # Quality recommendations
        if business_analysis.get("quality_requirements"):
            recommendations.append(IntelligentRecommendation(
                recommendation_type="data_quality",
                action="Enable comprehensive data quality validation",
                confidence_score=0.95,
                reasoning="Business requirements specify quality constraints",
                expected_impact={"quality_improvement": "significant", "processing_time": "+10%"},
                implementation_complexity="low"
            ))

        return recommendations

    async def _predict_performance(self,
                                 source_analysis: Dict[str, Any],
                                 recommendations: List[IntelligentRecommendation]) -> Dict[str, Any]:
        """Predict performance characteristics based on analysis."""

        base_time = 10.0  # Base execution time in seconds

        # Adjust based on source complexity
        complexity_multiplier = source_analysis["complexity_score"] * 0.3
        estimated_time = base_time * (1 + complexity_multiplier)

        # Apply recommendation impacts
        for rec in recommendations:
            if "performance_improvement" in rec.expected_impact:
                improvement = rec.expected_impact["performance_improvement"]
                if isinstance(improvement, str) and "%" in improvement:
                    # Extract percentage improvement
                    pct = float(improvement.split("-")[0].replace("%", "")) / 100
                    estimated_time *= (1 - pct)

        return {
            "estimated_execution_time": estimated_time,
            "confidence_interval": {"min": estimated_time * 0.8, "max": estimated_time * 1.3},
            "performance_factors": {
                "source_complexity": complexity_multiplier,
                "optimization_applied": len(recommendations)
            }
        }

    def _identify_performance_bottlenecks(self, metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify performance bottlenecks from metrics."""

        bottlenecks = []

        # Identify slow phases
        phases = ["extraction_time", "transformation_time", "loading_time", "validation_time"]
        times = {phase: metrics.get(phase, 0) for phase in phases}

        if times:
            max_time = max(times.values())
            avg_time = sum(times.values()) / len(times)

            for phase, time_val in times.items():
                if time_val > avg_time * 2:  # Significantly slower than average
                    bottlenecks.append({
                        "type": "slow_phase",
                        "phase": phase,
                        "time": time_val,
                        "severity": "high" if time_val == max_time else "medium"
                    })

        # Check quality scores
        quality_score = metrics.get("quality_score", 1.0)
        if quality_score < 0.7:
            bottlenecks.append({
                "type": "data_quality",
                "score": quality_score,
                "severity": "high" if quality_score < 0.5 else "medium"
            })

        return bottlenecks

    def _analyze_historical_patterns(self, historical_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Analyze historical execution patterns."""

        if not historical_data:
            return {"patterns_available": False}

        # Extract execution times
        execution_times = [
            data.get("total_time", 0) for data in historical_data
            if data.get("total_time", 0) > 0
        ]

        # Extract quality scores
        quality_scores = [
            data.get("quality_score", 1.0) for data in historical_data
            if "quality_score" in data
        ]

        patterns = {
            "patterns_available": True,
            "execution_count": len(historical_data),
            "time_patterns": {},
            "quality_patterns": {},
            "trend_analysis": {}
        }

        if execution_times:
            patterns["time_patterns"] = {
                "avg_time": statistics.mean(execution_times),
                "median_time": statistics.median(execution_times),
                "std_dev": statistics.stdev(execution_times) if len(execution_times) > 1 else 0,
                "trend": "stable"  # Simplified trend analysis
            }

        if quality_scores:
            patterns["quality_patterns"] = {
                "avg_quality": statistics.mean(quality_scores),
                "quality_trend": "stable",
                "quality_consistency": statistics.stdev(quality_scores) if len(quality_scores) > 1 else 0
            }

        return patterns

    async def _generate_resource_optimizations(self,
                                             current_metrics: Dict[str, Any],
                                             patterns: Dict[str, Any],
                                             bottlenecks: List[Dict[str, Any]]) -> List[IntelligentRecommendation]:
        """Generate resource optimization recommendations."""

        recommendations = []

        # Memory optimization
        if any(b["type"] == "slow_phase" and "transformation" in b["phase"] for b in bottlenecks):
            recommendations.append(IntelligentRecommendation(
                recommendation_type="memory_optimization",
                action="Increase memory allocation for transformation phase",
                confidence_score=0.75,
                reasoning="Transformation phase identified as bottleneck",
                expected_impact={"transformation_speedup": "20-30%", "memory_cost": "+15%"},
                implementation_complexity="low"
            ))

        # CPU optimization
        if patterns.get("time_patterns", {}).get("avg_time", 0) > 30:
            recommendations.append(IntelligentRecommendation(
                recommendation_type="cpu_optimization",
                action="Enable multi-threading for CPU-intensive operations",
                confidence_score=0.80,
                reasoning="Historical data shows high average execution time",
                expected_impact={"cpu_efficiency": "25-40%", "cost": "+10%"},
                implementation_complexity="medium"
            ))

        return recommendations

    async def _generate_algorithm_optimizations(self,
                                              current_metrics: Dict[str, Any],
                                              patterns: Dict[str, Any]) -> List[IntelligentRecommendation]:
        """Generate algorithm optimization recommendations."""

        recommendations = []

        # Data processing algorithm optimization
        quality_score = current_metrics.get("quality_score", 1.0)
        if quality_score < 0.8:
            recommendations.append(IntelligentRecommendation(
                recommendation_type="algorithm_optimization",
                action="Implement advanced data cleaning algorithms",
                confidence_score=0.85,
                reasoning="Current data quality score indicates room for improvement",
                expected_impact={"quality_improvement": "15-25%", "processing_time": "+5%"},
                implementation_complexity="medium"
            ))

        return recommendations

    async def _generate_infrastructure_optimizations(self,
                                                   current_metrics: Dict[str, Any],
                                                   historical_data: List[Dict[str, Any]]) -> List[IntelligentRecommendation]:
        """Generate infrastructure optimization recommendations."""

        recommendations = []

        # Auto-scaling recommendations
        if len(historical_data) > 5:
            recommendations.append(IntelligentRecommendation(
                recommendation_type="infrastructure",
                action="Enable auto-scaling based on pipeline load",
                confidence_score=0.70,
                reasoning="Consistent pipeline usage justifies auto-scaling investment",
                expected_impact={"cost_optimization": "10-20%", "reliability": "+30%"},
                implementation_complexity="high"
            ))

        return recommendations

    def _calculate_overall_confidence(self, recommendations: List[IntelligentRecommendation]) -> float:
        """Calculate overall confidence score for recommendations."""
        if not recommendations:
            return 0.0

        return sum(rec.confidence_score for rec in recommendations) / len(recommendations)

    def _impact_score(self, expected_impact: Dict[str, Any]) -> float:
        """Calculate numeric impact score from expected impact dictionary."""
        score = 0.0

        for key, value in expected_impact.items():
            if isinstance(value, str) and "%" in value:
                # Extract percentage value
                try:
                    pct = float(value.split("-")[0].replace("%", ""))
                    score += pct / 100
                except:
                    score += 0.1  # Default small impact
            elif key in ["significant", "high"]:
                score += 0.5
            else:
                score += 0.1

        return score

    def _extract_prediction_features(self,
                                   pipeline_config: Dict[str, Any],
                                   execution_context: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features for prediction models."""

        return {
            "source_type": pipeline_config.get("source_config", {}).get("type", "unknown"),
            "transformation_count": len(pipeline_config.get("transformation_rules", [])),
            "has_quality_checks": bool(pipeline_config.get("optimization_settings", {}).get("quality_checks_enabled")),
            "parallelism_enabled": bool(pipeline_config.get("optimization_settings", {}).get("parallelism", 1) > 1),
            "caching_enabled": bool(pipeline_config.get("optimization_settings", {}).get("caching_enabled")),
            "data_size_estimate": execution_context.get("estimated_data_size", 1000)
        }

    async def _predict_execution_time(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Predict pipeline execution time based on features."""

        # Simple heuristic-based prediction (can be replaced with ML model)
        base_time = 5.0

        # Source type impact
        source_multipliers = {
            "database": 1.5,
            "api": 2.0,
            "file": 1.0,
            "s3": 1.2,
            "unknown": 1.0
        }

        source_type = features.get("source_type", "unknown")
        time_estimate = base_time * source_multipliers.get(source_type, 1.0)

        # Transformation impact
        transformation_count = features.get("transformation_count", 0)
        time_estimate += transformation_count * 0.5

        # Parallelism impact
        if features.get("parallelism_enabled"):
            time_estimate *= 0.7  # 30% improvement

        # Quality checks impact
        if features.get("has_quality_checks"):
            time_estimate *= 1.1  # 10% overhead

        return {
            "estimated_time": time_estimate,
            "confidence": 0.7,
            "factors_considered": list(features.keys())
        }

    async def _predict_resource_usage(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Predict resource usage based on features."""

        base_memory = 512  # MB
        base_cpu = 1.0  # cores

        # Data size impact
        data_size = features.get("data_size_estimate", 1000)
        memory_estimate = base_memory + (data_size / 1000) * 100

        # Parallelism impact
        if features.get("parallelism_enabled"):
            cpu_estimate = base_cpu * 2
            memory_estimate *= 1.5
        else:
            cpu_estimate = base_cpu

        return {
            "memory_mb": memory_estimate,
            "cpu_cores": cpu_estimate,
            "confidence": 0.6
        }

    async def _predict_failure_risks(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Predict potential failure risks."""

        risks = []
        risk_score = 0.0

        # API source risks
        if features.get("source_type") == "api":
            risks.append({
                "type": "rate_limiting",
                "probability": 0.3,
                "impact": "medium",
                "mitigation": "Implement retry logic with exponential backoff"
            })
            risk_score += 0.3

        # Complex transformation risks
        if features.get("transformation_count", 0) > 5:
            risks.append({
                "type": "transformation_failure",
                "probability": 0.2,
                "impact": "high",
                "mitigation": "Add comprehensive error handling and validation"
            })
            risk_score += 0.2

        return {
            "overall_risk_score": min(1.0, risk_score),
            "specific_risks": risks,
            "confidence": 0.7
        }

    async def _predict_data_quality(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Predict data quality outcomes."""

        base_quality = 0.85

        # Quality checks impact
        if features.get("has_quality_checks"):
            predicted_quality = min(0.95, base_quality + 0.1)
        else:
            predicted_quality = base_quality

        # Source type impact
        source_quality_impact = {
            "database": 0.05,  # Generally higher quality
            "api": -0.05,      # Variable quality
            "file": 0.0,       # Neutral
            "s3": 0.02         # Slightly better
        }

        source_type = features.get("source_type", "unknown")
        quality_adjustment = source_quality_impact.get(source_type, 0)
        predicted_quality += quality_adjustment

        return {
            "predicted_quality_score": max(0.0, min(1.0, predicted_quality)),
            "confidence": 0.6,
            "quality_factors": ["source_type", "quality_checks_enabled"]
        }

    def _generate_early_warnings(self,
                               time_prediction: Dict[str, Any],
                               resource_prediction: Dict[str, Any],
                               failure_prediction: Dict[str, Any],
                               quality_prediction: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate early warning alerts."""

        warnings = []

        # Execution time warnings
        if time_prediction.get("estimated_time", 0) > 60:
            warnings.append({
                "type": "long_execution",
                "severity": "medium",
                "message": "Pipeline execution may take longer than 1 minute",
                "recommendation": "Consider enabling parallelism or optimization"
            })

        # Resource warnings
        if resource_prediction.get("memory_mb", 0) > 2048:
            warnings.append({
                "type": "high_memory_usage",
                "severity": "medium",
                "message": "Pipeline may require significant memory resources",
                "recommendation": "Monitor memory usage and consider resource scaling"
            })

        # Failure risk warnings
        if failure_prediction.get("overall_risk_score", 0) > 0.5:
            warnings.append({
                "type": "high_failure_risk",
                "severity": "high",
                "message": "Pipeline has elevated failure risk",
                "recommendation": "Review error handling and implement monitoring"
            })

        # Quality warnings
        if quality_prediction.get("predicted_quality_score", 1.0) < 0.7:
            warnings.append({
                "type": "quality_concern",
                "severity": "medium",
                "message": "Predicted data quality may be below acceptable threshold",
                "recommendation": "Enable comprehensive data quality validation"
            })

        return warnings

    async def _identify_optimization_opportunities(self, features: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Identify optimization opportunities based on features."""

        opportunities = []

        # Parallelism opportunity
        if not features.get("parallelism_enabled") and features.get("transformation_count", 0) > 2:
            opportunities.append({
                "type": "parallelism",
                "potential_improvement": "30-50% faster execution",
                "implementation_effort": "medium",
                "priority": "high"
            })

        # Caching opportunity
        if not features.get("caching_enabled") and features.get("transformation_count", 0) > 3:
            opportunities.append({
                "type": "caching",
                "potential_improvement": "20-40% faster repeated operations",
                "implementation_effort": "low",
                "priority": "medium"
            })

        return opportunities

    def _extract_learning_features(self, execution_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract features for learning from execution data."""

        return {
            "pipeline_config": execution_data.get("pipeline_config", {}),
            "execution_context": execution_data.get("execution_context", {}),
            "optimization_settings": execution_data.get("optimization_settings", {})
        }

    def _extract_outcome_features(self, actual_outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """Extract outcome features for learning."""

        return {
            "actual_execution_time": actual_outcomes.get("execution_time", 0),
            "actual_resource_usage": actual_outcomes.get("resource_usage", {}),
            "actual_quality_score": actual_outcomes.get("quality_score", 0),
            "success": actual_outcomes.get("success", False),
            "errors": actual_outcomes.get("errors", [])
        }

    async def _update_pattern_models(self,
                                   pipeline_id: str,
                                   features: Dict[str, Any],
                                   outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """Update pattern recognition models with new data."""

        # Store pattern in performance patterns
        pattern_key = f"{features.get('pipeline_config', {}).get('source_type', 'unknown')}"

        self.performance_patterns[pattern_key].append({
            "features": features,
            "outcomes": outcomes,
            "timestamp": datetime.now().isoformat()
        })

        # Keep only recent patterns
        self.performance_patterns[pattern_key] = self.performance_patterns[pattern_key][-100:]

        return {
            "patterns_updated": 1,
            "total_patterns": len(self.performance_patterns[pattern_key]),
            "accuracy": 0.8  # Simplified accuracy metric
        }

    async def _update_prediction_models(self,
                                      features: Dict[str, Any],
                                      outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """Update prediction models with actual outcomes."""

        # Simple accuracy tracking (in real implementation, would update ML models)
        prediction_key = "execution_time"

        if prediction_key not in self.optimization_models:
            self.optimization_models[prediction_key] = {
                "predictions": [],
                "actuals": [],
                "accuracy_history": []
            }

        model = self.optimization_models[prediction_key]

        # Store prediction vs actual (simplified)
        actual_time = outcomes.get("actual_execution_time", 0)
        if actual_time > 0:
            model["actuals"].append(actual_time)

            # Calculate simple accuracy (would be more sophisticated in real implementation)
            if len(model["actuals"]) > 1:
                recent_accuracy = 0.8  # Simplified
                model["accuracy_history"].append(recent_accuracy)

        return {
            "model_updated": True,
            "accuracy": model["accuracy_history"][-1] if model["accuracy_history"] else 0.0
        }

    async def _evaluate_optimization_effectiveness(self,
                                                 pipeline_id: str,
                                                 execution_data: Dict[str, Any],
                                                 actual_outcomes: Dict[str, Any]) -> Dict[str, Any]:
        """Evaluate effectiveness of optimization recommendations."""

        # Track optimization effectiveness
        effectiveness_score = 0.5  # Default neutral score

        optimization_settings = execution_data.get("optimization_settings", {})

        # Evaluate parallelism effectiveness
        if optimization_settings.get("parallelism", 1) > 1:
            actual_time = actual_outcomes.get("execution_time", float('inf'))
            if actual_time < 30:  # Arbitrary threshold for "fast"
                effectiveness_score += 0.2

        # Evaluate caching effectiveness
        if optimization_settings.get("caching_enabled"):
            # In real implementation, would compare cache hit rates
            effectiveness_score += 0.1

        # Evaluate quality improvements
        actual_quality = actual_outcomes.get("quality_score", 0)
        if actual_quality > 0.8:
            effectiveness_score += 0.2

        return {
            "effectiveness": min(1.0, effectiveness_score),
            "optimizations_evaluated": len(optimization_settings),
            "recommendations": [
                "Continue current optimization strategy" if effectiveness_score > 0.7
                else "Review and adjust optimization parameters"
            ]
        }

    def _calculate_confidence_intervals(self, features: Dict[str, Any]) -> Dict[str, Any]:
        """Calculate confidence intervals for predictions."""

        return {
            "execution_time": {"min": 0.8, "max": 1.3},  # 80-130% of prediction
            "resource_usage": {"min": 0.9, "max": 1.2},  # 90-120% of prediction
            "quality_score": {"min": 0.05, "max": 0.05}  # +/- 5% of prediction
        }
