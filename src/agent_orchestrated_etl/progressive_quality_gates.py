"""
Progressive Quality Gates System
Enhanced quality validation that adapts to pipeline complexity and execution patterns.
"""
import json
import logging
import time
from datetime import datetime, timedelta
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Tuple
from dataclasses import dataclass, asdict
import asyncio
from pathlib import Path

logger = logging.getLogger(__name__)


class QualityLevel(Enum):
    BASIC = "basic"
    STANDARD = "standard"
    ADVANCED = "advanced"
    ENTERPRISE = "enterprise"


class GateStatus(Enum):
    PENDING = "pending"
    RUNNING = "running"
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class QualityGateResult:
    gate_id: str
    name: str
    status: GateStatus
    score: float
    threshold: float
    execution_time: float
    details: Dict[str, Any]
    timestamp: datetime
    suggestions: List[str]


@dataclass
class QualityMetrics:
    code_coverage: float
    performance_score: float
    security_score: float
    reliability_score: float
    maintainability_score: float
    complexity_score: float
    test_success_rate: float


class ProgressiveQualityGates:
    """Dynamic quality gates that adapt to pipeline complexity and execution patterns."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = config_path or "config/quality_gates.json"
        self.results_history: List[QualityGateResult] = []
        self.adaptive_thresholds: Dict[str, float] = {}
        self.quality_level = QualityLevel.BASIC
        self.execution_patterns: Dict[str, Any] = {}
        
        # Progressive thresholds based on quality level
        self.level_thresholds = {
            QualityLevel.BASIC: {
                "code_coverage": 70.0,
                "performance_score": 60.0,
                "security_score": 75.0,
                "reliability_score": 65.0,
                "maintainability_score": 60.0,
                "complexity_score": 70.0,
                "test_success_rate": 85.0
            },
            QualityLevel.STANDARD: {
                "code_coverage": 80.0,
                "performance_score": 75.0,
                "security_score": 85.0,
                "reliability_score": 80.0,
                "maintainability_score": 75.0,
                "complexity_score": 80.0,
                "test_success_rate": 90.0
            },
            QualityLevel.ADVANCED: {
                "code_coverage": 90.0,
                "performance_score": 85.0,
                "security_score": 95.0,
                "reliability_score": 90.0,
                "maintainability_score": 85.0,
                "complexity_score": 85.0,
                "test_success_rate": 95.0
            },
            QualityLevel.ENTERPRISE: {
                "code_coverage": 95.0,
                "performance_score": 95.0,
                "security_score": 98.0,
                "reliability_score": 95.0,
                "maintainability_score": 90.0,
                "complexity_score": 90.0,
                "test_success_rate": 98.0
            }
        }
        
        # Initialize adaptive system
        self._load_configuration()
        self._initialize_adaptive_thresholds()
    
    def _load_configuration(self) -> None:
        """Load quality gate configuration with fallback defaults."""
        try:
            if Path(self.config_path).exists():
                with open(self.config_path, 'r') as f:
                    config = json.load(f)
                    self.quality_level = QualityLevel(config.get('quality_level', 'basic'))
                    self.adaptive_thresholds.update(config.get('adaptive_thresholds', {}))
        except Exception as e:
            logger.warning(f"Failed to load quality gates config: {e}")
            self.quality_level = QualityLevel.BASIC
    
    def _initialize_adaptive_thresholds(self) -> None:
        """Initialize adaptive thresholds based on current quality level."""
        base_thresholds = self.level_thresholds[self.quality_level]
        for metric, threshold in base_thresholds.items():
            if metric not in self.adaptive_thresholds:
                self.adaptive_thresholds[metric] = threshold
    
    def analyze_pipeline_complexity(self, pipeline_metadata: Dict[str, Any]) -> QualityLevel:
        """Analyze pipeline complexity to determine appropriate quality level."""
        complexity_score = 0
        
        # Factor 1: Number of operations
        ops_count = len(pipeline_metadata.get('operations', []))
        if ops_count > 20:
            complexity_score += 3
        elif ops_count > 10:
            complexity_score += 2
        elif ops_count > 5:
            complexity_score += 1
        
        # Factor 2: Data volume
        data_volume = pipeline_metadata.get('estimated_data_volume', 0)
        if data_volume > 10_000_000:  # 10M+ records
            complexity_score += 3
        elif data_volume > 1_000_000:  # 1M+ records
            complexity_score += 2
        elif data_volume > 100_000:   # 100K+ records
            complexity_score += 1
        
        # Factor 3: External dependencies
        external_deps = len(pipeline_metadata.get('external_dependencies', []))
        if external_deps > 10:
            complexity_score += 3
        elif external_deps > 5:
            complexity_score += 2
        elif external_deps > 2:
            complexity_score += 1
        
        # Factor 4: Business criticality
        criticality = pipeline_metadata.get('business_criticality', 'low')
        if criticality == 'critical':
            complexity_score += 4
        elif criticality == 'high':
            complexity_score += 2
        elif criticality == 'medium':
            complexity_score += 1
        
        # Determine quality level based on complexity score
        if complexity_score >= 10:
            return QualityLevel.ENTERPRISE
        elif complexity_score >= 7:
            return QualityLevel.ADVANCED
        elif complexity_score >= 4:
            return QualityLevel.STANDARD
        else:
            return QualityLevel.BASIC
    
    async def execute_progressive_gates(self, 
                                      pipeline_metadata: Dict[str, Any],
                                      quality_metrics: QualityMetrics) -> List[QualityGateResult]:
        """Execute progressive quality gates with adaptive thresholds."""
        start_time = time.time()
        results = []
        
        # Auto-adjust quality level based on pipeline complexity
        suggested_level = self.analyze_pipeline_complexity(pipeline_metadata)
        if suggested_level.value != self.quality_level.value:
            logger.info(f"Adjusting quality level from {self.quality_level.value} to {suggested_level.value}")
            self.quality_level = suggested_level
            self._initialize_adaptive_thresholds()
        
        # Progressive gate execution
        gate_definitions = self._get_progressive_gates()
        
        for gate_def in gate_definitions:
            gate_result = await self._execute_single_gate(gate_def, quality_metrics)
            results.append(gate_result)
            
            # Early termination on critical failures
            if gate_result.status == GateStatus.FAILED and gate_def.get('critical', False):
                logger.error(f"Critical gate failed: {gate_result.name}")
                break
        
        # Learn from execution patterns
        self._update_execution_patterns(results, time.time() - start_time)
        self._adapt_thresholds(results)
        
        # Store results for historical analysis
        self.results_history.extend(results)
        self._save_results_history()
        
        return results
    
    def _get_progressive_gates(self) -> List[Dict[str, Any]]:
        """Get progressive gate definitions based on quality level."""
        base_gates = [
            {
                "id": "code_coverage",
                "name": "Code Coverage Analysis",
                "metric": "code_coverage",
                "critical": True,
                "weight": 1.0
            },
            {
                "id": "test_success",
                "name": "Test Success Rate",
                "metric": "test_success_rate",
                "critical": True,
                "weight": 1.0
            },
            {
                "id": "security_scan",
                "name": "Security Vulnerability Scan",
                "metric": "security_score",
                "critical": True,
                "weight": 1.2
            }
        ]
        
        # Add gates based on quality level
        if self.quality_level in [QualityLevel.STANDARD, QualityLevel.ADVANCED, QualityLevel.ENTERPRISE]:
            base_gates.extend([
                {
                    "id": "performance_check",
                    "name": "Performance Benchmarks",
                    "metric": "performance_score",
                    "critical": False,
                    "weight": 0.9
                },
                {
                    "id": "reliability_check",
                    "name": "Reliability Assessment",
                    "metric": "reliability_score",
                    "critical": False,
                    "weight": 0.8
                }
            ])
        
        if self.quality_level in [QualityLevel.ADVANCED, QualityLevel.ENTERPRISE]:
            base_gates.extend([
                {
                    "id": "maintainability",
                    "name": "Code Maintainability",
                    "metric": "maintainability_score",
                    "critical": False,
                    "weight": 0.7
                },
                {
                    "id": "complexity",
                    "name": "Complexity Analysis",
                    "metric": "complexity_score",
                    "critical": False,
                    "weight": 0.6
                }
            ])
        
        return base_gates
    
    async def _execute_single_gate(self, gate_def: Dict[str, Any], 
                                 metrics: QualityMetrics) -> QualityGateResult:
        """Execute a single quality gate with adaptive logic."""
        start_time = time.time()
        gate_id = gate_def["id"]
        metric_name = gate_def["metric"]
        
        # Get current metric value
        metric_value = getattr(metrics, metric_name, 0.0)
        
        # Get adaptive threshold
        threshold = self.adaptive_thresholds.get(metric_name, 70.0)
        
        # Apply weight-based adjustment
        weight = gate_def.get("weight", 1.0)
        adjusted_threshold = threshold * weight
        
        # Determine pass/fail
        passed = metric_value >= adjusted_threshold
        status = GateStatus.PASSED if passed else GateStatus.FAILED
        
        # Generate suggestions for failed gates
        suggestions = []
        if not passed:
            suggestions = self._generate_suggestions(metric_name, metric_value, adjusted_threshold)
        
        execution_time = time.time() - start_time
        
        return QualityGateResult(
            gate_id=gate_id,
            name=gate_def["name"],
            status=status,
            score=metric_value,
            threshold=adjusted_threshold,
            execution_time=execution_time,
            details={
                "metric_name": metric_name,
                "weight": weight,
                "quality_level": self.quality_level.value,
                "trend_analysis": self._analyze_metric_trend(metric_name)
            },
            timestamp=datetime.now(),
            suggestions=suggestions
        )
    
    def _generate_suggestions(self, metric_name: str, current_value: float, 
                            threshold: float) -> List[str]:
        """Generate improvement suggestions for failed gates."""
        gap = threshold - current_value
        suggestions = []
        
        if metric_name == "code_coverage":
            suggestions = [
                f"Increase test coverage by {gap:.1f}% to meet threshold",
                "Focus on testing edge cases and error handling",
                "Consider adding integration tests for complex workflows"
            ]
        elif metric_name == "security_score":
            suggestions = [
                f"Address security issues to improve score by {gap:.1f} points",
                "Run security scanning tools (bandit, safety)",
                "Review input validation and authentication mechanisms"
            ]
        elif metric_name == "performance_score":
            suggestions = [
                f"Optimize performance to improve score by {gap:.1f} points",
                "Profile code for bottlenecks and memory usage",
                "Consider async operations and caching strategies"
            ]
        elif metric_name == "test_success_rate":
            suggestions = [
                f"Fix failing tests to achieve {threshold:.1f}% success rate",
                "Investigate and resolve test instabilities",
                "Review test data dependencies and setup/teardown"
            ]
        
        return suggestions
    
    def _analyze_metric_trend(self, metric_name: str) -> Dict[str, Any]:
        """Analyze historical trends for a metric."""
        recent_results = [r for r in self.results_history[-10:] 
                         if r.details.get("metric_name") == metric_name]
        
        if len(recent_results) < 2:
            return {"trend": "insufficient_data", "direction": "unknown"}
        
        scores = [r.score for r in recent_results]
        trend_direction = "improving" if scores[-1] > scores[0] else "declining"
        
        return {
            "trend": "established",
            "direction": trend_direction,
            "recent_average": sum(scores) / len(scores),
            "volatility": max(scores) - min(scores)
        }
    
    def _update_execution_patterns(self, results: List[QualityGateResult], 
                                 total_time: float) -> None:
        """Update execution patterns for adaptive learning."""
        pattern_key = f"{self.quality_level.value}_{len(results)}"
        
        if pattern_key not in self.execution_patterns:
            self.execution_patterns[pattern_key] = {
                "executions": 0,
                "avg_time": 0.0,
                "success_rate": 0.0
            }
        
        pattern = self.execution_patterns[pattern_key]
        pattern["executions"] += 1
        
        # Update average execution time
        pattern["avg_time"] = (
            (pattern["avg_time"] * (pattern["executions"] - 1) + total_time) 
            / pattern["executions"]
        )
        
        # Update success rate
        passed_gates = sum(1 for r in results if r.status == GateStatus.PASSED)
        success_rate = passed_gates / len(results) if results else 0.0
        pattern["success_rate"] = (
            (pattern["success_rate"] * (pattern["executions"] - 1) + success_rate) 
            / pattern["executions"]
        )
    
    def _adapt_thresholds(self, results: List[QualityGateResult]) -> None:
        """Adapt thresholds based on execution results and trends."""
        for result in results:
            metric_name = result.details.get("metric_name")
            if not metric_name:
                continue
            
            trend = result.details.get("trend_analysis", {})
            
            # Adaptive threshold adjustment logic
            current_threshold = self.adaptive_thresholds.get(metric_name, 70.0)
            
            # If consistently passing, gradually increase threshold
            if (result.status == GateStatus.PASSED and 
                trend.get("direction") == "improving" and
                result.score > current_threshold + 10):
                
                new_threshold = min(current_threshold + 2.0, 98.0)
                self.adaptive_thresholds[metric_name] = new_threshold
                logger.info(f"Increased threshold for {metric_name}: {current_threshold:.1f} -> {new_threshold:.1f}")
            
            # If consistently failing, slightly decrease threshold (with limits)
            elif (result.status == GateStatus.FAILED and 
                  trend.get("direction") == "declining"):
                
                base_threshold = self.level_thresholds[self.quality_level][metric_name]
                min_threshold = base_threshold * 0.9  # Never go below 90% of base
                
                if current_threshold > min_threshold:
                    new_threshold = max(current_threshold - 1.0, min_threshold)
                    self.adaptive_thresholds[metric_name] = new_threshold
                    logger.warning(f"Decreased threshold for {metric_name}: {current_threshold:.1f} -> {new_threshold:.1f}")
    
    def _save_results_history(self) -> None:
        """Save results history for persistence."""
        try:
            history_path = Path("reports/quality_gates_history.json")
            history_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Convert results to serializable format
            serializable_history = []
            for result in self.results_history[-100:]:  # Keep last 100 results
                result_dict = asdict(result)
                result_dict["timestamp"] = result.timestamp.isoformat()
                result_dict["status"] = result.status.value
                serializable_history.append(result_dict)
            
            with open(history_path, 'w') as f:
                json.dump(serializable_history, f, indent=2)
        
        except Exception as e:
            logger.warning(f"Failed to save quality gates history: {e}")
    
    def get_quality_dashboard(self) -> Dict[str, Any]:
        """Generate quality dashboard data."""
        recent_results = self.results_history[-20:]
        
        if not recent_results:
            return {"status": "no_data", "message": "No quality gate executions found"}
        
        # Calculate overall metrics
        total_gates = len(recent_results)
        passed_gates = sum(1 for r in recent_results if r.status == GateStatus.PASSED)
        success_rate = (passed_gates / total_gates) * 100 if total_gates > 0 else 0
        
        # Calculate average scores by metric
        metric_averages = {}
        for result in recent_results:
            metric = result.details.get("metric_name")
            if metric:
                if metric not in metric_averages:
                    metric_averages[metric] = []
                metric_averages[metric].append(result.score)
        
        for metric in metric_averages:
            metric_averages[metric] = sum(metric_averages[metric]) / len(metric_averages[metric])
        
        return {
            "status": "active",
            "quality_level": self.quality_level.value,
            "overall_success_rate": success_rate,
            "total_executions": len(self.results_history),
            "recent_executions": total_gates,
            "metric_averages": metric_averages,
            "adaptive_thresholds": self.adaptive_thresholds.copy(),
            "execution_patterns": self.execution_patterns.copy(),
            "last_execution": recent_results[-1].timestamp.isoformat() if recent_results else None
        }


# Global progressive quality gates instance
progressive_gates = ProgressiveQualityGates()


async def run_progressive_quality_gates(pipeline_metadata: Dict[str, Any],
                                      quality_metrics: QualityMetrics) -> List[QualityGateResult]:
    """Run progressive quality gates for a pipeline."""
    return await progressive_gates.execute_progressive_gates(pipeline_metadata, quality_metrics)


def get_quality_dashboard() -> Dict[str, Any]:
    """Get current quality dashboard data."""
    return progressive_gates.get_quality_dashboard()