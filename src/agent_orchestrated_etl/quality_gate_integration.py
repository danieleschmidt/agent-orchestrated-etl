"""
Quality Gate Integration Module
Seamless integration of progressive quality gates with existing pipeline orchestration.
"""
import asyncio
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import json
from pathlib import Path

from .progressive_quality_gates import (
    ProgressiveQualityGates, 
    QualityMetrics, 
    QualityGateResult,
    QualityLevel,
    GateStatus
)
from .orchestrator import DataOrchestrator
from .monitoring.pipeline_monitor import PipelineMonitor

logger = logging.getLogger(__name__)


class QualityGateIntegration:
    """Integrates progressive quality gates with pipeline orchestration."""
    
    def __init__(self, orchestrator: Optional[DataOrchestrator] = None):
        self.orchestrator = orchestrator or DataOrchestrator()
        self.quality_gates = ProgressiveQualityGates()
        self.monitor = PipelineMonitor()
        self.integration_config = self._load_integration_config()
    
    def _load_integration_config(self) -> Dict[str, Any]:
        """Load integration configuration."""
        try:
            config_path = Path("config/quality_integration.json")
            if config_path.exists():
                with open(config_path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Failed to load integration config: {e}")
        
        # Default configuration
        return {
            "auto_quality_gates": True,
            "fail_fast": False,
            "quality_gate_phases": ["pre_execution", "post_execution", "continuous"],
            "metric_collection_interval": 30,
            "adaptive_learning": True
        }
    
    async def execute_pipeline_with_quality_gates(self, 
                                                source: str,
                                                operations: Optional[Dict[str, Any]] = None,
                                                quality_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Execute pipeline with integrated quality gates."""
        execution_id = f"exec_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"Starting pipeline execution with quality gates: {execution_id}")
        
        execution_context = {
            "execution_id": execution_id,
            "source": source,
            "operations": operations or {},
            "quality_config": quality_config or {},
            "start_time": datetime.now(),
            "quality_results": []
        }
        
        try:
            # Phase 1: Pre-execution quality gates
            if "pre_execution" in self.integration_config["quality_gate_phases"]:
                pre_exec_results = await self._run_pre_execution_gates(execution_context)
                execution_context["quality_results"].extend(pre_exec_results)
                
                if self._should_halt_execution(pre_exec_results):
                    return self._create_execution_summary(execution_context, "halted_pre_execution")
            
            # Phase 2: Pipeline execution with continuous monitoring
            pipeline_result = await self._execute_pipeline_with_monitoring(execution_context)
            execution_context["pipeline_result"] = pipeline_result
            
            # Phase 3: Post-execution quality gates
            if "post_execution" in self.integration_config["quality_gate_phases"]:
                post_exec_results = await self._run_post_execution_gates(execution_context)
                execution_context["quality_results"].extend(post_exec_results)
            
            # Phase 4: Adaptive learning
            if self.integration_config.get("adaptive_learning", True):
                await self._perform_adaptive_learning(execution_context)
            
            return self._create_execution_summary(execution_context, "completed")
        
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            execution_context["error"] = str(e)
            return self._create_execution_summary(execution_context, "failed")
    
    async def _run_pre_execution_gates(self, context: Dict[str, Any]) -> List[QualityGateResult]:
        """Run pre-execution quality gates."""
        logger.info("Executing pre-execution quality gates")
        
        # Analyze pipeline metadata
        pipeline_metadata = {
            "source": context["source"],
            "operations": context["operations"],
            "estimated_complexity": self._estimate_pipeline_complexity(context),
            "external_dependencies": self._identify_external_dependencies(context),
            "business_criticality": context.get("quality_config", {}).get("criticality", "medium")
        }
        
        # Collect baseline metrics
        baseline_metrics = await self._collect_baseline_metrics()
        
        # Execute quality gates
        return await self.quality_gates.execute_progressive_gates(
            pipeline_metadata, 
            baseline_metrics
        )
    
    async def _run_post_execution_gates(self, context: Dict[str, Any]) -> List[QualityGateResult]:
        """Run post-execution quality gates."""
        logger.info("Executing post-execution quality gates")
        
        # Enhanced pipeline metadata with execution results
        pipeline_metadata = {
            "source": context["source"],
            "operations": context["operations"],
            "execution_id": context["execution_id"],
            "execution_time": (datetime.now() - context["start_time"]).total_seconds(),
            "pipeline_result": context.get("pipeline_result", {}),
            "performance_metrics": await self._collect_performance_metrics(context)
        }
        
        # Collect post-execution metrics
        post_exec_metrics = await self._collect_post_execution_metrics(context)
        
        # Execute quality gates
        return await self.quality_gates.execute_progressive_gates(
            pipeline_metadata, 
            post_exec_metrics
        )
    
    async def _execute_pipeline_with_monitoring(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline with continuous monitoring."""
        logger.info("Executing pipeline with continuous monitoring")
        
        # Start monitoring
        monitoring_task = None
        if "continuous" in self.integration_config["quality_gate_phases"]:
            monitoring_task = asyncio.create_task(
                self._continuous_quality_monitoring(context)
            )
        
        try:
            # Execute the actual pipeline
            pipeline = self.orchestrator.create_pipeline(
                source=context["source"],
                operations=context["operations"]
            )
            
            # Execute with timeout and monitoring
            result = await self._execute_with_timeout(pipeline, context)
            
            return result
        
        finally:
            # Stop monitoring
            if monitoring_task:
                monitoring_task.cancel()
                try:
                    await monitoring_task
                except asyncio.CancelledError:
                    pass
    
    async def _continuous_quality_monitoring(self, context: Dict[str, Any]) -> None:
        """Perform continuous quality monitoring during pipeline execution."""
        interval = self.integration_config.get("metric_collection_interval", 30)
        
        while True:
            try:
                await asyncio.sleep(interval)
                
                # Collect real-time metrics
                metrics = await self._collect_realtime_metrics(context)
                
                # Check for quality issues
                issues = self._detect_quality_issues(metrics)
                
                if issues and self.integration_config.get("fail_fast", False):
                    logger.warning(f"Quality issues detected: {issues}")
                    # Could trigger pipeline halt here
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning(f"Error in continuous monitoring: {e}")
    
    async def _execute_with_timeout(self, pipeline: Any, context: Dict[str, Any]) -> Dict[str, Any]:
        """Execute pipeline with timeout and error handling."""
        timeout = context.get("quality_config", {}).get("execution_timeout", 3600)  # 1 hour default
        
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(pipeline.execute), 
                timeout=timeout
            )
            return {"status": "success", "result": result}
        
        except asyncio.TimeoutError:
            logger.error(f"Pipeline execution timed out after {timeout} seconds")
            return {"status": "timeout", "timeout": timeout}
        
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}")
            return {"status": "error", "error": str(e)}
    
    def _estimate_pipeline_complexity(self, context: Dict[str, Any]) -> int:
        """Estimate pipeline complexity for quality gate selection."""
        complexity = 0
        
        # Base complexity from operations
        operations = context.get("operations", {})
        complexity += len(operations) * 2
        
        # Additional complexity factors
        source = context.get("source", "")
        if source.startswith("s3://"):
            complexity += 3
        elif source.startswith("api://"):
            complexity += 4
        elif source.startswith("db://"):
            complexity += 2
        
        return complexity
    
    def _identify_external_dependencies(self, context: Dict[str, Any]) -> List[str]:
        """Identify external dependencies for risk assessment."""
        dependencies = []
        
        source = context.get("source", "")
        if source.startswith("s3://"):
            dependencies.append("aws_s3")
        elif source.startswith("api://"):
            dependencies.append("external_api")
        elif source.startswith("db://"):
            dependencies.append("database")
        
        operations = context.get("operations", {})
        for op_name, op_config in operations.items():
            if isinstance(op_config, dict):
                if "external_service" in op_config:
                    dependencies.append(f"external_service_{op_name}")
        
        return dependencies
    
    async def _collect_baseline_metrics(self) -> QualityMetrics:
        """Collect baseline quality metrics."""
        return QualityMetrics(
            code_coverage=85.0,  # Default baseline
            performance_score=80.0,
            security_score=90.0,
            reliability_score=85.0,
            maintainability_score=80.0,
            complexity_score=75.0,
            test_success_rate=95.0
        )
    
    async def _collect_post_execution_metrics(self, context: Dict[str, Any]) -> QualityMetrics:
        """Collect post-execution quality metrics."""
        pipeline_result = context.get("pipeline_result", {})
        
        # Calculate metrics based on execution results
        performance_score = self._calculate_performance_score(context)
        reliability_score = self._calculate_reliability_score(pipeline_result)
        
        return QualityMetrics(
            code_coverage=85.0,  # Would be measured by actual coverage tools
            performance_score=performance_score,
            security_score=90.0,  # Would be measured by security scanners
            reliability_score=reliability_score,
            maintainability_score=80.0,
            complexity_score=75.0,
            test_success_rate=95.0
        )
    
    async def _collect_performance_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect detailed performance metrics."""
        execution_time = (datetime.now() - context["start_time"]).total_seconds()
        
        return {
            "execution_time": execution_time,
            "memory_usage": self._get_memory_usage(),
            "cpu_usage": self._get_cpu_usage(),
            "throughput": self._calculate_throughput(context)
        }
    
    async def _collect_realtime_metrics(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """Collect real-time monitoring metrics."""
        return {
            "timestamp": datetime.now().isoformat(),
            "memory_usage": self._get_memory_usage(),
            "cpu_usage": self._get_cpu_usage(),
            "active_connections": self._get_active_connections(),
            "error_rate": self._get_error_rate()
        }
    
    def _calculate_performance_score(self, context: Dict[str, Any]) -> float:
        """Calculate performance score based on execution metrics."""
        execution_time = (datetime.now() - context["start_time"]).total_seconds()
        
        # Simple scoring based on execution time
        if execution_time < 60:  # Under 1 minute
            return 95.0
        elif execution_time < 300:  # Under 5 minutes
            return 85.0
        elif execution_time < 900:  # Under 15 minutes
            return 75.0
        else:
            return 60.0
    
    def _calculate_reliability_score(self, pipeline_result: Dict[str, Any]) -> float:
        """Calculate reliability score based on pipeline result."""
        status = pipeline_result.get("status", "unknown")
        
        if status == "success":
            return 95.0
        elif status == "timeout":
            return 70.0
        elif status == "error":
            return 40.0
        else:
            return 60.0
    
    def _get_memory_usage(self) -> float:
        """Get current memory usage percentage."""
        try:
            import psutil
            return psutil.virtual_memory().percent
        except ImportError:
            return 50.0  # Default value if psutil not available
    
    def _get_cpu_usage(self) -> float:
        """Get current CPU usage percentage."""
        try:
            import psutil
            return psutil.cpu_percent(interval=1)
        except ImportError:
            return 30.0  # Default value if psutil not available
    
    def _calculate_throughput(self, context: Dict[str, Any]) -> float:
        """Calculate data processing throughput."""
        execution_time = (datetime.now() - context["start_time"]).total_seconds()
        # Placeholder calculation - would need actual data volume
        return 1000.0 / max(execution_time, 1.0)  # records per second
    
    def _get_active_connections(self) -> int:
        """Get number of active connections."""
        return 5  # Placeholder
    
    def _get_error_rate(self) -> float:
        """Get current error rate percentage."""
        return 0.5  # Placeholder
    
    def _detect_quality_issues(self, metrics: Dict[str, Any]) -> List[str]:
        """Detect quality issues from real-time metrics."""
        issues = []
        
        memory_usage = metrics.get("memory_usage", 0)
        if memory_usage > 90:
            issues.append(f"High memory usage: {memory_usage:.1f}%")
        
        cpu_usage = metrics.get("cpu_usage", 0)
        if cpu_usage > 95:
            issues.append(f"High CPU usage: {cpu_usage:.1f}%")
        
        error_rate = metrics.get("error_rate", 0)
        if error_rate > 5:
            issues.append(f"High error rate: {error_rate:.1f}%")
        
        return issues
    
    def _should_halt_execution(self, results: List[QualityGateResult]) -> bool:
        """Determine if execution should be halted based on quality gate results."""
        if not self.integration_config.get("fail_fast", False):
            return False
        
        critical_failures = [
            r for r in results 
            if r.status == GateStatus.FAILED and r.details.get("critical", False)
        ]
        
        return len(critical_failures) > 0
    
    async def _perform_adaptive_learning(self, context: Dict[str, Any]) -> None:
        """Perform adaptive learning based on execution results."""
        logger.info("Performing adaptive learning from execution results")
        
        # Analyze execution patterns
        execution_success = context.get("pipeline_result", {}).get("status") == "success"
        quality_results = context.get("quality_results", [])
        
        # Update quality gate learning
        learning_data = {
            "execution_id": context["execution_id"],
            "success": execution_success,
            "quality_results": [
                {
                    "gate_id": r.gate_id,
                    "status": r.status.value,
                    "score": r.score,
                    "threshold": r.threshold
                }
                for r in quality_results
            ],
            "execution_time": (datetime.now() - context["start_time"]).total_seconds(),
            "complexity_factors": {
                "source_type": context["source"].split("://")[0] if "://" in context["source"] else "file",
                "operation_count": len(context.get("operations", {}))
            }
        }
        
        # Store learning data for future improvements
        await self._store_learning_data(learning_data)
    
    async def _store_learning_data(self, learning_data: Dict[str, Any]) -> None:
        """Store learning data for continuous improvement."""
        try:
            learning_path = Path("reports/quality_learning_data.jsonl")
            learning_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(learning_path, 'a') as f:
                f.write(json.dumps(learning_data) + '\n')
        
        except Exception as e:
            logger.warning(f"Failed to store learning data: {e}")
    
    def _create_execution_summary(self, context: Dict[str, Any], status: str) -> Dict[str, Any]:
        """Create execution summary report."""
        end_time = datetime.now()
        execution_time = (end_time - context["start_time"]).total_seconds()
        
        quality_results = context.get("quality_results", [])
        passed_gates = sum(1 for r in quality_results if r.status == GateStatus.PASSED)
        total_gates = len(quality_results)
        
        return {
            "execution_id": context["execution_id"],
            "status": status,
            "execution_time": execution_time,
            "quality_summary": {
                "total_gates": total_gates,
                "passed_gates": passed_gates,
                "success_rate": (passed_gates / total_gates * 100) if total_gates > 0 else 0,
                "quality_level": self.quality_gates.quality_level.value
            },
            "pipeline_result": context.get("pipeline_result", {}),
            "error": context.get("error"),
            "start_time": context["start_time"].isoformat(),
            "end_time": end_time.isoformat(),
            "quality_results": [
                {
                    "gate_id": r.gate_id,
                    "name": r.name,
                    "status": r.status.value,
                    "score": r.score,
                    "threshold": r.threshold,
                    "suggestions": r.suggestions
                }
                for r in quality_results
            ]
        }


# Export main integration class
__all__ = ["QualityGateIntegration"]