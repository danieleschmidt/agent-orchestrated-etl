"""Core pipeline service with intelligent orchestration capabilities."""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from ..core import DataQualityValidator, primary_data_extraction, transform_data
from ..exceptions import DataProcessingException, ValidationError
from ..logging_config import get_logger
from ..models.pipeline_models import ExecutionContext, PipelineConfig


class PipelineStatus(Enum):
    """Pipeline execution status enumeration."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"


@dataclass
class PipelineExecution:
    """Represents a single pipeline execution instance."""
    execution_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    pipeline_id: str = ""
    status: PipelineStatus = PipelineStatus.PENDING
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    data_quality_report: Dict[str, Any] = field(default_factory=dict)
    optimization_suggestions: List[str] = field(default_factory=list)


class PipelineService:
    """Advanced pipeline service with AI-driven optimization and monitoring."""

    def __init__(self):
        self.logger = get_logger("agent_etl.services.pipeline")
        self.validator = DataQualityValidator()
        self.active_executions: Dict[str, PipelineExecution] = {}
        self.pipeline_registry: Dict[str, PipelineConfig] = {}
        self.execution_history: List[PipelineExecution] = []
        self.optimization_cache: Dict[str, Dict[str, Any]] = {}

    async def create_pipeline(self,
                            source_config: Dict[str, Any],
                            transformation_rules: Optional[List[Dict[str, Any]]] = None,
                            load_config: Optional[Dict[str, Any]] = None,
                            pipeline_id: Optional[str] = None) -> str:
        """Create an intelligent pipeline with automatic optimization."""

        pipeline_id = pipeline_id or f"pipeline_{uuid.uuid4().hex[:8]}"

        # Analyze source characteristics for optimization
        source_analysis = await self._analyze_source(source_config)

        # Generate optimized configuration
        optimized_config = self._generate_optimized_config(
            source_config,
            transformation_rules or [],
            load_config or {},
            source_analysis
        )

        # Create pipeline configuration
        pipeline_config = PipelineConfig(
            pipeline_id=pipeline_id,
            source_config=source_config,
            transformation_rules=transformation_rules or [],
            load_config=load_config or {},
            optimization_settings=optimized_config,
            created_at=datetime.now()
        )

        self.pipeline_registry[pipeline_id] = pipeline_config

        self.logger.info(f"Created pipeline {pipeline_id} with optimized configuration")
        return pipeline_id

    async def execute_pipeline(self,
                             pipeline_id: str,
                             execution_context: Optional[ExecutionContext] = None) -> PipelineExecution:
        """Execute pipeline with comprehensive monitoring and error handling."""

        if pipeline_id not in self.pipeline_registry:
            raise ValidationError(f"Pipeline {pipeline_id} not found")

        config = self.pipeline_registry[pipeline_id]
        execution = PipelineExecution(pipeline_id=pipeline_id)
        execution.start_time = datetime.now()
        execution.status = PipelineStatus.RUNNING

        self.active_executions[execution.execution_id] = execution

        try:
            # Phase 1: Data Extraction with intelligent retry
            self.logger.info(f"Starting extraction phase for pipeline {pipeline_id}")
            extracted_data = await self._execute_extraction_phase(config, execution)

            # Phase 2: Data Quality Validation
            self.logger.info(f"Starting validation phase for pipeline {pipeline_id}")
            quality_report = await self._execute_validation_phase(extracted_data, execution)
            execution.data_quality_report = quality_report

            # Phase 3: Data Transformation with optimization
            self.logger.info(f"Starting transformation phase for pipeline {pipeline_id}")
            transformed_data = await self._execute_transformation_phase(
                extracted_data, config, execution
            )

            # Phase 4: Data Loading with performance monitoring
            self.logger.info(f"Starting loading phase for pipeline {pipeline_id}")
            load_result = await self._execute_loading_phase(
                transformed_data, config, execution
            )

            # Phase 5: Generate optimization suggestions
            optimization_suggestions = self._generate_optimization_suggestions(execution)
            execution.optimization_suggestions = optimization_suggestions

            execution.status = PipelineStatus.COMPLETED
            execution.end_time = datetime.now()

            self.logger.info(f"Pipeline {pipeline_id} completed successfully")

        except Exception as e:
            execution.status = PipelineStatus.FAILED
            execution.end_time = datetime.now()
            execution.errors.append(str(e))

            self.logger.error(f"Pipeline {pipeline_id} failed: {e}")

            # Attempt intelligent error recovery
            if await self._should_retry(execution, e):
                execution.status = PipelineStatus.RETRYING
                self.logger.info(f"Retrying pipeline {pipeline_id}")
                return await self.execute_pipeline(pipeline_id, execution_context)

        finally:
            # Clean up active execution
            if execution.execution_id in self.active_executions:
                del self.active_executions[execution.execution_id]

            # Add to history
            self.execution_history.append(execution)

            # Update optimization cache
            self._update_optimization_cache(pipeline_id, execution)

        return execution

    async def _analyze_source(self, source_config: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze data source characteristics for optimization."""
        analysis = {
            "source_type": source_config.get("type", "unknown"),
            "estimated_size": "unknown",
            "data_patterns": [],
            "recommended_batch_size": 1000,
            "recommended_parallelism": 1
        }

        try:
            # Sample data to understand patterns
            sample_data = primary_data_extraction(source_config=source_config)

            if sample_data:
                analysis["estimated_size"] = len(sample_data)
                analysis["data_patterns"] = self._detect_data_patterns(sample_data)
                analysis["recommended_batch_size"] = min(10000, max(100, len(sample_data) // 10))

                # Determine optimal parallelism based on data complexity
                complexity_score = self._calculate_data_complexity(sample_data)
                analysis["recommended_parallelism"] = min(4, max(1, complexity_score // 2))

        except Exception as e:
            self.logger.warning(f"Source analysis failed: {e}")

        return analysis

    def _generate_optimized_config(self,
                                 source_config: Dict[str, Any],
                                 transformation_rules: List[Dict[str, Any]],
                                 load_config: Dict[str, Any],
                                 source_analysis: Dict[str, Any]) -> Dict[str, Any]:
        """Generate optimized pipeline configuration based on analysis."""

        return {
            "batch_size": source_analysis.get("recommended_batch_size", 1000),
            "parallelism": source_analysis.get("recommended_parallelism", 1),
            "memory_optimization": True,
            "caching_enabled": len(transformation_rules) > 3,
            "compression_enabled": source_analysis.get("estimated_size", 0) > 10000,
            "quality_checks_enabled": True,
            "performance_monitoring": True,
            "error_recovery_enabled": True,
            "optimization_hints": source_analysis.get("data_patterns", [])
        }

    async def _execute_extraction_phase(self,
                                      config: PipelineConfig,
                                      execution: PipelineExecution) -> List[Dict[str, Any]]:
        """Execute data extraction with intelligent error handling."""
        start_time = time.time()

        try:
            # Apply optimization settings
            batch_size = config.optimization_settings.get("batch_size", 1000)

            if config.source_config.get("type") == "batch":
                # Handle large datasets in batches
                data = await self._extract_in_batches(config.source_config, batch_size)
            else:
                # Standard extraction
                data = primary_data_extraction(source_config=config.source_config)

            extraction_time = time.time() - start_time
            execution.metrics["extraction_time"] = extraction_time
            execution.metrics["records_extracted"] = len(data) if data else 0

            self.logger.info(f"Extracted {len(data) if data else 0} records in {extraction_time:.2f}s")
            return data

        except Exception as e:
            execution.errors.append(f"Extraction failed: {e}")
            raise DataProcessingException(f"Extraction phase failed: {e}") from e

    async def _execute_validation_phase(self,
                                      data: List[Dict[str, Any]],
                                      execution: PipelineExecution) -> Dict[str, Any]:
        """Execute comprehensive data quality validation."""
        start_time = time.time()

        try:
            # Perform data quality validation
            quality_report = self.validator.validate_data_quality(data)

            validation_time = time.time() - start_time
            execution.metrics["validation_time"] = validation_time
            execution.metrics["quality_score"] = quality_report.get("quality_score", 0.0)

            # Log quality issues if any
            if quality_report.get("issues"):
                for issue in quality_report["issues"]:
                    self.logger.warning(f"Data quality issue: {issue}")

            return quality_report

        except Exception as e:
            execution.errors.append(f"Validation failed: {e}")
            raise DataProcessingException(f"Validation phase failed: {e}") from e

    async def _execute_transformation_phase(self,
                                          data: List[Dict[str, Any]],
                                          config: PipelineConfig,
                                          execution: PipelineExecution) -> List[Dict[str, Any]]:
        """Execute data transformation with optimization."""
        start_time = time.time()

        try:
            # Apply transformations with caching if enabled
            if config.optimization_settings.get("caching_enabled"):
                cache_key = self._generate_cache_key(config.transformation_rules)
                if cache_key in self.optimization_cache:
                    self.logger.info("Using cached transformation results")
                    return self.optimization_cache[cache_key]["data"]

            # Execute transformations
            transformed_data = transform_data(data, transformation_rules=config.transformation_rules)

            transformation_time = time.time() - start_time
            execution.metrics["transformation_time"] = transformation_time
            execution.metrics["records_transformed"] = len(transformed_data) if transformed_data else 0

            # Cache results if enabled
            if config.optimization_settings.get("caching_enabled"):
                cache_key = self._generate_cache_key(config.transformation_rules)
                self.optimization_cache[cache_key] = {
                    "data": transformed_data,
                    "timestamp": datetime.now()
                }

            return transformed_data

        except Exception as e:
            execution.errors.append(f"Transformation failed: {e}")
            raise DataProcessingException(f"Transformation phase failed: {e}") from e

    async def _execute_loading_phase(self,
                                   data: List[Dict[str, Any]],
                                   config: PipelineConfig,
                                   execution: PipelineExecution) -> Dict[str, Any]:
        """Execute data loading with performance monitoring."""
        start_time = time.time()

        try:
            # Default loading (can be extended with specific loaders)
            load_config = config.load_config or {}
            destination = load_config.get("destination", "memory")

            if destination == "file":
                result = await self._load_to_file(data, load_config)
            elif destination == "database":
                result = await self._load_to_database(data, load_config)
            elif destination == "s3":
                result = await self._load_to_s3(data, load_config)
            else:
                # Default memory storage
                result = {"destination": "memory", "records_loaded": len(data)}

            loading_time = time.time() - start_time
            execution.metrics["loading_time"] = loading_time
            execution.metrics["records_loaded"] = result.get("records_loaded", 0)

            return result

        except Exception as e:
            execution.errors.append(f"Loading failed: {e}")
            raise DataProcessingException(f"Loading phase failed: {e}") from e

    def _detect_data_patterns(self, data: List[Dict[str, Any]]) -> List[str]:
        """Detect common data patterns for optimization."""
        patterns = []

        if not data:
            return patterns

        sample = data[0] if data else {}

        # Check for timestamp fields
        for key, value in sample.items():
            if isinstance(value, str) and any(keyword in key.lower() for keyword in ['time', 'date', 'created', 'updated']):
                patterns.append("temporal_data")
                break

        # Check for nested data
        if any(isinstance(value, (dict, list)) for value in sample.values()):
            patterns.append("nested_data")

        # Check for high cardinality fields
        if len(data) > 100:
            for key in sample.keys():
                unique_values = len(set(record.get(key) for record in data[:100] if record.get(key) is not None))
                if unique_values > 50:
                    patterns.append("high_cardinality")
                    break

        return patterns

    def _calculate_data_complexity(self, data: List[Dict[str, Any]]) -> int:
        """Calculate data complexity score for optimization decisions."""
        if not data:
            return 1

        complexity_score = 1
        sample = data[0] if data else {}

        # Field count complexity
        field_count = len(sample)
        complexity_score += min(3, field_count // 5)

        # Nested data complexity
        nested_fields = sum(1 for value in sample.values() if isinstance(value, (dict, list)))
        complexity_score += nested_fields * 2

        # Data type diversity
        data_types = set(type(value).__name__ for value in sample.values())
        complexity_score += len(data_types)

        return complexity_score

    async def _extract_in_batches(self, source_config: Dict[str, Any], batch_size: int) -> List[Dict[str, Any]]:
        """Extract data in batches for large datasets."""
        # This is a simplified implementation - in reality would handle pagination/batching
        data = primary_data_extraction(source_config=source_config)
        return data

    async def _load_to_file(self, data: List[Dict[str, Any]], load_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to file destination."""
        import json

        file_path = load_config.get("file_path", "output.json")
        file_format = load_config.get("format", "json")

        try:
            if file_format == "json":
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)
            else:
                # Default to JSON
                with open(file_path, 'w') as f:
                    json.dump(data, f, indent=2, default=str)

            return {
                "destination": "file",
                "file_path": file_path,
                "records_loaded": len(data)
            }
        except Exception as e:
            raise DataProcessingException(f"File loading failed: {e}")

    async def _load_to_database(self, data: List[Dict[str, Any]], load_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to database destination."""
        # Simplified implementation - would use actual database connections
        return {
            "destination": "database",
            "table": load_config.get("table", "default_table"),
            "records_loaded": len(data)
        }

    async def _load_to_s3(self, data: List[Dict[str, Any]], load_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to S3 destination."""
        # Simplified implementation - would use boto3
        return {
            "destination": "s3",
            "bucket": load_config.get("bucket", "default-bucket"),
            "key": load_config.get("key", "data.json"),
            "records_loaded": len(data)
        }

    def _generate_cache_key(self, transformation_rules: List[Dict[str, Any]]) -> str:
        """Generate cache key for transformation rules."""
        import hashlib
        import json

        rules_str = json.dumps(transformation_rules, sort_keys=True)
        return hashlib.md5(rules_str.encode()).hexdigest()

    def _generate_optimization_suggestions(self, execution: PipelineExecution) -> List[str]:
        """Generate intelligent optimization suggestions based on execution metrics."""
        suggestions = []
        metrics = execution.metrics

        # Performance suggestions
        if metrics.get("extraction_time", 0) > 10:
            suggestions.append("Consider increasing batch size or implementing parallel extraction")

        if metrics.get("transformation_time", 0) > 5:
            suggestions.append("Enable caching for repeated transformation operations")

        if metrics.get("quality_score", 1.0) < 0.8:
            suggestions.append("Implement data quality improvements at the source")

        # Resource optimization suggestions
        total_time = sum([
            metrics.get("extraction_time", 0),
            metrics.get("transformation_time", 0),
            metrics.get("loading_time", 0)
        ])

        if total_time > 30:
            suggestions.append("Consider implementing pipeline parallelization")

        return suggestions

    async def _should_retry(self, execution: PipelineExecution, error: Exception) -> bool:
        """Intelligent retry decision based on error type and execution history."""
        # Simple retry logic - can be enhanced with ML-based decisions
        retry_count = len([e for e in execution.errors if "retry" in e.lower()])

        if retry_count >= 3:
            return False

        # Retry for transient errors
        transient_errors = ["timeout", "connection", "temporary", "rate limit"]
        error_str = str(error).lower()

        return any(err_type in error_str for err_type in transient_errors)

    def _update_optimization_cache(self, pipeline_id: str, execution: PipelineExecution):
        """Update optimization cache with execution learnings."""
        cache_key = f"pipeline_{pipeline_id}"

        if cache_key not in self.optimization_cache:
            self.optimization_cache[cache_key] = {
                "executions": [],
                "avg_metrics": {},
                "optimization_history": []
            }

        cache_entry = self.optimization_cache[cache_key]
        cache_entry["executions"].append({
            "execution_id": execution.execution_id,
            "status": execution.status.value,
            "metrics": execution.metrics,
            "timestamp": execution.start_time
        })

        # Keep only last 10 executions
        cache_entry["executions"] = cache_entry["executions"][-10:]

    def get_pipeline_status(self, pipeline_id: str) -> Dict[str, Any]:
        """Get current status and metrics for a pipeline."""
        if pipeline_id not in self.pipeline_registry:
            raise ValidationError(f"Pipeline {pipeline_id} not found")

        config = self.pipeline_registry[pipeline_id]

        # Get recent executions
        recent_executions = [
            exec for exec in self.execution_history
            if exec.pipeline_id == pipeline_id
        ][-5:]  # Last 5 executions

        return {
            "pipeline_id": pipeline_id,
            "created_at": config.created_at.isoformat(),
            "total_executions": len([e for e in self.execution_history if e.pipeline_id == pipeline_id]),
            "recent_executions": [
                {
                    "execution_id": e.execution_id,
                    "status": e.status.value,
                    "start_time": e.start_time.isoformat() if e.start_time else None,
                    "end_time": e.end_time.isoformat() if e.end_time else None,
                    "quality_score": e.data_quality_report.get("quality_score", 0)
                }
                for e in recent_executions
            ],
            "optimization_suggestions": recent_executions[-1].optimization_suggestions if recent_executions else []
        }

    def list_active_executions(self) -> List[Dict[str, Any]]:
        """List all currently active pipeline executions."""
        return [
            {
                "execution_id": execution.execution_id,
                "pipeline_id": execution.pipeline_id,
                "status": execution.status.value,
                "start_time": execution.start_time.isoformat() if execution.start_time else None,
                "duration": (datetime.now() - execution.start_time).total_seconds() if execution.start_time else 0
            }
            for execution in self.active_executions.values()
        ]

    def cancel_execution(self, execution_id: str) -> bool:
        """Cancel an active pipeline execution."""
        if execution_id in self.active_executions:
            execution = self.active_executions[execution_id]
            execution.status = PipelineStatus.CANCELLED
            execution.end_time = datetime.now()
            return True
        return False
