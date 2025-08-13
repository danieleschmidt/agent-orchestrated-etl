"""Enhanced data orchestrator with intelligent pipeline management."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from .agents.etl_agent import ETLAgent
from .agents.monitor_agent import MonitorAgent
from .agents.orchestrator_agent import OrchestratorAgent
from .core import primary_data_extraction, transform_data
from .enhanced_validation import EnhancedDataValidator
from .exceptions import PipelineExecutionException
from .logging_config import get_logger
from .orchestrator import load_data
from .security import AuditLogger, SecurityValidator


@dataclass
class PipelineConfig:
    """Configuration for ETL pipeline execution."""
    source_config: Dict[str, Any] = field(default_factory=dict)
    transformation_rules: List[Dict[str, Any]] = field(default_factory=list)
    destination_config: Dict[str, Any] = field(default_factory=dict)
    monitoring_enabled: bool = True
    parallel_execution: bool = False
    max_retries: int = 3
    timeout_seconds: int = 300


class EnhancedDataOrchestrator:
    """Enhanced orchestrator with agent coordination and intelligent pipeline management."""

    def __init__(self, enable_security: bool = True, enable_validation: bool = True):
        self.logger = get_logger("enhanced_orchestrator")
        self.orchestrator_agent = None
        self.etl_agents: Dict[str, ETLAgent] = {}
        self.monitor_agent = None
        self.pipeline_history: List[Dict[str, Any]] = []

        # Security and validation components
        self.security_validator = SecurityValidator() if enable_security else None
        self.data_validator = EnhancedDataValidator() if enable_validation else None
        self.audit_logger = AuditLogger() if enable_security else None

        self.logger.info("Enhanced orchestrator initialized with security and validation")

    async def initialize_agents(self) -> None:
        """Initialize the agent system."""
        try:
            self.orchestrator_agent = OrchestratorAgent()
            self.etl_agents["extraction"] = ETLAgent(specialization="extraction")
            self.etl_agents["transformation"] = ETLAgent(specialization="transformation")
            self.etl_agents["loading"] = ETLAgent(specialization="loading")
            self.monitor_agent = MonitorAgent()

            self.logger.info("Agent system initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize agents: {str(e)}")
            raise PipelineExecutionException(f"Agent initialization failed: {str(e)}")

    def create_pipeline(
        self,
        source: str,
        operations: Optional[Dict[str, Callable]] = None,
        config: Optional[PipelineConfig] = None
    ) -> DataPipeline:
        """Create an intelligent data pipeline."""
        if config is None:
            config = PipelineConfig()

        # Analyze source and set appropriate configuration
        if source.startswith("s3://"):
            config.source_config = {
                "type": "s3",
                "bucket": source.split("/")[2],
                "key": "/".join(source.split("/")[3:]) if len(source.split("/")) > 3 else ""
            }
        elif source.startswith("api://"):
            config.source_config = {
                "type": "api",
                "endpoint": source.replace("api://", "https://")
            }
        elif source.startswith("db://"):
            config.source_config = {
                "type": "database",
                "connection_string": source
            }
        else:
            config.source_config = {
                "type": "file",
                "path": source
            }

        return DataPipeline(self, config, operations)

    async def execute_pipeline(
        self,
        config: PipelineConfig,
        operations: Optional[Dict[str, Callable]] = None
    ) -> Dict[str, Any]:
        """Execute a complete ETL pipeline."""
        execution_id = f"pipeline_{int(time.time())}"
        start_time = time.time()

        try:
            self.logger.info(f"Starting pipeline execution: {execution_id}")

            # Initialize agents if not already done
            if self.orchestrator_agent is None:
                await self.initialize_agents()

            # Step 1: Data Extraction
            extraction_start = time.time()
            extracted_data = primary_data_extraction(source_config=config.source_config)
            extraction_time = time.time() - extraction_start

            self.logger.info(f"Extracted {len(extracted_data)} records in {extraction_time:.2f}s")

            # Step 2: Data Transformation
            transformation_start = time.time()
            if operations and "transform" in operations:
                transformed_data = operations["transform"](extracted_data)
            else:
                transformed_data = transform_data(
                    extracted_data,
                    config.transformation_rules
                )
            transformation_time = time.time() - transformation_start

            self.logger.info(f"Transformed data in {transformation_time:.2f}s")

            # Step 3: Data Loading
            loading_start = time.time()
            if operations and "load" in operations:
                load_result = operations["load"](transformed_data)
            else:
                load_result = load_data(transformed_data, config.destination_config)
            loading_time = time.time() - loading_start

            total_time = time.time() - start_time

            # Prepare execution summary
            result = {
                "execution_id": execution_id,
                "status": "success",
                "total_time": total_time,
                "metrics": {
                    "extraction_time": extraction_time,
                    "transformation_time": transformation_time,
                    "loading_time": loading_time,
                    "records_processed": len(extracted_data),
                    "records_loaded": load_result.get("records_loaded", 0)
                },
                "data": transformed_data[:10] if len(transformed_data) > 10 else transformed_data
            }

            self.pipeline_history.append(result)
            self.logger.info(f"Pipeline {execution_id} completed successfully in {total_time:.2f}s")

            return result

        except Exception as e:
            error_result = {
                "execution_id": execution_id,
                "status": "error",
                "error": str(e),
                "total_time": time.time() - start_time
            }
            self.pipeline_history.append(error_result)
            self.logger.error(f"Pipeline {execution_id} failed: {str(e)}")
            raise PipelineExecutionException(f"Pipeline execution failed: {str(e)}")


class DataPipeline:
    """Represents a configured data pipeline ready for execution."""

    def __init__(
        self,
        orchestrator: EnhancedDataOrchestrator,
        config: PipelineConfig,
        operations: Optional[Dict[str, Callable]] = None
    ):
        self.orchestrator = orchestrator
        self.config = config
        self.operations = operations or {}
        self.logger = get_logger("data_pipeline")

    def execute(self) -> Dict[str, Any]:
        """Execute the pipeline synchronously."""
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        return loop.run_until_complete(
            self.orchestrator.execute_pipeline(self.config, self.operations)
        )

    async def execute_async(self) -> Dict[str, Any]:
        """Execute the pipeline asynchronously."""
        return await self.orchestrator.execute_pipeline(self.config, self.operations)

    def preview_tasks(self) -> List[str]:
        """Preview the tasks that will be executed in this pipeline."""
        tasks = []
        source_type = self.config.source_config.get("type", "unknown")

        tasks.append(f"Extract data from {source_type} source")

        if self.config.transformation_rules:
            tasks.append(f"Apply {len(self.config.transformation_rules)} transformation rules")
        elif "transform" in self.operations:
            tasks.append("Apply custom transformation function")
        else:
            tasks.append("Apply default data transformations")

        if "load" in self.operations:
            tasks.append("Load data using custom function")
        else:
            tasks.append("Load data to configured destination")

        if self.config.monitoring_enabled:
            tasks.append("Monitor pipeline execution")

        return tasks


# Backward compatibility
DataOrchestrator = EnhancedDataOrchestrator
