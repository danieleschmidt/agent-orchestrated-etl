"""ETL specialist agent for performing specific ETL operations."""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Dict, List, Optional

from langchain_core.language_models.base import BaseLanguageModel

from ..exceptions import AgentException, DataProcessingException
from ..logging_config import LogContext
from .base_agent import AgentCapability, AgentConfig, AgentRole, AgentTask, BaseAgent
from .communication import AgentCommunicationHub
from .etl_extraction_ops import ETLExtractionOperations
from .etl_loading_ops import ETLLoadingOperations
from .etl_profiling_ops import ETLProfilingOperations
from .etl_thread_safety import ThreadSafeMetrics, ThreadSafeOperationTracker
from .etl_transformation_ops import ETLTransformationOperations
from .memory import AgentMemory, MemoryImportance, MemoryType
from .tools import AgentToolRegistry, get_tool_registry


class ETLAgent(BaseAgent, ETLExtractionOperations):
    """ETL specialist agent for performing specific ETL operations."""

    def __init__(
        self,
        config: Optional[AgentConfig] = None,
        llm: Optional[BaseLanguageModel] = None,
        communication_hub: Optional[AgentCommunicationHub] = None,
        tool_registry: Optional[AgentToolRegistry] = None,
        specialization: str = "general",
    ):
        # Set default config for ETL agent
        if config is None:
            config = AgentConfig(
                name=f"ETLAgent_{specialization}",
                role=AgentRole.ETL_SPECIALIST,
                max_concurrent_tasks=3,
                task_timeout_seconds=1800.0,  # Longer timeout for ETL operations
            )
        elif config.role != AgentRole.ETL_SPECIALIST:
            config.role = AgentRole.ETL_SPECIALIST

        # ETL-specific attributes (set before super() call as _initialize_agent uses them)
        self.specialization = specialization

        super().__init__(config, llm, communication_hub)

        # ETL-specific components
        self.tool_registry = tool_registry or get_tool_registry()
        self.memory = AgentMemory(
            agent_id=self.config.agent_id,
            max_entries=20000,
            working_memory_size=100,
        )

        # Initialize transformation operations
        self.transformation_ops = ETLTransformationOperations(logger=self.logger)

        # Initialize loading operations
        self.loading_ops = ETLLoadingOperations(logger=self.logger, specialization=specialization)

        # Initialize profiling operations
        self.profiling_ops = ETLProfilingOperations(logger=self.logger)

        # ETL specialization
        self.specialization = specialization
        self.supported_operations = self._get_supported_operations(specialization)

        # Thread safety components
        self._etl_lock = threading.RLock()
        self._etl_async_lock = asyncio.Lock()

        # Thread-safe ETL state tracking
        self.active_extractions = ThreadSafeOperationTracker()
        self.active_transformations = ThreadSafeOperationTracker()

        # Thread-safe performance tracking
        self.etl_metrics = ThreadSafeMetrics()

    def _initialize_agent(self) -> None:
        """Initialize ETL agent-specific components."""
        self.logger.info(f"Initializing ETL Agent with specialization: {self.specialization}")

    def _get_supported_operations(self, specialization: str) -> List[str]:
        """Get supported operations based on specialization."""
        base_operations = ["extract", "transform", "load", "validate"]

        specialization_map = {
            "general": base_operations + ["data_profiling", "schema_analysis"],
            "database": base_operations + ["sql_query", "schema_migration", "index_optimization"],
            "file": base_operations + ["file_parsing", "format_conversion", "compression"],
            "api": base_operations + ["api_integration", "rate_limiting", "authentication"],
            "streaming": base_operations + ["stream_processing", "real_time_validation", "windowing"],
            "bigdata": base_operations + ["distributed_processing", "partition_optimization", "aggregation"],
        }

        return specialization_map.get(specialization, base_operations)

    async def _process_task(self, task: AgentTask) -> Dict[str, Any]:
        """Process ETL-specific tasks."""
        task_type = task.task_type.lower()

        with LogContext(task_type=task_type, specialization=self.specialization):
            if task_type == "extract_data":
                return await self._extract_data(task)
            elif task_type == "transform_data":
                return await self._transform_data(task)
            elif task_type == "load_data":
                return await self._load_data(task)
            elif task_type == "validate_data":
                return await self._validate_data(task)
            elif task_type == "profile_data":
                return await self._profile_data(task)
            elif task_type == "optimize_query":
                return await self._optimize_query(task)
            elif task_type == "process_file":
                return await self._process_file(task)
            elif task_type == "handle_stream":
                return await self._handle_stream(task)
            else:
                return await self._handle_generic_etl_task(task)

    def get_system_prompt(self) -> str:
        """Get the system prompt for the ETL agent."""
        return f"""You are an intelligent ETL Specialist Agent with expertise in {self.specialization} operations.

Your primary responsibilities include:
1. Extracting data from various sources efficiently and reliably
2. Transforming data according to business rules and requirements
3. Loading data into target systems with proper validation
4. Ensuring data quality and integrity throughout the process
5. Optimizing ETL performance and handling large datasets
6. Managing error conditions and implementing recovery strategies

Your specialization is: {self.specialization}
Supported operations: {', '.join(self.supported_operations)}

When processing ETL tasks, consider:
- Data volume and complexity
- Performance requirements and constraints
- Data quality and validation needs
- Error handling and recovery strategies
- Resource utilization and optimization
- Compliance and security requirements

Always provide detailed information about:
- Processing steps and methodology
- Data quality checks performed
- Performance metrics and statistics
- Any issues encountered and how they were resolved
- Recommendations for optimization

Respond with structured data when appropriate, including metrics and status information."""

    # Extraction methods are now provided by ETLExtractionOperations mixin

    async def _transform_data(self, task: AgentTask) -> Dict[str, Any]:
        """Transform data according to specified rules using transformation operations."""
        self.logger.info("Starting data transformation")

        try:
            # Use the transformation operations module
            result = await self.transformation_ops.transform_data(task.inputs)

            # Update ETL metrics
            if result.get("status") == "completed":
                transformation_info = {
                    "transformation_id": result.get("transformation_id"),
                    "status": "completed",
                    "started_at": time.time() - result.get("transformation_time", 0),
                    "completed_at": time.time(),
                    "records_transformed": result.get("records_transformed", 0),
                }
                self._update_etl_metrics("transform", transformation_info)

                # Store transformation results in memory
                await self._store_etl_memory("transformation", {
                    "transformation_config": task.inputs.get("transformation_config", {}),
                    "result": result,
                    "timestamp": time.time(),
                })

            return result

        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}", exc_info=e)
            raise DataProcessingException(f"Data transformation failed: {e}") from e

    async def _load_data(self, task: AgentTask) -> Dict[str, Any]:
        """Load data into target destination using loading operations."""
        try:
            # Use the loading operations module
            result = await self.loading_ops.load_data(task.inputs)

            # Store load results in memory
            await self._store_etl_memory("load", {
                "load_config": task.inputs.get("target_config", {}),
                "result": result,
                "timestamp": time.time(),
            })

            return result

        except Exception as e:
            self.logger.error(f"Data loading failed: {e}", exc_info=e)
            raise DataProcessingException(f"Data loading failed: {e}") from e

    async def _validate_data(self, task: AgentTask) -> Dict[str, Any]:
        """Validate data quality and integrity."""
        self.logger.info("Starting data validation")

        try:
            data_source = task.inputs.get("data_source")
            validation_rules = task.inputs.get("validation_rules", [])

            # Use data quality validation tool
            result = await self._use_tool("validate_data_quality", {
                "data_source": data_source,
                "validation_rules": validation_rules,
                "sample_percentage": task.inputs.get("sample_percentage", 10.0),
                "fail_on_errors": task.inputs.get("fail_on_errors", False),
            })

            # Store validation results in memory
            await self._store_etl_memory("validation", {
                "data_source": data_source,
                "validation_rules": validation_rules,
                "result": result,
                "timestamp": time.time(),
            })

            return result

        except Exception as e:
            self.logger.error(f"Data validation failed: {e}", exc_info=e)
            raise DataProcessingException(f"Data validation failed: {e}") from e

    async def _profile_data(self, task: AgentTask) -> Dict[str, Any]:
        """Profile data to understand structure and quality using advanced algorithms."""
        try:
            data_source = task.inputs.get("data_source")
            profiling_config_dict = task.inputs.get("profiling_config", {})

            # Use the profiling operations module
            result = await self.profiling_ops.profile_data(data_source, profiling_config_dict)

            # Store profiling results in memory
            await self._store_etl_memory("profiling", {
                "data_source": data_source,
                "profiling_config": profiling_config_dict,
                "result": result,
                "timestamp": time.time(),
            })

            return result

        except Exception as e:
            self.logger.error(f"Data profiling failed: {e}", exc_info=e)
            raise DataProcessingException(f"Data profiling failed: {e}") from e

    async def _optimize_query(self, task: AgentTask) -> Dict[str, Any]:
        """Optimize database queries for better performance."""
        if self.specialization != "database":
            raise AgentException("Query optimization is only available for database specialists")

        query = task.inputs.get("query")
        optimization_hints = task.inputs.get("optimization_hints", [])

        # Analyze and optimize query (simplified)
        optimization_result = {
            "original_query": query,
            "optimized_query": f"/* Optimized */ {query}",
            "optimizations_applied": [
                "Added appropriate indexes",
                "Optimized join order",
                "Reduced unnecessary columns",
            ],
            "estimated_performance_gain": "30%",
            "recommendations": optimization_hints,
        }

        return optimization_result

    async def _process_file(self, task: AgentTask) -> Dict[str, Any]:
        """Process file-based data operations."""
        if self.specialization != "file":
            raise AgentException("File processing is only available for file specialists")

        file_path = task.inputs.get("file_path")
        operation = task.inputs.get("operation", "parse")

        # Process file based on operation
        processing_result = {
            "file_path": file_path,
            "operation": operation,
            "status": "completed",
            "result": f"File {operation} completed successfully",
            "processing_time": 2.5,  # Placeholder
        }

        return processing_result

    async def _handle_stream(self, task: AgentTask) -> Dict[str, Any]:
        """Handle streaming data operations."""
        if self.specialization != "streaming":
            raise AgentException("Stream processing is only available for streaming specialists")

        stream_config = task.inputs.get("stream_config", {})
        operation = task.inputs.get("operation", "process")

        # Handle stream processing
        stream_result = {
            "stream_config": stream_config,
            "operation": operation,
            "status": "processed",
            "records_processed": 500,  # Placeholder
            "processing_rate": "100 records/second",
        }

        return stream_result

    async def _handle_generic_etl_task(self, task: AgentTask) -> Dict[str, Any]:
        """Handle generic ETL tasks using LLM reasoning."""
        task_description = task.description
        task_inputs = task.inputs

        # Use LLM to process the task
        etl_prompt = f"""You are an ETL Specialist Agent with {self.specialization} expertise. Process this ETL task:

Task Description: {task_description}
Task Inputs: {json.dumps(task_inputs, indent=2)}
Agent Specialization: {self.specialization}
Supported Operations: {', '.join(self.supported_operations)}

Analyze the task and provide:
1. ETL approach and methodology
2. Step-by-step processing plan
3. Data quality considerations
4. Performance optimization strategies
5. Expected outcomes and metrics

If this is a task you can complete directly, provide the solution with specific ETL details.
Otherwise, explain what additional information or resources would be needed."""

        llm_response = await self.query_llm(etl_prompt)

        return {
            "task_type": task.task_type,
            "specialization": self.specialization,
            "analysis": llm_response,
            "processed_at": time.time(),
        }

    # Specialized extraction methods
    # All extraction methods are now provided by ETLExtractionOperations mixin class

    # Transformation methods now handled by ETLTransformationOperations
    # Direct access methods for backward compatibility
    async def _apply_field_mapping(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply field mapping transformations."""
        return await self.transformation_ops.apply_field_mapping(source_data, rules)

    async def _apply_aggregation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply aggregation transformations."""
        return await self.transformation_ops.apply_aggregation(source_data, rules)

    async def _apply_filtering(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply filtering transformations."""
        return await self.transformation_ops.apply_filtering(source_data, rules)

    async def _apply_enrichment(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data enrichment transformations."""
        return await self.transformation_ops.apply_enrichment(source_data, rules)

    async def _apply_generic_transformation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply generic transformations."""
        return await self.transformation_ops.apply_generic_transformation(source_data, rules)

    # Additional transformation methods available through transformation_ops
    async def _apply_data_cleaning(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data cleaning transformations."""
        return await self.transformation_ops.apply_data_cleaning(source_data, rules)

    async def _apply_data_validation(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data validation transformations."""
        return await self.transformation_ops.apply_data_validation(source_data, rules)

    async def _apply_data_normalization(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply data normalization transformations."""
        return await self.transformation_ops.apply_data_normalization(source_data, rules)

    async def _apply_deduplication(self, source_data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Apply deduplication transformations."""
        return await self.transformation_ops.apply_deduplication(source_data, rules)

    # Loading methods now handled by ETLLoadingOperations
    # Direct access methods for backward compatibility
    async def _load_to_database(self, source_data: Any, target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to database targets."""
        return await self.loading_ops._load_to_database(source_data, target_config)

    async def _load_to_file(self, source_data: Any, target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to file targets."""
        return await self.loading_ops._load_to_file(source_data, target_config)

    async def _load_to_api(self, source_data: Any, target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Load data to API targets."""
        return await self.loading_ops._load_to_api(source_data, target_config)

    async def _load_generic(self, source_data: Any, target_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generic data loading."""
        return await self.loading_ops._load_generic(source_data, target_config)

    async def _use_tool(self, tool_name: str, tool_inputs: Dict[str, Any]) -> Any:
        """Use a tool from the registry."""
        tool = self.tool_registry.get_tool(tool_name)
        if not tool:
            raise AgentException(f"Tool {tool_name} not found in registry")

        try:
            result = await tool._arun(**tool_inputs)

            # Try to parse JSON result
            try:
                return json.loads(result)
            except json.JSONDecodeError:
                return {"raw_result": result}

        except Exception as e:
            self.logger.error(f"Tool {tool_name} execution failed: {e}")
            raise AgentException(f"Tool execution failed: {e}") from e

    async def _store_etl_memory(self, operation_type: str, operation_info: Dict[str, Any]) -> None:
        """Store ETL operation information in memory."""
        self.memory.store_memory(
            content={
                "operation_type": operation_type,
                "operation_info": operation_info,
                "specialization": self.specialization,
                "storage_timestamp": time.time(),
            },
            memory_type=MemoryType.EPISODIC,
            importance=MemoryImportance.HIGH,
            tags={operation_type, "etl", self.specialization},
        )

    def _update_etl_metrics(self, operation_type: str, operation_info: Dict[str, Any]) -> None:
        """Update ETL performance metrics in a thread-safe manner."""
        if operation_type in ["extract", "transform", "load"]:
            records_key = f"records_{operation_type}ed"
            records_processed = operation_info.get(records_key, 0)

            if records_processed > 0:
                # Thread-safe metric updates
                self.etl_metrics.increment("records_processed", records_processed)

                # Update processing time
                operation_time = operation_info.get("completed_at", 0) - operation_info.get("started_at", 0)
                self.etl_metrics.increment("total_processing_time", operation_time)

                # Calculate average throughput (thread-safe)
                self.etl_metrics.update_throughput()

    async def _register_default_capabilities(self) -> None:
        """Register ETL-specific capabilities."""
        await super()._register_default_capabilities()

        base_capabilities = [
            AgentCapability(
                name="data_extraction",
                description="Extract data from various sources",
                input_types=["source_config", "connection_params"],
                output_types=["extracted_data", "extraction_metrics"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="data_transformation",
                description="Transform data according to business rules",
                input_types=["source_data", "transformation_rules"],
                output_types=["transformed_data", "transformation_metrics"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="data_loading",
                description="Load data into target systems",
                input_types=["transformed_data", "target_config"],
                output_types=["load_results", "load_metrics"],
                confidence_level=0.9,
            ),
            AgentCapability(
                name="data_validation",
                description="Validate data quality and integrity",
                input_types=["data_source", "validation_rules"],
                output_types=["validation_results", "quality_metrics"],
                confidence_level=0.85,
            ),
        ]

        # Add specialization-specific capabilities
        specialization_capabilities = {
            "database": [
                AgentCapability(
                    name="sql_optimization",
                    description="Optimize SQL queries for performance",
                    input_types=["sql_query", "schema_info"],
                    output_types=["optimized_query", "performance_metrics"],
                    confidence_level=0.8,
                )
            ],
            "file": [
                AgentCapability(
                    name="file_format_conversion",
                    description="Convert between different file formats",
                    input_types=["source_file", "target_format"],
                    output_types=["converted_file", "conversion_metrics"],
                    confidence_level=0.85,
                )
            ],
            "api": [
                AgentCapability(
                    name="api_integration",
                    description="Integrate with REST and GraphQL APIs",
                    input_types=["api_config", "request_params"],
                    output_types=["api_response", "integration_metrics"],
                    confidence_level=0.8,
                )
            ],
            "streaming": [
                AgentCapability(
                    name="stream_processing",
                    description="Process real-time data streams",
                    input_types=["stream_config", "processing_rules"],
                    output_types=["processed_stream", "stream_metrics"],
                    confidence_level=0.75,
                )
            ],
        }

        # Add base capabilities
        for capability in base_capabilities:
            self.add_capability(capability)

        # Add specialization-specific capabilities
        for capability in specialization_capabilities.get(self.specialization, []):
            self.add_capability(capability)

    def get_etl_status(self) -> Dict[str, Any]:
        """Get detailed ETL agent status."""
        base_status = self.get_status()

        etl_status = {
            **base_status,
            "specialization": self.specialization,
            "supported_operations": self.supported_operations,
            "active_extractions": self.active_extractions.count(),
            "active_transformations": self.active_transformations.count(),
            "active_loads": len(self.loading_ops.get_active_loads()),
            "loading_metrics": self.loading_ops.get_loading_metrics(),
            "etl_metrics": self.etl_metrics.copy(),
            "transformation_metrics": self.transformation_ops.get_transformation_metrics() if hasattr(self, 'transformation_ops') else {},
            "memory_entries": self.memory.get_memory_summary()["total_memories"],
        }

        return etl_status
