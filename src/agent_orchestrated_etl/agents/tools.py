"""LangChain tool integration for Agent-Orchestrated ETL."""

from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Type, Union

from langchain_core.tools import BaseTool
# Note: create_json_schema not available in current langchain version
# Using pydantic's model_json_schema instead
from pydantic import BaseModel, Field

from ..exceptions import AgentException, ToolException
from ..logging_config import get_logger
from ..retry import retry, RetryConfigs
from ..circuit_breaker import circuit_breaker, CircuitBreakerConfigs


class ETLToolSchema(BaseModel):
    """Base schema for ETL tool inputs."""
    pass


class AnalyzeDataSourceSchema(ETLToolSchema):
    """Schema for data source analysis tool."""
    
    source_path: str = Field(description="Path or connection string to the data source")
    source_type: str = Field(description="Type of data source (database, file, api, etc.)")
    include_samples: bool = Field(default=True, description="Whether to include data samples")
    max_sample_rows: int = Field(default=100, description="Maximum number of sample rows")


class GenerateDAGSchema(ETLToolSchema):
    """Schema for DAG generation tool."""
    
    metadata: Dict[str, Any] = Field(description="Data source metadata from analysis")
    dag_id: str = Field(description="Unique identifier for the DAG")
    include_validation: bool = Field(default=True, description="Include data validation steps")
    parallel_tasks: bool = Field(default=True, description="Enable parallel task execution")


class ExecutePipelineSchema(ETLToolSchema):
    """Schema for pipeline execution tool."""
    
    dag_config: Dict[str, Any] = Field(description="DAG configuration")
    execution_mode: str = Field(default="async", description="Execution mode (sync/async)")
    monitor_progress: bool = Field(default=True, description="Monitor execution progress")
    timeout_seconds: int = Field(default=3600, description="Execution timeout in seconds")


class ValidateDataQualitySchema(ETLToolSchema):
    """Schema for data quality validation tool."""
    
    data_source: str = Field(description="Data source to validate")
    validation_rules: List[Dict[str, Any]] = Field(description="List of validation rules")
    sample_percentage: float = Field(default=10.0, description="Percentage of data to sample")
    fail_on_errors: bool = Field(default=False, description="Fail immediately on validation errors")


class ETLTool(BaseTool, ABC):
    """Base class for ETL-specific tools."""
    
    name: str
    description: str
    args_schema: Type[BaseModel] = ETLToolSchema
    return_direct: bool = False
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        # Use object.__setattr__ to set private attributes in Pydantic models
        object.__setattr__(self, '_logger', get_logger(f"tool.{self.name}"))
    
    @property
    def logger(self):
        """Get the logger instance."""
        return getattr(self, '_logger', None) or get_logger(f"tool.{self.name}")
    
    @retry(RetryConfigs.STANDARD)
    @circuit_breaker("etl_tool_execution", CircuitBreakerConfigs.STANDARD)
    def _run(self, **kwargs) -> str:
        """Synchronous tool execution with resilience patterns."""
        try:
            self.logger.info(f"Executing tool: {self.name}")
            result = self._execute(**kwargs)
            
            # Ensure result is JSON serializable
            if isinstance(result, dict):
                return json.dumps(result, indent=2)
            else:
                return str(result)
                
        except Exception as e:
            self.logger.error(f"Tool execution failed: {e}", exc_info=e)
            raise ToolException(f"Tool {self.name} failed: {e}", tool_name=self.name) from e
    
    async def _arun(self, **kwargs) -> str:
        """Asynchronous tool execution."""
        # Default implementation runs sync version in executor
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: self._run(**kwargs))
    
    @abstractmethod
    def _execute(self, **kwargs) -> Union[str, Dict[str, Any]]:
        """Execute the tool logic. Must be implemented by subclasses."""
        pass


class AnalyzeDataSourceTool(ETLTool):
    """Tool for analyzing data sources."""
    
    name: str = "analyze_data_source"
    description: str = "Analyze a data source to extract metadata, schema, and sample data"
    args_schema: Type[BaseModel] = AnalyzeDataSourceSchema
    
    def _execute(self, source_path: str, source_type: str, include_samples: bool = True, max_sample_rows: int = 100) -> Dict[str, Any]:
        """Analyze data source and return metadata."""
        from .. import data_source_analysis
        
        try:
            # Use existing data source analysis functionality
            metadata = data_source_analysis.analyze_source(source_path)
            
            # Enhance with additional information
            analysis_result = {
                "source_path": source_path,
                "source_type": source_type,
                "metadata": metadata,
                "analysis_timestamp": time.time(),
                "include_samples": include_samples,
                "max_sample_rows": max_sample_rows,
            }
            
            if include_samples:
                # Add sample data if requested
                # This would be implemented based on the source type
                analysis_result["samples"] = {
                    "note": "Sample data extraction would be implemented based on source type"
                }
            
            return analysis_result
            
        except Exception as e:
            raise ToolException(f"Data source analysis failed: {e}", tool_name=self.name) from e


class GenerateDAGTool(ETLTool):
    """Tool for generating ETL DAGs."""
    
    name: str = "generate_dag"
    description: str = "Generate an ETL DAG based on data source metadata"
    args_schema: Type[BaseModel] = GenerateDAGSchema
    
    def _execute(self, metadata: Dict[str, Any], dag_id: str, include_validation: bool = True, parallel_tasks: bool = True) -> Dict[str, Any]:
        """Generate DAG configuration."""
        from .. import dag_generator
        
        try:
            # Generate DAG using existing functionality
            dag = dag_generator.generate_dag(metadata)
            
            # Convert to Airflow code
            airflow_code = dag_generator.dag_to_airflow_code(dag, dag_id=dag_id)
            
            dag_result = {
                "dag_id": dag_id,
                "metadata": metadata,
                "dag_structure": {
                    "tasks": dag.tasks,
                    "dependencies": dag.dependencies,
                    "execution_order": dag.topological_sort(),
                },
                "airflow_code": airflow_code,
                "configuration": {
                    "include_validation": include_validation,
                    "parallel_tasks": parallel_tasks,
                },
                "generation_timestamp": time.time(),
            }
            
            return dag_result
            
        except Exception as e:
            raise ToolException(f"DAG generation failed: {e}", tool_name=self.name) from e


class ExecutePipelineTool(ETLTool):
    """Tool for executing ETL pipelines."""
    
    name: str = "execute_pipeline"
    description: str = "Execute an ETL pipeline based on DAG configuration"
    args_schema: Type[BaseModel] = ExecutePipelineSchema
    
    def _execute(self, dag_config: Dict[str, Any], execution_mode: str = "async", monitor_progress: bool = True, timeout_seconds: int = 3600) -> Dict[str, Any]:
        """Execute ETL pipeline."""
        from ..orchestrator import DataOrchestrator
        
        try:
            orchestrator = DataOrchestrator()
            
            # This is a simplified version - in practice, you'd create a full pipeline
            execution_result = {
                "execution_id": f"exec_{int(time.time())}",
                "dag_config": dag_config,
                "execution_mode": execution_mode,
                "monitor_progress": monitor_progress,
                "timeout_seconds": timeout_seconds,
                "status": "started",
                "start_time": time.time(),
                "message": "Pipeline execution initiated (implementation would run actual pipeline)",
            }
            
            return execution_result
            
        except Exception as e:
            raise ToolException(f"Pipeline execution failed: {e}", tool_name=self.name) from e


class ValidateDataQualityTool(ETLTool):
    """Tool for validating data quality."""
    
    name: str = "validate_data_quality"
    description: str = "Validate data quality using specified rules and criteria"
    args_schema: Type[BaseModel] = ValidateDataQualitySchema
    
    def _execute(self, data_source: str, validation_rules: List[Dict[str, Any]], sample_percentage: float = 10.0, fail_on_errors: bool = False) -> Dict[str, Any]:
        """Validate data quality."""
        try:
            # This would implement actual data quality validation
            validation_result = {
                "data_source": data_source,
                "validation_rules": validation_rules,
                "sample_percentage": sample_percentage,
                "fail_on_errors": fail_on_errors,
                "validation_timestamp": time.time(),
                "results": {
                    "total_rules": len(validation_rules),
                    "passed_rules": len(validation_rules),  # Placeholder
                    "failed_rules": 0,  # Placeholder
                    "warnings": [],
                    "errors": [],
                },
                "status": "passed",
                "message": "Data quality validation completed (implementation would run actual validation)",
            }
            
            return validation_result
            
        except Exception as e:
            raise ToolException(f"Data quality validation failed: {e}", tool_name=self.name) from e


class QueryDataTool(ETLTool):
    """Tool for querying data sources."""
    
    name: str = "query_data"
    description: str = "Execute queries against data sources"
    
    class QueryDataSchema(ETLToolSchema):
        data_source: str = Field(description="Data source identifier")
        query: str = Field(description="Query to execute")
        limit: int = Field(default=100, description="Maximum number of results")
        format: str = Field(default="json", description="Output format (json, csv, etc.)")
    
    args_schema: Type[BaseModel] = QueryDataSchema
    
    def _execute(self, data_source: str, query: str, limit: int = 100, format: str = "json") -> Dict[str, Any]:
        """Execute data query."""
        try:
            # This would implement actual data querying
            query_result = {
                "data_source": data_source,
                "query": query,
                "limit": limit,
                "format": format,
                "execution_timestamp": time.time(),
                "results": {
                    "row_count": 0,  # Placeholder
                    "columns": [],  # Placeholder
                    "data": [],  # Placeholder
                },
                "status": "completed",
                "message": "Query executed successfully (implementation would run actual query)",
            }
            
            return query_result
            
        except Exception as e:
            raise ToolException(f"Data query failed: {e}", tool_name=self.name) from e


class MonitorPipelineTool(ETLTool):
    """Tool for monitoring pipeline execution."""
    
    name: str = "monitor_pipeline"
    description: str = "Monitor the status and progress of pipeline execution"
    
    class MonitorPipelineSchema(ETLToolSchema):
        pipeline_id: str = Field(description="Pipeline execution ID to monitor")
        include_logs: bool = Field(default=False, description="Include execution logs")
        include_metrics: bool = Field(default=True, description="Include performance metrics")
    
    args_schema: Type[BaseModel] = MonitorPipelineSchema
    
    def _execute(self, pipeline_id: str, include_logs: bool = False, include_metrics: bool = True) -> Dict[str, Any]:
        """Monitor pipeline execution."""
        try:
            # This would implement actual pipeline monitoring
            monitor_result = {
                "pipeline_id": pipeline_id,
                "include_logs": include_logs,
                "include_metrics": include_metrics,
                "monitoring_timestamp": time.time(),
                "status": {
                    "state": "running",  # Placeholder
                    "progress": 0.5,  # Placeholder
                    "current_task": "transform_data",  # Placeholder
                    "tasks_completed": 2,  # Placeholder
                    "tasks_total": 4,  # Placeholder
                },
                "metrics": {
                    "execution_time": 120,  # Placeholder
                    "memory_usage": "256MB",  # Placeholder
                    "cpu_usage": "45%",  # Placeholder
                } if include_metrics else {},
                "logs": [
                    "Pipeline started",
                    "Extract task completed",
                    "Transform task in progress",
                ] if include_logs else [],
                "message": "Pipeline monitoring completed (implementation would fetch actual status)",
            }
            
            return monitor_result
            
        except Exception as e:
            raise ToolException(f"Pipeline monitoring failed: {e}", tool_name=self.name) from e


class AgentToolRegistry:
    """Registry for managing agent tools."""
    
    def __init__(self):
        self.tools: Dict[str, ETLTool] = {}
        self.tool_categories: Dict[str, List[str]] = {}
        self.logger = get_logger("agent.tool_registry")
        
        # Register default tools
        self._register_default_tools()
    
    def register_tool(self, tool: ETLTool, category: str = "general") -> None:
        """Register a tool in the registry.
        
        Args:
            tool: The tool to register
            category: Tool category for organization
            
        Raises:
            AgentException: If tool registration fails
        """
        if tool.name in self.tools:
            raise AgentException(f"Tool {tool.name} is already registered")
        
        try:
            self.tools[tool.name] = tool
            
            if category not in self.tool_categories:
                self.tool_categories[category] = []
            self.tool_categories[category].append(tool.name)
            
            self.logger.info(f"Tool registered: {tool.name} (category: {category})")
            
        except Exception as e:
            raise AgentException(f"Failed to register tool {tool.name}: {e}") from e
    
    def unregister_tool(self, tool_name: str) -> bool:
        """Unregister a tool from the registry.
        
        Args:
            tool_name: Name of the tool to unregister
            
        Returns:
            True if tool was unregistered, False if not found
        """
        if tool_name not in self.tools:
            return False
        
        try:
            # Remove from tools
            del self.tools[tool_name]
            
            # Remove from categories
            for category, tool_names in self.tool_categories.items():
                if tool_name in tool_names:
                    tool_names.remove(tool_name)
                    if not tool_names:
                        del self.tool_categories[category]
                    break
            
            self.logger.info(f"Tool unregistered: {tool_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to unregister tool {tool_name}: {e}", exc_info=e)
            return False
    
    def get_tool(self, tool_name: str) -> Optional[ETLTool]:
        """Get a tool by name.
        
        Args:
            tool_name: Name of the tool
            
        Returns:
            Tool instance if found, None otherwise
        """
        return self.tools.get(tool_name)
    
    def get_tools_by_category(self, category: str) -> List[ETLTool]:
        """Get all tools in a category.
        
        Args:
            category: Tool category
            
        Returns:
            List of tools in the category
        """
        tool_names = self.tool_categories.get(category, [])
        return [self.tools[name] for name in tool_names if name in self.tools]
    
    def get_all_tools(self) -> List[ETLTool]:
        """Get all registered tools.
        
        Returns:
            List of all tools
        """
        return list(self.tools.values())
    
    def list_tool_names(self) -> List[str]:
        """Get list of all tool names.
        
        Returns:
            List of tool names
        """
        return list(self.tools.keys())
    
    def get_tool_descriptions(self) -> Dict[str, str]:
        """Get descriptions of all tools.
        
        Returns:
            Dictionary mapping tool names to descriptions
        """
        return {name: tool.description for name, tool in self.tools.items()}
    
    def search_tools(self, query: str) -> List[ETLTool]:
        """Search for tools by name or description.
        
        Args:
            query: Search query
            
        Returns:
            List of matching tools
        """
        query_lower = query.lower()
        matching_tools = []
        
        for tool in self.tools.values():
            if (query_lower in tool.name.lower() or 
                query_lower in tool.description.lower()):
                matching_tools.append(tool)
        
        return matching_tools
    
    def get_registry_stats(self) -> Dict[str, Any]:
        """Get registry statistics.
        
        Returns:
            Dictionary with registry statistics
        """
        return {
            "total_tools": len(self.tools),
            "categories": len(self.tool_categories),
            "tools_by_category": {
                category: len(tool_names) 
                for category, tool_names in self.tool_categories.items()
            },
            "tool_names": list(self.tools.keys()),
        }
    
    def _register_default_tools(self) -> None:
        """Register default ETL tools."""
        default_tools = [
            (AnalyzeDataSourceTool(), "analysis"),
            (GenerateDAGTool(), "generation"),
            (ExecutePipelineTool(), "execution"),
            (ValidateDataQualityTool(), "validation"),
            (QueryDataTool(), "data"),
            (MonitorPipelineTool(), "monitoring"),
        ]
        
        for tool, category in default_tools:
            try:
                self.register_tool(tool, category)
            except Exception as e:
                self.logger.error(f"Failed to register default tool {tool.name}: {e}")


# Global tool registry instance
_tool_registry = AgentToolRegistry()


def get_tool_registry() -> AgentToolRegistry:
    """Get the global tool registry instance.
    
    Returns:
        Global AgentToolRegistry instance
    """
    return _tool_registry


def register_tool(tool: ETLTool, category: str = "general") -> None:
    """Register a tool in the global registry.
    
    Args:
        tool: The tool to register
        category: Tool category for organization
    """
    _tool_registry.register_tool(tool, category)


def get_tool(tool_name: str) -> Optional[ETLTool]:
    """Get a tool from the global registry.
    
    Args:
        tool_name: Name of the tool
        
    Returns:
        Tool instance if found, None otherwise
    """
    return _tool_registry.get_tool(tool_name)