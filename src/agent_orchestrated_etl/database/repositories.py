"""Repository layer for data access patterns."""

from __future__ import annotations

import time
import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypeVar

from ..exceptions import DatabaseException
from ..logging_config import get_logger
from ..models import (
    AgentConfiguration,
    AgentPerformanceMetrics,
    DataQualityMetrics,
    PipelineConfig,
    PipelineExecution,
)
from .connection import DatabaseManager, get_database_manager

T = TypeVar('T')


class BaseRepository(ABC):
    """Base repository with common CRUD operations."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        """Initialize repository.
        
        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager or get_database_manager()
        self.logger = get_logger(f"agent_etl.repository.{self.__class__.__name__}")

    @abstractmethod
    def get_table_name(self) -> str:
        """Get the table name for this repository."""
        pass

    async def create(self, data: Dict[str, Any]) -> str:
        """Create a new record.
        
        Args:
            data: Record data
            
        Returns:
            Created record ID
        """
        table_name = self.get_table_name()
        record_id = str(uuid.uuid4())

        # Add common fields
        data.update({
            'id': record_id,
            'created_at': time.time(),
            'updated_at': time.time(),
        })

        # Build insert query
        columns = ', '.join(data.keys())
        placeholders = ', '.join([f'${i+1}' for i in range(len(data))])
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        try:
            await self.db_manager.execute_query(
                query=query,
                parameters=data,
                fetch_mode="none"
            )

            self.logger.info(f"Created record {record_id} in {table_name}")
            return record_id

        except Exception as e:
            self.logger.error(f"Failed to create record in {table_name}: {e}")
            raise DatabaseException(f"Create operation failed: {e}") from e

    async def get_by_id(self, record_id: str) -> Optional[Dict[str, Any]]:
        """Get a record by ID.
        
        Args:
            record_id: Record identifier
            
        Returns:
            Record data or None if not found
        """
        table_name = self.get_table_name()
        query = f"SELECT * FROM {table_name} WHERE id = $1"

        try:
            result = await self.db_manager.execute_query(
                query=query,
                parameters={'id': record_id},
                fetch_mode="one"
            )

            if result:
                self.logger.debug(f"Retrieved record {record_id} from {table_name}")

            return result

        except Exception as e:
            self.logger.error(f"Failed to get record {record_id} from {table_name}: {e}")
            raise DatabaseException(f"Get operation failed: {e}") from e

    async def update(self, record_id: str, data: Dict[str, Any]) -> bool:
        """Update a record.
        
        Args:
            record_id: Record identifier
            data: Updated data
            
        Returns:
            True if record was updated
        """
        table_name = self.get_table_name()

        # Add updated timestamp
        data['updated_at'] = time.time()

        # Build update query
        set_clauses = [f"{key} = ${i+2}" for i, key in enumerate(data.keys())]
        query = f"UPDATE {table_name} SET {', '.join(set_clauses)} WHERE id = $1"

        try:
            await self.db_manager.execute_query(
                query=query,
                parameters={'id': record_id, **data},
                fetch_mode="none"
            )

            self.logger.info(f"Updated record {record_id} in {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to update record {record_id} in {table_name}: {e}")
            raise DatabaseException(f"Update operation failed: {e}") from e

    async def delete(self, record_id: str) -> bool:
        """Delete a record.
        
        Args:
            record_id: Record identifier
            
        Returns:
            True if record was deleted
        """
        table_name = self.get_table_name()
        query = f"DELETE FROM {table_name} WHERE id = $1"

        try:
            await self.db_manager.execute_query(
                query=query,
                parameters={'id': record_id},
                fetch_mode="none"
            )

            self.logger.info(f"Deleted record {record_id} from {table_name}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to delete record {record_id} from {table_name}: {e}")
            raise DatabaseException(f"Delete operation failed: {e}") from e

    async def list_all(
        self,
        limit: int = 100,
        offset: int = 0,
        order_by: str = "created_at",
        order_desc: bool = True
    ) -> List[Dict[str, Any]]:
        """List all records.
        
        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip
            order_by: Column to order by
            order_desc: Whether to order in descending order
            
        Returns:
            List of records
        """
        table_name = self.get_table_name()
        order_direction = "DESC" if order_desc else "ASC"

        query = f"""
            SELECT * FROM {table_name} 
            ORDER BY {order_by} {order_direction}
            LIMIT $1 OFFSET $2
        """

        try:
            results = await self.db_manager.execute_query(
                query=query,
                parameters={'limit': limit, 'offset': offset},
                fetch_mode="all"
            )

            self.logger.debug(f"Retrieved {len(results)} records from {table_name}")
            return results or []

        except Exception as e:
            self.logger.error(f"Failed to list records from {table_name}: {e}")
            raise DatabaseException(f"List operation failed: {e}") from e

    async def count(self) -> int:
        """Count total records.
        
        Returns:
            Total number of records
        """
        table_name = self.get_table_name()
        query = f"SELECT COUNT(*) as count FROM {table_name}"

        try:
            result = await self.db_manager.execute_query(
                query=query,
                fetch_mode="one"
            )

            count = result['count'] if result else 0
            self.logger.debug(f"Total records in {table_name}: {count}")
            return count

        except Exception as e:
            self.logger.error(f"Failed to count records in {table_name}: {e}")
            raise DatabaseException(f"Count operation failed: {e}") from e


class PipelineRepository(BaseRepository):
    """Repository for pipeline-related data."""

    def get_table_name(self) -> str:
        return "pipeline_metadata.pipeline_executions"

    async def create_pipeline_execution(self, pipeline_config: PipelineConfig) -> str:
        """Create a new pipeline execution record.
        
        Args:
            pipeline_config: Pipeline configuration
            
        Returns:
            Execution ID
        """
        execution_data = {
            'pipeline_id': pipeline_config.pipeline_id,
            'status': 'pending',
            'configuration': pipeline_config.dict(),
            'results': {},
        }

        return await self.create(execution_data)

    async def update_pipeline_execution(
        self,
        execution_id: str,
        pipeline_execution: PipelineExecution
    ) -> bool:
        """Update pipeline execution.
        
        Args:
            execution_id: Execution identifier
            pipeline_execution: Updated execution data
            
        Returns:
            True if updated successfully
        """
        update_data = {
            'status': pipeline_execution.status.value,
            'started_at': pipeline_execution.started_at,
            'completed_at': pipeline_execution.completed_at,
            'duration_seconds': pipeline_execution.duration_seconds,
            'current_tasks': pipeline_execution.current_tasks,
            'completed_tasks': pipeline_execution.completed_tasks,
            'failed_tasks': pipeline_execution.failed_tasks,
            'completion_percentage': pipeline_execution.completion_percentage,
        }

        return await self.update(execution_id, update_data)

    async def get_pipeline_execution(self, execution_id: str) -> Optional[PipelineExecution]:
        """Get pipeline execution by ID.
        
        Args:
            execution_id: Execution identifier
            
        Returns:
            Pipeline execution or None if not found
        """
        record = await self.get_by_id(execution_id)
        if not record:
            return None

        # Convert database record to PipelineExecution model
        try:
            execution = PipelineExecution(
                execution_id=record['id'],
                pipeline_id=record['pipeline_id'],
                status=record['status'],
                started_at=record.get('started_at'),
                completed_at=record.get('completed_at'),
                duration_seconds=record.get('duration_seconds'),
                current_tasks=record.get('current_tasks', []),
                completed_tasks=record.get('completed_tasks', []),
                failed_tasks=record.get('failed_tasks', []),
                completion_percentage=record.get('completion_percentage', 0.0),
                configuration_snapshot=record.get('configuration'),
            )
            return execution

        except Exception as e:
            self.logger.error(f"Failed to parse pipeline execution {execution_id}: {e}")
            return None

    async def list_pipeline_executions(
        self,
        pipeline_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100
    ) -> List[PipelineExecution]:
        """List pipeline executions with optional filters.
        
        Args:
            pipeline_id: Filter by pipeline ID
            status: Filter by status
            limit: Maximum number of results
            
        Returns:
            List of pipeline executions
        """
        table_name = self.get_table_name()
        conditions = []
        parameters = {}
        param_counter = 1

        if pipeline_id:
            conditions.append(f"pipeline_id = ${param_counter}")
            parameters[f'param_{param_counter}'] = pipeline_id
            param_counter += 1

        if status:
            conditions.append(f"status = ${param_counter}")
            parameters[f'param_{param_counter}'] = status
            param_counter += 1

        where_clause = " WHERE " + " AND ".join(conditions) if conditions else ""

        query = f"""
            SELECT * FROM {table_name}
            {where_clause}
            ORDER BY created_at DESC
            LIMIT ${param_counter}
        """
        parameters[f'param_{param_counter}'] = limit

        try:
            records = await self.db_manager.execute_query(
                query=query,
                parameters=parameters,
                fetch_mode="all"
            )

            executions = []
            for record in records or []:
                try:
                    execution = PipelineExecution(
                        execution_id=record['id'],
                        pipeline_id=record['pipeline_id'],
                        status=record['status'],
                        started_at=record.get('started_at'),
                        completed_at=record.get('completed_at'),
                        configuration_snapshot=record.get('configuration'),
                    )
                    executions.append(execution)
                except Exception as e:
                    self.logger.warning(f"Failed to parse execution record {record.get('id')}: {e}")

            return executions

        except Exception as e:
            self.logger.error(f"Failed to list pipeline executions: {e}")
            raise DatabaseException(f"List pipeline executions failed: {e}") from e


class AgentRepository(BaseRepository):
    """Repository for agent-related data."""

    def get_table_name(self) -> str:
        return "agent_state.agent_status"

    async def create_agent(self, agent_config: AgentConfiguration) -> str:
        """Create a new agent record.
        
        Args:
            agent_config: Agent configuration
            
        Returns:
            Agent ID
        """
        agent_data = {
            'id': agent_config.agent_id,
            'status': 'initializing',
            'configuration': agent_config.dict(),
            'metrics': {},
            'last_heartbeat': time.time(),
        }

        return await self.create(agent_data)

    async def update_agent_status(
        self,
        agent_id: str,
        status: str,
        metrics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Update agent status and metrics.
        
        Args:
            agent_id: Agent identifier
            status: New status
            metrics: Optional metrics data
            
        Returns:
            True if updated successfully
        """
        update_data = {
            'status': status,
            'last_heartbeat': time.time(),
        }

        if metrics:
            update_data['metrics'] = metrics

        return await self.update(agent_id, update_data)

    async def get_agent_status(self, agent_id: str) -> Optional[Dict[str, Any]]:
        """Get agent status information.
        
        Args:
            agent_id: Agent identifier
            
        Returns:
            Agent status data or None if not found
        """
        return await self.get_by_id(agent_id)

    async def list_active_agents(self, heartbeat_timeout: int = 300) -> List[Dict[str, Any]]:
        """List agents that have sent heartbeats recently.
        
        Args:
            heartbeat_timeout: Timeout in seconds for considering an agent active
            
        Returns:
            List of active agents
        """
        table_name = self.get_table_name()
        cutoff_time = time.time() - heartbeat_timeout

        query = f"""
            SELECT * FROM {table_name}
            WHERE last_heartbeat > $1
            ORDER BY last_heartbeat DESC
        """

        try:
            results = await self.db_manager.execute_query(
                query=query,
                parameters={'cutoff_time': cutoff_time},
                fetch_mode="all"
            )

            return results or []

        except Exception as e:
            self.logger.error(f"Failed to list active agents: {e}")
            raise DatabaseException(f"List active agents failed: {e}") from e

    async def record_agent_metrics(
        self,
        agent_id: str,
        metrics: AgentPerformanceMetrics
    ) -> str:
        """Record agent performance metrics.
        
        Args:
            agent_id: Agent identifier
            metrics: Performance metrics
            
        Returns:
            Metrics record ID
        """
        metrics_data = {
            'agent_id': agent_id,
            'measurement_period_start': metrics.measurement_period_start,
            'measurement_period_end': metrics.measurement_period_end,
            'metrics_data': metrics.dict(),
        }

        # Use a separate metrics table (would need to be created)
        table_name = "agent_state.agent_metrics"

        try:
            # For now, update the agent's metrics field
            return await self.update_agent_status(agent_id, status=None, metrics=metrics.dict())

        except Exception as e:
            self.logger.error(f"Failed to record agent metrics for {agent_id}: {e}")
            raise DatabaseException(f"Record agent metrics failed: {e}") from e


class QualityMetricsRepository(BaseRepository):
    """Repository for data quality metrics."""

    def get_table_name(self) -> str:
        return "data_quality.quality_metrics"

    async def record_quality_metrics(
        self,
        data_source_id: str,
        metrics: DataQualityMetrics
    ) -> str:
        """Record data quality metrics.
        
        Args:
            data_source_id: Data source identifier
            metrics: Quality metrics
            
        Returns:
            Metrics record ID
        """
        metrics_data = {
            'data_source_id': data_source_id,
            'measurement_timestamp': metrics.measurement_timestamp,
            'overall_score': metrics.overall_quality_score,
            'dimension_scores': dict(metrics.dimension_scores),
            'detailed_metrics': {
                'completeness': metrics.completeness_metrics,
                'accuracy': metrics.accuracy_metrics,
                'consistency': metrics.consistency_metrics,
                'validity': metrics.validity_metrics,
                'uniqueness': metrics.uniqueness_metrics,
                'timeliness': metrics.timeliness_metrics,
                'integrity': metrics.integrity_metrics,
            },
            'issues': {
                'critical_issues': metrics.critical_issues,
                'warnings': metrics.warnings,
                'anomalies': metrics.anomalies,
            },
            'record_count': metrics.record_count,
            'field_profiles': metrics.field_profiles,
        }

        return await self.create(metrics_data)

    async def get_latest_quality_metrics(
        self,
        data_source_id: str
    ) -> Optional[DataQualityMetrics]:
        """Get the latest quality metrics for a data source.
        
        Args:
            data_source_id: Data source identifier
            
        Returns:
            Latest quality metrics or None if not found
        """
        table_name = self.get_table_name()

        query = f"""
            SELECT * FROM {table_name}
            WHERE data_source_id = $1
            ORDER BY measurement_timestamp DESC
            LIMIT 1
        """

        try:
            record = await self.db_manager.execute_query(
                query=query,
                parameters={'data_source_id': data_source_id},
                fetch_mode="one"
            )

            if not record:
                return None

            # Convert database record to DataQualityMetrics model
            metrics = DataQualityMetrics(
                metrics_id=record['id'],
                data_source_id=record['data_source_id'],
                measurement_timestamp=record['measurement_timestamp'],
                overall_quality_score=record['overall_score'],
                dimension_scores=record.get('dimension_scores', {}),
                record_count=record.get('record_count', 0),
                field_profiles=record.get('field_profiles', {}),
                **record.get('detailed_metrics', {}),
                **record.get('issues', {}),
            )

            return metrics

        except Exception as e:
            self.logger.error(f"Failed to get latest quality metrics for {data_source_id}: {e}")
            raise DatabaseException(f"Get quality metrics failed: {e}") from e

    async def get_quality_trends(
        self,
        data_source_id: str,
        days: int = 30
    ) -> List[Dict[str, Any]]:
        """Get quality metrics trends over time.
        
        Args:
            data_source_id: Data source identifier
            days: Number of days to look back
            
        Returns:
            List of quality metrics over time
        """
        table_name = self.get_table_name()
        cutoff_time = time.time() - (days * 24 * 60 * 60)

        query = f"""
            SELECT 
                measurement_timestamp,
                overall_score,
                dimension_scores,
                record_count
            FROM {table_name}
            WHERE data_source_id = $1 
                AND measurement_timestamp > $2
            ORDER BY measurement_timestamp ASC
        """

        try:
            results = await self.db_manager.execute_query(
                query=query,
                parameters={
                    'data_source_id': data_source_id,
                    'cutoff_time': cutoff_time
                },
                fetch_mode="all"
            )

            return results or []

        except Exception as e:
            self.logger.error(f"Failed to get quality trends for {data_source_id}: {e}")
            raise DatabaseException(f"Get quality trends failed: {e}") from e
