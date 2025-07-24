# Agent API Reference

## Overview

The Agent API provides comprehensive interfaces for interacting with the autonomous agent system within the Agent Orchestrated ETL platform. This includes agent lifecycle management, task execution, capability management, and performance monitoring.

## Core Agent Classes

### BaseAgent

Base class for all agents in the system providing common functionality.

#### Methods

##### `async start() -> None`
Start the agent and transition to ready state.

**Raises:**
- `AgentException`: If agent cannot start due to invalid state or initialization errors

**Example:**
```python
agent = ETLAgent(config)
await agent.start()
```

##### `async stop() -> None`
Stop the agent gracefully, canceling active tasks and cleaning up resources.

##### `async execute_task(task: AgentTask) -> Dict[str, Any]`
Execute a task with resilience patterns including retry logic and circuit breakers.

**Parameters:**
- `task`: AgentTask object containing task details and metadata

**Returns:**
- Dictionary containing task execution results

**Raises:**
- `AgentException`: If task execution fails or agent not ready

#### Performance Methods

##### `get_performance_metrics() -> Dict[str, Any]`
Get current performance metrics for agent selection algorithms.

**Returns:**
```python
{
    "total_tasks": 150,
    "successful_tasks": 147,
    "failed_tasks": 3,
    "success_rate": 0.98,
    "avg_execution_time": 12.5,
    "throughput": 4.8,
    "availability": 0.99
}
```

##### `get_current_load() -> float`
Get current load factor (0.0 = idle, 1.0 = fully loaded).

##### `get_specialization_score(task_type: str) -> float`
Get specialization score for a specific task type based on historical performance.

### AgentCoordinator

Central orchestration component for intelligent agent selection and task distribution.

#### Enhanced Selection Methods

##### `_get_enhanced_capabilities(task: CoordinationTask) -> List[str]`
Intelligent capability inference from task context including volume, complexity, and data sources.

**Parameters:**
- `task`: CoordinationTask with metadata for capability inference

**Returns:**
- List of inferred capability requirements

##### `_calculate_performance_score(agent: BaseAgent, task: CoordinationTask) -> float`
Calculate performance-based agent selection score using multiple metrics.

**Scoring Factors:**
- Success rate (40% weight for high-performance tasks)
- Execution time (30% weight with penalty for slow agents)
- Availability (20% weight for reliability)
- Error rate (10% weight with failure penalty)

##### `_find_suitable_agent_with_load_balancing(task: CoordinationTask, required_capabilities: List[str]) -> str`
Find optimal agent using load-balanced selection with comprehensive scoring.

**Returns:**
- Agent ID of selected agent or None if no suitable agent found

#### Audit and Monitoring

##### `get_selection_audit_trail() -> List[Dict[str, Any]]`
Retrieve comprehensive audit trail of agent selection decisions.

**Returns:**
```python
[
    {
        "task_id": "task_123",
        "selected_agent": "agent_456",
        "selection_score": 0.87,
        "alternatives_considered": 5,
        "timestamp": 1672531200.0,
        "selection_time_ms": 2.3
    }
]
```

##### `get_load_balancing_metrics() -> Dict[str, Any]`
Get real-time load balancing metrics and effectiveness measures.

### ETL Agent

Specialized agent for data processing operations with comprehensive ETL capabilities.

#### Data Extraction Methods

##### `async _load_real_sample_data(data_source_config: Dict[str, Any], config: ProfilingConfig) -> Dict[str, Any]`
Load real sample data using intelligent sampling strategies.

**Supported Sampling Strategies:**
- `random`: Random sampling with configurable size
- `systematic`: Systematic sampling with interval calculation  
- `stratified`: Stratified sampling based on key columns
- `reservoir`: Reservoir sampling for streaming data

**Parameters:**
- `data_source_config`: Configuration for data source connection
- `config`: ProfilingConfig with sampling parameters

**Returns:**
```python
{
    "sample_data": [...],
    "sampling_metadata": {
        "strategy": "random",
        "total_records": 100000,
        "sample_size": 1000,
        "sampling_ratio": 0.01
    }
}
```

#### Data Profiling Methods

##### `_profile_data_column(column_data: List[Any], column_name: str) -> DataProfile`
Comprehensive statistical profiling of data columns.

**Analysis Features:**
- Data type inference with confidence scoring
- Statistical measures (mean, median, std, percentiles)
- Missing value analysis and patterns
- Outlier detection using IQR method
- Value distribution and frequency analysis

### Communication Hub

Inter-agent communication infrastructure with message routing and security.

#### Message Methods

##### `async send_message(message: Message) -> bool`
Send message between agents with guaranteed delivery and retry logic.

##### `async register_agent(agent: BaseAgent) -> None` 
Register agent for message routing and discovery.

##### `async broadcast_message(message: Message, agent_filter: Optional[str] = None) -> List[str]`
Broadcast message to multiple agents with optional filtering.

## Configuration Classes

### AgentConfig

Configuration dataclass for agent initialization.

**Fields:**
- `agent_id`: Unique identifier (auto-generated UUID)
- `name`: Human-readable agent name
- `role`: AgentRole enum value
- `max_concurrent_tasks`: Maximum parallel task execution (default: 3)
- `task_timeout_seconds`: Task execution timeout (default: 300.0)
- `extra_config`: Additional agent-specific configuration

### AgentCapability

Enhanced capability model with metadata for intelligent selection.

**Core Fields:**
- `name`: Capability identifier
- `description`: Human-readable description
- `input_types`: Supported input data types
- `output_types`: Produced output data types
- `confidence_level`: Agent confidence (0.0-1.0)

**Enhanced Metadata:**
- `specialization_areas`: Fine-grained specialization tags
- `performance_metrics`: Historical performance data
- `resource_requirements`: Memory/CPU requirements
- `version`: Capability version for compatibility
- `tags`: Searchable capability tags
- `prerequisites`: Required dependencies

## Error Handling

### AgentException
Base exception for all agent-related errors.

### ValidationException  
Raised for data validation and configuration errors.

### Common Error Patterns

```python
try:
    result = await agent.execute_task(task)
except AgentException as e:
    logger.error(f"Agent execution failed: {e}")
    # Handle graceful degradation
except ValidationException as e:
    logger.error(f"Task validation failed: {e}")
    # Handle input validation errors
```

## Performance Considerations

### Async Best Practices
- All agent methods are async for non-blocking execution
- Use `asyncio.gather()` for parallel agent operations
- Implement proper timeout handling for long-running tasks

### Memory Management
- Agents automatically clean up task history (1000 entry limit)
- Performance metrics use bounded memory with automatic cleanup
- Vector memory implements LRU eviction policies

### Load Balancing
- Agent selection considers real-time load factors
- Automatic load distribution prevents agent overload
- Performance-based selection optimizes resource utilization

## Security Features

### Secure Communication
- All inter-agent messages are validated and sanitized
- Agent registration requires proper authentication
- Message routing includes authorization checks

### Audit Trail
- Complete audit trail for all agent operations
- Selection decisions logged with full context
- Performance metrics tracked for compliance

### Resource Protection
- Task execution limits prevent resource exhaustion
- Circuit breakers protect against cascading failures
- Graceful degradation maintains system stability

---

*For implementation examples and integration patterns, see the Developer Onboarding Guide.*