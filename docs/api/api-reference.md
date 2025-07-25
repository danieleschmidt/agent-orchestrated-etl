# API Reference Documentation

## Overview

The Agent Orchestrated ETL system provides both REST APIs for external integration and internal APIs for agent communication. This document covers all available APIs, their usage patterns, and integration examples.

## REST API Endpoints

### Authentication

All API endpoints require authentication using JWT tokens.

**Authenticate**:
```http
POST /api/v1/auth/login
Content-Type: application/json

{
  "username": "your_username",
  "password": "your_password"
}
```

**Response**:
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "refresh_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "expires_in": 3600
}
```

### Workflow Management

#### Create Workflow

Creates a new ETL workflow with specified requirements.

```http
POST /api/v1/workflows
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "name": "Customer Data ETL",
  "description": "Extract customer data from CRM and load to warehouse",
  "requirements": {
    "data_source": "postgresql://crm.example.com/customers",
    "target": "data_warehouse",
    "schedule": "0 2 * * *",
    "priority": "high"
  },
  "configuration": {
    "batch_size": 1000,
    "max_retries": 3,
    "timeout": 3600
  }
}
```

**Response**:
```json
{
  "workflow_id": "workflow_20250725_001",
  "status": "created",
  "created_at": "2025-07-25T10:00:00Z",
  "estimated_duration": 1800,
  "agents_assigned": [
    {
      "agent_id": "etl_worker_001",
      "role": "extractor",
      "specialization": "database"
    }
  ]
}
```

#### Get Workflow Status

Retrieves the current status and progress of a workflow.

```http
GET /api/v1/workflows/{workflow_id}
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "workflow_id": "workflow_20250725_001",
  "name": "Customer Data ETL",
  "status": "running",
  "progress": {
    "completed_tasks": 2,
    "total_tasks": 4,
    "percentage": 50
  },
  "current_step": {
    "step_name": "data_transformation",
    "status": "in_progress",
    "started_at": "2025-07-25T10:15:00Z",
    "estimated_completion": "2025-07-25T10:25:00Z"
  },
  "execution_plan": {
    "steps": [
      {
        "step_id": "extract",
        "name": "Data Extraction",
        "status": "completed",
        "duration": 300,
        "records_processed": 10000
      },
      {
        "step_id": "transform",
        "name": "Data Transformation", 
        "status": "in_progress",
        "started_at": "2025-07-25T10:15:00Z"
      }
    ]
  },
  "metrics": {
    "records_processed": 10000,
    "data_volume_mb": 250,
    "processing_rate": 500.5,
    "error_count": 0
  }
}
```

#### List Workflows

Retrieves a list of workflows with optional filtering.

```http
GET /api/v1/workflows?status=running&limit=10&offset=0
Authorization: Bearer {access_token}
```

**Query Parameters**:
- `status`: Filter by workflow status (created, running, completed, failed)
- `priority`: Filter by priority level (low, medium, high, critical)
- `limit`: Maximum number of results (default: 20, max: 100)
- `offset`: Number of results to skip (default: 0)
- `created_after`: ISO 8601 timestamp filter
- `created_before`: ISO 8601 timestamp filter

**Response**:
```json
{
  "workflows": [
    {
      "workflow_id": "workflow_20250725_001",
      "name": "Customer Data ETL",
      "status": "running",
      "priority": "high",
      "created_at": "2025-07-25T10:00:00Z",
      "progress": 50
    }
  ],
  "pagination": {
    "total": 25,
    "limit": 10,
    "offset": 0,
    "has_next": true
  }
}
```

#### Cancel Workflow

Cancels a running workflow gracefully.

```http
DELETE /api/v1/workflows/{workflow_id}
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "reason": "Manual cancellation",
  "force": false
}
```

**Response**:
```json
{
  "workflow_id": "workflow_20250725_001",
  "status": "cancelling",
  "message": "Workflow cancellation initiated",
  "cancelled_at": "2025-07-25T10:30:00Z"
}
```

### Agent Management

#### List Agents

Retrieves information about all registered agents.

```http
GET /api/v1/agents
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "agents": [
    {
      "agent_id": "orchestrator_001",
      "type": "orchestrator",
      "status": "healthy",
      "version": "1.0.0",
      "capabilities": [
        "workflow_creation",
        "task_coordination",
        "agent_selection"
      ],
      "performance": {
        "active_workflows": 3,
        "completed_tasks": 150,
        "success_rate": 0.98,
        "average_response_time": 250
      },
      "resource_usage": {
        "cpu_percent": 35.2,
        "memory_mb": 245,
        "memory_percent": 24.5
      },
      "last_heartbeat": "2025-07-25T10:29:45Z"
    },
    {
      "agent_id": "etl_worker_001",
      "type": "etl",
      "status": "busy",
      "specialization": "database",
      "current_task": {
        "task_id": "extract_customer_data",
        "workflow_id": "workflow_20250725_001",
        "started_at": "2025-07-25T10:15:00Z"
      },
      "performance": {
        "completed_tasks": 89,
        "success_rate": 0.99,
        "average_task_duration": 180
      }
    }
  ]
}
```

#### Get Agent Details

Retrieves detailed information about a specific agent.

```http
GET /api/v1/agents/{agent_id}
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "agent_id": "etl_worker_001",
  "type": "etl",
  "status": "healthy",
  "version": "1.0.0",
  "specialization": "database",
  "capabilities": [
    "postgresql_extraction",
    "mysql_extraction",
    "data_transformation",
    "data_validation"
  ],
  "configuration": {
    "max_concurrent_tasks": 5,
    "batch_size": 1000,
    "timeout": 3600,
    "retry_attempts": 3
  },
  "performance_history": [
    {
      "timestamp": "2025-07-25T10:00:00Z",
      "cpu_percent": 45.2,
      "memory_mb": 312,
      "active_tasks": 2,
      "task_completion_rate": 15.5
    }
  ],
  "recent_tasks": [
    {
      "task_id": "extract_001",
      "workflow_id": "workflow_20250725_001",
      "status": "completed",
      "duration": 245,
      "records_processed": 5000
    }
  ]
}
```

### Data Source Management

#### Register Data Source

Registers a new data source for ETL operations.

```http
POST /api/v1/data-sources
Authorization: Bearer {access_token}
Content-Type: application/json

{
  "name": "CRM Database",
  "type": "postgresql",
  "connection": {
    "host": "crm.example.com",
    "port": 5432,
    "database": "crm_prod",
    "ssl_enabled": true
  },
  "credentials": {
    "secret_name": "crm-db-credentials",
    "secret_provider": "aws_secrets_manager"
  },
  "configuration": {
    "pool_size": 5,
    "timeout": 30,
    "read_only": true
  },
  "metadata": {
    "business_owner": "sales-team",
    "data_classification": "sensitive",
    "backup_schedule": "daily"
  }
}
```

**Response**:
```json
{
  "source_id": "src_20250725_001",
  "name": "CRM Database",
  "status": "registered",
  "connection_test": {
    "status": "success",
    "response_time": 25,
    "tested_at": "2025-07-25T10:00:00Z"
  },
  "schema_info": {
    "tables": 15,
    "views": 3,
    "estimated_size_mb": 2500
  }
}
```

#### Test Data Source Connection

Tests connectivity to a data source.

```http
POST /api/v1/data-sources/{source_id}/test
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "source_id": "src_20250725_001",
  "connection_test": {
    "status": "success",
    "response_time": 23,
    "tested_at": "2025-07-25T10:35:00Z",
    "details": {
      "host_reachable": true,
      "authentication": "success",
      "permissions": "read_only",
      "schema_accessible": true
    }
  }
}
```

### Monitoring and Metrics

#### Get System Metrics

Retrieves system-wide performance metrics.

```http
GET /api/v1/metrics
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "timestamp": "2025-07-25T10:30:00Z",
  "system_health": {
    "status": "healthy",
    "uptime_seconds": 86400,
    "version": "1.0.0"
  },
  "workflow_metrics": {
    "active_workflows": 5,
    "completed_today": 25,
    "failed_today": 1,
    "success_rate": 0.96,
    "average_duration": 1800
  },
  "agent_metrics": {
    "total_agents": 8,
    "healthy_agents": 8,
    "busy_agents": 4,
    "average_cpu_usage": 45.2,
    "average_memory_usage": 312.5
  },
  "data_metrics": {
    "records_processed_today": 1500000,
    "data_volume_mb_today": 25600,
    "processing_rate": 850.5,
    "error_rate": 0.001
  },
  "infrastructure_metrics": {
    "database_connections": 15,
    "cache_hit_rate": 0.89,
    "network_latency_ms": 12.5,
    "disk_usage_percent": 65.2
  }
}
```

#### Get Workflow Metrics

Retrieves detailed metrics for a specific workflow.

```http
GET /api/v1/workflows/{workflow_id}/metrics
Authorization: Bearer {access_token}
```

**Response**:
```json
{
  "workflow_id": "workflow_20250725_001",
  "execution_metrics": {
    "total_duration": 1850,
    "processing_time": 1650,
    "overhead_time": 200,
    "records_processed": 50000,
    "processing_rate": 27.0,
    "data_volume_mb": 1250
  },
  "step_metrics": [
    {
      "step_name": "extraction",
      "duration": 480,
      "records_processed": 50000,
      "success_rate": 1.0,
      "resource_usage": {
        "cpu_percent": 65.0,
        "memory_mb": 450,
        "network_io_mb": 1250
      }
    },
    {
      "step_name": "transformation",
      "duration": 920,
      "records_processed": 50000,
      "success_rate": 0.999,
      "validation_errors": 25
    }
  ],
  "quality_metrics": {
    "data_completeness": 0.998,
    "data_accuracy": 0.995,
    "schema_compliance": 1.0,
    "duplicate_rate": 0.002
  }
}
```

## WebSocket API

### Real-time Updates

Connect to the WebSocket endpoint for real-time updates.

**Connection**:
```javascript
const ws = new WebSocket('wss://agent-etl.example.com/ws');

// Authentication
ws.onopen = function() {
    ws.send(JSON.stringify({
        type: 'auth',
        token: 'your_jwt_token'
    }));
};

// Subscribe to workflow updates
ws.send(JSON.stringify({
    type: 'subscribe',
    channel: 'workflow_updates',
    workflow_id: 'workflow_20250725_001'
}));
```

**Message Types**:

**Workflow Status Update**:
```json
{
  "type": "workflow_status",
  "workflow_id": "workflow_20250725_001",
  "status": "running",
  "progress": 75,
  "current_step": "data_loading",
  "timestamp": "2025-07-25T10:35:00Z"
}
```

**Agent Status Update**:
```json
{
  "type": "agent_status",
  "agent_id": "etl_worker_001",
  "status": "busy",
  "current_task": "transform_customer_data",
  "resource_usage": {
    "cpu_percent": 75.2,
    "memory_mb": 512
  },
  "timestamp": "2025-07-25T10:35:00Z"
}
```

**System Alert**:
```json
{
  "type": "system_alert",
  "severity": "warning",
  "message": "High memory usage detected on etl_worker_002",
  "details": {
    "agent_id": "etl_worker_002",
    "memory_percent": 88.5,
    "threshold": 85.0
  },
  "timestamp": "2025-07-25T10:35:00Z"
}
```

## Internal Agent APIs

### Agent Communication Protocol

#### Message Format

All inter-agent messages follow this format:

```json
{
  "message_id": "msg_20250725_001",
  "message_type": "task_assignment|status_update|coordination|error",
  "source_agent": "orchestrator_001",
  "target_agent": "etl_worker_001",
  "timestamp": "2025-07-25T10:00:00Z",
  "payload": {
    // Message-specific data
  },
  "correlation_id": "workflow_20250725_001",
  "priority": 5,
  "expires_at": "2025-07-25T11:00:00Z"
}
```

#### Task Assignment Message

```json
{
  "message_type": "task_assignment",
  "payload": {
    "task": {
      "task_id": "extract_001",
      "task_type": "data_extraction",
      "description": "Extract customer data from PostgreSQL",
      "inputs": {
        "source_config": {
          "type": "postgresql",
          "connection_string": "postgresql://...",
          "query": "SELECT * FROM customers WHERE updated_at > %s",
          "parameters": ["2025-07-24T00:00:00Z"]
        },
        "batch_size": 1000,
        "timeout": 3600
      },
      "constraints": {
        "memory_limit": "1GB",
        "cpu_limit": "80%",
        "max_duration": 3600
      }
    }
  }
}
```

#### Status Update Message

```json
{
  "message_type": "status_update",
  "payload": {
    "task_id": "extract_001",
    "status": "completed",
    "result": {
      "records_extracted": 50000,
      "data_volume_mb": 1250,
      "execution_time": 480,
      "quality_score": 0.995,
      "validation_errors": 25
    },
    "metadata": {
      "agent_load": 0.65,
      "memory_usage": 450,
      "next_available": "2025-07-25T10:45:00Z"
    }
  }
}
```

### Agent Registration API

#### Register Agent

```python
# Agent registration with communication hub
async def register_agent(self, agent_info: Dict[str, Any]) -> bool:
    registration_message = {
        "message_type": "agent_registration",
        "payload": {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "version": "1.0.0",
            "capabilities": self.capabilities,
            "specialization": getattr(self, 'specialization', None),
            "max_concurrent_tasks": self.max_concurrent_tasks,
            "resource_limits": {
                "memory_mb": 1024,
                "cpu_cores": 2
            },
            "endpoint": f"http://{self.host}:{self.port}",
            "health_check_url": "/health"
        }
    }
    
    return await self.communication_hub.register_agent(registration_message)
```

#### Heartbeat

```python
# Regular heartbeat to maintain registration
async def send_heartbeat(self):
    heartbeat_message = {
        "message_type": "heartbeat",
        "payload": {
            "agent_id": self.agent_id,
            "status": self.status,
            "current_load": self.get_current_load(),
            "active_tasks": len(self.active_tasks),
            "resource_usage": {
                "cpu_percent": psutil.cpu_percent(),
                "memory_mb": psutil.virtual_memory().used // 1024 // 1024,
                "memory_percent": psutil.virtual_memory().percent
            },
            "last_task_completion": self.last_task_completion
        }
    }
    
    await self.communication_hub.send_message(heartbeat_message)
```

## Error Handling

### HTTP Status Codes

- `200 OK`: Request successful
- `201 Created`: Resource created successfully
- `400 Bad Request`: Invalid request parameters
- `401 Unauthorized`: Authentication required
- `403 Forbidden`: Insufficient permissions
- `404 Not Found`: Resource not found
- `409 Conflict`: Resource conflict (e.g., duplicate workflow name)
- `422 Unprocessable Entity`: Validation errors
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: Server error
- `502 Bad Gateway`: Downstream service error
- `503 Service Unavailable`: System overloaded

### Error Response Format

```json
{
  "error": {
    "code": "WORKFLOW_NOT_FOUND",
    "message": "Workflow with ID 'workflow_invalid' was not found",
    "details": {
      "workflow_id": "workflow_invalid",
      "suggestion": "Check the workflow ID and try again"
    },
    "timestamp": "2025-07-25T10:30:00Z",
    "request_id": "req_20250725_001"
  }
}
```

### Common Error Codes

- `INVALID_CREDENTIALS`: Authentication failed
- `INSUFFICIENT_PERMISSIONS`: Authorization failed
- `WORKFLOW_NOT_FOUND`: Workflow does not exist
- `AGENT_UNAVAILABLE`: No suitable agents available
- `DATA_SOURCE_ERROR`: Data source connection failed
- `VALIDATION_ERROR`: Input validation failed
- `RESOURCE_LIMIT_EXCEEDED`: System resource limits exceeded
- `TIMEOUT_ERROR`: Operation timed out
- `INTERNAL_ERROR`: Unexpected system error

## Rate Limiting

### Rate Limit Headers

All API responses include rate limiting headers:

```http
X-RateLimit-Limit: 1000
X-RateLimit-Remaining: 995
X-RateLimit-Reset: 1753425600
X-RateLimit-Window: 3600
```

### Rate Limits by Endpoint

- **Authentication**: 10 requests/minute
- **Workflow Creation**: 100 requests/hour
- **Workflow Status**: 1000 requests/hour
- **Agent Management**: 500 requests/hour
- **Metrics**: 200 requests/minute

## SDK Examples

### Python SDK

```python
from agent_etl_client import AgentETLClient

# Initialize client
client = AgentETLClient(
    base_url="https://agent-etl.example.com",
    api_key="your_api_key"
)

# Create workflow
workflow = client.workflows.create({
    "name": "Customer ETL",
    "requirements": {
        "data_source": "postgresql://crm.db/customers",
        "target": "data_warehouse"
    }
})

# Monitor workflow
status = client.workflows.get_status(workflow.id)
print(f"Workflow {workflow.id} is {status.status}")

# List agents
agents = client.agents.list()
for agent in agents:
    print(f"Agent {agent.id}: {agent.status}")
```

### JavaScript SDK

```javascript
import { AgentETLClient } from '@agent-etl/client';

// Initialize client
const client = new AgentETLClient({
  baseURL: 'https://agent-etl.example.com',
  apiKey: 'your_api_key'
});

// Create workflow
const workflow = await client.workflows.create({
  name: 'Customer ETL',
  requirements: {
    data_source: 'postgresql://crm.db/customers',
    target: 'data_warehouse'
  }
});

// Subscribe to real-time updates
client.websocket.subscribe('workflow_updates', workflow.id, (update) => {
  console.log(`Workflow ${workflow.id}: ${update.status}`);
});
```

## Authentication Examples

### JWT Token Usage

```bash
# Get access token
curl -X POST https://agent-etl.example.com/api/v1/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"user","password":"pass"}'

# Use token in subsequent requests
curl -X GET https://agent-etl.example.com/api/v1/workflows \
  -H "Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
```

### API Key Usage

```bash
# Using API key header
curl -X GET https://agent-etl.example.com/api/v1/workflows \
  -H "X-API-Key: your_api_key_here"
```

---

*Last Updated: 2025-07-25*  
*Document Version: 1.0*  
*Maintained by: Terragon Labs - Agent Orchestrated ETL Team*