# Developer Onboarding Guide

## Welcome to Agent Orchestrated ETL

This guide will help you get started as a developer on the Agent Orchestrated ETL project. By the end of this guide, you'll have a complete development environment set up and understand the system architecture and development workflows.

## Getting Started

### Prerequisites

#### Required Software
- **Python 3.8+** (3.11 recommended)
- **Git 2.30+**
- **Docker 20.10+** (for local services)
- **IDE/Editor**: VS Code, PyCharm, or your preferred Python IDE

#### Recommended Tools
- **Docker Compose** (for orchestrated services)
- **PostgreSQL Client** (psql or pgAdmin)
- **REST Client** (Postman, curl, or httpie)
- **Git Flow** (for branch management)

### Development Environment Setup

#### 1. Clone Repository
```bash
# Clone the repository
git clone https://github.com/yourorg/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# Set up git hooks for code quality
./scripts/setup-git-hooks.sh
```

#### 2. Python Environment
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Upgrade pip and install tools
pip install --upgrade pip
pip install -r requirements-dev.txt
```

#### 3. Local Services Setup
```bash
# Start local development services
docker-compose -f docker-compose.dev.yml up -d

# Verify services are running
docker-compose ps
```

#### 4. Environment Configuration
```bash
# Copy development environment template
cp .env.example .env.dev

# Edit configuration for local development
nano .env.dev
```

**Development Environment Variables:**
```bash
# Development configuration
ENVIRONMENT=development
DEBUG=true
LOG_LEVEL=DEBUG

# Local database
DATABASE_URL=postgresql://dev_user:dev_password@localhost:5433/agent_etl_dev

# Local vector store
CHROMADB_HOST=localhost
CHROMADB_PORT=8001

# Development secrets (change these)
SECRET_KEY=dev-secret-key-change-me
ENCRYPTION_KEY=dev-encryption-key-32-chars-long
```

#### 5. Database Initialization
```bash
# Initialize development database
python scripts/init_database.py --env=dev

# Run database migrations
python scripts/migrate_database.py --env=dev

# Load sample data (optional)
python scripts/load_sample_data.py --env=dev
```

#### 6. Verification
```bash
# Run system health check
python scripts/health_check.py --env=dev

# Run basic tests
pytest tests/unit/ -v

# Start development server
python -m src.agent_orchestrated_etl.main --env=dev
```

### IDE Configuration

#### VS Code Setup
1. **Install Python Extension**: Microsoft Python extension
2. **Configure Python Interpreter**: Select your virtual environment
3. **Install Recommended Extensions**:
   ```json
   {
     "recommendations": [
       "ms-python.python",
       "ms-python.flake8",
       "ms-python.black-formatter",
       "ms-python.isort",
       "ms-python.mypy-type-checker",
       "ms-vscode.vscode-json"
     ]
   }
   ```

4. **Workspace Settings** (`.vscode/settings.json`):
   ```json
   {
     "python.defaultInterpreterPath": "./venv/bin/python",
     "python.formatting.provider": "black",
     "python.linting.enabled": true,
     "python.linting.flake8Enabled": true,
     "python.linting.mypyEnabled": true,
     "python.testing.pytestEnabled": true,
     "python.testing.pytestArgs": ["tests"],
     "files.exclude": {
       "**/__pycache__": true,
       "**/*.pyc": true
     }
   }
   ```

#### PyCharm Setup
1. **Configure Project Interpreter**: Point to your virtual environment
2. **Enable Code Inspections**: Python, SQL, Security inspections
3. **Configure Code Style**: Black formatter, import optimization
4. **Set up Run Configurations**: For main application and tests

## Architecture Overview

### System Components

#### 1. Agent Layer
- **Agent Coordinator**: Central orchestration and intelligent agent selection
- **ETL Agent**: Data processing operations (extract, transform, load)
- **Monitoring Agent**: System health and performance monitoring
- **Quality Agent**: Data validation and quality assessment

#### 2. Core Services
- **Communication Hub**: Inter-agent message routing and coordination
- **Vector Memory Store**: Semantic memory using ChromaDB
- **Tool Registry**: Comprehensive tool ecosystem for data operations
- **Cache System**: Intelligent caching with LRU cleanup

#### 3. Data Processing
- **Multi-Source Extraction**: Databases, APIs, files, streams
- **Intelligent Transformation**: Rule-based and ML-enhanced processing
- **Quality Validation**: Comprehensive data quality assessment
- **Performance Optimization**: Caching, connection pooling, parallel processing

### Key Design Patterns

#### 1. Agent-Based Architecture
```python
# Agent base class example
class BaseAgent(ABC):
    async def execute_task(self, task: AgentTask) -> Dict[str, Any]:
        # Common execution pattern with resilience
        async with self._task_lock:
            try:
                result = await self._process_task(task)
                self._update_metrics(success=True)
                return result
            except Exception as e:
                self._update_metrics(success=False)
                raise AgentException(f"Task execution failed: {e}")
```

#### 2. Tool Pattern
```python
# Tool base class example
class ETLTool(ABC):
    @abstractmethod
    async def execute(self, **kwargs) -> Dict[str, Any]:
        """Execute tool with standardized interface."""
        pass
    
    def validate_inputs(self, inputs: Dict[str, Any]) -> bool:
        """Validate tool inputs before execution."""
        return True
```

#### 3. Event-Driven Communication
```python
# Message passing pattern
class Message(BaseModel):
    sender_id: str
    recipient_id: str
    message_type: MessageType
    content: Dict[str, Any]
    timestamp: float = Field(default_factory=time.time)
```

## Development Workflow

### Git Workflow

#### Branch Strategy
- **main**: Production-ready code
- **develop**: Integration branch for features
- **feature/\***: Individual feature development
- **hotfix/\***: Emergency fixes for production
- **release/\***: Release preparation branches

#### Development Process
```bash
# Start new feature
git checkout develop
git pull origin develop
git checkout -b feature/your-feature-name

# Make changes and commit
git add .
git commit -m "feat: implement your feature"

# Push and create pull request
git push origin feature/your-feature-name
# Create PR from feature branch to develop
```

#### Commit Message Convention
```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### Test-Driven Development (TDD)

#### TDD Cycle: RED-GREEN-REFACTOR

1. **RED Phase**: Write failing tests
```python
# test_new_feature.py
def test_new_agent_capability():
    """Test new agent capability (should fail initially)."""
    agent = create_test_agent()
    result = agent.execute_new_capability("test_data")
    assert result["status"] == "success"
    assert "processed_data" in result
```

2. **GREEN Phase**: Implement minimal code to pass tests
```python
# agent.py
def execute_new_capability(self, data: str) -> Dict[str, Any]:
    """Minimal implementation to pass test."""
    return {
        "status": "success",
        "processed_data": f"processed_{data}"
    }
```

3. **REFACTOR Phase**: Improve code quality and design
```python
# agent.py (refactored)
def execute_new_capability(self, data: str) -> Dict[str, Any]:
    """Robust implementation with error handling."""
    try:
        validated_data = self._validate_input(data)
        processed = self._process_data(validated_data)
        return {
            "status": "success",
            "processed_data": processed,
            "metadata": self._generate_metadata()
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}
```

#### Testing Best Practices

1. **Unit Tests**: Test individual components in isolation
```python
@pytest.fixture
def mock_database():
    """Mock database for testing."""
    with patch('src.database.get_connection') as mock:
        mock.return_value = MockConnection()
        yield mock

def test_data_extraction(mock_database):
    """Test data extraction logic."""
    extractor = DataExtractor()
    result = extractor.extract_data("test_query")
    assert result is not None
    mock_database.assert_called_once()
```

2. **Integration Tests**: Test component interactions
```python
@pytest.mark.integration
async def test_agent_coordination():
    """Test agent coordination workflow."""
    coordinator = AgentCoordinator(mock_hub)
    task = create_test_task()
    
    result = await coordinator.assign_task(task)
    assert result["status"] == "assigned"
    assert result["assigned_agent"] is not None
```

3. **End-to-End Tests**: Test complete workflows
```python
@pytest.mark.e2e
async def test_complete_etl_pipeline():
    """Test complete ETL pipeline execution."""
    pipeline_config = {
        "name": "test_pipeline",
        "tasks": [
            {"type": "extract", "source": "test_db"},
            {"type": "transform", "rules": "test_rules"},
            {"type": "load", "destination": "test_dest"}
        ]
    }
    
    result = await execute_pipeline(pipeline_config)
    assert result["status"] == "completed"
    assert result["records_processed"] > 0
```

### Code Quality Standards

#### Code Formatting
```bash
# Format code with Black
black src/ tests/

# Sort imports with isort
isort src/ tests/

# Check formatting
black --check src/ tests/
isort --check-only src/ tests/
```

#### Linting
```bash
# Run flake8 for style checking
flake8 src/ tests/

# Run mypy for type checking
mypy src/

# Run pylint for comprehensive analysis
pylint src/
```

#### Security Scanning
```bash
# Scan for security vulnerabilities
bandit -r src/

# Check dependencies for vulnerabilities
safety check

# Scan for secrets in code
detect-secrets scan
```

## Development Tools and Utilities

### Debugging Tools

#### Database Debugging
```python
# Database query profiler
class QueryProfiler:
    def profile_query(self, query: str) -> Dict[str, Any]:
        """Profile database query performance."""
        start_time = time.time()
        
        with get_database_connection() as conn:
            explain_result = conn.execute(f"EXPLAIN ANALYZE {query}")
            query_result = conn.execute(query)
        
        execution_time = time.time() - start_time
        
        return {
            "query": query,
            "execution_time_ms": execution_time * 1000,
            "query_plan": explain_result.fetchall(),
            "result_count": len(query_result.fetchall())
        }
```

#### Agent Debugging
```python
# Agent state inspector
class AgentDebugger:
    def inspect_agent_state(self, agent_id: str) -> Dict[str, Any]:
        """Inspect current agent state for debugging."""
        agent = self.get_agent(agent_id)
        
        return {
            "agent_id": agent_id,
            "current_state": agent.state.value,
            "active_tasks": len(agent.active_tasks),
            "performance_metrics": agent.get_performance_metrics(),
            "current_load": agent.get_current_load(),
            "memory_usage": self._get_memory_usage(agent),
            "recent_errors": self._get_recent_errors(agent)
        }
```

### Performance Profiling

#### CPU Profiling
```python
import cProfile
import pstats

def profile_function(func, *args, **kwargs):
    """Profile function execution."""
    profiler = cProfile.Profile()
    profiler.enable()
    
    result = func(*args, **kwargs)
    
    profiler.disable()
    stats = pstats.Stats(profiler)
    stats.sort_stats('cumulative')
    stats.print_stats(10)
    
    return result
```

#### Memory Profiling
```python
from memory_profiler import profile

@profile
def memory_intensive_function():
    """Function with memory profiling."""
    # Function implementation
    pass
```

### Development Scripts

#### Data Generation
```bash
# Generate test data
python scripts/generate_test_data.py --records=1000 --format=json

# Create sample pipelines
python scripts/create_sample_pipelines.py --count=5

# Load benchmark data
python scripts/load_benchmark_data.py --dataset=large
```

#### Environment Management
```bash
# Reset development environment
python scripts/reset_dev_env.py

# Update dependencies
python scripts/update_dependencies.py

# Clean up temporary files
python scripts/cleanup_dev.py
```

## Contributing Guidelines

### Code Review Process

1. **Self-Review Checklist**:
   - [ ] Code follows style guidelines
   - [ ] Tests added for new functionality
   - [ ] Documentation updated
   - [ ] Security considerations addressed
   - [ ] Performance impact assessed

2. **Pull Request Template**:
   ```markdown
   ## Description
   Brief description of changes
   
   ## Type of Change
   - [ ] Bug fix
   - [ ] New feature
   - [ ] Documentation update
   - [ ] Performance improvement
   
   ## Testing
   - [ ] Unit tests pass
   - [ ] Integration tests pass
   - [ ] Manual testing completed
   
   ## Security Review
   - [ ] No security vulnerabilities introduced
   - [ ] Sensitive data handling reviewed
   - [ ] Input validation implemented
   ```

3. **Review Criteria**:
   - Code quality and maintainability
   - Test coverage and quality
   - Security implications
   - Performance impact
   - Documentation completeness

### Documentation Standards

#### Code Documentation
```python
def process_data(
    data: List[Dict[str, Any]], 
    config: ProcessingConfig
) -> ProcessingResult:
    """Process data according to configuration.
    
    Args:
        data: List of data records to process
        config: Processing configuration object
        
    Returns:
        ProcessingResult containing processed data and metadata
        
    Raises:
        ProcessingError: If data processing fails
        ValidationError: If data validation fails
        
    Example:
        >>> config = ProcessingConfig(rules=["validate_email"])
        >>> data = [{"email": "user@example.com"}]
        >>> result = process_data(data, config)
        >>> assert result.status == "success"
    """
    # Implementation here
```

#### API Documentation
- Use OpenAPI/Swagger for REST API documentation
- Include request/response examples
- Document error codes and messages
- Provide SDK examples and tutorials

## Resources and Support

### Documentation Links
- [Architecture Overview](../CODEBASE_OVERVIEW.md)
- [API Reference](../api/)
- [Deployment Guide](../deployment/)
- [Security Documentation](../security/)

### Development Resources
- **Issue Tracker**: GitHub Issues for bug reports and feature requests
- **Wiki**: Confluence/GitHub Wiki for detailed documentation
- **Chat**: Slack/Discord for real-time communication
- **Code Review**: GitHub PR review process

### Learning Resources
- **Python Best Practices**: PEP 8, typing, async programming
- **Agent Patterns**: Multi-agent systems, coordination algorithms
- **ETL Design**: Data pipeline patterns, stream processing
- **Security**: OWASP guidelines, secure coding practices

### Getting Help

1. **Documentation First**: Check existing documentation
2. **Search Issues**: Look for similar problems in issue tracker
3. **Ask Team**: Reach out to team members via chat
4. **Create Issue**: Document and report new issues
5. **Pair Programming**: Schedule sessions for complex problems

## Next Steps

After completing this onboarding:

1. **Complete Tutorial**: Work through the hands-on tutorial
2. **Choose First Task**: Pick a "good first issue" from the backlog
3. **Set Up Monitoring**: Configure development monitoring tools
4. **Join Team Meetings**: Participate in standups and planning
5. **Contribute**: Start making meaningful contributions to the project

Welcome to the team! We're excited to have you contribute to the Agent Orchestrated ETL project.

---

*For additional development resources and team-specific information, check the internal wiki and team channels.*