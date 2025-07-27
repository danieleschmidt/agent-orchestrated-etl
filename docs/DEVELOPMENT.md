# Development Guide

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose
- Git
- Make (optional, for convenience commands)

### Quick Setup

```bash
# Clone the repository
git clone https://github.com/your-org/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# Set up development environment
make install-dev

# Copy environment template
cp .env.example .env

# Start development services
make docker-compose

# Run tests to verify setup
make test
```

## Development Environment

### Option 1: Local Development

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e .[dev]

# Install pre-commit hooks
pre-commit install

# Set up environment variables
cp .env.example .env
# Edit .env with your configuration
```

### Option 2: Dev Container (Recommended)

```bash
# Open in VS Code with Dev Containers extension
code .

# VS Code will prompt to reopen in container
# Or use Command Palette: "Dev Containers: Reopen in Container"
```

### Option 3: Docker Development

```bash
# Build and start all services
docker-compose up -d

# Access the development container
docker exec -it agent-etl-app bash

# Run commands inside container
pytest tests/
python -m agent_orchestrated_etl.cli --help
```

## Project Structure

```
agent-orchestrated-etl/
├── src/
│   └── agent_orchestrated_etl/    # Main application code
│       ├── agents/                # Agent implementations
│       ├── monitoring/            # Monitoring and health checks
│       ├── cli.py                 # Command-line interface
│       ├── config.py              # Configuration management
│       ├── orchestrator.py        # Main orchestrator logic
│       └── health.py              # Health check utilities
├── tests/                         # Test suite
│   ├── integration/               # Integration tests
│   ├── performance/               # Performance tests
│   ├── fixtures/                  # Test data and fixtures
│   └── conftest.py                # Pytest configuration
├── docs/                          # Documentation
│   ├── api/                       # API documentation
│   ├── architecture/              # Architecture docs
│   ├── deployment/                # Deployment guides
│   ├── developer/                 # Developer guides
│   ├── operations/                # Operations runbooks
│   └── security/                  # Security documentation
├── monitoring/                    # Monitoring configuration
│   ├── prometheus.yml             # Prometheus config
│   ├── alert_rules.yml            # Alert rules
│   └── grafana/                   # Grafana dashboards
├── .github/                       # GitHub workflows and templates
├── .devcontainer/                 # Dev container configuration
├── .vscode/                       # VS Code settings
├── docker-compose.yml             # Development services
├── Dockerfile                     # Application container
├── Makefile                       # Build automation
└── pyproject.toml                 # Python project configuration
```

## Development Workflow

### 1. Feature Development

```bash
# Create feature branch
git checkout -b feature/your-feature-name

# Make changes and commit frequently
git add .
git commit -m "feat: add new feature"

# Run tests and quality checks
make validate

# Push and create pull request
git push origin feature/your-feature-name
```

### 2. Code Quality

We maintain high code quality through automated checks:

```bash
# Run all quality checks
make validate

# Individual checks
make lint          # Code linting
make format        # Code formatting
make typecheck     # Type checking
make test          # Run tests
make security      # Security scanning
```

### 3. Testing

```bash
# Run all tests
make test

# Run specific test types
make test-unit        # Unit tests only
make test-integration # Integration tests only

# Run with coverage
make test-coverage

# Run performance tests
pytest tests/performance/ -v
```

### 4. Documentation

```bash
# Build documentation
make docs

# Serve documentation locally
make docs-serve

# Generate API documentation
# API docs are auto-generated from docstrings
```

## Coding Standards

### Python Style

- Follow PEP 8 with 88-character line length
- Use type hints for all function signatures
- Write docstrings for all public functions and classes
- Use meaningful variable and function names

Example:
```python
def process_data_source(
    source_url: str, 
    options: Dict[str, Any] = None
) -> ProcessingResult:
    """Process data from the specified source.
    
    Args:
        source_url: URL or identifier for the data source
        options: Optional processing configuration
        
    Returns:
        ProcessingResult containing processed data and metadata
        
    Raises:
        DataSourceError: If source cannot be accessed
        ValidationError: If data validation fails
    """
    # Implementation here
    pass
```

### Error Handling

- Use specific exception types
- Provide meaningful error messages
- Log errors appropriately
- Include context in error messages

```python
try:
    result = process_data(data)
except ValidationError as e:
    logger.error("Data validation failed for source %s: %s", source_id, e)
    raise ProcessingError(f"Failed to process {source_id}: {e}") from e
```

### Logging

```python
import logging

logger = logging.getLogger(__name__)

# Use appropriate log levels
logger.debug("Detailed debugging information")
logger.info("General information about execution")
logger.warning("Something unexpected happened")
logger.error("An error occurred")
logger.critical("A serious error occurred")
```

## Agent Development

### Creating a New Agent

```python
from agent_orchestrated_etl.agents.base_agent import BaseAgent

class MyCustomAgent(BaseAgent):
    """Custom agent for specific functionality."""
    
    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.agent_type = "custom"
    
    async def execute(self, task: Task) -> TaskResult:
        """Execute the agent's main functionality."""
        # Implementation here
        pass
    
    def validate_config(self, config: Dict[str, Any]) -> None:
        """Validate agent configuration."""
        required_fields = ["field1", "field2"]
        for field in required_fields:
            if field not in config:
                raise ConfigurationError(f"Missing required field: {field}")
```

### Agent Registration

```python
# Register your agent
from agent_orchestrated_etl.orchestrator import register_agent

register_agent("custom", MyCustomAgent)
```

## Testing Guidelines

### Unit Tests

```python
import pytest
from unittest.mock import Mock, patch

class TestMyComponent:
    
    def test_successful_processing(self):
        # Arrange
        component = MyComponent()
        test_data = {"key": "value"}
        
        # Act
        result = component.process(test_data)
        
        # Assert
        assert result.success is True
        assert result.data == expected_data
    
    @patch('external_service.api_call')
    def test_external_service_failure(self, mock_api):
        # Mock external dependency
        mock_api.side_effect = ConnectionError("Service unavailable")
        
        component = MyComponent()
        
        with pytest.raises(ProcessingError):
            component.process_with_external_service(test_data)
```

### Integration Tests

```python
@pytest.mark.integration
class TestPipelineIntegration:
    
    def test_end_to_end_pipeline(self, sample_data_file):
        orchestrator = DataOrchestrator()
        pipeline = orchestrator.create_pipeline(
            source=f"file://{sample_data_file}"
        )
        
        result = pipeline.execute()
        
        assert result is not None
        assert len(result) > 0
```

## Configuration Management

### Environment Variables

Use the `.env` file for local development:

```bash
# Database
DATABASE_URL=postgresql://user:pass@localhost:5432/db

# External Services
OPENAI_API_KEY=your-api-key
AWS_ACCESS_KEY_ID=your-access-key

# Feature Flags
ENABLE_FEATURE_X=true
DEBUG_MODE=false
```

### Configuration Classes

```python
from pydantic import BaseSettings

class Settings(BaseSettings):
    database_url: str
    openai_api_key: str
    debug_mode: bool = False
    
    class Config:
        env_file = ".env"

settings = Settings()
```

## Debugging

### Local Debugging

```bash
# Set debug environment
export DEBUG=true

# Run with verbose logging
python -m agent_orchestrated_etl.cli --verbose

# Use Python debugger
import pdb; pdb.set_trace()
```

### Container Debugging

```bash
# Access running container
docker exec -it agent-etl-app bash

# View logs
docker logs agent-etl-app -f

# Debug specific service
docker-compose logs -f app
```

### VS Code Debugging

Use the provided launch configurations in `.vscode/launch.json`:

- "Python: Current File" - Debug current file
- "Python: Agent ETL CLI" - Debug CLI commands
- "Python: Pytest Current File" - Debug current test file

## Performance Optimization

### Profiling

```bash
# Profile code execution
python -m cProfile -o profile.out your_script.py

# Analyze profile
python -c "import pstats; pstats.Stats('profile.out').sort_stats('cumulative').print_stats(20)"

# Memory profiling
pip install memory-profiler
python -m memory_profiler your_script.py
```

### Common Optimizations

1. **Database Queries**: Use connection pooling and query optimization
2. **Memory Usage**: Process data in chunks for large datasets
3. **Concurrency**: Use async/await for I/O-bound operations
4. **Caching**: Implement caching for frequently accessed data

## Troubleshooting

### Common Issues

1. **Import Errors**: Ensure PYTHONPATH includes `src/`
2. **Database Connection**: Check database is running and accessible
3. **Permission Errors**: Ensure proper file permissions in containers
4. **Port Conflicts**: Check if required ports are available

### Debug Commands

```bash
# Check environment
make env-check

# Verify dependencies
pip list

# Test database connection
python -c "from agent_orchestrated_etl.config import test_db_connection; test_db_connection()"

# Health check
curl http://localhost:8793/health
```

## Contributing

### Pull Request Process

1. Fork the repository
2. Create a feature branch
3. Make changes with tests
4. Run quality checks
5. Submit pull request
6. Address review feedback

### Code Review Checklist

- [ ] Code follows style guidelines
- [ ] Tests are included and passing
- [ ] Documentation is updated
- [ ] Security considerations addressed
- [ ] Performance impact considered
- [ ] Error handling implemented

## Resources

- [Python Documentation](https://docs.python.org/)
- [LangChain Documentation](https://python.langchain.com/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Docker Documentation](https://docs.docker.com/)
- [Pytest Documentation](https://docs.pytest.org/)

## Getting Help

- Check existing documentation
- Search GitHub issues
- Ask in team channels
- Create new GitHub issue with details