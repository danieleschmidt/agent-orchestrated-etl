# Testing Guide

This document provides comprehensive guidance for testing the Agent-Orchestrated-ETL system.

## Overview

Our testing strategy follows a multi-layered approach:

- **Unit Tests**: Test individual components in isolation
- **Integration Tests**: Test component interactions
- **End-to-End Tests**: Test complete workflows
- **Performance Tests**: Validate system performance
- **Security Tests**: Ensure security requirements are met
- **Contract Tests**: Validate API contracts

## Test Structure

```
tests/
├── conftest.py              # Global fixtures and configuration
├── utils/                   # Testing utilities and helpers
│   ├── __init__.py
│   └── test_helpers.py
├── fixtures/                # Test data and fixtures
│   ├── __init__.py
│   └── sample_data.py
├── unit/                    # Unit tests
├── integration/             # Integration tests
├── e2e/                     # End-to-end tests
├── performance/             # Performance tests
├── security/                # Security tests
└── contract/                # Contract tests
```

## Running Tests

### Basic Commands

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=src --cov-report=html

# Run specific test types
pytest -m unit
pytest -m integration
pytest -m e2e
pytest -m performance
pytest -m security

# Run tests in parallel
pytest -n auto

# Run tests with specific patterns
pytest -k "test_agent"
pytest tests/unit/
```

### Test Configuration

Configure test execution in `pyproject.toml`:

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
markers = [
    "unit: unit tests",
    "integration: integration tests", 
    "e2e: end-to-end tests",
    "performance: performance tests",
    "security: security tests",
    "slow: slow tests (skip with -m 'not slow')"
]
testpaths = ["tests"]
addopts = "-v --tb=short --strict-markers"
```

## Writing Tests

### Unit Tests

Unit tests should test individual components in isolation:

```python
import pytest
from unittest.mock import Mock, AsyncMock
from agent_orchestrated_etl.agents.etl_agent import ETLAgent

class TestETLAgent:
    """Test ETL Agent functionality."""
    
    async def test_extract_data_success(self, mock_config, mock_s3_client):
        """Test successful data extraction."""
        # Arrange
        agent = ETLAgent(config=mock_config)
        agent.s3_client = mock_s3_client
        
        mock_s3_client.get_object.return_value = {
            "Body": Mock(read=Mock(return_value=b"test,data\n1,2"))
        }
        
        # Act
        result = await agent.extract_data("s3://bucket/key")
        
        # Assert
        assert result is not None
        assert len(result) > 0
        mock_s3_client.get_object.assert_called_once()
```

### Integration Tests

Integration tests validate component interactions:

```python
import pytest
from agent_orchestrated_etl.orchestrator import DataOrchestrator

@pytest.mark.integration
class TestPipelineIntegration:
    """Test pipeline integration scenarios."""
    
    async def test_full_pipeline_execution(self, test_config, sample_pipeline_config):
        """Test complete pipeline execution."""
        # Arrange
        orchestrator = DataOrchestrator(config=test_config)
        
        # Act
        pipeline = await orchestrator.create_pipeline(sample_pipeline_config)
        result = await pipeline.execute()
        
        # Assert
        assert result.success is True
        assert result.records_processed > 0
```

### End-to-End Tests

E2E tests validate complete user workflows:

```python
import pytest
from agent_orchestrated_etl.cli import main

@pytest.mark.e2e
class TestCLIWorkflows:
    """Test CLI end-to-end workflows."""
    
    async def test_pipeline_creation_and_execution(self, temp_dir):
        """Test creating and executing pipeline via CLI."""
        # Arrange
        config_file = temp_dir / "config.yaml"
        output_file = temp_dir / "output.json"
        
        # Act
        result = await main([
            "run_pipeline", "s3",
            "--config", str(config_file),
            "--output", str(output_file)
        ])
        
        # Assert
        assert result.exit_code == 0
        assert output_file.exists()
```

### Performance Tests

Performance tests validate system performance:

```python
import pytest
import time
from agent_orchestrated_etl.agents.orchestrator_agent import OrchestratorAgent

@pytest.mark.performance
class TestAgentPerformance:
    """Test agent performance characteristics."""
    
    @pytest.mark.timeout(5)
    async def test_agent_decision_latency(self, mock_orchestrator_agent):
        """Test agent decision-making latency."""
        # Arrange
        agent = mock_orchestrator_agent
        request = {"source": "s3", "size": "large"}
        
        # Act
        start_time = time.perf_counter()
        decision = await agent.make_decision(request)
        end_time = time.perf_counter()
        
        # Assert
        latency = end_time - start_time
        assert latency < 1.0  # Should respond within 1 second
        assert decision is not None
    
    async def test_concurrent_pipeline_processing(self, orchestrator):
        """Test handling multiple concurrent pipelines."""
        # Arrange
        pipeline_configs = [create_test_pipeline_config() for _ in range(10)]
        
        # Act
        tasks = [orchestrator.create_pipeline(config) for config in pipeline_configs]
        results = await asyncio.gather(*tasks)
        
        # Assert
        assert len(results) == 10
        assert all(result.success for result in results)
```

### Security Tests

Security tests validate security requirements:

```python
import pytest
from agent_orchestrated_etl.validation import InputValidator

@pytest.mark.security
class TestSecurityValidation:
    """Test security validation."""
    
    def test_sql_injection_prevention(self):
        """Test SQL injection prevention."""
        validator = InputValidator()
        
        malicious_inputs = [
            "'; DROP TABLE users; --",
            "' OR '1'='1",
            "UNION SELECT * FROM passwords"
        ]
        
        for malicious_input in malicious_inputs:
            with pytest.raises(SecurityError):
                validator.validate_sql_query(malicious_input)
    
    def test_path_traversal_prevention(self):
        """Test path traversal prevention."""
        validator = InputValidator()
        
        malicious_paths = [
            "../../../etc/passwd",
            "..\\..\\windows\\system32",
            "/etc/shadow"
        ]
        
        for malicious_path in malicious_paths:
            with pytest.raises(SecurityError):
                validator.validate_file_path(malicious_path)
```

## Test Fixtures

### Global Fixtures (conftest.py)

Global fixtures are available to all tests:

```python
@pytest.fixture
def test_config():
    """Test configuration."""
    return {
        "database": {"url": "sqlite:///:memory:"},
        "agents": {"orchestrator": {"timeout": 5}}
    }

@pytest.fixture
def mock_orchestrator_agent(mock_config):
    """Mock orchestrator agent."""
    agent = Mock(spec=OrchestratorAgent)
    agent.config = mock_config
    return agent
```

### Local Fixtures

Create fixtures specific to test modules:

```python
# In test_etl_agent.py
@pytest.fixture
def etl_agent(mock_config):
    """ETL agent instance for testing."""
    return ETLAgent(config=mock_config)

@pytest.fixture
def sample_csv_data():
    """Sample CSV data for testing."""
    return "id,name,value\n1,Alice,100\n2,Bob,200"
```

## Test Data Management

### Sample Data

Use the `SampleDataFixtures` class for consistent test data:

```python
from tests.fixtures.sample_data import SampleDataFixtures

def test_csv_processing():
    csv_data = SampleDataFixtures.simple_csv_data()
    # Process CSV data
```

### Temporary Files

Use temporary files for file-based tests:

```python
def test_file_processing(temp_dir):
    # Create test file
    test_file = temp_dir / "test.csv"
    test_file.write_text("id,name\n1,Alice")
    
    # Process file
    result = process_csv_file(test_file)
    
    # Assertions
    assert result is not None
```

## Mocking Guidelines

### External Dependencies

Mock external dependencies consistently:

```python
@pytest.fixture
def mock_s3_client():
    with patch('boto3.client') as mock:
        s3_mock = Mock()
        mock.return_value = s3_mock
        yield s3_mock

@pytest.fixture
def mock_database():
    with patch('sqlalchemy.create_engine') as mock:
        db_mock = Mock()
        mock.return_value = db_mock
        yield db_mock
```

### Agent Interactions

Mock agent interactions for isolated testing:

```python
def test_orchestrator_coordination(mock_etl_agent, mock_monitor_agent):
    orchestrator = OrchestratorAgent()
    orchestrator.etl_agent = mock_etl_agent
    orchestrator.monitor_agent = mock_monitor_agent
    
    # Test coordination logic
    result = orchestrator.coordinate_pipeline_execution()
    
    assert mock_etl_agent.execute.called
    assert mock_monitor_agent.track_progress.called
```

## Test Utilities

### Helper Functions

Use test helpers for common operations:

```python
from tests.utils.test_helpers import AgentTestHelper, DataTestHelper

def test_agent_status():
    agent = AgentTestHelper.create_mock_agent("etl", "etl-001")
    assert agent.agent_type == "etl"
    assert agent.agent_id == "etl-001"

def test_dataframe_processing():
    df = DataTestHelper.create_test_dataframe(100)
    assert len(df) == 100
    assert "id" in df.columns
```

### Custom Assertions

Create custom assertions for domain-specific validations:

```python
def assert_valid_pipeline_config(config):
    """Assert pipeline configuration is valid."""
    assert "pipeline_id" in config
    assert "source" in config
    assert "destination" in config
    assert config["pipeline_id"] != ""

def assert_successful_pipeline_execution(result):
    """Assert pipeline execution was successful."""
    assert result.success is True
    assert result.records_processed > 0
    assert result.error_count == 0
```

## Continuous Integration

### GitHub Actions

Configure automated testing in `.github/workflows/test.yml`:

```yaml
name: Tests
on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, 3.10, 3.11]
    
    steps:
    - uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    
    - name: Install dependencies
      run: |
        pip install -e .[dev]
    
    - name: Run tests
      run: |
        pytest --cov=src --cov-report=xml
    
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

### Test Quality Gates

Enforce quality gates:

- Minimum test coverage: 80%
- All tests must pass
- No security vulnerabilities
- Performance benchmarks met

## Best Practices

### Test Organization

1. **Group related tests** in classes
2. **Use descriptive test names** that explain what is being tested
3. **Follow AAA pattern**: Arrange, Act, Assert
4. **Keep tests independent** and isolated
5. **Use appropriate test markers** for categorization

### Test Data

1. **Use fixtures** for reusable test data
2. **Create minimal test data** that covers the test case
3. **Avoid hard-coded values** in tests
4. **Clean up resources** after tests

### Assertions

1. **Use specific assertions** rather than generic ones
2. **Test both positive and negative cases**
3. **Verify expected side effects**
4. **Use custom assertions** for complex validations

### Performance

1. **Keep tests fast** - unit tests should run in milliseconds
2. **Use parallel execution** for independent tests
3. **Mock expensive operations** like network calls
4. **Use appropriate test timeouts**

### Maintenance

1. **Keep tests simple** and focused
2. **Refactor tests** alongside production code
3. **Remove obsolete tests**
4. **Update tests** when requirements change

## Troubleshooting

### Common Issues

**Tests failing intermittently:**
- Check for race conditions in async tests
- Ensure proper cleanup of resources
- Use deterministic test data

**Slow test execution:**
- Profile test execution to identify bottlenecks
- Mock expensive operations
- Use parallel test execution

**Mock issues:**
- Verify mock specifications match real objects
- Check that mocks are properly configured
- Ensure mocks are reset between tests

**Test data issues:**
- Use fixtures for consistent test data
- Avoid dependencies between tests
- Clean up test data after each test

### Debugging Tests

```bash
# Run with debugging output
pytest -v -s

# Drop into debugger on failure
pytest --pdb

# Run specific test with debugging
pytest -v -s tests/unit/test_agent.py::TestAgent::test_method
```

## Resources

- [pytest documentation](https://docs.pytest.org/)
- [unittest.mock documentation](https://docs.python.org/3/library/unittest.mock.html)
- [Testing async code](https://pytest-asyncio.readthedocs.io/)
- [Test coverage with pytest-cov](https://pytest-cov.readthedocs.io/)