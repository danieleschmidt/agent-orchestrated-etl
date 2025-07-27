# Contributing to Agent-Orchestrated-ETL

First off, thank you for considering contributing to Agent-Orchestrated-ETL! 🎉

It's people like you that make Agent-Orchestrated-ETL such a great tool for intelligent data pipeline orchestration.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#development-setup)
- [Pull Request Process](#pull-request-process)
- [Style Guidelines](#style-guidelines)
- [Community](#community)

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## Getting Started

### Prerequisites

- Python 3.8 or higher
- Docker and Docker Compose
- Git
- Basic understanding of ETL concepts and data pipelines

### Quick Development Setup

```bash
# 1. Fork the repository on GitHub
# 2. Clone your fork
git clone https://github.com/YOUR-USERNAME/agent-orchestrated-etl.git
cd agent-orchestrated-etl

# 3. Add upstream remote
git remote add upstream https://github.com/your-org/agent-orchestrated-etl.git

# 4. Set up development environment
make install-dev

# 5. Start development services
make docker-compose

# 6. Run tests to verify setup
make test
```

## How Can I Contribute?

### 🐛 Reporting Bugs

Before creating bug reports, please check the [existing issues](https://github.com/your-org/agent-orchestrated-etl/issues) to see if the problem has already been reported.

When creating a bug report, please include:

- **Use a clear and descriptive title**
- **Describe the exact steps to reproduce the problem**
- **Provide specific examples to demonstrate the steps**
- **Describe the behavior you observed after following the steps**
- **Explain which behavior you expected to see instead and why**
- **Include screenshots if applicable**
- **Environment details** (OS, Python version, Docker version)

### 💡 Suggesting Enhancements

Enhancement suggestions are tracked as GitHub issues. When creating an enhancement suggestion, please include:

- **Use a clear and descriptive title**
- **Provide a step-by-step description of the suggested enhancement**
- **Provide specific examples to demonstrate the steps**
- **Describe the current behavior and explain which behavior you expected to see instead**
- **Explain why this enhancement would be useful**

### 🔧 Contributing Code

#### First Time Contributors

Look for issues labeled `good first issue` - these are specifically chosen to be approachable for new contributors.

#### Types of Contributions

1. **Bug Fixes**: Fix existing bugs in the codebase
2. **New Features**: Add new functionality to the project
3. **Documentation**: Improve or add documentation
4. **Tests**: Add or improve test coverage
5. **Performance**: Optimize existing code
6. **Refactoring**: Improve code structure without changing functionality

### 📚 Improving Documentation

Documentation improvements are always welcome! This includes:

- Fixing typos or clarifying existing documentation
- Adding examples and tutorials
- Translating documentation
- Adding API documentation
- Creating video tutorials or blog posts

## Development Setup

### Detailed Setup

1. **Clone the repository**:
   ```bash
   git clone https://github.com/your-org/agent-orchestrated-etl.git
   cd agent-orchestrated-etl
   ```

2. **Set up Python environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -e .[dev]
   ```

3. **Set up pre-commit hooks**:
   ```bash
   pre-commit install
   ```

4. **Copy environment configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your configuration
   ```

5. **Start development services**:
   ```bash
   docker-compose up -d
   ```

6. **Verify setup**:
   ```bash
   make test
   curl http://localhost:8793/health
   ```

### Development Workflow

1. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. **Make your changes**:
   - Write code following our style guidelines
   - Add tests for new functionality
   - Update documentation as needed

3. **Test your changes**:
   ```bash
   make validate  # Runs all quality checks
   make test      # Runs all tests
   ```

4. **Commit your changes**:
   ```bash
   git add .
   git commit -m "feat: add new feature description"
   ```

5. **Push and create a Pull Request**:
   ```bash
   git push origin feature/your-feature-name
   # Create PR on GitHub
   ```

## Pull Request Process

### Before Submitting

- [ ] Fork the repository and create your branch from `main`
- [ ] Ensure all tests pass: `make test`
- [ ] Ensure code quality checks pass: `make validate`
- [ ] Add tests that prove your fix is effective or that your feature works
- [ ] Update documentation for any new or changed functionality
- [ ] Ensure your commit messages follow our conventions

### Pull Request Guidelines

1. **Use a clear and descriptive title**
2. **Fill out the pull request template completely**
3. **Link to any relevant issues**
4. **Include screenshots or GIFs for UI changes**
5. **Ensure CI/CD checks pass**
6. **Request review from maintainers**

### Commit Message Convention

We use [Conventional Commits](https://www.conventionalcommits.org/) for commit messages:

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: A new feature
- `fix`: A bug fix
- `docs`: Documentation only changes
- `style`: Changes that don't affect code meaning (white-space, formatting)
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `perf`: Code change that improves performance
- `test`: Adding missing tests or correcting existing tests
- `chore`: Changes to build process or auxiliary tools

**Examples:**
```bash
feat(agents): add new transformation agent for JSON data
fix(orchestrator): resolve memory leak in pipeline execution
docs(api): update API documentation with new endpoints
test(integration): add comprehensive pipeline tests
```

### Review Process

1. **Automated Checks**: All PRs must pass automated tests and quality checks
2. **Code Review**: At least one maintainer must review and approve the PR
3. **Testing**: Reviewers will test the changes in their environment
4. **Documentation**: Ensure all changes are properly documented
5. **Merge**: Maintainers will merge approved PRs

## Style Guidelines

### Python Code Style

- **PEP 8**: Follow Python's official style guide
- **Line Length**: Maximum 88 characters (Black formatter)
- **Type Hints**: Use type hints for all function signatures
- **Docstrings**: Use Google-style docstrings for all public functions and classes

```python
def process_data_source(
    source_url: str, 
    options: Dict[str, Any] = None
) -> ProcessingResult:
    """Process data from the specified source.
    
    Args:
        source_url: URL or identifier for the data source.
        options: Optional processing configuration.
        
    Returns:
        ProcessingResult containing processed data and metadata.
        
    Raises:
        DataSourceError: If source cannot be accessed.
        ValidationError: If data validation fails.
    """
    pass
```

### Documentation Style

- **Markdown**: Use standard Markdown for documentation
- **Clear Headers**: Use descriptive headers and subheaders
- **Code Examples**: Include working code examples
- **Links**: Use relative links for internal documentation

### Testing Guidelines

- **Coverage**: Maintain at least 80% test coverage
- **Test Types**: Include unit, integration, and performance tests
- **Test Naming**: Use descriptive test names that explain what is being tested
- **Fixtures**: Use pytest fixtures for common test data

```python
def test_pipeline_creation_with_valid_source():
    """Test that pipeline is created successfully with valid data source."""
    orchestrator = DataOrchestrator()
    pipeline = orchestrator.create_pipeline(source="mock://test-data")
    
    assert pipeline is not None
    assert pipeline.source == "mock://test-data"
```

## Community

### Communication Channels

- **GitHub Issues**: For bugs, feature requests, and technical discussions
- **GitHub Discussions**: For general questions and community discussions
- **Discord**: For real-time chat and community support (if available)
- **Email**: For security issues or private communications

### Getting Help

If you need help:

1. **Check the documentation**: Look in the `docs/` directory
2. **Search existing issues**: Someone might have already asked your question
3. **Create a new issue**: If you can't find an answer, create a new issue
4. **Join the discussion**: Participate in GitHub Discussions

### Recognition

We appreciate all contributors! Contributors will be:

- Listed in our `CONTRIBUTORS.md` file
- Mentioned in release notes for significant contributions
- Invited to join our contributor community
- Recognized in project documentation

## Development Resources

### Useful Commands

```bash
# Development
make install-dev          # Install development dependencies
make test                 # Run all tests
make test-watch          # Run tests in watch mode
make lint                # Run linting
make format              # Format code
make typecheck           # Run type checking

# Docker
make docker-compose      # Start all services
make docker-build        # Build Docker image
make docker-logs         # View logs

# Documentation
make docs                # Build documentation
make docs-serve          # Serve docs locally

# Quality
make validate            # Run all quality checks
make security            # Run security checks
make clean               # Clean build artifacts
```

### Project Structure

```
agent-orchestrated-etl/
├── src/agent_orchestrated_etl/    # Main application code
├── tests/                         # Test suite
├── docs/                          # Documentation
├── .github/                       # GitHub workflows and templates
├── monitoring/                    # Monitoring configuration
├── scripts/                       # Utility scripts
└── examples/                      # Usage examples
```

### Learning Resources

- [LangChain Documentation](https://python.langchain.com/)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Docker Documentation](https://docs.docker.com/)
- [Pytest Documentation](https://docs.pytest.org/)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)

## License

By contributing to Agent-Orchestrated-ETL, you agree that your contributions will be licensed under the same license as the project (MIT License).

## Questions?

Don't hesitate to ask questions! We're here to help. Create an issue or start a discussion, and we'll do our best to help you get started.

Thank you for your interest in contributing to Agent-Orchestrated-ETL! 🚀