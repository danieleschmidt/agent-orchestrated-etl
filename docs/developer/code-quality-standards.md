# Code Quality Standards

This document outlines the code quality standards and tools used in the agent-orchestrated-etl project.

## Overview

We maintain high code quality through automated tools and consistent practices. This ensures maintainability, security, and reliability of the codebase.

## Tools

### Static Analysis

#### Ruff
- **Configuration**: `.ruff.toml`
- **Purpose**: Fast Python linting and formatting
- **Usage**: `ruff check src/` and `ruff format src/`
- **Integration**: Pre-commit hooks and CI/CD pipeline

#### MyPy
- **Configuration**: `mypy.ini`
- **Purpose**: Static type checking
- **Usage**: `mypy src/`
- **Target**: Python 3.8+ compatibility with strict settings

#### Pre-commit Hooks
- **Configuration**: `.pre-commit-config.yaml`
- **Purpose**: Automated quality checks before commits
- **Setup**: `pre-commit install`
- **Coverage**: Linting, formatting, type checking, security scanning

### Security Tools

#### Bandit
- **Purpose**: Security vulnerability scanning
- **Usage**: Integrated in pre-commit hooks
- **Output**: `bandit-report.json`

#### Detect Secrets
- **Purpose**: Prevent secret leakage
- **Baseline**: `.secrets.baseline`
- **Usage**: Scans for API keys, passwords, tokens

### Documentation

#### Pydocstyle
- **Convention**: Google docstring style
- **Purpose**: Ensures consistent documentation
- **Excludes**: Tests and docs directories

## Standards

### Code Style
- **Line Length**: 88 characters (Black compatible)
- **Quotes**: Double quotes for strings
- **Imports**: Sorted with isort, first-party packages grouped
- **Docstrings**: Google style for all public APIs

### Type Annotations
- **Requirement**: All public functions and methods must have type annotations
- **Strictness**: MyPy strict mode enabled
- **Compatibility**: Python 3.8+ type hints

### Security
- **No Hardcoded Secrets**: All credentials via AWS Secrets Manager
- **Input Validation**: All user inputs must be validated
- **SQL Injection Prevention**: Use parameterized queries only
- **Dependency Security**: Regular security scans of dependencies

### Testing
- **Coverage**: Aim for >90% test coverage
- **Types**: Unit, integration, and end-to-end tests
- **Framework**: pytest with async support
- **Naming**: `test_*.py` pattern for test files

## CI/CD Integration

### Pre-commit Checks
All commits must pass:
- Ruff linting and formatting
- MyPy type checking
- Security scans (Bandit, detect-secrets)
- Basic file checks (trailing whitespace, merge conflicts)

### Pull Request Requirements
- All automated checks must pass
- Code review required for security-sensitive changes
- Documentation updates for API changes

## Development Workflow

### Setup
```bash
# Install development dependencies
pip install -e ".[dev]"

# Install pre-commit hooks
pre-commit install

# Run initial code quality check
pre-commit run --all-files
```

### Before Committing
```bash
# Run linting
ruff check src/ --fix

# Run formatting
ruff format src/

# Run type checking
mypy src/

# Run security scan
bandit -r src/
```

### IDE Configuration

#### VS Code
Recommended extensions:
- Python (Microsoft)
- Pylance (Microsoft)
- Ruff (Astral Software)
- MyPy Type Checker

Settings:
```json
{
  "python.linting.enabled": true,
  "python.linting.ruffEnabled": true,
  "python.formatting.provider": "ruff",
  "python.typing.typecheckingmode": "strict"
}
```

#### PyCharm
- Enable external tool integration for Ruff
- Configure MyPy as external tool
- Set up code style to match Ruff configuration

## Exceptions and Overrides

### Per-file Ignores
Configured in `.ruff.toml` for:
- Test files: Relaxed docstring requirements
- CLI scripts: Allow print statements
- Configuration files: Allow some security exceptions

### Type Checking Exceptions
- Third-party libraries without stubs: Configured in `mypy.ini`
- Test files: Less strict type checking allowed

## Metrics and Monitoring

### Code Quality Metrics
- Cyclomatic complexity tracking
- Technical debt monitoring
- Security vulnerability counts
- Test coverage percentages

### Reporting
- Pre-commit hook results
- CI/CD pipeline reports
- Security scan summaries
- Code quality trends

## Continuous Improvement

### Regular Reviews
- Monthly code quality assessment
- Quarterly tool updates
- Annual standard reviews

### Tool Updates
- Pre-commit hooks auto-update weekly
- Manual review of major version updates
- Testing of new tools and configurations

## Resources

- [Ruff Documentation](https://docs.astral.sh/ruff/)
- [MyPy Documentation](https://mypy.readthedocs.io/)
- [Pre-commit Documentation](https://pre-commit.com/)
- [Google Python Style Guide](https://google.github.io/styleguide/pyguide.html)
- [Bandit Security Linter](https://bandit.readthedocs.io/)

## Getting Help

For questions about code quality standards:
1. Check this documentation first
2. Review tool-specific documentation
3. Ask in team code review discussions
4. Propose standard updates via pull request