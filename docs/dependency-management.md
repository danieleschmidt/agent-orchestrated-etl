# Dependency Management Guide

This document outlines the dependency management strategy for Agent-Orchestrated-ETL.

## Overview

The project uses modern Python packaging standards with `pyproject.toml` as the primary configuration file for dependency management.

## Dependency Categories

### Core Dependencies
Located in `[project.dependencies]` in `pyproject.toml`:
- **LangChain**: Core LLM framework and integrations
- **Data Processing**: pandas, pyarrow, sqlalchemy
- **Cloud Services**: boto3 for AWS integration
- **Utilities**: pydantic for data validation, PyYAML for configuration

### Development Dependencies
Located in `[project.optional-dependencies.dev]`:
- **Testing**: pytest, pytest-asyncio, coverage
- **Code Quality**: ruff, black, mypy, bandit
- **Security**: detect-secrets, safety
- **Documentation**: pydocstyle

### Vector Storage Dependencies (Optional)
Located in `[project.optional-dependencies.vector]`:
- **Vector Database**: chromadb
- **Embeddings**: sentence-transformers
- **Numerical Computing**: numpy

## Dependency Updates

### Automated Updates
- **Dependabot**: Configured to check for updates weekly
- **Pre-commit CI**: Automatically updates hook versions
- **Security Updates**: High-priority security patches applied immediately

### Manual Updates
```bash
# Check for outdated packages
pip list --outdated

# Update specific package
pip install --upgrade package-name

# Update all development dependencies
pip install -e .[dev] --upgrade

# Verify updates don't break tests
npm run test
npm run lint
npm run typecheck
```

### Version Pinning Strategy

#### Production Dependencies
- **Minor version pinning** for critical dependencies (e.g., `langchain>=0.1.0,<0.2.0`)
- **Patch version pinning** for security-sensitive packages
- **Compatible release clauses** using `~=` for stable APIs

#### Development Dependencies
- **Exact version pinning** for reproducible development environments
- **Regular updates** through automated PR reviews

## Security Scanning

### Tools Used
1. **Safety**: Scans for known security vulnerabilities
2. **Bandit**: Static security analysis for Python code
3. **Dependabot**: GitHub's dependency vulnerability alerts

### Automated Scanning
```bash
# Run security scan
npm run security

# Check for known vulnerabilities
safety check

# Generate security report
safety check --json --output safety-report.json
```

## Dependency Resolution

### Installation Commands
```bash
# Production installation
pip install -e .

# Development installation
pip install -e .[dev]

# With vector capabilities
pip install -e .[dev,vector]

# From requirements file (if needed)
pip install -r requirements.txt
```

### Lock File Management
While not using poetry/pipenv lock files, we maintain reproducibility through:
- **Exact versions in CI/CD**: Use `pip freeze` in deployment
- **Docker base images**: Pin to specific Python versions
- **Version constraints**: Careful specification in pyproject.toml

## Troubleshooting

### Common Issues

#### Dependency Conflicts
```bash
# Check for conflicts
pip check

# Show dependency tree
pip show --verbose package-name

# Resolve conflicts by updating constraints
edit pyproject.toml
```

#### Version Incompatibilities
```bash
# Check which packages use a dependency
pip show --files package-name

# Install specific compatible versions
pip install "package-name>=1.0,<2.0"
```

### Environment Issues
```bash
# Clean install in fresh environment
python -m venv fresh-env
source fresh-env/bin/activate
pip install -e .[dev]

# Verify installation
python -c "import agent_orchestrated_etl; print('OK')"
```

## Best Practices

### Adding New Dependencies

1. **Evaluate Necessity**: Consider if the functionality can be implemented without adding a dependency
2. **Security Review**: Check the package's security history and maintainer reputation
3. **License Compatibility**: Ensure license is compatible with MIT
4. **Size Impact**: Consider the package size and its dependencies
5. **Maintenance Status**: Prefer actively maintained packages

### Version Constraints

```toml
# Good: Allows patches, prevents breaking changes
package = ">=1.2.0,<2.0.0"

# Better: Compatible release (equivalent to >=1.2.0,<1.3.0)
package = "~=1.2.0"

# Development: Exact versions for reproducibility
pytest = "==8.4.1"

# Security-critical: Pin to specific versions
cryptography = "==41.0.7"
```

### Dependency Groups

```toml
[project.optional-dependencies]
# Testing dependencies
test = [
    "pytest>=7.0.0",
    "pytest-asyncio>=0.21.0",
    "coverage>=7.0.0"
]

# Production monitoring
monitoring = [
    "prometheus-client>=0.17.0",
    "opentelemetry-api>=1.20.0"
]

# Development tools
dev = [
    "test",  # Include test dependencies
    "ruff>=0.1.0",
    "mypy>=1.0.0"
]
```

## Compliance and Licensing

### License Scanning
```bash
# Install license checker
pip install pip-licenses

# Generate license report
pip-licenses --format=json --output-file=licenses.json

# Check for incompatible licenses
pip-licenses --fail-on="GPL"
```

### SBOM Generation
```bash
# Generate Software Bill of Materials
python scripts/generate_sbom.py > sbom.json

# CycloneDX format
pip install cyclonedx-bom
cyclonedx-py -o sbom-cyclonedx.json
```

## Future Considerations

### Migration to uv/rye
Consider migrating to modern Python package managers:
- **uv**: Fast package installer and resolver
- **rye**: Experimental package management tool
- **PDM**: PEP 582 compliant package manager

### Dependency Caching
- **Docker layer caching**: Optimize Dockerfile for dependency caching
- **CI/CD caching**: Cache pip packages between runs
- **Local development**: Use pip cache for faster installs

## Monitoring and Metrics

### Key Metrics
- Dependency update frequency
- Security vulnerability exposure time
- Build success rate after dependency updates
- Package size and installation time

### Tools Integration
- **GitHub Dependabot**: Automated dependency updates
- **Snyk**: Continuous security monitoring
- **WhiteSource/Mend**: License compliance monitoring