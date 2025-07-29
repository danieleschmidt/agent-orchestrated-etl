# GitHub Actions Workflows Setup Guide

This document provides comprehensive setup instructions for implementing CI/CD workflows in the Agent-Orchestrated-ETL project. 

## Required Workflows

### 1. CI/CD Pipeline (`ci.yml`)

**Purpose**: Continuous Integration and Deployment pipeline

**Triggers**:
- Push to `main` branch
- Pull requests to `main` branch
- Manual workflow dispatch

**Jobs**:

#### Test Job
```yaml
name: Test
runs-on: ubuntu-latest
strategy:
  matrix:
    python-version: [3.8, 3.9, "3.10", "3.11"]
steps:
  - uses: actions/checkout@v4
  - name: Set up Python
    uses: actions/setup-python@v4
    with:
      python-version: ${{ matrix.python-version }}
  - name: Install dependencies
    run: |
      python -m pip install --upgrade pip
      pip install -e .[dev]
  - name: Run tests
    run: |
      python -m pytest tests/ -v --cov=src/agent_orchestrated_etl --cov-report=xml
  - name: Upload coverage to Codecov
    uses: codecov/codecov-action@v3
```

#### Quality Job
```yaml
name: Quality
runs-on: ubuntu-latest
steps:
  - uses: actions/checkout@v4
  - name: Set up Python
    uses: actions/setup-python@v4
    with:
      python-version: 3.8
  - name: Install dependencies
    run: |
      python -m pip install --upgrade pip
      pip install -e .[dev]
  - name: Run linting
    run: python -m ruff check src/ tests/
  - name: Run type checking
    run: python -m mypy src/
  - name: Run security checks
    run: python -m bandit -r src/
  - name: Check code formatting
    run: python -m ruff format --check src/ tests/
```

#### Security Job
```yaml
name: Security
runs-on: ubuntu-latest
steps:
  - uses: actions/checkout@v4
  - name: Run secret detection
    run: |
      pip install detect-secrets
      detect-secrets scan --all-files --baseline .secrets.baseline
  - name: Run dependency vulnerability check
    run: |
      pip install safety
      safety check
```

### 2. Security Scanning (`security.yml`)

**Purpose**: Advanced security scanning and SBOM generation

**Triggers**:
- Schedule: Daily at 2 AM UTC
- Push to `main` branch
- Manual workflow dispatch

**Features**:
- CodeQL analysis
- Dependency vulnerability scanning
- Container image scanning
- SBOM generation
- License compliance checking

### 3. Release Pipeline (`release.yml`)

**Purpose**: Automated releases and package publishing

**Triggers**:
- Push of version tags (v*.*.*)
- Manual workflow dispatch

**Features**:
- Automated changelog generation
- Package building and publishing to PyPI
- GitHub release creation
- Docker image building and publishing

### 4. Documentation (`docs.yml`)

**Purpose**: Documentation building and deployment

**Triggers**:
- Push to `main` branch (docs changes)
- Pull requests (docs validation)

**Features**:
- MkDocs site building
- API documentation generation
- Documentation deployment to GitHub Pages

## Environment Variables and Secrets

The workflows require the following secrets to be configured in GitHub:

### Required Secrets
- `PYPI_API_TOKEN`: For package publishing to PyPI
- `DOCKER_HUB_USERNAME`: For Docker image publishing
- `DOCKER_HUB_ACCESS_TOKEN`: For Docker image publishing
- `CODECOV_TOKEN`: For coverage reporting

### Environment Variables
- `PYTHON_VERSION`: Default Python version (3.8)
- `NODE_VERSION`: Node.js version for documentation tools

## Deployment Environments

### Staging Environment
- **URL**: https://staging.agent-orchestrated-etl.example.com
- **Deployment**: Automatic on merge to `main` branch
- **Database**: Staging PostgreSQL instance
- **Monitoring**: Basic health checks

### Production Environment
- **URL**: https://agent-orchestrated-etl.example.com
- **Deployment**: Manual approval required
- **Database**: Production PostgreSQL cluster
- **Monitoring**: Full observability stack

## Branch Protection Rules

Configure the following branch protection rules for `main`:

1. **Require pull request reviews before merging**
   - Required approving reviews: 1
   - Dismiss stale reviews when new commits are pushed

2. **Require status checks to pass before merging**
   - Test (3.8, 3.9, 3.10, 3.11)
   - Quality
   - Security

3. **Require branches to be up to date before merging**

4. **Require conversation resolution before merging**

5. **Restrict pushes that create files**
   - Restrict pushes to files matching: `*.yml`, `*.yaml` in `.github/workflows/`

## Manual Setup Instructions

1. **Create CI/CD Workflow**:
   ```bash
   # Copy the workflow templates from docs/workflows/templates/
   cp docs/workflows/templates/ci.yml .github/workflows/
   cp docs/workflows/templates/security.yml .github/workflows/
   cp docs/workflows/templates/release.yml .github/workflows/
   ```

2. **Configure Secrets**:
   - Go to Settings > Secrets and variables > Actions
   - Add all required secrets listed above

3. **Set up Branch Protection**:
   - Go to Settings > Branches
   - Add rule for `main` branch with the configurations above

4. **Enable Dependency Scanning**:
   - Go to Settings > Code security and analysis
   - Enable Dependabot alerts and security updates

For detailed workflow files, see the templates in `docs/workflows/templates/`.