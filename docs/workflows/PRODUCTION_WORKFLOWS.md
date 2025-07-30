# Production GitHub Workflows

**⚠️ IMPORTANT**: These workflow files require manual setup due to GitHub security restrictions on workflow creation.

## Setup Instructions

1. **Create `.github/workflows/` directory** in your repository root
2. **Copy each workflow file** from the templates below
3. **Configure repository secrets** as documented
4. **Enable workflow permissions** in repository settings

## Required Repository Secrets

Configure these in Repository Settings → Secrets and variables → Actions:

```yaml
# Code coverage
CODECOV_TOKEN: "your-codecov-token"

# Container registry
DOCKER_HUB_USERNAME: "your-docker-username"
DOCKER_HUB_ACCESS_TOKEN: "your-docker-access-token"

# Observability (optional)
OTEL_EXPORTER_OTLP_ENDPOINT: "https://your-otel-endpoint"
OTEL_EXPORTER_OTLP_TOKEN: "your-otel-token"

# Security scanning (optional)
SNYK_TOKEN: "your-snyk-token"
```

## Workflow Files

### 1. Main CI/CD Pipeline (`.github/workflows/ci.yml`)

```yaml
# CI/CD Pipeline for Agent-Orchestrated ETL
# Comprehensive testing, security, and deployment automation

name: CI/CD Pipeline

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  workflow_dispatch:
  schedule:
    # Run security scans weekly on Mondays at 3 AM UTC
    - cron: '0 3 * * 1'

env:
  PYTHON_VERSION: "3.8"
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  test:
    name: Test Python ${{ matrix.python-version }}
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        python-version: ["3.8", "3.9", "3.10", "3.11"]

    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v5
      with:
        python-version: ${{ matrix.python-version }}

    - name: Cache pip packages
      uses: actions/cache@v4
      with:
        path: ~/.cache/pip
        key: ${{ runner.os }}-pip-${{ hashFiles('**/pyproject.toml') }}
        restore-keys: |
          ${{ runner.os }}-pip-

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run unit tests
      run: |
        python -m pytest tests/ -v --cov=src/agent_orchestrated_etl --cov-report=xml --cov-report=html --cov-report=term
        
    - name: Run integration tests
      run: |
        python -m pytest tests/integration/ -v -m integration

    - name: Run performance tests
      if: matrix.python-version == '3.8'
      run: |
        python -m pytest tests/performance/ -v --benchmark-only

    - name: Upload coverage to Codecov
      if: matrix.python-version == '3.8'
      uses: codecov/codecov-action@v4
      with:
        token: ${{ secrets.CODECOV_TOKEN }}
        file: ./coverage.xml
        fail_ci_if_error: true

  quality:
    name: Code Quality & Security
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run linting with Ruff
      run: |
        python -m ruff check src/ tests/ --output-format=github

    - name: Check code formatting
      run: |
        python -m ruff format --check src/ tests/

    - name: Run type checking
      run: |
        python -m mypy src/

    - name: Run security checks with Bandit
      run: |
        python -m bandit -r src/ -f json -o bandit-report.json
        python -m bandit -r src/

    - name: Check code complexity
      run: |
        python -m radon cc src/agent_orchestrated_etl -s -a

    - name: Run secret detection
      run: |
        detect-secrets scan --all-files --baseline .secrets.baseline

    - name: Upload security reports
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: security-reports-${{ github.run_id }}
        path: |
          bandit-report.json

  build:
    name: Build & Package
    runs-on: ubuntu-latest
    needs: [test, quality]
    outputs:
      version: ${{ steps.version.outputs.version }}

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install build dependencies
      run: |
        python -m pip install --upgrade pip build

    - name: Generate version
      id: version
      run: |
        VERSION=$(python -c "import datetime; print(f'0.0.1.dev{datetime.datetime.now().strftime(\"%Y%m%d%H%M%S\")}')")
        echo "version=$VERSION" >> $GITHUB_OUTPUT

    - name: Build package
      run: |
        python -m build

    - name: Upload build artifacts
      uses: actions/upload-artifact@v4
      with:
        name: dist-${{ github.run_id }}
        path: dist/

  security-scan:
    name: Advanced Security Scanning
    runs-on: ubuntu-latest
    if: github.event_name == 'schedule' || github.event_name == 'workflow_dispatch'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install security tools
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev] safety

    - name: Run dependency vulnerability check
      run: |
        safety check --json --output safety-report.json
        safety check

    - name: Generate SBOM
      run: |
        python scripts/generate_sbom.py

    - name: Run custom vulnerability scanner
      run: |
        python scripts/vulnerability_scanner.py

    - name: Upload security artifacts
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: security-scan-${{ github.run_id }}
        path: |
          safety-report.json
          sbom.json

  docker:
    name: Build Container Image
    runs-on: ubuntu-latest
    needs: [test, quality, build]
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Log in to Container Registry
      uses: docker/login-action@v3
      with:
        registry: ${{ env.REGISTRY }}
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v5
      with:
        images: ${{ env.REGISTRY }}/${{ env.IMAGE_NAME }}
        tags: |
          type=ref,event=branch
          type=ref,event=pr
          type=sha,prefix={{branch}}-
          type=raw,value=latest,enable={{is_default_branch}}

    - name: Build and push
      uses: docker/build-push-action@v5
      with:
        context: .
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max
        platforms: linux/amd64,linux/arm64

  chaos-testing:
    name: Chaos Engineering Tests
    runs-on: ubuntu-latest
    needs: [test, quality]
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run chaos engineering tests
      run: |
        python chaos-engineering/chaos-runner.py
        
    - name: Upload chaos test results
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: chaos-test-results-${{ github.run_id }}
        path: chaos-results/

  performance-benchmark:
    name: Performance Benchmarking
    runs-on: ubuntu-latest
    needs: [test, quality]
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run performance benchmarks
      run: |
        python scripts/performance_benchmark.py

    - name: Upload benchmark results
      uses: actions/upload-artifact@v4
      with:
        name: benchmark-results-${{ github.run_id }}
        path: benchmark-results.json
```

### 2. Release Pipeline (`.github/workflows/release.yml`)

```yaml
# Automated Release Pipeline
# Handles semantic versioning, changelog generation, and deployment

name: Release Pipeline

on:
  push:
    tags:
      - 'v*'
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g., 1.0.0)'
        required: true
        type: string
      release_type:
        description: 'Type of release'
        required: true
        default: 'minor'
        type: choice
        options:
        - patch
        - minor
        - major

env:
  PYTHON_VERSION: "3.8"
  REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  validate-release:
    name: Validate Release
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.version.outputs.version }}

    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Determine version
      id: version
      run: |
        if [ "${{ github.event_name }}" = "push" ]; then
          VERSION=${GITHUB_REF#refs/tags/v}
        else
          VERSION="${{ github.event.inputs.version }}"
        fi
        echo "version=$VERSION" >> $GITHUB_OUTPUT
        echo "Releasing version: $VERSION"

    - name: Validate version format
      run: |
        VERSION="${{ steps.version.outputs.version }}"
        if ! [[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
          echo "Error: Invalid version format. Expected: X.Y.Z"
          exit 1
        fi

    - name: Run full test suite
      run: |
        python -m pytest tests/ -v --cov=src/agent_orchestrated_etl

    - name: Run security scans
      run: |
        python -m bandit -r src/
        detect-secrets scan --all-files --baseline .secrets.baseline

  build-release-artifacts:
    name: Build Release Artifacts
    runs-on: ubuntu-latest
    needs: validate-release

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install build tools
      run: |
        python -m pip install --upgrade pip build twine

    - name: Update version in pyproject.toml
      run: |
        VERSION="${{ needs.validate-release.outputs.version }}"
        sed -i "s/version = \".*\"/version = \"$VERSION\"/" pyproject.toml

    - name: Build Python package
      run: |
        python -m build

    - name: Verify package
      run: |
        python -m twine check dist/*

    - name: Upload package artifacts
      uses: actions/upload-artifact@v4
      with:
        name: python-package-${{ needs.validate-release.outputs.version }}
        path: dist/

  create-github-release:
    name: Create GitHub Release
    runs-on: ubuntu-latest
    needs: [validate-release, build-release-artifacts]

    steps:
    - uses: actions/checkout@v4

    - name: Download package artifacts
      uses: actions/download-artifact@v4
      with:
        name: python-package-${{ needs.validate-release.outputs.version }}
        path: dist/

    - name: Generate changelog
      id: changelog
      run: |
        VERSION="${{ needs.validate-release.outputs.version }}"
        echo "changelog=Automated release for version $VERSION" >> $GITHUB_OUTPUT

    - name: Create GitHub Release
      uses: actions/create-release@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        tag_name: v${{ needs.validate-release.outputs.version }}
        release_name: Release v${{ needs.validate-release.outputs.version }}
        body: ${{ steps.changelog.outputs.changelog }}
        draft: false
        prerelease: false
```

### 3. Dependency Management (`.github/workflows/dependency-update.yml`)

```yaml
# Automated Dependency Management and Security Updates
# Complements Dependabot with advanced dependency analysis

name: Dependency Management

on:
  schedule:
    # Run daily at 2 AM UTC for security updates
    - cron: '0 2 * * *'
  workflow_dispatch:
    inputs:
      force_update:
        description: 'Force dependency updates'
        required: false
        default: false
        type: boolean

jobs:
  analyze-dependencies:
    name: Analyze Dependency Health
    runs-on: ubuntu-latest

    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.8"

    - name: Install dependency analysis tools
      run: |
        python -m pip install --upgrade pip
        pip install pip-audit safety

    - name: Run security audit
      run: |
        pip-audit --format=json --output=security-audit.json
        pip-audit

    - name: Check for outdated packages
      run: |
        pip list --outdated --format=json > outdated-packages.json

    - name: Upload dependency analysis artifacts
      uses: actions/upload-artifact@v4
      with:
        name: dependency-analysis-${{ github.run_id }}
        path: |
          security-audit.json
          outdated-packages.json

  security-updates:
    name: Create Security Update PR
    runs-on: ubuntu-latest
    needs: analyze-dependencies
    if: needs.analyze-dependencies.outputs.security-updates > 0

    steps:
    - uses: actions/checkout@v4

    - name: Create security update PR
      uses: peter-evans/create-pull-request@v5
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
        commit-message: "security: update dependencies with critical vulnerabilities"
        title: "🔒 Security: Critical Dependency Updates"
        body: |
          ## Security Dependency Updates
          
          This PR addresses critical security vulnerabilities in dependencies.
          
          🤖 *This PR was automatically generated by the Dependency Management workflow*
        branch: security/dependency-updates-${{ github.run_id }}
        labels: |
          security
          dependencies
          critical
```

## Post-Setup Validation

After setting up the workflows, validate the configuration:

1. **Test the CI pipeline** by creating a test PR
2. **Verify security scans** are running correctly
3. **Check artifact uploads** are working
4. **Validate notification settings** for failed builds

## Monitoring & Maintenance

- **Weekly reviews** of workflow performance and success rates
- **Monthly updates** of action versions for security
- **Quarterly optimization** of pipeline efficiency
- **Annual review** of workflow architecture and needs

This comprehensive workflow setup provides enterprise-grade CI/CD capabilities with security-first principles and intelligent automation.