# GitHub Actions Workflows Setup

This document provides the complete GitHub Actions workflows for implementing the autonomous SDLC enhancements. These workflows need to be manually created in the `.github/workflows/` directory.

## Overview

The enhanced CI/CD pipeline consists of four main workflows:

1. **CI Pipeline** (`ci.yml`) - Continuous Integration with testing and security
2. **CD Pipeline** (`cd.yml`) - Continuous Deployment with releases
3. **Documentation** (`docs.yml`) - Documentation build and deployment
4. **Quality Analysis** (`quality.yml`) - Code quality and security analysis

## Prerequisites

Before setting up these workflows, ensure you have the following secrets configured in your GitHub repository:

- `CODECOV_TOKEN` - For coverage reporting
- `SONAR_TOKEN` - For SonarQube analysis
- `PYPI_API_TOKEN` - For PyPI publishing
- `AWS_STAGING_ROLE_ARN` - For staging deployment
- `AWS_PRODUCTION_ROLE_ARN` - For production deployment

## Manual Setup Instructions

1. Create the `.github/workflows/` directory in your repository root
2. Copy each workflow file from the templates below
3. Configure the required secrets in your repository settings
4. Enable GitHub Pages with Actions source for documentation

## Workflow Templates

### 1. CI Pipeline Template

Create `.github/workflows/ci.yml`:

```yaml
# Continuous Integration Workflow
# Runs on every push and pull request to ensure code quality

name: CI

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main, develop ]
  workflow_dispatch:

concurrency:
  group: ${{ github.workflow }}-${{ github.ref }}
  cancel-in-progress: true

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
        cache: 'pip'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Lint with ruff
      run: |
        ruff check src/ tests/
        ruff format --check src/ tests/

    - name: Type check with mypy
      run: mypy src/

    - name: Security check with bandit
      run: bandit -r src/ -f json -o bandit-report.json

    - name: Test with pytest
      run: |
        coverage run -m pytest tests/ -v
        coverage report -m
        coverage xml

    - name: Upload coverage to Codecov
      uses: codecov/codecov-action@v4
      if: matrix.python-version == '3.9'
      with:
        token: ${{ secrets.CODECOV_TOKEN }}
        file: ./coverage.xml
        fail_ci_if_error: false

  integration-test:
    name: Integration Tests
    runs-on: ubuntu-latest
    needs: test
    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: testdb
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5

    steps:
    - uses: actions/checkout@v4
    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"
        cache: 'pip'

    - name: Install dependencies
      run: |
        pip install -e .[dev]

    - name: Run integration tests
      run: pytest tests/ -v -m integration
      env:
        DATABASE_URL: postgresql://postgres:postgres@localhost:5432/testdb

  security:
    name: Security Scan
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
    - name: Run Trivy vulnerability scanner
      uses: aquasecurity/trivy-action@master
      with:
        scan-type: 'fs'
        scan-ref: '.'
        format: 'sarif'
        output: 'trivy-results.sarif'

    - name: Upload Trivy scan results to GitHub Security tab
      uses: github/codeql-action/upload-sarif@v3
      if: always()
      with:
        sarif_file: 'trivy-results.sarif'
```

### 2. CD Pipeline Template

Create `.github/workflows/cd.yml`:

```yaml
# Continuous Deployment Workflow
# Handles release automation and deployment

name: CD

on:
  push:
    branches: [ main ]
    tags: [ 'v*' ]
  workflow_dispatch:

permissions:
  contents: write
  packages: write
  id-token: write

jobs:
  build:
    name: Build and Test
    runs-on: ubuntu-latest
    outputs:
      version: ${{ steps.version.outputs.version }}
      should-release: ${{ steps.version.outputs.should-release }}

    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0
        token: ${{ secrets.GITHUB_TOKEN }}

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"
        cache: 'pip'

    - name: Install dependencies
      run: |
        pip install -e .[dev]
        pip install build twine

    - name: Run tests
      run: |
        pytest tests/ -v
        ruff check src/ tests/
        mypy src/

    - name: Build package
      run: python -m build

    - name: Check package
      run: twine check dist/*

    - name: Upload build artifacts
      uses: actions/upload-artifact@v4
      with:
        name: python-package
        path: dist/

    - name: Determine version
      id: version
      run: |
        if [[ $GITHUB_REF == refs/tags/v* ]]; then
          echo "version=${GITHUB_REF#refs/tags/v}" >> $GITHUB_OUTPUT
          echo "should-release=true" >> $GITHUB_OUTPUT
        else
          echo "should-release=false" >> $GITHUB_OUTPUT
        fi

  docker:
    name: Build Docker Image
    runs-on: ubuntu-latest
    needs: build
    steps:
    - uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Login to GitHub Container Registry
      uses: docker/login-action@v3
      with:
        registry: ghcr.io
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - name: Extract metadata
      id: meta
      uses: docker/metadata-action@v5
      with:
        images: ghcr.io/${{ github.repository }}
        tags: |
          type=ref,event=branch
          type=ref,event=pr
          type=semver,pattern={{version}}
          type=semver,pattern={{major}}.{{minor}}
          type=sha,prefix={{branch}}-

    - name: Build and push
      uses: docker/build-push-action@v5
      with:
        context: .
        push: true
        tags: ${{ steps.meta.outputs.tags }}
        labels: ${{ steps.meta.outputs.labels }}
        cache-from: type=gha
        cache-to: type=gha,mode=max

  release:
    name: Create Release
    runs-on: ubuntu-latest
    needs: [build, docker]
    if: needs.build.outputs.should-release == 'true'
    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Download build artifacts
      uses: actions/download-artifact@v4
      with:
        name: python-package
        path: dist/

    - name: Create GitHub Release
      uses: softprops/action-gh-release@v2
      with:
        files: dist/*
        generate_release_notes: true
        draft: false
        prerelease: ${{ contains(github.ref, '-') }}

    - name: Publish to PyPI
      uses: pypa/gh-action-pypi-publish@release/v1
      with:
        password: ${{ secrets.PYPI_API_TOKEN }}

  deploy-staging:
    name: Deploy to Staging
    runs-on: ubuntu-latest
    needs: [build]
    if: github.ref == 'refs/heads/main'
    environment:
      name: staging
      url: https://staging-agent-etl.terragon-labs.com

    steps:
    - uses: actions/checkout@v4

    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v4
      with:
        role-to-assume: ${{ secrets.AWS_STAGING_ROLE_ARN }}
        aws-region: us-east-1

    - name: Deploy to ECS
      run: |
        aws ecs update-service \
          --cluster staging-cluster \
          --service agent-etl-service \
          --force-new-deployment

  deploy-production:
    name: Deploy to Production
    runs-on: ubuntu-latest
    needs: [release]
    if: needs.build.outputs.should-release == 'true'
    environment:
      name: production
      url: https://agent-etl.terragon-labs.com

    steps:
    - uses: actions/checkout@v4

    - name: Configure AWS credentials
      uses: aws-actions/configure-aws-credentials@v4
      with:
        role-to-assume: ${{ secrets.AWS_PRODUCTION_ROLE_ARN }}
        aws-region: us-east-1

    - name: Deploy to ECS
      run: |
        aws ecs update-service \
          --cluster production-cluster \
          --service agent-etl-service \
          --force-new-deployment
```

### 3. Documentation Template

Create `.github/workflows/docs.yml`:

```yaml
# Documentation Build and Deploy
# Builds and deploys documentation using MkDocs

name: Documentation

on:
  push:
    branches: [ main ]
    paths: [ 'docs/**', 'mkdocs.yml', '.github/workflows/docs.yml' ]
  pull_request:
    branches: [ main ]
    paths: [ 'docs/**', 'mkdocs.yml' ]
  workflow_dispatch:

permissions:
  contents: read
  pages: write
  id-token: write

concurrency:
  group: "pages"
  cancel-in-progress: false

jobs:
  build:
    name: Build Documentation
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"
        cache: 'pip'

    - name: Install MkDocs and dependencies  
      run: |
        pip install mkdocs-material mkdocs-mermaid2-plugin
        pip install mkdocstrings[python] mkdocs-gen-files
        pip install mkdocs-literate-nav mkdocs-section-index

    - name: Build documentation
      run: mkdocs build --strict

    - name: Upload documentation artifact
      uses: actions/upload-pages-artifact@v3
      with:
        path: ./site

  deploy:
    name: Deploy to GitHub Pages
    runs-on: ubuntu-latest
    needs: build
    if: github.ref == 'refs/heads/main'
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
    - name: Deploy to GitHub Pages
      id: deployment
      uses: actions/deploy-pages@v4
```

### 4. Quality Analysis Template

Create `.github/workflows/quality.yml`:

```yaml
# Code Quality and Analysis Workflow
# Runs SonarQube analysis and other quality checks

name: Quality

on:
  push:
    branches: [ main, develop ]
  pull_request:
    branches: [ main ]
  schedule:
    - cron: '0 2 * * 1'  # Weekly on Monday at 2 AM
  workflow_dispatch:

permissions:
  contents: read
  security-events: write
  pull-requests: read

jobs:
  sonarqube:
    name: SonarQube Analysis
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"
        cache: 'pip'

    - name: Install dependencies
      run: |
        pip install -e .[dev]
        pip install pylint

    - name: Run tests with coverage
      run: |
        coverage run -m pytest tests/ --junitxml=pytest-report.xml
        coverage xml

    - name: Run pylint
      run: |
        pylint src/ --output-format=json > pylint-report.json || true

    - name: Run bandit security scan
      run: |
        bandit -r src/ -f json -o bandit-report.json

    - name: SonarQube Scan
      uses: sonarqube-quality-gate-action@master
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
        SONAR_TOKEN: ${{ secrets.SONAR_TOKEN }}

  codeql:
    name: CodeQL Analysis
    runs-on: ubuntu-latest
    permissions:
      security-events: write
      contents: read
      actions: read

    steps:
    - name: Checkout repository
      uses: actions/checkout@v4

    - name: Initialize CodeQL
      uses: github/codeql-action/init@v3
      with:
        languages: python
        queries: security-extended,security-and-quality

    - name: Autobuild
      uses: github/codeql-action/autobuild@v3

    - name: Perform CodeQL Analysis
      uses: github/codeql-action/analyze@v3
      with:
        category: "/language:python"

  complexity:
    name: Code Complexity Analysis
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"

    - name: Install radon
      run: pip install radon

    - name: Check cyclomatic complexity
      run: |
        radon cc src/ -s -a --total-average
        radon cc src/ -s -a --total-average --json > complexity-report.json

    - name: Check maintainability index
      run: |
        radon mi src/ -s
        radon mi src/ -s --json > maintainability-report.json

    - name: Upload complexity reports
      uses: actions/upload-artifact@v4
      with:
        name: complexity-reports
        path: |
          complexity-report.json
          maintainability-report.json

  dependency_check:
    name: Dependency Vulnerability Check
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"

    - name: Install safety
      run: pip install safety

    - name: Check dependencies
      run: |
        pip install -e .
        safety check --json > safety-report.json || true
        safety check --short-report

    - name: Upload safety report
      uses: actions/upload-artifact@v4
      if: always()
      with:
        name: safety-report
        path: safety-report.json

  secrets_scan:
    name: Secrets Detection
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4
      with:
        fetch-depth: 0

    - name: Run detect-secrets
      run: |
        pip install detect-secrets
        detect-secrets scan --all-files --baseline .secrets.baseline || true
        detect-secrets audit .secrets.baseline

  license_check:
    name: License Compliance
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v5
      with:
        python-version: "3.9"

    - name: Install pip-licenses
      run: pip install pip-licenses

    - name: Install dependencies
      run: pip install -e .

    - name: Check licenses
      run: |
        pip-licenses --format=json > licenses-report.json
        pip-licenses --format=table
        
    - name: Upload license report
      uses: actions/upload-artifact@v4
      with:
        name: license-report
        path: licenses-report.json
```

## Next Steps

1. **Create the workflow files**: Copy each template above into the corresponding file in `.github/workflows/`
2. **Configure secrets**: Add the required secrets to your repository settings
3. **Enable branch protection**: Set up required status checks for the new workflows
4. **Test the workflows**: Push a small change to trigger the CI pipeline

## Repository Settings Configuration

### Required Secrets
Navigate to Settings → Secrets and variables → Actions and add:

- `CODECOV_TOKEN`: Get from [codecov.io](https://codecov.io)
- `SONAR_TOKEN`: Get from your SonarQube instance
- `PYPI_API_TOKEN`: Generate from [PyPI account settings](https://pypi.org/manage/account/)
- `AWS_STAGING_ROLE_ARN`: ARN of your staging deployment role
- `AWS_PRODUCTION_ROLE_ARN`: ARN of your production deployment role

### Branch Protection Rules
Navigate to Settings → Branches and add protection for `main`:

- Require status checks to pass before merging
- Require branches to be up to date before merging
- Required status checks:
  - `test (3.9)` (at minimum)
  - `integration-test`
  - `security`

### GitHub Pages Setup
Navigate to Settings → Pages:

- Source: GitHub Actions
- This will enable automatic documentation deployment

## Troubleshooting

### Common Issues

1. **Workflow permission errors**: Ensure your GitHub token has the necessary permissions
2. **Secret not found errors**: Verify all required secrets are configured
3. **Test failures**: Check that all dependencies are properly specified in `pyproject.toml`