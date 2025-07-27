# GitHub Workflows Manual Setup Guide

Due to GitHub App permission restrictions, the CI/CD workflows need to be manually added to your repository. This guide provides all the workflow files that should be created in `.github/workflows/`.

## Quick Setup

1. Create the `.github/workflows/` directory in your repository
2. Copy each workflow file below into the directory
3. Commit and push the changes
4. Configure the required secrets in your GitHub repository settings

## Required GitHub Secrets

Go to your repository → Settings → Secrets and variables → Actions, and add:

```
PYPI_API_TOKEN          # For PyPI package publishing
SNYK_TOKEN             # For Snyk security scanning (optional)
SLACK_WEBHOOK_URL      # For Slack notifications (optional)
```

## Workflow Files

### 1. CI/CD Pipeline (`.github/workflows/ci.yml`)

```yaml
# CI/CD Pipeline for Agent-Orchestrated-ETL
name: CI/CD Pipeline

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main, develop]
  release:
    types: [published]

env:
  PYTHON_VERSION: '3.11'
  DOCKER_REGISTRY: ghcr.io
  IMAGE_NAME: ${{ github.repository }}

jobs:
  quality-checks:
    name: Code Quality & Security
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run linting
      run: |
        ruff check src/ tests/
        ruff format --check src/ tests/

    - name: Run type checking
      run: mypy src/

    - name: Run security scan
      run: |
        bandit -r src/ -f json -o bandit-report.json
        safety check

  test:
    name: Test Suite
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest]
        python-version: ['3.8', '3.9', '3.10', '3.11']

    services:
      postgres:
        image: postgres:15
        env:
          POSTGRES_PASSWORD: postgres
          POSTGRES_DB: test_agent_etl
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 5432:5432

      redis:
        image: redis:7-alpine
        options: >-
          --health-cmd "redis-cli ping"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
        ports:
          - 6379:6379

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python ${{ matrix.python-version }}
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]

    - name: Run tests
      run: |
        pytest tests/ -v --cov=src --cov-report=xml
      env:
        DATABASE_URL: postgresql://postgres:postgres@localhost:5432/test_agent_etl
        REDIS_URL: redis://localhost:6379/1

    - name: Upload coverage
      uses: codecov/codecov-action@v3
      if: matrix.python-version == '3.11'
      with:
        file: ./coverage.xml

  docker:
    name: Docker Build
    runs-on: ubuntu-latest
    needs: [quality-checks]

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Docker Buildx
      uses: docker/setup-buildx-action@v3

    - name: Log in to Container Registry
      if: github.event_name != 'pull_request'
      uses: docker/login-action@v3
      with:
        registry: ${{ env.DOCKER_REGISTRY }}
        username: ${{ github.actor }}
        password: ${{ secrets.GITHUB_TOKEN }}

    - name: Build and push
      uses: docker/build-push-action@v5
      with:
        context: .
        push: ${{ github.event_name != 'pull_request' }}
        tags: ${{ env.DOCKER_REGISTRY }}/${{ env.IMAGE_NAME }}:latest
```

### 2. Security Scanning (`.github/workflows/security.yml`)

```yaml
# Security Scanning Workflow
name: Security Scanning

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]
  schedule:
    - cron: '0 2 * * *'

jobs:
  security-scan:
    name: Security Analysis
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Install dependencies
      run: |
        python -m pip install --upgrade pip
        pip install bandit[toml] safety

    - name: Run Bandit security scanner
      run: |
        bandit -r src/ -f json -o bandit-report.json
        bandit -r src/

    - name: Run Safety dependency scanner
      run: |
        safety check

    - name: Upload security reports
      uses: actions/upload-artifact@v3
      if: always()
      with:
        name: security-reports
        path: bandit-report.json
```

### 3. Dependency Updates (`.github/workflows/dependency-update.yml`)

```yaml
# Automated Dependency Updates
name: Dependency Updates

on:
  schedule:
    - cron: '0 9 * * 1'  # Weekly on Mondays
  workflow_dispatch:

jobs:
  update-deps:
    name: Update Dependencies
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4
      with:
        token: ${{ secrets.GITHUB_TOKEN }}

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'

    - name: Update dependencies
      run: |
        python -m pip install --upgrade pip pip-tools
        pip-compile --upgrade pyproject.toml --output-file requirements.txt

    - name: Create Pull Request
      uses: peter-evans/create-pull-request@v5
      with:
        token: ${{ secrets.GITHUB_TOKEN }}
        commit-message: 'chore: update dependencies'
        title: '🔄 Weekly dependency updates'
        body: |
          Automated dependency updates
          
          - Updated to latest compatible versions
          - All tests should pass
        branch: chore/update-deps
        delete-branch: true
```

### 4. Release Management (`.github/workflows/release.yml`)

```yaml
# Release Management Workflow
name: Release

on:
  push:
    tags:
      - 'v*.*.*'

env:
  PYTHON_VERSION: '3.11'

jobs:
  test:
    name: Test Release
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Install and test
      run: |
        python -m pip install --upgrade pip
        pip install -e .[dev]
        pytest tests/

  build:
    name: Build Release
    runs-on: ubuntu-latest
    needs: test

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ env.PYTHON_VERSION }}

    - name: Build packages
      run: |
        python -m pip install --upgrade pip build
        python -m build

    - name: Upload packages
      uses: actions/upload-artifact@v3
      with:
        name: packages
        path: dist/

  publish:
    name: Publish to PyPI
    runs-on: ubuntu-latest
    needs: build
    environment: release

    steps:
    - name: Download packages
      uses: actions/download-artifact@v3
      with:
        name: packages
        path: dist/

    - name: Publish to PyPI
      uses: pypa/gh-action-pypi-publish@release/v1
      with:
        user: __token__
        password: ${{ secrets.PYPI_API_TOKEN }}
        skip_existing: true

  create-release:
    name: Create GitHub Release
    runs-on: ubuntu-latest
    needs: [test, build]

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Create Release
      uses: actions/create-release@v1
      env:
        GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      with:
        tag_name: ${{ github.ref_name }}
        release_name: Release ${{ github.ref_name }}
        body: |
          ## What's Changed
          
          See [CHANGELOG.md](CHANGELOG.md) for detailed changes.
          
          ## Installation
          
          ```bash
          pip install agent-orchestrated-etl==${{ github.ref_name }}
          ```
        draft: false
        prerelease: false
```

### 5. Maintenance (`.github/workflows/maintenance.yml`)

```yaml
# Automated Maintenance
name: Maintenance

on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM
  workflow_dispatch:

jobs:
  cleanup:
    name: Repository Cleanup
    runs-on: ubuntu-latest

    steps:
    - name: Checkout code
      uses: actions/checkout@v4

    - name: Clean old workflow runs
      uses: actions/github-script@v6
      with:
        script: |
          const { data: workflows } = await github.rest.actions.listRepoWorkflows({
            owner: context.repo.owner,
            repo: context.repo.repo,
          });

          for (const workflow of workflows.workflows) {
            const { data: runs } = await github.rest.actions.listWorkflowRuns({
              owner: context.repo.owner,
              repo: context.repo.repo,
              workflow_id: workflow.id,
              status: 'completed',
              per_page: 100,
            });

            const runsToDelete = runs.workflow_runs.slice(30);
            
            for (const run of runsToDelete) {
              if (new Date() - new Date(run.created_at) > 30 * 24 * 60 * 60 * 1000) {
                try {
                  await github.rest.actions.deleteWorkflowRun({
                    owner: context.repo.owner,
                    repo: context.repo.repo,
                    run_id: run.id,
                  });
                } catch (error) {
                  console.log(`Failed to delete run ${run.id}`);
                }
              }
            }
          }
```

## Setup Instructions

1. **Create the workflows directory**:
   ```bash
   mkdir -p .github/workflows
   ```

2. **Add each workflow file** to `.github/workflows/` with the exact content above

3. **Configure repository secrets**:
   - Go to Settings → Secrets and variables → Actions
   - Add the required secrets listed above

4. **Enable branch protection** (recommended):
   - Go to Settings → Branches
   - Add protection rules for `main` branch
   - Require status checks to pass
   - Require up-to-date branches

5. **Test the workflows**:
   - Create a test branch and PR to verify CI works
   - Check that all status checks pass

## Troubleshooting

- **Workflow not triggering**: Check the file is in `.github/workflows/` and has `.yml` extension
- **Permission errors**: Ensure required secrets are configured
- **Test failures**: Check that dependencies are correctly specified in `pyproject.toml`

For more detailed troubleshooting, see `docs/guides/troubleshooting.md`.