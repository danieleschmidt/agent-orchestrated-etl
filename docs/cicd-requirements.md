# CI/CD Requirements and Recommendations

## Overview
This document outlines the required CI/CD workflows for the agent-orchestrated-etl project.

## Required GitHub Actions Workflows

### 1. Pull Request Validation (`.github/workflows/pr-validation.yml`)
- **Triggers**: Pull requests to main/develop branches
- **Jobs**:
  - Code quality checks (ruff, black, mypy)
  - Security scanning (bandit, detect-secrets)
  - Unit and integration tests with coverage
  - Build verification
  - Performance regression tests
  - Documentation checks

### 2. Security Scanning (`.github/workflows/security.yml`)
- **Triggers**: Push to main, scheduled daily
- **Jobs**:
  - CodeQL analysis
  - Dependency vulnerability scanning (Snyk/Dependabot)
  - Container security scanning (Trivy)
  - Secret scanning
  - SBOM generation

### 3. Continuous Integration (`.github/workflows/ci.yml`)
- **Triggers**: Push to main/develop
- **Jobs**:
  - Matrix testing (Python 3.8, 3.9, 3.10, 3.11)
  - Integration tests with real infrastructure
  - Build and push container images
  - Deploy to staging environment
  - Smoke tests

### 4. Release Pipeline (`.github/workflows/release.yml`)
- **Triggers**: Release creation, tag push
- **Jobs**:
  - Build production containers
  - Security scanning of release artifacts
  - Deploy to production
  - Generate release notes
  - Publish to package registries
  - Update documentation

### 5. Dependency Updates (`.github/workflows/dependency-update.yml`)
- **Triggers**: Scheduled weekly
- **Jobs**:
  - Automated dependency updates
  - Security patch prioritization
  - Test compatibility
  - Create pull requests

## Branch Protection Rules

### Main Branch
- Require pull request reviews (2 reviewers)
- Require status checks to pass:
  - `ci/pr-validation`
  - `security/scan`
  - `tests/unit`
  - `tests/integration`
  - `quality/lint`
  - `quality/typecheck`
- Restrict pushes to admins only
- Require up-to-date branches

### Develop Branch
- Require pull request reviews (1 reviewer)
- Require status checks to pass (same as main)
- Allow force pushes by admins

## Environment Secrets

### Required Secrets
- `DOCKERHUB_USERNAME` / `DOCKERHUB_TOKEN`
- `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`
- `OPENAI_API_KEY` (for testing)
- `SLACK_WEBHOOK_URL` (notifications)
- `CODECOV_TOKEN` (coverage reporting)

### Environment-Specific
- **Development**: Mock credentials, test databases
- **Staging**: Staging infrastructure credentials
- **Production**: Production credentials with least privilege

## Deployment Strategy

### Staging Environment
- Auto-deploy on merge to develop
- Blue-green deployment
- Automated rollback on failure
- Performance and load testing

### Production Environment
- Manual approval required
- Canary deployment (10% -> 50% -> 100%)
- Database migration handling
- Zero-downtime deployment
- Comprehensive monitoring

## Quality Gates

### Required Checks
- Code coverage ≥ 80% (unit tests)
- Code coverage ≥ 70% (integration tests)
- No critical security vulnerabilities
- All pre-commit hooks pass
- Documentation is up-to-date

### Performance Benchmarks
- Pipeline creation < 1s (10 pipelines)
- Large dataset processing < 5s (10k records)
- Memory usage increase < 100MB (20 pipelines)

## Monitoring and Alerting

### CI/CD Metrics
- Build success rate
- Test pass rate
- Deployment frequency
- Lead time for changes
- Mean time to recovery

### Alerts
- Failed builds/deployments
- Security vulnerabilities detected
- Performance regressions
- High resource usage during CI

## Implementation Notes

**Note**: This project follows Terragon Labs policies where GitHub Actions workflows cannot be modified by automated tools. The workflows described above should be implemented manually by authorized personnel.

## Manual Workflow Setup

See [WORKFLOWS_MANUAL_SETUP.md](../WORKFLOWS_MANUAL_SETUP.md) for detailed instructions on setting up these workflows manually.