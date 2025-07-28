# Release Process

## Overview

This document outlines the release process for Agent-Orchestrated-ETL, including versioning strategy, release automation, and deployment procedures.

## Versioning Strategy

We follow [Semantic Versioning (SemVer)](https://semver.org/) with the format `MAJOR.MINOR.PATCH`:

- **MAJOR**: Breaking changes that require user intervention
- **MINOR**: New features that are backward compatible
- **PATCH**: Bug fixes and small improvements

### Version Examples
- `1.0.0` - First stable release
- `1.1.0` - New features added
- `1.1.1` - Bug fixes
- `2.0.0` - Breaking changes introduced

## Commit Message Convention

We use [Conventional Commits](https://www.conventionalcommits.org/) for automated release generation:

### Format
```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

### Types
- `feat`: New feature (triggers MINOR release)
- `fix`: Bug fix (triggers PATCH release)
- `docs`: Documentation changes (no release)
- `style`: Code style changes (no release)
- `refactor`: Code refactoring (triggers PATCH release)
- `perf`: Performance improvements (triggers PATCH release)
- `test`: Test changes (no release)
- `build`: Build system changes (triggers PATCH release)
- `ci`: CI/CD changes (no release)
- `chore`: Maintenance tasks (no release)
- `revert`: Revert changes (triggers PATCH release)

### Breaking Changes
Add `BREAKING CHANGE:` in the footer or `!` after the type to trigger a MAJOR release:

```
feat!: remove deprecated API endpoints

BREAKING CHANGE: The /v1/legacy endpoint has been removed. Use /v2/pipelines instead.
```

### Examples
```bash
# Feature (MINOR release)
feat(agents): add support for custom transformation functions

# Bug fix (PATCH release)
fix(orchestrator): resolve memory leak in pipeline execution

# Breaking change (MAJOR release)
feat(api)!: restructure REST API endpoints

BREAKING CHANGE: All API endpoints now require authentication
```

## Release Automation

### Automated Release Process

1. **Commit Analysis**: Semantic Release analyzes commit messages
2. **Version Calculation**: Determines next version based on commit types
3. **Changelog Generation**: Creates release notes from commits
4. **Version Updates**: Updates `pyproject.toml`, `package.json`
5. **Build Artifacts**: Creates distribution packages and Docker images
6. **Git Operations**: Creates tags and commits version updates
7. **GitHub Release**: Publishes release with assets and notes
8. **Notifications**: Sends notifications to relevant channels

### Trigger Conditions

Releases are automatically triggered when:
- Commits are pushed to `main` branch
- Commits contain release-triggering types (`feat`, `fix`, `perf`, etc.)
- All CI checks pass

### Pre-release Versions

- **Beta releases**: Triggered by pushes to `develop` branch
  - Format: `1.0.0-beta.1`, `1.0.0-beta.2`
- **Release candidates**: Triggered by pushes to `release/*` branches
  - Format: `1.0.0-rc.1`, `1.0.0-rc.2`

## Manual Release Process

### Prerequisites

1. Install semantic-release:
   ```bash
   npm install -g @semantic-release/cli
   npm install -g conventional-changelog-cli
   ```

2. Ensure clean working directory:
   ```bash
   git status
   git pull origin main
   ```

### Step-by-Step Release

1. **Review Changes**
   ```bash
   # Review commits since last release
   git log $(git describe --tags --abbrev=0)..HEAD --oneline
   
   # Generate preview changelog
   conventional-changelog -p conventionalcommits -u
   ```

2. **Run Tests**
   ```bash
   # Run full test suite
   npm run test
   npm run test:integration
   npm run lint
   npm run typecheck
   ```

3. **Build and Validate**
   ```bash
   # Build production image
   docker build -t agent-orchestrated-etl:test --target production .
   
   # Test image
   docker run --rm agent-orchestrated-etl:test --version
   ```

4. **Create Release**
   ```bash
   # Dry run to preview release
   npx semantic-release --dry-run
   
   # Create actual release
   npx semantic-release
   ```

5. **Verify Release**
   ```bash
   # Check GitHub release
   gh release list
   
   # Verify Docker images
   docker images | grep agent-orchestrated-etl
   
   # Test deployment
   docker run --rm -p 8793:8793 agent-orchestrated-etl:latest
   curl http://localhost:8793/health
   ```

## Hotfix Process

For critical bug fixes that need immediate release:

1. **Create Hotfix Branch**
   ```bash
   git checkout main
   git pull origin main
   git checkout -b hotfix/critical-bug-fix
   ```

2. **Make Fix**
   ```bash
   # Implement fix
   git add .
   git commit -m "fix: resolve critical security vulnerability"
   ```

3. **Test Fix**
   ```bash
   npm run test
   npm run security
   ```

4. **Create Pull Request**
   ```bash
   git push origin hotfix/critical-bug-fix
   gh pr create --title "Hotfix: Critical Bug Fix" --body "Resolves critical issue"
   ```

5. **Fast-track Review**
   - Request expedited review
   - Merge immediately after approval
   - Monitor automated release

## Release Artifacts

### Generated Artifacts

Each release creates the following artifacts:

1. **Python Package**
   - Source distribution (`.tar.gz`)
   - Wheel distribution (`.whl`)
   - Published to PyPI (when configured)

2. **Docker Images**
   - Production image tagged with version
   - Latest tag for most recent release
   - Multi-architecture support (amd64, arm64)

3. **Documentation**
   - API documentation (OpenAPI spec)
   - Deployment guides
   - Release notes

4. **Security Artifacts**
   - Software Bill of Materials (SBOM)
   - Container vulnerability scan results
   - Security audit reports

### Artifact Storage

- **GitHub Releases**: Primary storage for all artifacts
- **Container Registry**: Docker images stored in GitHub Container Registry
- **PyPI**: Python packages (when configured)
- **S3**: Long-term archival storage

## Deployment Process

### Staging Deployment

1. **Automatic Deployment**
   - All releases automatically deploy to staging
   - Beta versions deploy to beta staging environment

2. **Validation Tests**
   ```bash
   # Run integration tests against staging
   ENVIRONMENT=staging npm run test:integration
   
   # Performance testing
   npm run test:performance -- --target=staging
   
   # Security scanning
   npm run security:scan -- --environment=staging
   ```

### Production Deployment

1. **Manual Approval Required**
   - Staging validation must pass
   - Security review completed
   - Change management approval

2. **Deployment Steps**
   ```bash
   # Deploy to production (requires approval)
   gh workflow run production-deploy --ref=v1.0.0
   
   # Monitor deployment
   kubectl rollout status deployment/agent-etl-app -n production
   
   # Verify health
   curl https://api.agent-etl.company.com/health
   ```

3. **Rollback Plan**
   ```bash
   # Immediate rollback if issues detected
   gh workflow run production-rollback --ref=v0.9.0
   ```

## Release Checklist

### Pre-Release
- [ ] All tests passing
- [ ] Security scans clean
- [ ] Documentation updated
- [ ] Breaking changes documented
- [ ] Migration scripts ready (if needed)
- [ ] Staging validation complete

### Release
- [ ] Version tagged correctly
- [ ] Release notes generated
- [ ] Artifacts built and uploaded
- [ ] Container images published
- [ ] PyPI package published (if applicable)

### Post-Release
- [ ] Production deployment successful
- [ ] Health checks passing
- [ ] Monitoring alerts configured
- [ ] Team notified
- [ ] Documentation site updated
- [ ] Security team informed

## Rollback Procedures

### Immediate Rollback (< 1 hour)
```bash
# Rollback to previous version
kubectl set image deployment/agent-etl-app agent-etl=agent-orchestrated-etl:v0.9.0

# Verify rollback
kubectl rollout status deployment/agent-etl-app
curl https://api.agent-etl.company.com/health
```

### Database Rollback (if needed)
```bash
# Restore database backup
pg_restore -h localhost -U agent_etl_user -d agent_etl backup_pre_release.sql

# Run rollback migrations
python manage.py migrate app 0001 --fake
```

### Full Environment Rollback
```bash
# Restore entire environment to previous state
terraform apply -var="app_version=v0.9.0"
```

## Communication

### Release Announcements

1. **Internal Channels**
   - Engineering team Slack channel
   - Product team notification
   - DevOps team alert

2. **External Channels**
   - Customer newsletter (major releases)
   - API documentation updates
   - GitHub release notes

3. **Emergency Communications**
   - Critical hotfix announcements
   - Security vulnerability disclosures
   - Service disruption notifications

### Templates

#### Release Announcement Template
```markdown
## 🚀 Agent-Orchestrated-ETL v1.0.0 Released!

We're excited to announce the release of Agent-Orchestrated-ETL v1.0.0!

### ✨ What's New
- Enhanced AI agent orchestration
- Improved pipeline performance
- New data source connectors

### 🐛 Bug Fixes
- Fixed memory leak in transformation agents
- Resolved connection pool issues

### 📚 Documentation
- Updated API documentation
- New deployment guides

### 🔧 Migration Guide
For users upgrading from v0.x, please see our [migration guide](docs/MIGRATION.md).

### 📦 Installation
```bash
pip install agent-orchestrated-etl==1.0.0
# or
docker pull ghcr.io/terragon-labs/agent-orchestrated-etl:v1.0.0
```

Questions? Join our [Discord](https://discord.gg/agent-etl) or open an [issue](https://github.com/terragon-labs/agent-orchestrated-etl/issues).
```

## Monitoring and Metrics

### Release Metrics
- Release frequency
- Time from commit to production
- Rollback rate
- Deployment success rate
- Time to resolution for hotfixes

### Quality Metrics
- Test coverage
- Security vulnerability count
- Performance regression detection
- User adoption rate

### Alerting
- Failed releases
- High rollback rate
- Security vulnerabilities in dependencies
- Performance degradation post-release