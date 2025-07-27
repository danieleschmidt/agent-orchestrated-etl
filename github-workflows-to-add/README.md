# GitHub Actions Workflows

These workflow files need to be manually added to `.github/workflows/` due to GitHub App permission restrictions.

## Files to Add:

1. **ci.yml** - Comprehensive CI pipeline with testing, security scanning, and quality checks
2. **cd.yml** - Continuous deployment pipeline for staging and production
3. **dependency-update.yml** - Automated dependency updates and security scanning

## Manual Setup Instructions:

1. Copy these files to `.github/workflows/` in your repository
2. Ensure the repository has the following secrets configured:
   - `PYPI_API_TOKEN` - For package publishing
   - `SLACK_WEBHOOK` - For deployment notifications (optional)
3. Configure branch protection rules for the main branch
4. Enable GitHub Actions in repository settings

## Features Included:

### CI Pipeline (ci.yml):
- Code quality checks (Ruff, MyPy, pre-commit)
- Security scanning (Bandit, Safety, CodeQL)
- Multi-version Python testing (3.8-3.11)
- Integration tests with PostgreSQL and Redis
- Docker build and vulnerability scanning
- Performance testing
- Test result reporting

### CD Pipeline (cd.yml):
- Staging deployment on main branch pushes
- Production deployment on tagged releases
- Package publishing to PyPI
- Documentation updates
- Release notes generation
- Slack notifications

### Dependency Updates (dependency-update.yml):
- Weekly security vulnerability scans
- Automated dependency updates (patch/minor)
- GitHub Actions version updates
- Pre-commit hook updates
- Automated PR creation for updates

## Branch Protection Setup:

Configure these required status checks:
- `CI Status`
- `Code Quality & Security`
- `Unit Tests`
- `Integration Tests`
- `Docker Build & Security Scan`

This ensures no code can be merged without passing all quality gates.