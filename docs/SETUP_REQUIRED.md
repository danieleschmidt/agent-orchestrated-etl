# Manual Setup Requirements

## Repository Administrator Tasks

The following items require manual setup by repository administrators with appropriate permissions:

### 1. GitHub Repository Settings
- Enable vulnerability alerts and Dependabot
- Configure repository topics and description
- Set homepage URL and social links

### 2. Branch Protection Rules
- Protect main branch from direct pushes
- Require pull request reviews (minimum 1)
- Require status checks before merging
- Enable automatic deletion of head branches

### 3. Repository Secrets (GitHub Settings > Secrets)
- `PYPI_TOKEN`: For package publishing
- `CODECOV_TOKEN`: For code coverage reporting  
- `SECURITY_EMAIL`: For security notifications

### 4. GitHub Actions Workflows
- Create CI/CD workflow files in `.github/workflows/`
- Configure automated testing and deployment
- Set up security scanning and dependency updates

### 5. External Integrations
- Configure monitoring and alerting systems
- Set up external security scanning tools
- Enable continuous integration services

### 6. Access Management
- Configure team permissions and access levels
- Set up CODEOWNERS file for automated review assignments
- Configure webhook notifications

## Reference Documentation

- [GitHub Repository Settings Guide](https://docs.github.com/en/repositories/managing-your-repositorys-settings-and-features)
- [Branch Protection Documentation](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-protected-branches)
- [GitHub Actions Setup Guide](https://docs.github.com/en/actions/quickstart)