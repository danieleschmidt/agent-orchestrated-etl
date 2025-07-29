# Workflow Setup Instructions

## Quick Setup Guide

Since GitHub Apps cannot create workflow files directly, follow these steps to implement the CI/CD workflows:

### Step 1: Create Workflow Files

```bash
# Create the workflows directory if it doesn't exist
mkdir -p .github/workflows

# Copy the CI/CD template
cp docs/workflows/templates/ci.yml .github/workflows/

# Make the file executable for GitHub Actions
chmod 644 .github/workflows/ci.yml
```

### Step 2: Configure GitHub Secrets

Go to your repository Settings > Secrets and variables > Actions and add:

- `CODECOV_TOKEN`: For coverage reporting
- `DOCKER_HUB_USERNAME`: For Docker publishing  
- `DOCKER_HUB_ACCESS_TOKEN`: For Docker publishing
- `PYPI_API_TOKEN`: For package publishing (when ready for releases)

### Step 3: Enable Branch Protection

In Settings > Branches, add a rule for `main` branch:
- ✅ Require pull request reviews before merging
- ✅ Require status checks to pass before merging
- ✅ Require branches to be up to date before merging

### Step 4: Test the Setup

1. Create a test branch
2. Make a small change
3. Open a pull request
4. Verify all checks pass

## Troubleshooting

If workflows fail:
1. Check the Actions tab for error details
2. Verify all secrets are configured
3. Ensure Python dependencies are properly specified
4. Check workflow syntax with GitHub's workflow validator

For detailed configuration, see the full documentation in [README.md](./README.md).