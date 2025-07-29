# Security Policy

## Supported Versions

| Version | Supported          |
| ------- | ------------------ |
| 0.0.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of Agent-Orchestrated-ETL seriously. If you believe you have found a security vulnerability, please report it to us through GitHub's Security Advisory feature.

### How to Report

1. **GitHub Security Advisories** (Preferred):
   - Go to the [Security tab](https://github.com/your-org/agent-orchestrated-etl/security/advisories)
   - Click "Report a vulnerability"
   - Fill out the advisory details

2. **Email** (Alternative):
   - Send details to: security@your-org.com
   - Include "Agent-Orchestrated-ETL Security" in the subject line

### What to Include

Please include the following information in your report:

- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact assessment
- Suggested fix (if available)
- Your contact information

### Response Timeline

- **Acknowledgment**: Within 24 hours
- **Initial Assessment**: Within 72 hours
- **Status Updates**: Weekly until resolved
- **Resolution**: Target 30 days for critical issues

### Responsible Disclosure

We ask that you:

- Do not publicly disclose the vulnerability until we've had a chance to fix it
- Do not access or modify data that doesn't belong to you
- Do not perform actions that could negatively impact our users or services

### Scope

This security policy applies to:

- The main application code in `src/`
- Docker configurations
- CI/CD workflows
- Documentation that contains security-relevant information

Out of scope:
- Dependencies (report to upstream maintainers)
- Third-party integrations (report to respective vendors)

### Security Best Practices

When contributing to this project:

1. **Never commit secrets or credentials**
2. **Use parameterized queries** to prevent SQL injection
3. **Validate all input data** before processing
4. **Follow principle of least privilege** for permissions
5. **Keep dependencies updated** to patch known vulnerabilities

### Security Features

This project implements several security measures:

- **Static Analysis**: Bandit for Python security linting
- **Dependency Scanning**: Safety for known vulnerability detection  
- **Secret Detection**: detect-secrets for credential scanning
- **Input Validation**: Pydantic models for data validation
- **SQL Injection Prevention**: SQLAlchemy ORM with parameterized queries

### Security Updates

Security updates will be:

- Released as patch versions (e.g., 0.0.1 → 0.0.2)
- Documented in the CHANGELOG.md
- Announced through GitHub Releases
- Communicated via security advisories for critical issues

### Questions?

If you have questions about this security policy, please:

- Open a GitHub Discussion in the Security category
- Email: security@your-org.com