"""Nox configuration for Agent Orchestrated ETL.

Nox is used for running tests, linting, and other development tasks
across multiple Python versions and environments.
"""

import nox

# Python versions to test against
PYTHON_VERSIONS = ["3.8", "3.9", "3.10", "3.11"]
# Default Python version for most sessions
DEFAULT_PYTHON = "3.9"

# Configure nox
nox.options.reuse_existing_virtualenvs = True
nox.options.sessions = ["tests", "lint", "type_check"]


@nox.session(python=PYTHON_VERSIONS)
def tests(session):
    """Run the test suite."""
    session.install("-e", ".[dev]")
    
    # Run tests with coverage
    session.run(
        "coverage", "run", "-m", "pytest", 
        "tests/", "-v", "--tb=short",
        *session.posargs
    )
    
    # Generate coverage report for the primary Python version
    if session.python == DEFAULT_PYTHON:
        session.run("coverage", "report", "-m")
        session.run("coverage", "html")
        session.run("coverage", "xml")


@nox.session(python=DEFAULT_PYTHON)
def integration_tests(session):
    """Run integration tests."""
    session.install("-e", ".[dev]")
    session.run(
        "pytest", 
        "tests/", "-v", "-m", "integration",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def performance_tests(session):
    """Run performance benchmarks."""
    session.install("-e", ".[dev]")
    session.run(
        "pytest", 
        "tests/performance/", "-v", "--benchmark-only",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def lint(session):
    """Run linting with ruff."""
    session.install("ruff")
    session.run("ruff", "check", "src/", "tests/", *session.posargs)


@nox.session(python=DEFAULT_PYTHON)
def format_check(session):
    """Check code formatting with ruff."""
    session.install("ruff")
    session.run("ruff", "format", "--check", "src/", "tests/", *session.posargs)


@nox.session(python=DEFAULT_PYTHON)
def format_fix(session):
    """Fix code formatting with ruff."""
    session.install("ruff")
    session.run("ruff", "format", "src/", "tests/", *session.posargs)


@nox.session(python=DEFAULT_PYTHON)
def type_check(session):
    """Run static type checking with mypy."""
    session.install("-e", ".[dev]")
    session.run("mypy", "src/", *session.posargs)


@nox.session(python=DEFAULT_PYTHON)
def security(session):
    """Run security checks."""
    session.install("bandit[toml]", "safety")
    
    # Run bandit security linter
    session.run(
        "bandit", "-r", "src/", 
        "-f", "json", "-o", "bandit-report.json"
    )
    
    # Check for known vulnerabilities
    session.run("safety", "check", "--json")


@nox.session(python=DEFAULT_PYTHON)
def docs(session):
    """Build documentation."""
    session.install(
        "mkdocs-material", 
        "mkdocs-mermaid2-plugin",
        "mkdocstrings[python]", 
        "mkdocs-gen-files",
        "mkdocs-literate-nav", 
        "mkdocs-section-index"
    )
    session.run("mkdocs", "build", "--strict")


@nox.session(python=DEFAULT_PYTHON)
def docs_serve(session):
    """Serve documentation locally."""
    session.install(
        "mkdocs-material", 
        "mkdocs-mermaid2-plugin",
        "mkdocstrings[python]", 
        "mkdocs-gen-files",
        "mkdocs-literate-nav", 
        "mkdocs-section-index"
    )
    session.run("mkdocs", "serve")


@nox.session(python=DEFAULT_PYTHON)
def pre_commit(session):
    """Run pre-commit hooks."""
    session.install("pre-commit")
    session.run("pre-commit", "run", "--all-files", *session.posargs)


@nox.session(python=DEFAULT_PYTHON)
def build(session):
    """Build package distributions."""
    session.install("build", "twine")
    session.run("python", "-m", "build")
    session.run("twine", "check", "dist/*")


@nox.session(python=DEFAULT_PYTHON)
def chaos_test(session):
    """Run chaos engineering tests."""
    session.install("-e", ".[dev]")
    session.run(
        "python", "chaos-engineering/chaos-runner.py",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def benchmark(session):
    """Run performance benchmarks."""
    session.install("-e", ".[dev]")
    session.run(
        "python", "scripts/performance_benchmark.py",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def sbom(session):
    """Generate Software Bill of Materials."""
    session.install("-e", ".[dev]")
    session.run(
        "python", "scripts/generate_sbom.py",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def vulnerability_scan(session):
    """Run comprehensive vulnerability scanning."""
    session.install("-e", ".[dev]")
    session.run(
        "python", "scripts/vulnerability_scanner.py",
        *session.posargs
    )


@nox.session(python=DEFAULT_PYTHON)
def clean(session):
    """Clean up build artifacts and caches."""
    import shutil
    from pathlib import Path
    
    # Directories to clean
    clean_dirs = [
        "build/", "dist/", "*.egg-info/", 
        ".pytest_cache/", ".mypy_cache/", ".ruff_cache/",
        "htmlcov/", ".coverage", ".nox/"
    ]
    
    for pattern in clean_dirs:
        for path in Path(".").glob(pattern):
            if path.is_dir():
                shutil.rmtree(path)
                session.log(f"Removed directory: {path}")
            elif path.is_file():
                path.unlink()
                session.log(f"Removed file: {path}")


@nox.session(python=DEFAULT_PYTHON)
def install_dev(session):
    """Install package in development mode."""
    session.install("-e", ".[dev]")
    session.log("Package installed in development mode")


# Composite sessions for common workflows
@nox.session(python=DEFAULT_PYTHON)
def ci(session):
    """Run all CI checks (fast)."""
    session.notify("lint")
    session.notify("type_check")
    session.notify("tests")


@nox.session(python=DEFAULT_PYTHON)
def full_test(session):
    """Run comprehensive testing suite."""
    session.notify("tests")
    session.notify("integration_tests")
    session.notify("performance_tests")
    session.notify("security")


@nox.session(python=DEFAULT_PYTHON)
def release_check(session):
    """Run all checks before release."""
    session.notify("clean")
    session.notify("full_test")
    session.notify("docs")
    session.notify("build")
    session.notify("sbom")
    session.notify("vulnerability_scan")