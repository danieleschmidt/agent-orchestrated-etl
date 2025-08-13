#!/usr/bin/env python3
"""Production readiness checklist and deployment validation."""

import os
import sys
import json
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Tuple


class ProductionReadinessChecker:
    """Comprehensive production readiness validation."""

    def __init__(self):
        self.checks = []
        self.passed = 0
        self.failed = 0
        self.warnings = 0

    def log_check(self, name: str, status: str, message: str = "", recommendation: str = ""):
        """Log a check result."""
        self.checks.append({
            "name": name,
            "status": status,
            "message": message,
            "recommendation": recommendation,
            "timestamp": datetime.utcnow().isoformat()
        })
        
        if status == "PASS":
            self.passed += 1
            print(f"✅ {name}")
            if message:
                print(f"   {message}")
        elif status == "FAIL":
            self.failed += 1
            print(f"❌ {name}")
            if message:
                print(f"   {message}")
            if recommendation:
                print(f"   💡 Recommendation: {recommendation}")
        elif status == "WARN":
            self.warnings += 1
            print(f"⚠️  {name}")
            if message:
                print(f"   {message}")
            if recommendation:
                print(f"   💡 Recommendation: {recommendation}")

    def check_code_quality(self) -> bool:
        """Check code quality and standards."""
        print("\n🔍 Code Quality Checks")
        print("-" * 40)
        
        # Check for Python files
        python_files = list(Path("src").rglob("*.py"))
        if not python_files:
            self.log_check("Python Source Files", "FAIL", "No Python files found in src/")
            return False
        
        self.log_check("Python Source Files", "PASS", f"Found {len(python_files)} Python files")
        
        # Check for __init__.py files
        init_files = list(Path("src").rglob("__init__.py"))
        self.log_check("Package Structure", "PASS" if init_files else "WARN", 
                      f"Found {len(init_files)} package init files")
        
        # Check for docstrings in main modules
        documented_modules = 0
        for py_file in python_files:
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    if '"""' in content and ('def ' in content or 'class ' in content):
                        documented_modules += 1
            except:
                pass
        
        doc_percentage = (documented_modules / len(python_files)) * 100
        if doc_percentage >= 80:
            self.log_check("Documentation Coverage", "PASS", f"{doc_percentage:.1f}% of modules documented")
        elif doc_percentage >= 50:
            self.log_check("Documentation Coverage", "WARN", f"{doc_percentage:.1f}% of modules documented",
                          "Consider adding more docstrings")
        else:
            self.log_check("Documentation Coverage", "FAIL", f"{doc_percentage:.1f}% of modules documented",
                          "Add comprehensive docstrings to modules")
        
        return True

    def check_security(self) -> bool:
        """Check security configurations and practices."""
        print("\n🔒 Security Checks")
        print("-" * 40)
        
        # Check for requirements.txt
        if Path("requirements.txt").exists():
            self.log_check("Dependency Management", "PASS", "requirements.txt found")
            
            # Check for known vulnerable packages (basic check)
            with open("requirements.txt", 'r') as f:
                deps = f.read().lower()
                if "flask" in deps and "==0.12" in deps:
                    self.log_check("Vulnerable Dependencies", "FAIL", "Flask 0.12 has known vulnerabilities",
                                  "Update to latest Flask version")
                else:
                    self.log_check("Vulnerable Dependencies", "PASS", "No obvious vulnerable dependencies")
        else:
            self.log_check("Dependency Management", "WARN", "requirements.txt not found",
                          "Create requirements.txt for dependency management")
        
        # Check for secrets in code
        secret_patterns = ["password", "secret", "api_key", "token"]
        secrets_found = []
        
        for py_file in Path("src").rglob("*.py"):
            try:
                with open(py_file, 'r') as f:
                    content = f.read().lower()
                    for pattern in secret_patterns:
                        if f"{pattern} = " in content and "example" not in content:
                            secrets_found.append(f"{py_file}:{pattern}")
            except:
                pass
        
        if secrets_found:
            self.log_check("Hardcoded Secrets", "WARN", f"Potential secrets found: {len(secrets_found)}",
                          "Review code for hardcoded secrets")
        else:
            self.log_check("Hardcoded Secrets", "PASS", "No obvious hardcoded secrets found")
        
        # Check for security modules
        security_files = [
            "src/agent_orchestrated_etl/security.py",
            "src/agent_orchestrated_etl/compliance.py"
        ]
        
        security_modules = sum(1 for f in security_files if Path(f).exists())
        if security_modules >= 2:
            self.log_check("Security Modules", "PASS", f"Found {security_modules} security modules")
        else:
            self.log_check("Security Modules", "WARN", f"Found {security_modules} security modules",
                          "Consider implementing comprehensive security modules")
        
        return True

    def check_configuration(self) -> bool:
        """Check configuration management."""
        print("\n⚙️ Configuration Checks")
        print("-" * 40)
        
        # Check for config files
        config_files = [
            "src/agent_orchestrated_etl/config.py",
            "config/production.json",
            "docker-compose.yml",
            "Dockerfile"
        ]
        
        found_configs = sum(1 for f in config_files if Path(f).exists())
        if found_configs >= 3:
            self.log_check("Configuration Files", "PASS", f"Found {found_configs}/{len(config_files)} config files")
        else:
            self.log_check("Configuration Files", "WARN", f"Found {found_configs}/{len(config_files)} config files",
                          "Ensure all necessary configuration files are present")
        
        # Check for environment variable usage
        env_usage = []
        for py_file in Path("src").rglob("*.py"):
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    if "os.getenv" in content or "os.environ" in content:
                        env_usage.append(py_file)
            except:
                pass
        
        if env_usage:
            self.log_check("Environment Variables", "PASS", f"Environment variables used in {len(env_usage)} files")
        else:
            self.log_check("Environment Variables", "WARN", "No environment variable usage found",
                          "Consider using environment variables for configuration")
        
        return True

    def check_monitoring(self) -> bool:
        """Check monitoring and logging setup."""
        print("\n📊 Monitoring & Logging Checks")
        print("-" * 40)
        
        # Check for logging configuration
        logging_files = [
            "src/agent_orchestrated_etl/logging_config.py",
            "src/agent_orchestrated_etl/health.py"
        ]
        
        logging_modules = sum(1 for f in logging_files if Path(f).exists())
        if logging_modules >= 2:
            self.log_check("Logging Setup", "PASS", f"Found {logging_modules} logging modules")
        else:
            self.log_check("Logging Setup", "WARN", f"Found {logging_modules} logging modules",
                          "Implement comprehensive logging")
        
        # Check for health check endpoints
        health_endpoints = []
        for py_file in Path("src").rglob("*.py"):
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    if "health" in content.lower() and ("endpoint" in content or "check" in content):
                        health_endpoints.append(py_file)
            except:
                pass
        
        if health_endpoints:
            self.log_check("Health Checks", "PASS", f"Health check functionality found")
        else:
            self.log_check("Health Checks", "WARN", "No health check endpoints found",
                          "Implement health check endpoints for monitoring")
        
        # Check for monitoring configuration
        monitoring_files = [
            "monitoring/prometheus.yml",
            "monitoring/grafana/dashboards/dashboard-config.yaml"
        ]
        
        monitoring_configs = sum(1 for f in monitoring_files if Path(f).exists())
        if monitoring_configs >= 1:
            self.log_check("Monitoring Configuration", "PASS", f"Found {monitoring_configs} monitoring configs")
        else:
            self.log_check("Monitoring Configuration", "WARN", "No monitoring configuration found",
                          "Add Prometheus/Grafana monitoring configuration")
        
        return True

    def check_deployment(self) -> bool:
        """Check deployment readiness."""
        print("\n🚀 Deployment Checks")
        print("-" * 40)
        
        # Check for Docker setup
        docker_files = ["Dockerfile", "docker-compose.yml", "docker-compose.prod.yml"]
        docker_configs = sum(1 for f in docker_files if Path(f).exists())
        
        if docker_configs >= 2:
            self.log_check("Docker Configuration", "PASS", f"Found {docker_configs} Docker files")
        else:
            self.log_check("Docker Configuration", "WARN", f"Found {docker_configs} Docker files",
                          "Ensure Docker setup is complete")
        
        # Check for Kubernetes manifests
        k8s_files = list(Path("kubernetes").glob("*.yaml")) if Path("kubernetes").exists() else []
        if k8s_files:
            self.log_check("Kubernetes Manifests", "PASS", f"Found {len(k8s_files)} K8s manifests")
        else:
            self.log_check("Kubernetes Manifests", "WARN", "No Kubernetes manifests found",
                          "Consider adding Kubernetes deployment manifests")
        
        # Check for CI/CD configuration
        cicd_files = [
            ".github/workflows",
            "Makefile",
            "scripts/build.sh"
        ]
        
        cicd_configs = sum(1 for f in cicd_files if Path(f).exists())
        if cicd_configs >= 2:
            self.log_check("CI/CD Configuration", "PASS", f"Found {cicd_configs} CI/CD components")
        else:
            self.log_check("CI/CD Configuration", "WARN", f"Found {cicd_configs} CI/CD components",
                          "Set up comprehensive CI/CD pipeline")
        
        # Check for deployment scripts
        deploy_scripts = [
            "deploy.sh",
            "scripts/deploy.sh",
            "deployment/production_setup.sh"
        ]
        
        deploy_found = sum(1 for f in deploy_scripts if Path(f).exists())
        if deploy_found >= 1:
            self.log_check("Deployment Scripts", "PASS", f"Found {deploy_found} deployment scripts")
        else:
            self.log_check("Deployment Scripts", "WARN", "No deployment scripts found",
                          "Create deployment automation scripts")
        
        return True

    def check_testing(self) -> bool:
        """Check testing setup and coverage."""
        print("\n🧪 Testing Checks")
        print("-" * 40)
        
        # Check for test files
        test_files = list(Path("tests").rglob("test_*.py")) if Path("tests").exists() else []
        if test_files:
            self.log_check("Test Files", "PASS", f"Found {len(test_files)} test files")
        else:
            self.log_check("Test Files", "FAIL", "No test files found",
                          "Create comprehensive test suite")
        
        # Check for pytest configuration
        if Path("pytest.ini").exists() or Path("pyproject.toml").exists():
            self.log_check("Test Configuration", "PASS", "Test configuration found")
        else:
            self.log_check("Test Configuration", "WARN", "No test configuration found",
                          "Configure pytest for testing")
        
        # Check for different test types
        test_types = {
            "unit": 0,
            "integration": 0,
            "e2e": 0,
            "performance": 0
        }
        
        for test_file in test_files:
            content = test_file.name.lower()
            if "unit" in content:
                test_types["unit"] += 1
            elif "integration" in content:
                test_types["integration"] += 1
            elif "e2e" in content or "end" in content:
                test_types["e2e"] += 1
            elif "performance" in content or "benchmark" in content:
                test_types["performance"] += 1
        
        covered_types = sum(1 for count in test_types.values() if count > 0)
        if covered_types >= 3:
            self.log_check("Test Coverage Types", "PASS", f"Found {covered_types} test types")
        elif covered_types >= 2:
            self.log_check("Test Coverage Types", "WARN", f"Found {covered_types} test types",
                          "Consider adding more test types")
        else:
            self.log_check("Test Coverage Types", "WARN", f"Found {covered_types} test types",
                          "Implement unit, integration, and e2e tests")
        
        return True

    def check_documentation(self) -> bool:
        """Check documentation completeness."""
        print("\n📚 Documentation Checks")
        print("-" * 40)
        
        # Check for main documentation files
        doc_files = [
            "README.md",
            "CONTRIBUTING.md",
            "CHANGELOG.md",
            "docs/README.md"
        ]
        
        found_docs = sum(1 for f in doc_files if Path(f).exists())
        if found_docs >= 3:
            self.log_check("Core Documentation", "PASS", f"Found {found_docs}/{len(doc_files)} core docs")
        else:
            self.log_check("Core Documentation", "WARN", f"Found {found_docs}/{len(doc_files)} core docs",
                          "Create comprehensive documentation")
        
        # Check README content
        if Path("README.md").exists():
            with open("README.md", 'r') as f:
                readme_content = f.read().lower()
                readme_sections = ["installation", "usage", "configuration", "deployment"]
                found_sections = sum(1 for section in readme_sections if section in readme_content)
                
                if found_sections >= 3:
                    self.log_check("README Completeness", "PASS", f"README contains {found_sections}/4 key sections")
                else:
                    self.log_check("README Completeness", "WARN", f"README contains {found_sections}/4 key sections",
                                  "Add installation, usage, configuration, and deployment sections")
        
        # Check for API documentation
        api_docs = list(Path("docs").rglob("*api*")) if Path("docs").exists() else []
        if api_docs:
            self.log_check("API Documentation", "PASS", f"Found {len(api_docs)} API doc files")
        else:
            self.log_check("API Documentation", "WARN", "No API documentation found",
                          "Document API endpoints and usage")
        
        return True

    def check_performance(self) -> bool:
        """Check performance considerations."""
        print("\n⚡ Performance Checks")
        print("-" * 40)
        
        # Check for performance optimization modules
        perf_modules = [
            "src/agent_orchestrated_etl/performance_optimizer.py",
            "src/agent_orchestrated_etl/adaptive_resources.py",
            "src/agent_orchestrated_etl/auto_scaling.py"
        ]
        
        found_perf = sum(1 for f in perf_modules if Path(f).exists())
        if found_perf >= 2:
            self.log_check("Performance Modules", "PASS", f"Found {found_perf} performance modules")
        else:
            self.log_check("Performance Modules", "WARN", f"Found {found_perf} performance modules",
                          "Implement performance optimization modules")
        
        # Check for caching implementation
        caching_found = []
        for py_file in Path("src").rglob("*.py"):
            try:
                with open(py_file, 'r') as f:
                    content = f.read().lower()
                    if "cache" in content or "redis" in content or "memcached" in content:
                        caching_found.append(py_file)
            except:
                pass
        
        if caching_found:
            self.log_check("Caching Implementation", "PASS", f"Caching found in {len(caching_found)} files")
        else:
            self.log_check("Caching Implementation", "WARN", "No caching implementation found",
                          "Implement caching for performance")
        
        # Check for async/await usage
        async_usage = []
        for py_file in Path("src").rglob("*.py"):
            try:
                with open(py_file, 'r') as f:
                    content = f.read()
                    if "async def" in content or "await " in content:
                        async_usage.append(py_file)
            except:
                pass
        
        if async_usage:
            self.log_check("Async Programming", "PASS", f"Async/await used in {len(async_usage)} files")
        else:
            self.log_check("Async Programming", "WARN", "No async programming found",
                          "Consider async programming for better performance")
        
        return True

    def check_global_features(self) -> bool:
        """Check global-first features implementation."""
        print("\n🌍 Global Features Checks")
        print("-" * 40)
        
        # Check for global feature modules
        global_modules = [
            "src/agent_orchestrated_etl/internationalization.py",
            "src/agent_orchestrated_etl/multi_region.py",
            "src/agent_orchestrated_etl/compliance.py",
            "src/agent_orchestrated_etl/global_deployment.py"
        ]
        
        found_global = sum(1 for f in global_modules if Path(f).exists())
        if found_global == len(global_modules):
            self.log_check("Global Feature Modules", "PASS", f"Found all {found_global} global modules")
        else:
            self.log_check("Global Feature Modules", "FAIL", f"Found {found_global}/{len(global_modules)} global modules",
                          "Implement all global-first features")
        
        # Check for localization files
        i18n_files = list(Path("src/agent_orchestrated_etl/i18n").glob("*.json")) if Path("src/agent_orchestrated_etl/i18n").exists() else []
        if i18n_files:
            self.log_check("Internationalization Files", "PASS", f"Found {len(i18n_files)} locale files")
        else:
            self.log_check("Internationalization Files", "WARN", "No locale files found",
                          "Add translation files for internationalization")
        
        return found_global == len(global_modules)

    def generate_report(self) -> Dict[str, Any]:
        """Generate comprehensive production readiness report."""
        total_checks = self.passed + self.failed + self.warnings
        score = (self.passed / total_checks * 100) if total_checks > 0 else 0
        
        if score >= 90:
            readiness_level = "PRODUCTION READY"
            readiness_color = "🟢"
        elif score >= 75:
            readiness_level = "MOSTLY READY"
            readiness_color = "🟡"
        elif score >= 50:
            readiness_level = "NEEDS WORK"
            readiness_color = "🟠"
        else:
            readiness_level = "NOT READY"
            readiness_color = "🔴"
        
        report = {
            "assessment_date": datetime.utcnow().isoformat(),
            "overall_score": round(score, 1),
            "readiness_level": readiness_level,
            "total_checks": total_checks,
            "passed": self.passed,
            "failed": self.failed,
            "warnings": self.warnings,
            "checks": self.checks,
            "recommendations": [
                check["recommendation"] for check in self.checks 
                if check["recommendation"] and check["status"] in ["FAIL", "WARN"]
            ]
        }
        
        print(f"\n{readiness_color} PRODUCTION READINESS: {readiness_level}")
        print(f"📊 Overall Score: {score:.1f}%")
        print(f"✅ Passed: {self.passed}")
        print(f"❌ Failed: {self.failed}")
        print(f"⚠️  Warnings: {self.warnings}")
        
        return report

    def run_all_checks(self) -> Dict[str, Any]:
        """Run all production readiness checks."""
        print("🏭 PRODUCTION READINESS ASSESSMENT")
        print("=" * 50)
        
        checks = [
            self.check_code_quality,
            self.check_security,
            self.check_configuration,
            self.check_monitoring,
            self.check_deployment,
            self.check_testing,
            self.check_documentation,
            self.check_performance,
            self.check_global_features
        ]
        
        for check in checks:
            try:
                check()
            except Exception as e:
                print(f"❌ Error running check: {e}")
        
        print("\n" + "=" * 50)
        print("📋 PRODUCTION READINESS SUMMARY")
        print("=" * 50)
        
        return self.generate_report()


def main():
    """Run production readiness assessment."""
    checker = ProductionReadinessChecker()
    report = checker.run_all_checks()
    
    # Save report to file
    with open("production_readiness_report.json", "w") as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📄 Full report saved to: production_readiness_report.json")
    
    # Print key recommendations
    if report["recommendations"]:
        print("\n💡 KEY RECOMMENDATIONS:")
        for i, rec in enumerate(report["recommendations"][:10], 1):  # Top 10
            print(f"{i}. {rec}")
    
    # Return success if mostly ready or better
    return report["overall_score"] >= 75


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)