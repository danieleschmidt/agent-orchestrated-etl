#!/usr/bin/env python3
"""
RED PHASE: Tests for ETL-015 Architecture Documentation
This test script validates that comprehensive architecture documentation exists.
"""

import re
from pathlib import Path

def test_codebase_overview_exists():
    """Test that CODEBASE_OVERVIEW.md exists and is comprehensive."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    assert overview_path.exists(), "CODEBASE_OVERVIEW.md must exist"
    
    content = overview_path.read_text()
    assert len(content) > 1000, "CODEBASE_OVERVIEW.md must be comprehensive (>1000 chars)"
    assert "TBD" not in content, "CODEBASE_OVERVIEW.md must not contain placeholder TBD"
    assert "Placeholder" not in content, "CODEBASE_OVERVIEW.md must not contain placeholder sections"

def test_architecture_diagrams_exist():
    """Test that architecture diagrams are present and detailed."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    content = overview_path.read_text()
    
    # Should have multiple mermaid diagrams
    mermaid_blocks = re.findall(r'```mermaid\n(.*?)\n```', content, re.DOTALL)
    assert len(mermaid_blocks) >= 3, "Must have at least 3 architecture diagrams"
    
    # Should have system overview, agent interaction, and data flow diagrams
    combined_content = ' '.join(mermaid_blocks)
    assert "flowchart" in combined_content or "graph" in combined_content, "Must include flowchart diagrams"
    assert "sequenceDiagram" in combined_content, "Must include sequence diagrams"

def test_component_documentation_exists():
    """Test that all major components are documented."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    content = overview_path.read_text()
    
    # Key components that must be documented
    required_components = [
        "ETL Agent", "Agent Coordinator", "Communication Hub",
        "Data Sources", "Tools", "Memory System", "Monitoring"
    ]
    
    for component in required_components:
        assert component in content, f"Component '{component}' must be documented"

def test_api_reference_exists():
    """Test that API reference documentation exists."""
    api_docs = [
        "docs/api/agents.md",
        "docs/api/tools.md", 
        "docs/api/coordination.md"
    ]
    
    for doc_path in api_docs:
        path = Path(doc_path)
        assert path.exists(), f"API documentation {doc_path} must exist"
        content = path.read_text()
        assert len(content) > 500, f"API documentation {doc_path} must be comprehensive"

def test_deployment_guide_exists():
    """Test that deployment guide exists."""
    deployment_path = Path("docs/deployment/README.md")
    assert deployment_path.exists(), "Deployment guide must exist"
    
    content = deployment_path.read_text()
    assert "Configuration" in content, "Deployment guide must include configuration"
    assert "Environment" in content, "Deployment guide must include environment setup"

def test_troubleshooting_guide_exists():
    """Test that troubleshooting guide exists."""
    troubleshooting_path = Path("docs/operations/troubleshooting.md")
    assert troubleshooting_path.exists(), "Troubleshooting guide must exist"
    
    content = troubleshooting_path.read_text()
    assert "Common Issues" in content, "Must document common issues"
    assert "Performance" in content, "Must include performance troubleshooting"

def test_security_documentation_exists():
    """Test that security documentation exists."""
    security_path = Path("docs/security/README.md")
    assert security_path.exists(), "Security documentation must exist"
    
    content = security_path.read_text()
    assert "Threat Model" in content, "Must include threat model"
    assert "Best Practices" in content, "Must include security best practices"

def test_developer_onboarding_exists():
    """Test that developer onboarding guide exists."""
    onboarding_path = Path("docs/developer/onboarding.md")
    assert onboarding_path.exists(), "Developer onboarding guide must exist"
    
    content = onboarding_path.read_text()
    assert "Getting Started" in content, "Must include getting started section"
    assert "Development Environment" in content, "Must include development setup"

if __name__ == "__main__":
    print("🔴 RED PHASE: Testing ETL-015 Architecture Documentation requirements")
    
    test_functions = [
        test_codebase_overview_exists,
        test_architecture_diagrams_exist,
        test_component_documentation_exists,
        test_api_reference_exists,
        test_deployment_guide_exists,
        test_troubleshooting_guide_exists,
        test_security_documentation_exists,
        test_developer_onboarding_exists
    ]
    
    failed_tests = []
    
    for test_func in test_functions:
        try:
            test_func()
            print(f"❌ {test_func.__name__}: FAIL (documentation missing)")
            failed_tests.append(test_func.__name__)
        except Exception as e:
            print(f"❌ {test_func.__name__}: FAIL - {str(e)}")
            failed_tests.append(test_func.__name__)
    
    print(f"\n📊 RED phase results: {len(failed_tests)}/{len(test_functions)} tests failed (expected)")
    print(f"❌ Failed tests: {', '.join(failed_tests)}")
    print("\n🎯 Proceeding to GREEN phase: Implementing comprehensive architecture documentation")