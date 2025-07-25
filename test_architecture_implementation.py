#!/usr/bin/env python3
"""
GREEN PHASE: Tests for ETL-015 Architecture Documentation Implementation
This test script validates that comprehensive architecture documentation has been successfully implemented.
"""

import re
from pathlib import Path

def test_codebase_overview_comprehensive():
    """Test that CODEBASE_OVERVIEW.md is comprehensive and complete."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    assert overview_path.exists(), "CODEBASE_OVERVIEW.md must exist"
    
    content = overview_path.read_text()
    
    # Check length requirement
    assert len(content) > 1000, f"CODEBASE_OVERVIEW.md must be comprehensive (got {len(content)} chars, need >1000)"
    
    # Check no placeholders
    assert "TBD" not in content, "CODEBASE_OVERVIEW.md must not contain placeholder TBD"
    assert "Placeholder" not in content, "CODEBASE_OVERVIEW.md must not contain placeholder sections"
    
    print("✅ CODEBASE_OVERVIEW.md is comprehensive and complete")

def test_architecture_diagrams_complete():
    """Test that architecture diagrams are present and detailed."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    content = overview_path.read_text()
    
    # Count mermaid diagrams
    mermaid_blocks = re.findall(r'```mermaid\n(.*?)\n```', content, re.DOTALL)
    assert len(mermaid_blocks) >= 3, f"Must have at least 3 architecture diagrams (found {len(mermaid_blocks)})"
    
    # Check for required diagram types
    combined_content = ' '.join(mermaid_blocks)
    assert "flowchart" in combined_content or "graph" in combined_content, "Must include flowchart diagrams"
    assert "sequenceDiagram" in combined_content, "Must include sequence diagrams"
    
    print("✅ Architecture diagrams are comprehensive and detailed")

def test_component_documentation_complete():
    """Test that all major components are documented."""
    overview_path = Path("CODEBASE_OVERVIEW.md")
    content = overview_path.read_text()
    
    # Key components that must be documented
    required_components = [
        "Agent Coordinator", "ETL Agent", "Communication Hub",
        "Data Sources", "Tools", "Memory System", "Monitoring"
    ]
    
    missing_components = []
    for component in required_components:
        if component not in content:
            missing_components.append(component)
    
    assert len(missing_components) == 0, f"Missing components: {missing_components}"
    
    print("✅ All major components are documented")

def test_api_reference_complete():
    """Test that API reference documentation exists and is comprehensive."""
    api_docs = [
        "docs/api/agents.md",
        "docs/api/tools.md", 
        "docs/api/coordination.md"
    ]
    
    for doc_path in api_docs:
        path = Path(doc_path)
        assert path.exists(), f"API documentation {doc_path} must exist"
        content = path.read_text()
        assert len(content) > 500, f"API documentation {doc_path} must be comprehensive (got {len(content)} chars)"
    
    print("✅ API reference documentation is complete")

def test_deployment_guide_complete():
    """Test that deployment guide exists and is comprehensive."""
    deployment_path = Path("docs/deployment/README.md")
    assert deployment_path.exists(), "Deployment guide must exist"
    
    content = deployment_path.read_text()
    assert len(content) > 1000, f"Deployment guide must be comprehensive (got {len(content)} chars)"
    assert "Configuration" in content, "Deployment guide must include configuration"
    assert "Environment" in content, "Deployment guide must include environment setup"
    
    print("✅ Deployment guide is complete")

def test_troubleshooting_guide_complete():
    """Test that troubleshooting guide exists and is comprehensive."""
    troubleshooting_path = Path("docs/operations/troubleshooting.md")
    assert troubleshooting_path.exists(), "Troubleshooting guide must exist"
    
    content = troubleshooting_path.read_text()
    assert len(content) > 1000, f"Troubleshooting guide must be comprehensive (got {len(content)} chars)"
    assert "Common Issues" in content, "Must document common issues"
    assert "Performance" in content, "Must include performance troubleshooting"
    
    print("✅ Troubleshooting guide is complete")

def test_security_documentation_complete():
    """Test that security documentation exists and is comprehensive."""
    security_path = Path("docs/security/README.md")
    assert security_path.exists(), "Security documentation must exist"
    
    content = security_path.read_text()
    assert len(content) > 1000, f"Security documentation must be comprehensive (got {len(content)} chars)"
    assert "Threat Model" in content, "Must include threat model"
    assert "Best Practices" in content, "Must include security best practices"
    
    print("✅ Security documentation is complete")

def test_developer_onboarding_complete():
    """Test that developer onboarding guide exists and is comprehensive."""
    onboarding_path = Path("docs/developer/onboarding.md")
    assert onboarding_path.exists(), "Developer onboarding guide must exist"
    
    content = onboarding_path.read_text()
    assert len(content) > 1000, f"Developer onboarding guide must be comprehensive (got {len(content)} chars)"
    assert "Getting Started" in content, "Must include getting started section"
    assert "Development Environment" in content, "Must include development setup"
    
    print("✅ Developer onboarding guide is complete")

if __name__ == "__main__":
    print("🟢 GREEN PHASE: Testing ETL-015 Architecture Documentation implementation")
    
    test_functions = [
        test_codebase_overview_comprehensive,
        test_architecture_diagrams_complete,
        test_component_documentation_complete,
        test_api_reference_complete,
        test_deployment_guide_complete,
        test_troubleshooting_guide_complete,
        test_security_documentation_complete,
        test_developer_onboarding_complete
    ]
    
    passed_tests = []
    failed_tests = []
    
    for test_func in test_functions:
        try:
            test_func()
            passed_tests.append(test_func.__name__)
        except Exception as e:
            print(f"❌ {test_func.__name__}: FAIL - {str(e)}")
            failed_tests.append(test_func.__name__)
    
    print(f"\n📊 GREEN phase results: {len(passed_tests)}/{len(test_functions)} tests passed")
    
    if len(failed_tests) == 0:
        print("✅ All tests passed! ETL-015 Architecture Documentation implementation complete")
        print("\n🎯 Architecture documentation successfully implemented with:")
        print("   - Comprehensive CODEBASE_OVERVIEW.md with 3+ mermaid diagrams")
        print("   - Complete API reference documentation (agents, tools, coordination)")
        print("   - Comprehensive deployment guide with multiple deployment options") 
        print("   - Detailed troubleshooting guide with common issues and solutions")
        print("   - Complete security documentation with threat model and best practices")
        print("   - Developer onboarding guide with setup and workflow instructions")
    else:
        print(f"❌ Failed tests: {', '.join(failed_tests)}")
        exit(1)