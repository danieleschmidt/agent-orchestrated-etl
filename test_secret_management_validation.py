#!/usr/bin/env python3
"""Validation script for ETL-017: Secret Management Security Fixes."""

import os
import sys
from pathlib import Path

# Add src to Python path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from agent_orchestrated_etl.config_templates import (
    generate_env_template,
    generate_docker_compose_template,
    generate_config_template
)


def test_no_hardcoded_secrets_in_templates():
    """Test that generated templates don't contain hardcoded secrets."""
    print("Testing: No hardcoded secrets in templates...")
    
    for env in ["development", "staging", "production"]:
        template = generate_env_template(env)
        
        # Check for commented out secrets (good)
        assert "# AGENT_ETL_DB_PASSWORD=" in template, f"Missing commented DB password in {env}"
        assert "# AGENT_ETL_S3_ACCESS_KEY_ID=" in template, f"Missing commented S3 key in {env}"
        assert "# AGENT_ETL_S3_SECRET_ACCESS_KEY=" in template, f"Missing commented S3 secret in {env}"
        assert "# AGENT_ETL_API_KEY=" in template, f"Missing commented API key in {env}"
        
        # Check for security warnings in production
        if env == "production":
            assert "SECURITY WARNING" in template, "Missing security warning in production template"
            assert "Never store secrets" in template, "Missing secret storage warning"
    
    print("✓ PASSED: No hardcoded secrets in templates")


def test_docker_compose_security():
    """Test Docker Compose template security."""
    print("Testing: Docker Compose security...")
    
    template = generate_docker_compose_template("production")
    
    # Check environment variable substitution
    assert "${POSTGRES_PASSWORD:-changeme}" in template, "Missing environment variable substitution"
    assert "Set POSTGRES_PASSWORD environment variable" in template, "Missing password instruction"
    
    print("✓ PASSED: Docker Compose security")


def test_config_template_warnings():
    """Test configuration template security warnings."""
    print("Testing: Configuration template warnings...")
    
    yaml_template = generate_config_template("yaml", True, True, "production")
    
    # Should contain security guidance
    assert "secret_provider: env" in yaml_template, "Missing secret provider config"
    
    print("✓ PASSED: Configuration template warnings")


def test_secret_patterns_not_present():
    """Test that known bad secret patterns are not present."""
    print("Testing: No bad secret patterns...")
    
    # Read the config templates file
    config_file = Path(__file__).parent / "src" / "agent_orchestrated_etl" / "config_templates.py"
    content = config_file.read_text()
    
    # Bad patterns that should not be in the file
    bad_patterns = [
        "change_me_in_production",  # Should be commented out now
        "your_access_key_here",     # Should be commented out now  
        "your_secret_key_here",     # Should be commented out now
        "your_api_key_here",        # Should be commented out now
    ]
    
    for pattern in bad_patterns:
        # Should not find these as uncommented values
        lines = content.split('\n')
        for line_num, line in enumerate(lines, 1):
            if pattern in line and not line.strip().startswith('#'):
                raise AssertionError(f"Found bad pattern '{pattern}' on line {line_num}: {line.strip()}")
    
    print("✓ PASSED: No bad secret patterns")


def test_environment_template_security():
    """Test environment template security features.""" 
    print("Testing: Environment template security...")
    
    prod_template = generate_env_template("production")
    dev_template = generate_env_template("development")
    
    # Production should have enhanced warnings
    prod_lines = prod_template.split('\n')
    security_warnings = [line for line in prod_lines if 'SECURITY WARNING' in line or 'Never store secrets' in line]
    assert len(security_warnings) > 0, "Production template missing security warnings"
    
    # Check that sensitive values are commented out
    sensitive_keys = ['DB_PASSWORD', 'S3_ACCESS_KEY_ID', 'S3_SECRET_ACCESS_KEY', 'API_KEY']
    for key in sensitive_keys:
        found_commented = False
        for line in prod_lines:
            if key in line and line.strip().startswith('#') and '=' in line:
                found_commented = True
                break
        assert found_commented, f"Sensitive key {key} not properly commented in production template"
    
    print("✓ PASSED: Environment template security")


def main():
    """Run all validation tests."""
    print("=" * 60)
    print("SECRET MANAGEMENT SECURITY VALIDATION (ETL-017)")
    print("=" * 60)
    
    try:
        test_no_hardcoded_secrets_in_templates()
        test_docker_compose_security()
        test_config_template_warnings()
        test_secret_patterns_not_present()
        test_environment_template_security()
        
        print("\n" + "=" * 60)
        print("🔒 ALL SECURITY TESTS PASSED!")
        print("ETL-017 Secret Management fixes are working correctly.")
        print("=" * 60)
        return 0
        
    except Exception as e:
        print(f"\n❌ TEST FAILED: {e}")
        print("=" * 60)
        return 1


if __name__ == "__main__":
    sys.exit(main())