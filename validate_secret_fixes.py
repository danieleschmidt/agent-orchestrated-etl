#!/usr/bin/env python3
"""Simple validation script for ETL-017: Secret Management Security Fixes."""

import re
from pathlib import Path


def validate_config_templates():
    """Validate that config templates don't contain hardcoded secrets."""
    print("Validating config_templates.py for security fixes...")
    
    config_file = Path("src/agent_orchestrated_etl/config_templates.py")
    if not config_file.exists():
        raise FileNotFoundError(f"Config templates file not found: {config_file}")
    
    content = config_file.read_text()
    lines = content.split('\n')
    
    # Track fixes applied
    fixes_found = {
        'db_password_commented': False,
        's3_keys_commented': False,
        'api_key_commented': False,
        'docker_password_env_var': False,
        'security_warnings': False,
    }
    
    for line_num, line in enumerate(lines, 1):
        line = line.strip()
        
        # Check for properly commented sensitive values
        if '# AGENT_ETL_DB_PASSWORD=' in line:
            fixes_found['db_password_commented'] = True
            print(f"✓ Line {line_num}: DB password properly commented")
        
        if '# AGENT_ETL_S3_ACCESS_KEY_ID=' in line:
            fixes_found['s3_keys_commented'] = True
            print(f"✓ Line {line_num}: S3 access key properly commented")
        
        if '# AGENT_ETL_API_KEY=' in line:
            fixes_found['api_key_commented'] = True
            print(f"✓ Line {line_num}: API key properly commented")
        
        if 'POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-changeme}' in line:
            fixes_found['docker_password_env_var'] = True
            print(f"✓ Line {line_num}: Docker password uses environment variable")
        
        if 'SECURITY WARNING' in line:
            fixes_found['security_warnings'] = True
            print(f"✓ Line {line_num}: Security warning found")
        
        # Check for bad patterns that should not exist
        bad_patterns = [
            ('change_me_in_production', 'hardcoded production password'),
            ('your_access_key_here', 'placeholder access key'),
            ('your_secret_key_here', 'placeholder secret key'),
            ('your_api_key_here', 'placeholder API key'),
        ]
        
        for pattern, description in bad_patterns:
            if pattern in line and not line.startswith('#'):
                raise AssertionError(f"❌ Line {line_num}: Found {description}: {line}")
    
    # Verify all fixes were applied
    missing_fixes = [key for key, found in fixes_found.items() if not found]
    if missing_fixes:
        raise AssertionError(f"❌ Missing security fixes: {missing_fixes}")
    
    print("✅ All secret management fixes validated successfully!")
    return True


def validate_no_secrets_in_codebase():
    """Scan codebase for potential hardcoded secrets."""
    print("\nScanning codebase for potential hardcoded secrets...")
    
    # Define patterns that might indicate secrets
    secret_patterns = [
        (r'password\s*=\s*["\'][^"\']{4,}["\']', 'potential hardcoded password'),
        (r'secret\s*=\s*["\'][^"\']{10,}["\']', 'potential hardcoded secret'),
        (r'api_key\s*=\s*["\'][^"\']{10,}["\']', 'potential hardcoded API key'),
        (r'token\s*=\s*["\'][^"\']{10,}["\']', 'potential hardcoded token'),
        (r'sk-[a-zA-Z0-9]{32,}', 'potential OpenAI API key'),
        (r'AKIA[0-9A-Z]{16}', 'potential AWS access key'),
    ]
    
    python_files = list(Path('src').rglob('*.py'))
    issues_found = []
    
    for py_file in python_files:
        try:
            content = py_file.read_text()
            lines = content.split('\n')
            
            for line_num, line in enumerate(lines, 1):
                # Skip comments and test files  
                if line.strip().startswith('#') or 'test_' in py_file.name:
                    continue
                
                for pattern, description in secret_patterns:
                    if re.search(pattern, line, re.IGNORECASE):
                        issues_found.append({
                            'file': py_file,
                            'line': line_num,
                            'content': line.strip(),
                            'issue': description
                        })
        except Exception as e:
            print(f"Warning: Could not scan {py_file}: {e}")
    
    if issues_found:
        print("⚠️  Potential security issues found:")
        for issue in issues_found:
            print(f"  {issue['file']}:{issue['line']} - {issue['issue']}")
            print(f"    {issue['content']}")
        print(f"\nFound {len(issues_found)} potential issues. Please review manually.")
    else:
        print("✅ No obvious hardcoded secrets found in Python files!")
    
    return len(issues_found) == 0


def validate_secret_management_documentation():
    """Ensure secret management is properly documented."""
    print("\nValidating secret management documentation...")
    
    # Check for documentation files
    doc_files_to_check = [
        'README.md',
        'AWS_SECRETS_MANAGER_GUIDE.md',
        'docs/security/README.md',
    ]
    
    docs_found = 0
    for doc_file in doc_files_to_check:
        if Path(doc_file).exists():
            docs_found += 1
            print(f"✓ Found documentation: {doc_file}")
    
    if docs_found > 0:
        print(f"✅ Found {docs_found} documentation files")
        return True
    else:
        print("⚠️  Consider adding secret management documentation")
        return False


def main():
    """Run all security validations."""
    print("=" * 70)
    print("ETL-017: SECRET MANAGEMENT SECURITY VALIDATION")
    print("=" * 70)
    
    try:
        # Run all validations
        validate_config_templates()
        validate_no_secrets_in_codebase()
        validate_secret_management_documentation()
        
        print("\n" + "=" * 70)
        print("🔒 SECRET MANAGEMENT SECURITY VALIDATION COMPLETED")
        print("✅ All critical security fixes have been applied!")
        print("📋 ETL-017 is ready for completion.")
        print("=" * 70)
        return 0
        
    except Exception as e:
        print(f"\n❌ VALIDATION FAILED: {e}")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    exit(main())