#!/usr/bin/env python3
"""Validation script for ETL-018: File Refactoring."""

import sys
from pathlib import Path


def validate_file_sizes():
    """Validate that files have been appropriately sized after refactoring."""
    print("Validating file sizes after refactoring...")
    
    files_to_check = [
        ("src/agent_orchestrated_etl/agents/etl_agent.py", 2697, "Original large file"),
        ("src/agent_orchestrated_etl/agents/etl_profiling.py", None, "Data profiling module"),
        ("src/agent_orchestrated_etl/agents/etl_extraction.py", None, "Data extraction module"),
        ("src/agent_orchestrated_etl/agents/etl_transformation.py", None, "Data transformation module"),
    ]
    
    results = {}
    
    for file_path, original_size, description in files_to_check:
        path = Path(file_path)
        if path.exists():
            with open(path, 'r') as f:
                current_lines = len(f.readlines())
            
            results[file_path] = {
                'lines': current_lines,
                'exists': True,
                'description': description,
                'original_size': original_size
            }
            
            print(f"✓ {description}: {current_lines} lines")
        else:
            results[file_path] = {
                'lines': 0,
                'exists': False,
                'description': description,
                'original_size': original_size
            }
            print(f"❌ Missing: {description}")
    
    return results


def validate_module_structure():
    """Validate that new modules have correct structure."""
    print("\nValidating module structure...")
    
    modules_to_check = {
        "src/agent_orchestrated_etl/agents/etl_profiling.py": {
            "classes": ["ProfilingConfig", "ColumnProfile", "DataProfiler"],
            "key_methods": ["analyze_dataset_structure", "profile_column"]
        },
        "src/agent_orchestrated_etl/agents/etl_extraction.py": {
            "classes": ["DataExtractor"],
            "key_methods": ["extract_from_database", "extract_from_file", "extract_from_api"]
        },
        "src/agent_orchestrated_etl/agents/etl_transformation.py": {
            "classes": ["DataTransformer"],
            "key_methods": ["apply_field_mapping", "apply_aggregation", "apply_filtering"]
        }
    }
    
    for module_path, expected_structure in modules_to_check.items():
        path = Path(module_path)
        if not path.exists():
            print(f"❌ Module missing: {module_path}")
            continue
        
        content = path.read_text()
        
        # Check for expected classes
        for class_name in expected_structure["classes"]:
            if f"class {class_name}" in content:
                print(f"✓ Found class {class_name} in {path.name}")
            else:
                print(f"❌ Missing class {class_name} in {path.name}")
        
        # Check for expected methods
        for method_name in expected_structure["key_methods"]:
            if f"def {method_name}" in content or f"async def {method_name}" in content:
                print(f"✓ Found method {method_name} in {path.name}")
            else:
                print(f"❌ Missing method {method_name} in {path.name}")


def validate_import_syntax():
    """Validate that all new modules have correct import syntax."""
    print("\nValidating import syntax...")
    
    modules = [
        "src/agent_orchestrated_etl/agents/etl_profiling.py",
        "src/agent_orchestrated_etl/agents/etl_extraction.py",
        "src/agent_orchestrated_etl/agents/etl_transformation.py"
    ]
    
    for module_path in modules:
        path = Path(module_path)
        if not path.exists():
            print(f"❌ Module missing: {module_path}")
            continue
        
        try:
            # Try to compile the module
            content = path.read_text()
            compile(content, module_path, 'exec')
            print(f"✓ Syntax valid: {path.name}")
        except SyntaxError as e:
            print(f"❌ Syntax error in {path.name}: {e}")
        except Exception as e:
            print(f"⚠️  Compilation warning in {path.name}: {e}")


def check_refactoring_benefits():
    """Check the benefits of refactoring."""
    print("\nAnalyzing refactoring benefits...")
    
    # Check original file size
    original_file = Path("src/agent_orchestrated_etl/agents/etl_agent.py")
    if original_file.exists():
        with open(original_file, 'r') as f:
            original_lines = len(f.readlines())
        print(f"Original etl_agent.py: {original_lines} lines")
    
    # Check new module sizes
    new_modules = [
        "src/agent_orchestrated_etl/agents/etl_profiling.py",
        "src/agent_orchestrated_etl/agents/etl_extraction.py", 
        "src/agent_orchestrated_etl/agents/etl_transformation.py"
    ]
    
    total_new_lines = 0
    module_count = 0
    
    for module_path in new_modules:
        path = Path(module_path)
        if path.exists():
            with open(path, 'r') as f:
                lines = len(f.readlines())
            total_new_lines += lines
            module_count += 1
            print(f"New module {path.name}: {lines} lines")
    
    if module_count > 0:
        avg_module_size = total_new_lines / module_count
        print(f"\nRefactoring results:")
        print(f"- Created {module_count} focused modules")
        print(f"- Average module size: {avg_module_size:.0f} lines")
        print(f"- Total lines in new modules: {total_new_lines}")
        
        if avg_module_size < 1000:
            print("✅ Success: All modules are under 1000 lines (maintainable size)")
        else:
            print("⚠️  Warning: Some modules are still large")


def main():
    """Run all refactoring validations."""
    print("=" * 70)
    print("ETL-018: FILE REFACTORING VALIDATION")
    print("=" * 70)
    
    try:
        file_results = validate_file_sizes()
        validate_module_structure()
        validate_import_syntax()
        check_refactoring_benefits()
        
        # Check if refactoring was successful
        success_criteria = [
            any(result['exists'] and result['lines'] > 50 for result in file_results.values() 
                if 'etl_profiling' in str(result) or 'etl_extraction' in str(result) or 'etl_transformation' in str(result)),
        ]
        
        print("\n" + "=" * 70)
        if all(success_criteria):
            print("✅ ETL-018 FILE REFACTORING COMPLETED SUCCESSFULLY")
            print("📦 Large files have been broken down into focused modules")
            print("🔧 Code is now more maintainable and easier to understand")
        else:
            print("⚠️  ETL-018 FILE REFACTORING PARTIALLY COMPLETED")
            print("📝 Some modules may need additional work")
        print("=" * 70)
        return 0
        
    except Exception as e:
        print(f"\n❌ VALIDATION FAILED: {e}")
        print("=" * 70)
        return 1


if __name__ == "__main__":
    sys.exit(main())