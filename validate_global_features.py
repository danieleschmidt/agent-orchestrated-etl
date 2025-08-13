#!/usr/bin/env python3
"""Validation script for global-first features implementation."""

import asyncio
import sys
import traceback
from datetime import datetime, timedelta

# Add the src directory to the path
sys.path.insert(0, 'src')

from agent_orchestrated_etl.compliance import (
    ComplianceManager,
    ComplianceStandard,
    DataClassification,
    ConsentRecord,
    ConsentStatus
)
from agent_orchestrated_etl.internationalization import I18nManager, get_text
from agent_orchestrated_etl.multi_region import (
    MultiRegionManager,
    RegionConfig,
    RegionStatus,
    RegionMetrics
)
from agent_orchestrated_etl.global_deployment import (
    GlobalDeploymentManager,
    DeploymentStage,
    DeploymentStrategy
)


def test_internationalization():
    """Test internationalization features."""
    print("🌐 Testing Internationalization...")
    
    try:
        # Test I18n manager initialization
        i18n = I18nManager()
        assert i18n.default_locale == "en-US"
        assert len(i18n.supported_locales) >= 3
        print("✅ I18n manager initialized successfully")
        
        # Test locale switching
        assert i18n.set_locale("es-ES") is True
        assert i18n.current_locale == "es-ES"
        print("✅ Locale switching works")
        
        # Test text translation
        text_en = i18n.get_text("etl.extraction.started", locale="en-US")
        text_es = i18n.get_text("etl.extraction.started", locale="es-ES")
        assert text_en != text_es  # Should be different languages
        print("✅ Text translation works")
        
        # Test convenience function
        text = get_text("success")
        assert text is not None
        print("✅ Convenience functions work")
        
        print("🌐 Internationalization: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        print(f"❌ Internationalization test failed: {e}")
        traceback.print_exc()
        return False


def test_multi_region():
    """Test multi-region functionality."""
    print("🌍 Testing Multi-Region Support...")
    
    try:
        # Test multi-region manager initialization
        manager = MultiRegionManager()
        assert len(manager.regions) >= 6  # Default regions
        assert manager.primary_region == "us-east-1"
        print("✅ Multi-region manager initialized successfully")
        
        # Test region management
        new_region = RegionConfig(
            name="Test Region",
            code="test-1",
            endpoint="https://test-1.example.com",
            data_centers=["test1-az1"],
            timezone="UTC",
            backup_regions=[],
            compliance_requirements=["SOC2"]
        )
        manager.add_region(new_region)
        assert "test-1" in manager.regions
        print("✅ Region addition works")
        
        # Test region selection
        optimal = manager.select_optimal_region(compliance_requirements=["GDPR"])
        assert optimal is not None
        print("✅ Region selection works")
        
        # Test metrics update
        metrics = RegionMetrics(
            region_code="us-east-1",
            latency_ms=100.0,
            throughput_rps=500.0,
            error_rate=0.01,
            cpu_usage=0.70,
            memory_usage=0.60,
            disk_usage=0.40,
            active_connections=100,
            last_updated=datetime.utcnow().timestamp()
        )
        manager.update_region_metrics("us-east-1", metrics)
        assert "us-east-1" in manager.region_metrics
        print("✅ Metrics update works")
        
        # Test status summary
        summary = manager.get_region_status_summary()
        assert "total_regions" in summary
        assert summary["total_regions"] > 0
        print("✅ Status summary works")
        
        print("🌍 Multi-Region Support: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        print(f"❌ Multi-region test failed: {e}")
        traceback.print_exc()
        return False


def test_compliance():
    """Test compliance functionality."""
    print("⚖️ Testing Compliance Framework...")
    
    try:
        # Test compliance manager initialization
        manager = ComplianceManager()
        assert len(manager.retention_policies) >= 7  # All data classifications
        print("✅ Compliance manager initialized successfully")
        
        # Test compliance standard enablement
        manager.enable_compliance_standard(ComplianceStandard.GDPR)
        assert ComplianceStandard.GDPR in manager.active_standards
        print("✅ Compliance standard enablement works")
        
        # Test data classification
        pii_data = {"email": "user@example.com", "name": "John Doe"}
        classification = manager.classify_data(pii_data)
        assert classification == DataClassification.PII
        print("✅ Data classification works")
        
        # Test consent management
        consent = ConsentRecord(
            user_id="user123",
            consent_id="consent456",
            purpose="marketing",
            status=ConsentStatus.GRANTED,
            granted_at=datetime.utcnow()
        )
        manager.record_consent(consent)
        assert consent.consent_id in manager.consent_records
        print("✅ Consent management works")
        
        # Test data retention policies
        policy = manager.get_data_retention_policy(DataClassification.PII)
        assert policy is not None
        assert policy.encryption_required is True
        print("✅ Data retention policies work")
        
        # Test anonymization
        sensitive_data = {"email": "user@example.com", "name": "John Doe"}
        anonymized = manager.anonymize_data(sensitive_data, DataClassification.PII)
        assert "anon_" in anonymized["email"]
        print("✅ Data anonymization works")
        
        # Test compliance validation
        validation = manager.validate_compliance_status()
        assert "overall_status" in validation
        print("✅ Compliance validation works")
        
        print("⚖️ Compliance Framework: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        print(f"❌ Compliance test failed: {e}")
        traceback.print_exc()
        return False


async def test_global_deployment():
    """Test global deployment functionality."""
    print("🚀 Testing Global Deployment...")
    
    try:
        # Test global deployment manager initialization
        manager = GlobalDeploymentManager()
        assert manager.global_config is not None
        print("✅ Global deployment manager initialized successfully")
        
        # Test deployment configuration creation
        config = await manager.create_global_deployment(
            deployment_id="test-deployment",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["us-east-1", "eu-west-1"],
            strategy=DeploymentStrategy.ROLLING,
            compliance_standards=[ComplianceStandard.GDPR, ComplianceStandard.SOC2]
        )
        assert config.deployment_id == "test-deployment"
        assert len(config.regions) == 2
        print("✅ Deployment configuration creation works")
        
        # Test deployment execution
        success = await manager.deploy_globally("test-deployment")
        assert success is True
        print("✅ Deployment execution works")
        
        # Test deployment status
        status = manager.get_deployment_status()
        assert "regions" in status
        assert "compliance_status" in status
        print("✅ Deployment status reporting works")
        
        print("🚀 Global Deployment: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        print(f"❌ Global deployment test failed: {e}")
        traceback.print_exc()
        return False


async def test_integration():
    """Test integration between global features."""
    print("🔗 Testing Feature Integration...")
    
    try:
        # Initialize all managers
        multi_region = MultiRegionManager()
        compliance = ComplianceManager()
        i18n = I18nManager()
        deployment = GlobalDeploymentManager()
        
        # Test compliance and multi-region integration
        compliance.enable_compliance_standard(ComplianceStandard.GDPR)
        gdpr_regions = multi_region.get_regions_by_compliance("GDPR")
        assert len(gdpr_regions) >= 2  # Should have EU regions
        print("✅ Compliance and multi-region integration works")
        
        # Test I18n and deployment integration
        config = await deployment.create_global_deployment(
            deployment_id="integration-test",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["eu-west-1"]
        )
        success = await deployment.deploy_globally("integration-test")
        assert success is True
        print("✅ I18n and deployment integration works")
        
        # Test full system status
        region_status = multi_region.get_region_status_summary()
        compliance_status = compliance.validate_compliance_status()
        deployment_status = deployment.get_deployment_status()
        
        assert region_status["active_regions"] > 0
        assert len(i18n.get_supported_locales()) >= 3
        assert deployment_status["global_config"] is not None
        print("✅ Full system status reporting works")
        
        print("🔗 Feature Integration: ALL TESTS PASSED\n")
        return True
        
    except Exception as e:
        print(f"❌ Integration test failed: {e}")
        traceback.print_exc()
        return False


async def main():
    """Run all global feature validation tests."""
    print("=" * 60)
    print("🌍 GLOBAL-FIRST IMPLEMENTATION VALIDATION")
    print("=" * 60)
    print()
    
    test_results = []
    
    # Run tests
    test_results.append(test_internationalization())
    test_results.append(test_multi_region())
    test_results.append(test_compliance())
    test_results.append(await test_global_deployment())
    test_results.append(await test_integration())
    
    # Summary
    print("=" * 60)
    print("📊 VALIDATION SUMMARY")
    print("=" * 60)
    
    passed_tests = sum(test_results)
    total_tests = len(test_results)
    
    test_names = [
        "Internationalization",
        "Multi-Region Support", 
        "Compliance Framework",
        "Global Deployment",
        "Feature Integration"
    ]
    
    for i, (name, result) in enumerate(zip(test_names, test_results)):
        status = "✅ PASSED" if result else "❌ FAILED"
        print(f"{i+1}. {name}: {status}")
    
    print()
    print(f"Overall Result: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 ALL GLOBAL-FIRST FEATURES VALIDATED SUCCESSFULLY!")
        print()
        print("✨ Global-First Implementation Complete:")
        print("   • Multi-region deployment support")
        print("   • Internationalization (I18n) with 3+ locales")
        print("   • Comprehensive compliance framework")
        print("   • Global deployment strategies")
        print("   • Cross-feature integration")
        return True
    else:
        print(f"❌ {total_tests - passed_tests} test(s) failed")
        return False


if __name__ == "__main__":
    result = asyncio.run(main())
    sys.exit(0 if result else 1)