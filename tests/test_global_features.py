"""Tests for global-first features (I18n, multi-region, compliance)."""

from datetime import datetime, timedelta

import pytest

from src.agent_orchestrated_etl.compliance import (
    ComplianceManager,
    ComplianceStandard,
    ConsentRecord,
    ConsentStatus,
    DataClassification,
)
from src.agent_orchestrated_etl.global_deployment import (
    DeploymentStage,
    DeploymentStrategy,
    GlobalDeploymentManager,
)
from src.agent_orchestrated_etl.internationalization import I18nManager, get_text
from src.agent_orchestrated_etl.multi_region import (
    MultiRegionManager,
    RegionConfig,
    RegionMetrics,
)


class TestInternationalization:
    """Test internationalization features."""

    def test_i18n_manager_initialization(self):
        """Test I18n manager initialization."""
        i18n = I18nManager()

        assert i18n.default_locale == "en-US"
        assert i18n.current_locale == "en-US"
        assert len(i18n.supported_locales) >= 3  # en-US, es-ES, fr-FR

    def test_locale_switching(self):
        """Test locale switching functionality."""
        i18n = I18nManager()

        # Test valid locale switch
        assert i18n.set_locale("es-ES") is True
        assert i18n.current_locale == "es-ES"

        # Test invalid locale switch
        assert i18n.set_locale("invalid-locale") is False
        assert i18n.current_locale == "es-ES"  # Should remain unchanged

    def test_text_translation(self):
        """Test text translation functionality."""
        i18n = I18nManager()

        # Test English (default)
        text_en = i18n.get_text("etl.extraction.started")
        assert "extraction started" in text_en.lower()

        # Test Spanish translation
        i18n.set_locale("es-ES")
        text_es = i18n.get_text("etl.extraction.started")
        assert "extracción" in text_es.lower()

        # Test French translation
        i18n.set_locale("fr-FR")
        text_fr = i18n.get_text("etl.extraction.started")
        assert "extraction" in text_fr.lower()

    def test_text_formatting(self):
        """Test text formatting with variables."""
        i18n = I18nManager()

        # Add a test translation with formatting
        i18n.add_translation("en-US", "test.format", "Hello {name}, you have {count} messages")

        formatted = i18n.get_text("test.format", name="John", count=5)
        assert formatted == "Hello John, you have 5 messages"

    def test_fallback_translation(self):
        """Test fallback to default locale and key."""
        i18n = I18nManager()
        i18n.set_locale("es-ES")

        # Test fallback to default locale
        missing_key = "nonexistent.key"
        result = i18n.get_text(missing_key)
        assert result == missing_key  # Should return key if no translation found

    def test_convenience_functions(self):
        """Test convenience functions."""
        # Test global get_text function
        text = get_text("success")
        assert text in ["Success", "Éxito", "Succès"]  # Could be any locale


class TestMultiRegion:
    """Test multi-region functionality."""

    def test_multi_region_manager_initialization(self):
        """Test multi-region manager initialization."""
        manager = MultiRegionManager()

        assert len(manager.regions) >= 6  # Default regions
        assert manager.primary_region == "us-east-1"
        assert len(manager.failover_regions) >= 2

    def test_region_management(self):
        """Test adding and removing regions."""
        manager = MultiRegionManager()

        # Test adding a new region
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

        # Test removing a region
        assert manager.remove_region("test-1") is True
        assert "test-1" not in manager.regions

        # Test removing non-existent region
        assert manager.remove_region("non-existent") is False

    def test_region_selection(self):
        """Test optimal region selection."""
        manager = MultiRegionManager()

        # Test selection with compliance requirements
        optimal = manager.select_optimal_region(compliance_requirements=["GDPR"])
        assert optimal in ["eu-west-1", "eu-central-1"]  # EU regions support GDPR

        # Test selection with data residency
        optimal = manager.select_optimal_region(data_residency=True)
        assert optimal is not None

    def test_region_metrics_update(self):
        """Test region metrics updating."""
        manager = MultiRegionManager()

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
        assert manager.region_metrics["us-east-1"].latency_ms == 100.0

    @pytest.mark.asyncio
    async def test_failover_functionality(self):
        """Test failover functionality."""
        manager = MultiRegionManager()

        # Test failover from non-primary region
        result = await manager.initiate_failover("us-west-2", "Test failover")
        assert result is True

        # Test failover from primary region
        old_primary = manager.primary_region
        result = await manager.initiate_failover(old_primary, "Primary region failure")
        assert result is True
        assert manager.primary_region != old_primary

    def test_region_status_summary(self):
        """Test region status summary."""
        manager = MultiRegionManager()

        summary = manager.get_region_status_summary()

        assert "total_regions" in summary
        assert "active_regions" in summary
        assert "primary_region" in summary
        assert "regions" in summary
        assert summary["total_regions"] > 0


class TestCompliance:
    """Test compliance functionality."""

    def test_compliance_manager_initialization(self):
        """Test compliance manager initialization."""
        manager = ComplianceManager()

        assert len(manager.retention_policies) >= 7  # All data classifications
        assert DataClassification.PII in manager.retention_policies
        assert DataClassification.PHI in manager.retention_policies

    def test_compliance_standard_enablement(self):
        """Test enabling compliance standards."""
        manager = ComplianceManager()

        # Test enabling GDPR
        manager.enable_compliance_standard(ComplianceStandard.GDPR)
        assert ComplianceStandard.GDPR in manager.active_standards

        # Test enabling HIPAA
        manager.enable_compliance_standard(ComplianceStandard.HIPAA)
        assert ComplianceStandard.HIPAA in manager.active_standards

    def test_data_classification(self):
        """Test data classification functionality."""
        manager = ComplianceManager()

        # Test PII classification
        pii_data = {"email": "user@example.com", "name": "John Doe"}
        classification = manager.classify_data(pii_data)
        assert classification == DataClassification.PII

        # Test PHI classification
        phi_data = {"patient_id": "12345", "medical_record": "diagnosis"}
        classification = manager.classify_data(phi_data)
        assert classification == DataClassification.PHI

        # Test PCI classification
        pci_data = {"credit_card": "4111111111111111", "cvv": "123"}
        classification = manager.classify_data(pci_data)
        assert classification == DataClassification.PCI

    def test_consent_management(self):
        """Test consent management functionality."""
        manager = ComplianceManager()

        # Test recording consent
        consent = ConsentRecord(
            user_id="user123",
            consent_id="consent456",
            purpose="marketing",
            status=ConsentStatus.GRANTED,
            granted_at=datetime.utcnow()
        )

        manager.record_consent(consent)
        assert consent.consent_id in manager.consent_records

        # Test checking consent validity
        assert manager.check_consent_validity("user123", "marketing") is True
        assert manager.check_consent_validity("user123", "analytics") is False

        # Test withdrawing consent
        assert manager.withdraw_consent("consent456") is True
        assert manager.check_consent_validity("user123", "marketing") is False

    def test_data_retention_policies(self):
        """Test data retention policies."""
        manager = ComplianceManager()

        # Test retention policy retrieval
        policy = manager.get_data_retention_policy(DataClassification.PII)
        assert policy is not None
        assert policy.encryption_required is True

        # Test deletion decision
        old_date = datetime.utcnow() - timedelta(days=1000)
        should_delete = manager.should_delete_data(old_date, DataClassification.PII)
        assert should_delete is True

        recent_date = datetime.utcnow() - timedelta(days=1)
        should_delete = manager.should_delete_data(recent_date, DataClassification.PII)
        assert should_delete is False

    def test_data_anonymization(self):
        """Test data anonymization."""
        manager = ComplianceManager()

        sensitive_data = {
            "email": "user@example.com",
            "name": "John Doe",
            "phone": "555-1234",
            "regular_field": "normal_value"
        }

        anonymized = manager.anonymize_data(sensitive_data, DataClassification.PII)

        # Check that sensitive fields are anonymized
        assert "anon_" in anonymized["email"]
        assert anonymized["regular_field"] == "normal_value"  # Non-sensitive fields unchanged

    def test_compliance_validation(self):
        """Test compliance validation."""
        manager = ComplianceManager()
        manager.enable_compliance_standard(ComplianceStandard.GDPR)

        validation = manager.validate_compliance_status()

        assert "overall_status" in validation
        assert "active_standards" in validation
        assert validation["overall_status"] in ["compliant", "partially_compliant", "non_compliant"]

    def test_compliance_reporting(self):
        """Test compliance reporting."""
        manager = ComplianceManager()
        manager.enable_compliance_standard(ComplianceStandard.GDPR)

        start_date = datetime.utcnow() - timedelta(days=30)
        end_date = datetime.utcnow()

        report = manager.generate_compliance_report(ComplianceStandard.GDPR, start_date, end_date)

        assert "standard" in report
        assert report["standard"] == "gdpr"
        assert "report_period" in report
        assert "summary" in report


class TestGlobalDeployment:
    """Test global deployment functionality."""

    @pytest.mark.asyncio
    async def test_global_deployment_manager_initialization(self):
        """Test global deployment manager initialization."""
        manager = GlobalDeploymentManager()

        assert manager.global_config is not None
        assert "deployment" in manager.global_config
        assert "compliance" in manager.global_config

    @pytest.mark.asyncio
    async def test_deployment_configuration_creation(self):
        """Test creating deployment configurations."""
        manager = GlobalDeploymentManager()

        config = await manager.create_global_deployment(
            deployment_id="test-deployment",
            stage=DeploymentStage.STAGING,
            target_regions=["us-east-1", "eu-west-1"],
            strategy=DeploymentStrategy.ROLLING,
            compliance_standards=[ComplianceStandard.GDPR, ComplianceStandard.SOC2]
        )

        assert config.deployment_id == "test-deployment"
        assert config.stage == DeploymentStage.STAGING
        assert len(config.regions) == 2
        assert ComplianceStandard.GDPR in config.compliance_standards

    @pytest.mark.asyncio
    async def test_rolling_deployment(self):
        """Test rolling deployment strategy."""
        manager = GlobalDeploymentManager()

        config = await manager.create_global_deployment(
            deployment_id="rolling-test",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["us-east-1", "us-west-2"],
            strategy=DeploymentStrategy.ROLLING
        )

        # This would normally deploy to actual infrastructure
        # For testing, we just verify the deployment process completes
        success = await manager.deploy_globally("rolling-test")
        assert success is True

    @pytest.mark.asyncio
    async def test_blue_green_deployment(self):
        """Test blue-green deployment strategy."""
        manager = GlobalDeploymentManager()

        config = await manager.create_global_deployment(
            deployment_id="blue-green-test",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["us-east-1"],
            strategy=DeploymentStrategy.BLUE_GREEN
        )

        success = await manager.deploy_globally("blue-green-test")
        assert success is True

    @pytest.mark.asyncio
    async def test_canary_deployment(self):
        """Test canary deployment strategy."""
        manager = GlobalDeploymentManager()

        config = await manager.create_global_deployment(
            deployment_id="canary-test",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["us-east-1", "us-west-2", "eu-west-1"],
            strategy=DeploymentStrategy.CANARY
        )

        success = await manager.deploy_globally("canary-test")
        assert success is True

    def test_deployment_status(self):
        """Test deployment status reporting."""
        manager = GlobalDeploymentManager()

        status = manager.get_deployment_status()

        assert "regions" in status
        assert "compliance_status" in status
        assert "supported_locales" in status
        assert "global_config" in status
        assert "timestamp" in status


class TestIntegration:
    """Test integration between global features."""

    @pytest.mark.asyncio
    async def test_compliance_and_multi_region_integration(self):
        """Test integration between compliance and multi-region features."""
        multi_region = MultiRegionManager()
        compliance = ComplianceManager()

        # Enable GDPR compliance
        compliance.enable_compliance_standard(ComplianceStandard.GDPR)

        # Find GDPR-compliant regions
        gdpr_regions = multi_region.get_regions_by_compliance("GDPR")
        assert len(gdpr_regions) >= 2  # Should have EU regions

        # Ensure all GDPR regions require data residency
        for region in gdpr_regions:
            assert region.data_residency_required is True

    @pytest.mark.asyncio
    async def test_i18n_and_deployment_integration(self):
        """Test integration between I18n and deployment features."""
        deployment = GlobalDeploymentManager()
        i18n = I18nManager()

        # Create deployment with EU regions
        config = await deployment.create_global_deployment(
            deployment_id="i18n-test",
            stage=DeploymentStage.DEVELOPMENT,
            target_regions=["eu-west-1", "eu-central-1"]
        )

        # Deploy and verify I18n configuration
        success = await deployment.deploy_globally("i18n-test")
        assert success is True

        # Verify supported locales include European locales
        supported_locales = i18n.get_supported_locales()
        assert any(locale.startswith("en") for locale in supported_locales)  # English
        assert any(locale.startswith("fr") for locale in supported_locales)  # French
        assert any(locale.startswith("de") for locale in supported_locales)  # German (if added)

    def test_full_global_configuration(self):
        """Test full global configuration with all features."""
        # Initialize all managers
        multi_region = MultiRegionManager()
        compliance = ComplianceManager()
        i18n = I18nManager()
        deployment = GlobalDeploymentManager()

        # Enable multiple compliance standards
        compliance.enable_compliance_standard(ComplianceStandard.GDPR)
        compliance.enable_compliance_standard(ComplianceStandard.SOC2)

        # Verify system status
        region_status = multi_region.get_region_status_summary()
        compliance_status = compliance.validate_compliance_status()
        deployment_status = deployment.get_deployment_status()

        assert region_status["active_regions"] > 0
        assert len(compliance_status["active_standards"]) >= 2
        assert len(i18n.get_supported_locales()) >= 3
        assert deployment_status["global_config"] is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
