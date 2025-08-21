"""Compliance and regulatory framework for Agent-Orchestrated ETL."""

from __future__ import annotations

import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set

from .logging_config import get_audit_logger, get_logger


class ComplianceStandard(Enum):
    """Supported compliance standards."""
    GDPR = "gdpr"  # General Data Protection Regulation (EU)
    CCPA = "ccpa"  # California Consumer Privacy Act (US)
    HIPAA = "hipaa"  # Health Insurance Portability and Accountability Act (US)
    SOC2 = "soc2"  # Service Organization Control 2
    ISO27001 = "iso27001"  # ISO/IEC 27001 Information Security Management
    PCI_DSS = "pci_dss"  # Payment Card Industry Data Security Standard
    PDPA = "pdpa"  # Personal Data Protection Act (Singapore)
    LGPD = "lgpd"  # Lei Geral de Proteção de Dados (Brazil)


class DataClassification(Enum):
    """Data classification levels."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PII = "pii"  # Personally Identifiable Information
    PHI = "phi"  # Protected Health Information
    PCI = "pci"  # Payment Card Information


class ConsentStatus(Enum):
    """User consent status."""
    GRANTED = "granted"
    DENIED = "denied"
    WITHDRAWN = "withdrawn"
    PENDING = "pending"
    EXPIRED = "expired"


@dataclass
class DataRetentionPolicy:
    """Data retention policy configuration."""
    classification: DataClassification
    retention_period_days: int
    auto_delete: bool = True
    encryption_required: bool = True
    backup_retention_days: Optional[int] = None
    legal_hold_exempt: bool = False


@dataclass
class ConsentRecord:
    """User consent record."""
    user_id: str
    consent_id: str
    purpose: str
    status: ConsentStatus
    granted_at: Optional[datetime] = None
    withdrawn_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    lawful_basis: Optional[str] = None
    data_categories: List[str] = field(default_factory=list)
    processing_purposes: List[str] = field(default_factory=list)


@dataclass
class DataProcessingRecord:
    """Data processing activity record."""
    record_id: str
    activity_name: str
    purpose: str
    data_categories: List[str]
    data_subjects: List[str]
    recipients: List[str]
    third_country_transfers: List[str]
    retention_period: str
    security_measures: List[str]
    lawful_basis: str
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class AuditEvent:
    """Audit event record."""
    event_id: str
    event_type: str
    user_id: Optional[str]
    resource: str
    action: str
    outcome: str
    timestamp: datetime
    source_ip: Optional[str] = None
    user_agent: Optional[str] = None
    additional_data: Dict[str, Any] = field(default_factory=dict)


class ComplianceManager:
    """Manages compliance and regulatory requirements."""

    def __init__(self):
        """Initialize compliance manager."""
        self.logger = get_logger("agent_etl.compliance")
        self.audit_logger = get_audit_logger()

        # Data retention policies
        self.retention_policies: Dict[DataClassification, DataRetentionPolicy] = {}

        # Consent management
        self.consent_records: Dict[str, ConsentRecord] = {}

        # Data processing records
        self.processing_records: Dict[str, DataProcessingRecord] = {}

        # Audit events
        self.audit_events: List[AuditEvent] = []

        # Active compliance standards
        self.active_standards: Set[ComplianceStandard] = set()

        # Initialize default policies
        self._initialize_default_policies()

    def _initialize_default_policies(self) -> None:
        """Initialize default compliance policies."""
        # Default data retention policies
        default_policies = {
            DataClassification.PUBLIC: DataRetentionPolicy(
                classification=DataClassification.PUBLIC,
                retention_period_days=2555,  # 7 years
                auto_delete=False,
                encryption_required=False
            ),
            DataClassification.INTERNAL: DataRetentionPolicy(
                classification=DataClassification.INTERNAL,
                retention_period_days=1825,  # 5 years
                auto_delete=True,
                encryption_required=True
            ),
            DataClassification.CONFIDENTIAL: DataRetentionPolicy(
                classification=DataClassification.CONFIDENTIAL,
                retention_period_days=1095,  # 3 years
                auto_delete=True,
                encryption_required=True,
                backup_retention_days=30
            ),
            DataClassification.RESTRICTED: DataRetentionPolicy(
                classification=DataClassification.RESTRICTED,
                retention_period_days=365,  # 1 year
                auto_delete=True,
                encryption_required=True,
                backup_retention_days=7
            ),
            DataClassification.PII: DataRetentionPolicy(
                classification=DataClassification.PII,
                retention_period_days=730,  # 2 years (GDPR default)
                auto_delete=True,
                encryption_required=True,
                backup_retention_days=30
            ),
            DataClassification.PHI: DataRetentionPolicy(
                classification=DataClassification.PHI,
                retention_period_days=2190,  # 6 years (HIPAA requirement)
                auto_delete=True,
                encryption_required=True,
                backup_retention_days=90,
                legal_hold_exempt=True
            ),
            DataClassification.PCI: DataRetentionPolicy(
                classification=DataClassification.PCI,
                retention_period_days=365,  # 1 year maximum (PCI-DSS)
                auto_delete=True,
                encryption_required=True,
                backup_retention_days=30
            )
        }

        for classification, policy in default_policies.items():
            self.retention_policies[classification] = policy

        self.logger.info("Initialized default compliance policies")

    def enable_compliance_standard(self, standard: ComplianceStandard) -> None:
        """Enable a compliance standard.
        
        Args:
            standard: Compliance standard to enable
        """
        self.active_standards.add(standard)

        # Apply standard-specific configurations
        if standard == ComplianceStandard.GDPR:
            self._configure_gdpr_compliance()
        elif standard == ComplianceStandard.HIPAA:
            self._configure_hipaa_compliance()
        elif standard == ComplianceStandard.PCI_DSS:
            self._configure_pci_compliance()
        elif standard == ComplianceStandard.CCPA:
            self._configure_ccpa_compliance()

        self.logger.info(f"Enabled compliance standard: {standard.value}")
        self.audit_compliance_event("compliance_standard_enabled", {"standard": standard.value})

    def _configure_gdpr_compliance(self) -> None:
        """Configure GDPR-specific compliance settings."""
        # Adjust PII retention to GDPR requirements
        if DataClassification.PII in self.retention_policies:
            policy = self.retention_policies[DataClassification.PII]
            policy.retention_period_days = min(policy.retention_period_days, 730)  # 2 years max
            policy.auto_delete = True
            policy.encryption_required = True

        self.logger.info("Applied GDPR compliance configuration")

    def _configure_hipaa_compliance(self) -> None:
        """Configure HIPAA-specific compliance settings."""
        # Ensure PHI has proper retention and security
        if DataClassification.PHI in self.retention_policies:
            policy = self.retention_policies[DataClassification.PHI]
            policy.retention_period_days = 2190  # 6 years minimum
            policy.encryption_required = True
            policy.backup_retention_days = 90

        self.logger.info("Applied HIPAA compliance configuration")

    def _configure_pci_compliance(self) -> None:
        """Configure PCI-DSS specific compliance settings."""
        # Strict PCI data retention
        if DataClassification.PCI in self.retention_policies:
            policy = self.retention_policies[DataClassification.PCI]
            policy.retention_period_days = 365  # 1 year maximum
            policy.auto_delete = True
            policy.encryption_required = True

        self.logger.info("Applied PCI-DSS compliance configuration")

    def _configure_ccpa_compliance(self) -> None:
        """Configure CCPA-specific compliance settings."""
        # Similar to GDPR but with California-specific requirements
        if DataClassification.PII in self.retention_policies:
            policy = self.retention_policies[DataClassification.PII]
            policy.auto_delete = True
            policy.encryption_required = True

        self.logger.info("Applied CCPA compliance configuration")

    def classify_data(self, data: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> DataClassification:
        """Classify data based on content and context.
        
        Args:
            data: Data to classify
            context: Additional context for classification
            
        Returns:
            Data classification level
        """
        # Analyze data content for sensitive patterns
        data_str = json.dumps(data, default=str).lower()

        # Check for PHI indicators
        phi_patterns = ['ssn', 'social_security', 'medical', 'health', 'diagnosis', 'treatment', 'patient']
        if any(pattern in data_str for pattern in phi_patterns):
            return DataClassification.PHI

        # Check for PCI indicators
        pci_patterns = ['credit_card', 'card_number', 'cvv', 'expiry', 'cardholder']
        if any(pattern in data_str for pattern in pci_patterns):
            return DataClassification.PCI

        # Check for PII indicators
        pii_patterns = ['email', 'phone', 'address', 'name', 'birthday', 'birth_date', 'personal']
        if any(pattern in data_str for pattern in pii_patterns):
            return DataClassification.PII

        # Check context for classification hints
        if context:
            classification_hint = context.get('classification')
            if classification_hint:
                try:
                    return DataClassification(classification_hint.lower())
                except ValueError:
                    pass

        # Default to internal if no specific classification found
        return DataClassification.INTERNAL

    def record_consent(self, consent: ConsentRecord) -> None:
        """Record user consent.
        
        Args:
            consent: Consent record to store
        """
        self.consent_records[consent.consent_id] = consent

        self.audit_compliance_event(
            "consent_recorded",
            {
                "user_id": consent.user_id,
                "consent_id": consent.consent_id,
                "purpose": consent.purpose,
                "status": consent.status.value
            }
        )

        self.logger.info(f"Recorded consent: {consent.consent_id} for user {consent.user_id}")

    def withdraw_consent(self, consent_id: str, reason: Optional[str] = None) -> bool:
        """Withdraw user consent.
        
        Args:
            consent_id: Consent ID to withdraw
            reason: Optional reason for withdrawal
            
        Returns:
            True if consent was withdrawn successfully
        """
        if consent_id not in self.consent_records:
            return False

        consent = self.consent_records[consent_id]
        consent.status = ConsentStatus.WITHDRAWN
        consent.withdrawn_at = datetime.utcnow()

        self.audit_compliance_event(
            "consent_withdrawn",
            {
                "user_id": consent.user_id,
                "consent_id": consent_id,
                "reason": reason
            }
        )

        self.logger.info(f"Withdrawn consent: {consent_id}")
        return True

    def check_consent_validity(self, user_id: str, purpose: str) -> bool:
        """Check if valid consent exists for a purpose.
        
        Args:
            user_id: User ID to check
            purpose: Processing purpose
            
        Returns:
            True if valid consent exists
        """
        current_time = datetime.utcnow()

        for consent in self.consent_records.values():
            if (consent.user_id == user_id and
                consent.purpose == purpose and
                consent.status == ConsentStatus.GRANTED and
                (consent.expires_at is None or consent.expires_at > current_time)):
                return True

        return False

    def record_data_processing(self, record: DataProcessingRecord) -> None:
        """Record data processing activity.
        
        Args:
            record: Data processing record
        """
        self.processing_records[record.record_id] = record

        self.audit_compliance_event(
            "data_processing_recorded",
            {
                "record_id": record.record_id,
                "activity_name": record.activity_name,
                "purpose": record.purpose,
                "data_categories": record.data_categories
            }
        )

        self.logger.info(f"Recorded data processing activity: {record.activity_name}")

    def get_data_retention_policy(self, classification: DataClassification) -> Optional[DataRetentionPolicy]:
        """Get data retention policy for a classification.
        
        Args:
            classification: Data classification
            
        Returns:
            Retention policy or None if not found
        """
        return self.retention_policies.get(classification)

    def should_delete_data(self, created_at: datetime, classification: DataClassification) -> bool:
        """Check if data should be deleted based on retention policy.
        
        Args:
            created_at: When the data was created
            classification: Data classification
            
        Returns:
            True if data should be deleted
        """
        policy = self.get_data_retention_policy(classification)
        if not policy or not policy.auto_delete:
            return False

        retention_period = timedelta(days=policy.retention_period_days)
        return datetime.utcnow() - created_at > retention_period

    def anonymize_data(self, data: Dict[str, Any], classification: DataClassification) -> Dict[str, Any]:
        """Anonymize data based on classification.
        
        Args:
            data: Data to anonymize
            classification: Data classification level
            
        Returns:
            Anonymized data
        """
        if classification in [DataClassification.PII, DataClassification.PHI, DataClassification.PCI]:
            anonymized = data.copy()

            # Anonymize common PII fields
            pii_fields = ['email', 'phone', 'ssn', 'social_security_number', 'credit_card', 'name', 'address']

            for field in pii_fields:
                if field in anonymized:
                    if isinstance(anonymized[field], str):
                        # Hash the value for anonymization
                        hash_value = hashlib.sha256(str(anonymized[field]).encode()).hexdigest()[:12]
                        anonymized[field] = f"anon_{hash_value}"
                    else:
                        anonymized[field] = "[ANONYMIZED]"

            # Anonymize email-like patterns
            for key, value in anonymized.items():
                if isinstance(value, str) and '@' in value and '.' in value:
                    hash_value = hashlib.sha256(value.encode()).hexdigest()[:12]
                    anonymized[key] = f"anon_{hash_value}@example.com"

            return anonymized

        return data

    def audit_compliance_event(self, event_type: str, details: Dict[str, Any]) -> None:
        """Log a compliance audit event.
        
        Args:
            event_type: Type of compliance event
            details: Event details
        """
        event = AuditEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            user_id=details.get('user_id'),
            resource='compliance',
            action=event_type,
            outcome='success',
            timestamp=datetime.utcnow(),
            additional_data=details
        )

        self.audit_events.append(event)

        # Log to audit logger
        self.audit_logger.info(
            f"Compliance event: {event_type}",
            extra={
                "event_id": event.event_id,
                "event_type": event_type,
                "compliance_event": True,
                **details
            }
        )

    def generate_compliance_report(self, standard: ComplianceStandard, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Generate a compliance report for a specific standard.
        
        Args:
            standard: Compliance standard to report on
            start_date: Report start date
            end_date: Report end date
            
        Returns:
            Compliance report
        """
        # Filter relevant audit events
        relevant_events = [
            event for event in self.audit_events
            if start_date <= event.timestamp <= end_date
        ]

        report = {
            "standard": standard.value,
            "report_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat()
            },
            "generated_at": datetime.utcnow().isoformat(),
            "summary": {
                "total_events": len(relevant_events),
                "consent_events": len([e for e in relevant_events if 'consent' in e.event_type]),
                "data_processing_events": len([e for e in relevant_events if 'data_processing' in e.event_type]),
                "compliance_violations": 0  # TODO: Implement violation detection
            },
            "data_processing_activities": len(self.processing_records),
            "active_consents": len([c for c in self.consent_records.values() if c.status == ConsentStatus.GRANTED]),
            "retention_policies": {
                classification.value: {
                    "retention_days": policy.retention_period_days,
                    "auto_delete": policy.auto_delete,
                    "encryption_required": policy.encryption_required
                }
                for classification, policy in self.retention_policies.items()
            }
        }

        # Add standard-specific information
        if standard == ComplianceStandard.GDPR:
            report["gdpr_specific"] = {
                "data_subject_requests": 0,  # TODO: Implement request tracking
                "breach_notifications": 0,  # TODO: Implement breach tracking
                "cross_border_transfers": len([r for r in self.processing_records.values() if r.third_country_transfers])
            }
        elif standard == ComplianceStandard.HIPAA:
            report["hipaa_specific"] = {
                "phi_access_logs": len([e for e in relevant_events if 'phi' in str(e.additional_data).lower()]),
                "security_incidents": 0,  # TODO: Implement incident tracking
                "business_associate_agreements": 0  # TODO: Implement BAA tracking
            }

        self.audit_compliance_event(
            "compliance_report_generated",
            {
                "standard": standard.value,
                "report_period_days": (end_date - start_date).days
            }
        )

        return report

    def validate_compliance_status(self) -> Dict[str, Any]:
        """Validate current compliance status.
        
        Returns:
            Compliance validation results
        """
        validation_results = {
            "overall_status": "compliant",
            "timestamp": datetime.utcnow().isoformat(),
            "active_standards": [s.value for s in self.active_standards],
            "issues": [],
            "recommendations": []
        }

        # Check retention policies
        for classification, policy in self.retention_policies.items():
            if classification in [DataClassification.PII, DataClassification.PHI, DataClassification.PCI]:
                if not policy.encryption_required:
                    validation_results["issues"].append(
                        f"Encryption not required for {classification.value} data"
                    )
                    validation_results["overall_status"] = "non_compliant"

        # Check for expired consents
        current_time = datetime.utcnow()
        expired_consents = [
            c for c in self.consent_records.values()
            if c.expires_at and c.expires_at < current_time and c.status == ConsentStatus.GRANTED
        ]

        if expired_consents:
            validation_results["issues"].append(
                f"{len(expired_consents)} expired consents still marked as granted"
            )
            validation_results["recommendations"].append("Update expired consent statuses")

        # Check for missing data processing records
        if len(self.processing_records) == 0:
            validation_results["recommendations"].append(
                "Document data processing activities for compliance"
            )

        if validation_results["issues"]:
            validation_results["overall_status"] = "non_compliant" if any("required" in issue for issue in validation_results["issues"]) else "partially_compliant"

        return validation_results


# Global compliance manager instance
_compliance_manager: Optional[ComplianceManager] = None


def get_compliance_manager() -> ComplianceManager:
    """Get the global compliance manager instance.
    
    Returns:
        ComplianceManager instance
    """
    global _compliance_manager

    if _compliance_manager is None:
        _compliance_manager = ComplianceManager()

        # Enable default compliance standards based on environment
        import os
        enabled_standards = os.getenv("COMPLIANCE_STANDARDS", "").split(",")
        for standard_str in enabled_standards:
            try:
                standard = ComplianceStandard(standard_str.strip().lower())
                _compliance_manager.enable_compliance_standard(standard)
            except ValueError:
                pass

    return _compliance_manager
