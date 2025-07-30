#!/usr/bin/env python3
"""
Advanced Compliance Engine
Automated compliance monitoring, reporting, and policy enforcement
"""

import asyncio
import json
import time
import hashlib
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Callable, Any, Union
from enum import Enum
from pathlib import Path
import logging
import yaml
import subprocess
import re
from collections import defaultdict

class ComplianceFramework(Enum):
    """Supported compliance frameworks."""
    SOC2 = "soc2"
    ISO27001 = "iso27001"
    GDPR = "gdpr"
    HIPAA = "hipaa"
    PCI_DSS = "pci_dss"
    NIST = "nist"
    CIS = "cis"
    CUSTOM = "custom"

class ComplianceStatus(Enum):
    """Compliance check status."""
    COMPLIANT = "compliant"
    NON_COMPLIANT = "non_compliant"
    PARTIAL = "partial"
    NOT_APPLICABLE = "not_applicable"
    UNKNOWN = "unknown"

class Severity(Enum):
    """Compliance violation severity."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
    INFO = "info"

@dataclass
class ComplianceCheck:
    """Individual compliance check definition."""
    check_id: str
    framework: ComplianceFramework
    title: str
    description: str
    requirement: str
    automated: bool
    severity: Severity
    remediation: str
    tags: List[str]

@dataclass
class ComplianceResult:
    """Result of a compliance check."""
    check_id: str
    status: ComplianceStatus
    timestamp: float
    details: str
    evidence: Dict[str, Any]
    remediation_required: bool
    risk_score: float
    metadata: Dict[str, Any]

@dataclass
class ComplianceReport:
    """Comprehensive compliance report."""
    report_id: str
    timestamp: float
    framework: ComplianceFramework
    overall_score: float
    total_checks: int
    compliant_checks: int
    non_compliant_checks: int
    results: List[ComplianceResult]
    recommendations: List[str]
    executive_summary: str

class AdvancedComplianceEngine:
    """Advanced compliance monitoring and automation engine."""
    
    def __init__(self, config_path: str = "compliance-config.yml"):
        self.config_path = config_path
        self.compliance_checks: Dict[str, ComplianceCheck] = {}
        self.compliance_results: List[ComplianceResult] = []
        self.policy_violations: List[Dict[str, Any]] = []
        self.logger = self._setup_logging()
        self.load_compliance_checks()
        
    def _setup_logging(self) -> logging.Logger:
        """Setup compliance audit logging."""
        logger = logging.getLogger("compliance_engine")
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            # Console handler
            console_handler = logging.StreamHandler()
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            logger.addHandler(console_handler)
            
            # Audit trail handler
            audit_handler = logging.FileHandler('compliance-audit.log')
            audit_formatter = logging.Formatter(
                '%(asctime)s - %(levelname)s - %(message)s - Check: %(funcName)s'
            )
            audit_handler.setFormatter(audit_formatter)
            logger.addHandler(audit_handler)
        
        return logger
    
    def load_compliance_checks(self):
        """Load compliance checks from configuration."""
        # Load built-in compliance checks
        self._load_built_in_checks()
        
        # Load custom checks if config file exists
        if Path(self.config_path).exists():
            self._load_custom_checks()
        
        self.logger.info(f"Loaded {len(self.compliance_checks)} compliance checks")
    
    def _load_built_in_checks(self):
        """Load built-in compliance checks for common frameworks."""
        
        # SOC 2 Type II Controls
        soc2_checks = [
            ComplianceCheck(
                check_id="SOC2-CC6.1",
                framework=ComplianceFramework.SOC2,
                title="Access Control Implementation",
                description="System implements logical access controls",
                requirement="Implement and maintain access controls",
                automated=True,
                severity=Severity.HIGH,
                remediation="Review and implement proper access controls",
                tags=["access_control", "authentication"]
            ),
            ComplianceCheck(
                check_id="SOC2-CC7.1",
                framework=ComplianceFramework.SOC2,
                title="System Monitoring",
                description="System monitoring and logging controls",
                requirement="Implement monitoring and logging mechanisms",
                automated=True,
                severity=Severity.MEDIUM,
                remediation="Enable comprehensive logging and monitoring",
                tags=["monitoring", "logging"]
            ),
            ComplianceCheck(
                check_id="SOC2-CC8.1", 
                framework=ComplianceFramework.SOC2,
                title="Change Management",
                description="Change management processes and controls",
                requirement="Implement formal change management",
                automated=False,
                severity=Severity.HIGH,
                remediation="Establish change management procedures",
                tags=["change_management", "process"]
            )
        ]
        
        # ISO 27001 Controls
        iso27001_checks = [
            ComplianceCheck(
                check_id="ISO27001-A.12.1.1",
                framework=ComplianceFramework.ISO27001,
                title="Documented Operating Procedures",
                description="Operating procedures shall be documented",
                requirement="Document all operating procedures",
                automated=True,
                severity=Severity.MEDIUM,
                remediation="Create and maintain operating procedure documentation",
                tags=["documentation", "procedures"]
            ),
            ComplianceCheck(
                check_id="ISO27001-A.12.6.1",
                framework=ComplianceFramework.ISO27001,
                title="Management of Technical Vulnerabilities", 
                description="Technical vulnerabilities management",
                requirement="Manage technical vulnerabilities effectively",
                automated=True,
                severity=Severity.HIGH,
                remediation="Implement vulnerability management program",
                tags=["vulnerability", "security"]
            )
        ]
        
        # GDPR Requirements
        gdpr_checks = [
            ComplianceCheck(
                check_id="GDPR-Art.32",
                framework=ComplianceFramework.GDPR,
                title="Security of Processing",
                description="Implement appropriate technical and organizational measures",
                requirement="Ensure appropriate security measures",
                automated=True,
                severity=Severity.CRITICAL,
                remediation="Implement data protection measures",
                tags=["data_protection", "security", "privacy"]
            ),
            ComplianceCheck(
                check_id="GDPR-Art.25",
                framework=ComplianceFramework.GDPR,
                title="Data Protection by Design",
                description="Data protection by design and by default",
                requirement="Implement privacy by design principles",
                automated=False,
                severity=Severity.HIGH,
                remediation="Review and implement privacy-by-design principles",
                tags=["privacy", "design", "data_protection"]
            )
        ]
        
        # NIST Cybersecurity Framework
        nist_checks = [
            ComplianceCheck(
                check_id="NIST-ID.AM-1",
                framework=ComplianceFramework.NIST,
                title="Physical Devices and Systems Inventory",
                description="Physical devices and systems are inventoried",
                requirement="Maintain inventory of physical devices",
                automated=True,
                severity=Severity.MEDIUM,
                remediation="Create and maintain device inventory",
                tags=["inventory", "asset_management"]
            ),
            ComplianceCheck(
                check_id="NIST-PR.AC-1",
                framework=ComplianceFramework.NIST,
                title="Identity and Access Management",
                description="Identities and credentials are managed",
                requirement="Implement identity and access management",
                automated=True,
                severity=Severity.HIGH,
                remediation="Implement comprehensive IAM solution",
                tags=["identity", "access_management"]
            )
        ]
        
        # Add all checks to the registry
        all_checks = soc2_checks + iso27001_checks + gdpr_checks + nist_checks
        for check in all_checks:
            self.compliance_checks[check.check_id] = check
    
    def _load_custom_checks(self):
        """Load custom compliance checks from configuration file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            custom_checks = config.get('custom_checks', [])
            for check_config in custom_checks:
                check = ComplianceCheck(
                    check_id=check_config['check_id'],
                    framework=ComplianceFramework(check_config.get('framework', 'custom')),
                    title=check_config['title'],
                    description=check_config['description'],
                    requirement=check_config['requirement'],
                    automated=check_config.get('automated', False),
                    severity=Severity(check_config.get('severity', 'medium')),
                    remediation=check_config.get('remediation', ''),
                    tags=check_config.get('tags', [])
                )
                self.compliance_checks[check.check_id] = check
                
        except Exception as e:
            self.logger.error(f"Error loading custom compliance checks: {e}")
    
    async def run_compliance_check(self, check_id: str) -> ComplianceResult:
        """Run a specific compliance check."""
        if check_id not in self.compliance_checks:
            raise ValueError(f"Unknown compliance check: {check_id}")
        
        check = self.compliance_checks[check_id]
        self.logger.info(f"Running compliance check: {check.title}")
        
        if not check.automated:
            return ComplianceResult(
                check_id=check_id,
                status=ComplianceStatus.UNKNOWN,
                timestamp=time.time(),
                details="Manual check required",
                evidence={},
                remediation_required=False,
                risk_score=0.0,
                metadata={"manual_check": True}
            )
        
        # Route to specific check implementation
        if check_id.startswith("SOC2"):
            return await self._run_soc2_check(check)
        elif check_id.startswith("ISO27001"):
            return await self._run_iso27001_check(check)
        elif check_id.startswith("GDPR"):
            return await self._run_gdpr_check(check)
        elif check_id.startswith("NIST"):
            return await self._run_nist_check(check)
        else:
            return await self._run_generic_check(check)
    
    async def _run_soc2_check(self, check: ComplianceCheck) -> ComplianceResult:
        """Run SOC 2 specific compliance checks."""
        evidence = {}
        status = ComplianceStatus.COMPLIANT
        details = ""
        risk_score = 0.0
        
        if check.check_id == "SOC2-CC6.1":  # Access Control
            # Check for authentication mechanisms
            auth_files = self._scan_for_authentication_code()
            if auth_files:
                evidence["authentication_implementations"] = auth_files
                details = f"Found {len(auth_files)} authentication implementations"
            else:
                status = ComplianceStatus.NON_COMPLIANT
                details = "No authentication mechanisms detected"
                risk_score = 8.5
        
        elif check.check_id == "SOC2-CC7.1":  # System Monitoring
            # Check for monitoring and logging setup
            monitoring_config = self._check_monitoring_configuration()
            if monitoring_config["has_monitoring"]:
                evidence["monitoring_setup"] = monitoring_config
                details = "Monitoring and logging mechanisms detected"
            else:
                status = ComplianceStatus.PARTIAL
                details = "Limited monitoring setup detected"
                risk_score = 5.5
        
        elif check.check_id == "SOC2-CC8.1":  # Change Management
            # Check for change management processes
            change_mgmt = self._assess_change_management()
            evidence["change_management"] = change_mgmt
            if change_mgmt["score"] >= 7:
                details = "Adequate change management processes"
            else:
                status = ComplianceStatus.NON_COMPLIANT
                details = "Insufficient change management processes"
                risk_score = 7.0
        
        return ComplianceResult(
            check_id=check.check_id,
            status=status,
            timestamp=time.time(),
            details=details,
            evidence=evidence,
            remediation_required=(status != ComplianceStatus.COMPLIANT),
            risk_score=risk_score,
            metadata={"framework": "SOC2", "automated": True}
        )
    
    async def _run_iso27001_check(self, check: ComplianceCheck) -> ComplianceResult:
        """Run ISO 27001 specific compliance checks."""
        evidence = {}
        status = ComplianceStatus.COMPLIANT
        details = ""
        risk_score = 0.0
        
        if check.check_id == "ISO27001-A.12.1.1":  # Operating Procedures
            # Check for documented procedures
            docs = self._scan_for_documentation()
            evidence["documentation"] = docs
            if docs["comprehensive_docs"] >= 5:
                details = f"Found {docs['comprehensive_docs']} documented procedures"
            else:
                status = ComplianceStatus.PARTIAL
                details = "Limited documentation detected"
                risk_score = 4.0
        
        elif check.check_id == "ISO27001-A.12.6.1":  # Vulnerability Management
            # Check vulnerability management
            vuln_mgmt = await self._assess_vulnerability_management()
            evidence["vulnerability_management"] = vuln_mgmt
            if vuln_mgmt["has_scanning"] and vuln_mgmt["has_process"]:
                details = "Vulnerability management processes detected"
            else:
                status = ComplianceStatus.NON_COMPLIANT
                details = "Inadequate vulnerability management"
                risk_score = 8.0
        
        return ComplianceResult(
            check_id=check.check_id,
            status=status,
            timestamp=time.time(),
            details=details,
            evidence=evidence,
            remediation_required=(status != ComplianceStatus.COMPLIANT),
            risk_score=risk_score,
            metadata={"framework": "ISO27001", "automated": True}
        )
    
    async def _run_gdpr_check(self, check: ComplianceCheck) -> ComplianceResult:
        """Run GDPR specific compliance checks."""
        evidence = {}
        status = ComplianceStatus.COMPLIANT
        details = ""
        risk_score = 0.0
        
        if check.check_id == "GDPR-Art.32":  # Security of Processing
            # Check data protection measures
            data_protection = self._assess_data_protection()
            evidence["data_protection"] = data_protection
            
            if data_protection["encryption_score"] >= 8 and data_protection["access_control_score"] >= 7:
                details = "Adequate data protection measures"
            else:
                status = ComplianceStatus.NON_COMPLIANT
                details = "Insufficient data protection measures"
                risk_score = 9.0
        
        elif check.check_id == "GDPR-Art.25":  # Privacy by Design
            # Manual check - cannot be fully automated
            status = ComplianceStatus.UNKNOWN
            details = "Privacy by design requires manual assessment"
            evidence["note"] = "Manual review required for privacy by design principles"
        
        return ComplianceResult(
            check_id=check.check_id,
            status=status,
            timestamp=time.time(),
            details=details,
            evidence=evidence,
            remediation_required=(status != ComplianceStatus.COMPLIANT),
            risk_score=risk_score,
            metadata={"framework": "GDPR", "automated": True}
        )
    
    async def _run_nist_check(self, check: ComplianceCheck) -> ComplianceResult:
        """Run NIST Cybersecurity Framework checks."""
        evidence = {}
        status = ComplianceStatus.COMPLIANT
        details = ""
        risk_score = 0.0
        
        if check.check_id == "NIST-ID.AM-1":  # Asset Inventory
            # Check for asset inventory
            inventory = self._assess_asset_inventory()
            evidence["asset_inventory"] = inventory
            
            if inventory["has_inventory"]:
                details = f"Asset inventory found with {inventory['asset_count']} items"
            else:
                status = ComplianceStatus.NON_COMPLIANT
                details = "No asset inventory detected"
                risk_score = 6.0
        
        elif check.check_id == "NIST-PR.AC-1":  # Identity and Access Management
            # Check IAM implementation
            iam_assessment = self._assess_iam_implementation()
            evidence["iam"] = iam_assessment
            
            if iam_assessment["score"] >= 7:
                details = "Adequate IAM implementation"
            else:
                status = ComplianceStatus.PARTIAL
                details = "IAM implementation needs improvement"
                risk_score = 5.5
        
        return ComplianceResult(
            check_id=check.check_id,
            status=status,
            timestamp=time.time(),
            details=details,
            evidence=evidence,
            remediation_required=(status != ComplianceStatus.COMPLIANT),
            risk_score=risk_score,
            metadata={"framework": "NIST", "automated": True}
        )
    
    async def _run_generic_check(self, check: ComplianceCheck) -> ComplianceResult:
        """Run generic compliance check."""
        return ComplianceResult(
            check_id=check.check_id,
            status=ComplianceStatus.UNKNOWN,
            timestamp=time.time(),
            details="Generic check - requires specific implementation",
            evidence={},
            remediation_required=False,
            risk_score=0.0,
            metadata={"framework": check.framework.value, "automated": False}
        )
    
    def _scan_for_authentication_code(self) -> List[str]:
        """Scan codebase for authentication implementations."""
        auth_patterns = [
            r"@login_required",
            r"authenticate\(",
            r"jwt\.encode",
            r"passport\.authenticate",
            r"OAuth",
            r"BasicAuth",
            r"TokenAuth"
        ]
        
        auth_files = []
        try:
            for pattern in auth_patterns:
                result = subprocess.run(
                    ["grep", "-r", "-l", pattern, "src/"],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    auth_files.extend(result.stdout.strip().split('\n'))
        except Exception as e:
            self.logger.warning(f"Error scanning for authentication code: {e}")
        
        return list(set(auth_files))  # Remove duplicates
    
    def _check_monitoring_configuration(self) -> Dict[str, Any]:
        """Check for monitoring and logging configuration."""
        monitoring_indicators = {
            "has_prometheus": Path("monitoring/prometheus.yml").exists(),
            "has_grafana": Path("monitoring/grafana").exists(),
            "has_logging_config": Path("src/agent_orchestrated_etl/logging_config.py").exists(),
            "has_observability": Path("observability").exists()
        }
        
        monitoring_score = sum(monitoring_indicators.values())
        
        return {
            "has_monitoring": monitoring_score >= 2,
            "monitoring_score": monitoring_score,
            "indicators": monitoring_indicators
        }
    
    def _assess_change_management(self) -> Dict[str, Any]:
        """Assess change management processes."""
        change_indicators = {
            "has_pr_template": Path(".github/PULL_REQUEST_TEMPLATE.md").exists(),
            "has_contributing_guide": Path("CONTRIBUTING.md").exists(),
            "has_code_review": Path("CODE_REVIEW.md").exists(),
            "has_precommit_hooks": Path(".pre-commit-config.yaml").exists(),
            "has_ci_cd": Path(".github/workflows").exists() or Path("docs/workflows").exists(),
            "has_branch_protection": False  # Would need GitHub API to check
        }
        
        score = sum(change_indicators.values())
        
        return {
            "score": score,
            "max_score": len(change_indicators),
            "indicators": change_indicators,
            "percentage": (score / len(change_indicators)) * 100
        }
    
    def _scan_for_documentation(self) -> Dict[str, Any]:
        """Scan for comprehensive documentation."""
        doc_files = []
        doc_dirs = ["docs/", "DOCS/", "./"]
        
        for doc_dir in doc_dirs:
            if Path(doc_dir).exists():
                for ext in ["*.md", "*.rst", "*.txt"]:
                    doc_files.extend(Path(doc_dir).glob(f"**/{ext}"))
        
        comprehensive_docs = len([f for f in doc_files if f.stat().st_size > 1000])  # Files > 1KB
        
        return {
            "total_docs": len(doc_files),
            "comprehensive_docs": comprehensive_docs,
            "doc_files": [str(f) for f in doc_files[:10]]  # First 10 for evidence
        }
    
    async def _assess_vulnerability_management(self) -> Dict[str, Any]:
        """Assess vulnerability management capabilities."""
        vuln_indicators = {
            "has_bandit": self._check_tool_availability("bandit"),
            "has_safety": self._check_tool_availability("safety"),
            "has_dependabot": Path(".github/dependabot.yml").exists(),
            "has_security_policy": Path("SECURITY.md").exists(),
            "has_vuln_scanning_script": Path("scripts/vulnerability_scanner.py").exists()
        }
        
        return {
            "has_scanning": sum(vuln_indicators.values()) >= 2,
            "has_process": vuln_indicators["has_security_policy"],
            "indicators": vuln_indicators,
            "score": sum(vuln_indicators.values())
        }
    
    def _assess_data_protection(self) -> Dict[str, Any]:
        """Assess data protection measures."""
        # Check for encryption implementations
        encryption_files = []
        try:
            for pattern in ["encrypt", "decrypt", "AES", "RSA", "bcrypt", "hashlib"]:
                result = subprocess.run(
                    ["grep", "-r", "-l", pattern, "src/"],
                    capture_output=True,
                    text=True
                )
                if result.returncode == 0:
                    encryption_files.extend(result.stdout.strip().split('\n'))
        except Exception:
            pass
        
        # Check for access control
        access_control_score = len(self._scan_for_authentication_code())
        
        return {
            "encryption_score": min(10, len(set(encryption_files))),
            "access_control_score": min(10, access_control_score),
            "encryption_files": list(set(encryption_files))[:5]
        }
    
    def _assess_asset_inventory(self) -> Dict[str, Any]:
        """Assess asset inventory practices."""
        inventory_indicators = {
            "has_dockerfile": Path("Dockerfile").exists(),
            "has_docker_compose": Path("docker-compose.yml").exists(),
            "has_kubernetes": Path("infrastructure/kubernetes").exists(),
            "has_terraform": Path("infrastructure/terraform").exists(),
            "has_requirements": Path("requirements.txt").exists() or Path("pyproject.toml").exists()
        }
        
        return {
            "has_inventory": sum(inventory_indicators.values()) >= 3,
            "asset_count": sum(inventory_indicators.values()),
            "indicators": inventory_indicators
        }
    
    def _assess_iam_implementation(self) -> Dict[str, Any]:
        """Assess Identity and Access Management implementation."""
        iam_indicators = {
            "has_authentication": len(self._scan_for_authentication_code()) > 0,
            "has_rbac": self._check_rbac_implementation(),
            "has_session_management": self._check_session_management(),
            "has_password_policy": self._check_password_policy(),
            "has_mfa": self._check_mfa_implementation()
        }
        
        score = sum(iam_indicators.values())
        
        return {
            "score": score,
            "max_score": len(iam_indicators),
            "indicators": iam_indicators,
            "percentage": (score / len(iam_indicators)) * 100
        }
    
    def _check_tool_availability(self, tool_name: str) -> bool:
        """Check if a security tool is available."""
        try:
            result = subprocess.run([tool_name, "--version"], capture_output=True)
            return result.returncode == 0
        except Exception:
            return False
    
    def _check_rbac_implementation(self) -> bool:
        """Check for Role-Based Access Control implementation."""
        rbac_patterns = ["@require_role", "role", "permission", "authorize"]
        for pattern in rbac_patterns:
            try:
                result = subprocess.run(
                    ["grep", "-r", "-i", pattern, "src/"],
                    capture_output=True
                )
                if result.returncode == 0:
                    return True
            except Exception:
                pass
        return False
    
    def _check_session_management(self) -> bool:
        """Check for session management implementation."""
        session_patterns = ["session", "cookie", "csrf", "token"]
        for pattern in session_patterns:
            try:
                result = subprocess.run(
                    ["grep", "-r", "-i", pattern, "src/"],
                    capture_output=True
                )
                if result.returncode == 0:
                    return True
            except Exception:
                pass
        return False
    
    def _check_password_policy(self) -> bool:
        """Check for password policy implementation."""
        password_patterns = ["password.*policy", "password.*strength", "password.*requirement"]
        for pattern in password_patterns:
            try:
                result = subprocess.run(
                    ["grep", "-r", "-i", pattern, "src/"],
                    capture_output=True
                )
                if result.returncode == 0:
                    return True
            except Exception:
                pass
        return False
    
    def _check_mfa_implementation(self) -> bool:
        """Check for Multi-Factor Authentication implementation."""
        mfa_patterns = ["mfa", "2fa", "totp", "multi.*factor"]
        for pattern in mfa_patterns:
            try:
                result = subprocess.run(
                    ["grep", "-r", "-i", pattern, "src/"],
                    capture_output=True
                )
                if result.returncode == 0:
                    return True
            except Exception:
                pass
        return False
    
    async def run_comprehensive_audit(self, framework: Optional[ComplianceFramework] = None) -> ComplianceReport:
        """Run comprehensive compliance audit."""
        self.logger.info("Starting comprehensive compliance audit...")
        
        # Filter checks by framework if specified
        checks_to_run = self.compliance_checks
        if framework:
            checks_to_run = {k: v for k, v in self.compliance_checks.items() if v.framework == framework}
        
        # Run all compliance checks
        results = []
        for check_id in checks_to_run.keys():
            try:
                result = await self.run_compliance_check(check_id)
                results.append(result)
                self.compliance_results.append(result)
            except Exception as e:
                self.logger.error(f"Error running check {check_id}: {e}")
        
        # Calculate overall compliance score
        total_checks = len(results)
        compliant_checks = len([r for r in results if r.status == ComplianceStatus.COMPLIANT])
        non_compliant_checks = len([r for r in results if r.status == ComplianceStatus.NON_COMPLIANT])
        
        overall_score = (compliant_checks / total_checks * 100) if total_checks > 0 else 0
        
        # Generate recommendations
        recommendations = self._generate_compliance_recommendations(results)
        
        # Create executive summary
        executive_summary = self._generate_executive_summary(overall_score, total_checks, compliant_checks, non_compliant_checks)
        
        report = ComplianceReport(
            report_id=hashlib.md5(f"audit-{time.time()}".encode()).hexdigest()[:8],
            timestamp=time.time(),
            framework=framework or ComplianceFramework.CUSTOM,
            overall_score=overall_score,
            total_checks=total_checks,
            compliant_checks=compliant_checks,
            non_compliant_checks=non_compliant_checks,
            results=results,
            recommendations=recommendations,
            executive_summary=executive_summary
        )
        
        self.logger.info(f"Compliance audit completed: {overall_score:.1f}% compliance")
        return report
    
    def _generate_compliance_recommendations(self, results: List[ComplianceResult]) -> List[str]:
        """Generate actionable compliance recommendations."""
        recommendations = []
        
        # Group non-compliant results by severity
        critical_issues = [r for r in results if r.status == ComplianceStatus.NON_COMPLIANT and r.risk_score >= 8.0]
        high_issues = [r for r in results if r.status == ComplianceStatus.NON_COMPLIANT and 5.0 <= r.risk_score < 8.0]
        
        if critical_issues:
            recommendations.append(f"🚨 CRITICAL: Address {len(critical_issues)} critical compliance issues immediately")
            for issue in critical_issues[:3]:  # Top 3
                check = self.compliance_checks[issue.check_id]
                recommendations.append(f"   • {check.title}: {check.remediation}")
        
        if high_issues:
            recommendations.append(f"⚠️  HIGH: Remediate {len(high_issues)} high-priority compliance gaps")
        
        # Framework-specific recommendations
        frameworks = set([self.compliance_checks[r.check_id].framework for r in results if r.status == ComplianceStatus.NON_COMPLIANT])
        for framework in frameworks:
            recommendations.append(f"📋 Review {framework.value.upper()} compliance requirements and implement missing controls")
        
        # Automated vs manual checks
        manual_checks = [r for r in results if r.metadata.get("manual_check")]
        if manual_checks:
            recommendations.append(f"👥 Schedule manual review for {len(manual_checks)} compliance checks")
        
        return recommendations
    
    def _generate_executive_summary(self, overall_score: float, total_checks: int, compliant_checks: int, non_compliant_checks: int) -> str:
        """Generate executive summary for compliance report."""
        risk_level = "LOW" if overall_score >= 90 else "MEDIUM" if overall_score >= 70 else "HIGH"
        
        summary = f"""
        COMPLIANCE AUDIT EXECUTIVE SUMMARY
        
        Overall Compliance Score: {overall_score:.1f}%
        Risk Level: {risk_level}
        
        Assessment Overview:
        • Total Checks Performed: {total_checks}
        • Compliant Controls: {compliant_checks}
        • Non-Compliant Controls: {non_compliant_checks}
        • Compliance Rate: {(compliant_checks/total_checks*100):.1f}%
        
        Key Findings:
        {"• Strong compliance posture with minimal gaps" if overall_score >= 90 else 
         "• Good compliance foundation with some improvement areas" if overall_score >= 70 else
         "• Significant compliance gaps requiring immediate attention"}
        
        Recommended Actions:
        {"• Maintain current compliance practices" if overall_score >= 90 else
         "• Focus on addressing medium and high-risk findings" if overall_score >= 70 else
         "• Implement comprehensive remediation plan for critical issues"}
        """
        
        return summary.strip()
    
    def export_compliance_report(self, report: ComplianceReport, filename: str = None) -> str:
        """Export comprehensive compliance report."""
        if not filename:
            filename = f"compliance-report-{report.report_id}.json"
        
        report_data = {
            "report_metadata": {
                "report_id": report.report_id,
                "timestamp": report.timestamp,
                "framework": report.framework.value,
                "generated_by": "Advanced Compliance Engine"
            },
            "executive_summary": report.executive_summary,
            "compliance_overview": {
                "overall_score": report.overall_score,
                "total_checks": report.total_checks,
                "compliant_checks": report.compliant_checks,
                "non_compliant_checks": report.non_compliant_checks,
                "compliance_percentage": (report.compliant_checks / report.total_checks * 100) if report.total_checks > 0 else 0
            },
            "detailed_results": [asdict(result) for result in report.results],
            "recommendations": report.recommendations,
            "risk_assessment": self._generate_risk_assessment(report.results),
            "compliance_matrix": self._generate_compliance_matrix(report.results)
        }
        
        with open(filename, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        self.logger.info(f"Compliance report exported to {filename}")
        return filename
    
    def _generate_risk_assessment(self, results: List[ComplianceResult]) -> Dict[str, Any]:
        """Generate risk assessment from compliance results."""
        risk_scores = [r.risk_score for r in results if r.risk_score > 0]
        
        if not risk_scores:
            return {"overall_risk": 0.0, "risk_level": "LOW"}
        
        overall_risk = sum(risk_scores) / len(risk_scores)
        risk_level = "CRITICAL" if overall_risk >= 8.0 else "HIGH" if overall_risk >= 6.0 else "MEDIUM" if overall_risk >= 4.0 else "LOW"
        
        return {
            "overall_risk": overall_risk,
            "risk_level": risk_level,
            "high_risk_controls": len([r for r in results if r.risk_score >= 7.0]),
            "medium_risk_controls": len([r for r in results if 4.0 <= r.risk_score < 7.0]),
            "low_risk_controls": len([r for r in results if 0 < r.risk_score < 4.0])
        }
    
    def _generate_compliance_matrix(self, results: List[ComplianceResult]) -> Dict[str, Any]:
        """Generate compliance matrix by framework and status."""
        matrix = defaultdict(lambda: defaultdict(int))
        
        for result in results:
            check = self.compliance_checks[result.check_id]
            framework = check.framework.value
            status = result.status.value
            matrix[framework][status] += 1
        
        return dict(matrix)

# CLI usage example
async def main():
    """Example usage of advanced compliance engine."""
    engine = AdvancedComplianceEngine()
    
    print("Advanced Compliance Engine - Running Comprehensive Audit")
    
    # Run comprehensive audit for all frameworks
    report = await engine.run_comprehensive_audit()
    
    # Export report
    report_file = engine.export_compliance_report(report)
    
    print(f"\nCompliance Audit Results:")
    print(f"Overall Score: {report.overall_score:.1f}%")
    print(f"Total Checks: {report.total_checks}")
    print(f"Compliant: {report.compliant_checks}")
    print(f"Non-Compliant: {report.non_compliant_checks}")
    print(f"\nReport exported to: {report_file}")
    
    print("\nTop Recommendations:")
    for rec in report.recommendations[:5]:
        print(f"  • {rec}")

if __name__ == "__main__":
    asyncio.run(main())