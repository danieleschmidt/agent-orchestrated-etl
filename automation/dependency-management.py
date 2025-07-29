"""
Advanced dependency management and security automation for agent-orchestrated-etl.
Provides intelligent dependency updates, vulnerability management, and compliance tracking.
"""

import asyncio
import json
import logging
import re
import subprocess
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from enum import Enum

import aiohttp
import requests
from packaging import version
from packaging.requirements import Requirement


class VulnerabilitySeverity(Enum):
    """Vulnerability severity levels."""
    CRITICAL = "critical"
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class UpdateStrategy(Enum):
    """Dependency update strategies."""
    CONSERVATIVE = "conservative"  # Only patch updates
    MODERATE = "moderate"         # Minor and patch updates
    AGGRESSIVE = "aggressive"     # All updates including major


@dataclass
class Vulnerability:
    """Security vulnerability information."""
    id: str
    package: str
    affected_versions: str
    severity: VulnerabilitySeverity
    description: str
    cve_id: Optional[str] = None
    published_date: Optional[datetime] = None
    fixed_version: Optional[str] = None
    source: str = "unknown"


@dataclass
class DependencyUpdate:
    """Dependency update recommendation."""
    package: str
    current_version: str
    recommended_version: str
    update_type: str  # major, minor, patch
    breaking_changes: bool
    security_fixes: List[Vulnerability]
    release_notes: Optional[str] = None
    confidence: float = 0.5
    risk_level: str = "medium"


class SecurityScanner:
    """Scans dependencies for known vulnerabilities."""
    
    def __init__(self):
        """Initialize the security scanner."""
        self.logger = logging.getLogger(__name__)
        self.session = None
        
        # Vulnerability databases
        self.vuln_sources = {
            'pypi': 'https://pypi.org/pypi/{package}/json',
            'github': 'https://api.github.com/advisories',
            'osv': 'https://api.osv.dev/v1/query'
        }
    
    async def __aenter__(self):
        """Async context manager entry."""
        self.session = aiohttp.ClientSession()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self.session:
            await self.session.close()
    
    async def scan_requirements(self, requirements_path: str) -> List[Vulnerability]:
        """Scan requirements file for vulnerabilities."""
        vulnerabilities = []
        
        try:
            with open(requirements_path, 'r') as f:
                requirements = f.read()
            
            # Parse requirements
            dependencies = self._parse_requirements(requirements)
            
            # Scan each dependency
            for package, version_spec in dependencies.items():
                package_vulns = await self._scan_package(package, version_spec)
                vulnerabilities.extend(package_vulns)
                
        except Exception as e:
            self.logger.error(f"Error scanning requirements: {e}")
        
        return vulnerabilities
    
    async def scan_pyproject_toml(self, pyproject_path: str) -> List[Vulnerability]:
        """Scan pyproject.toml for vulnerabilities."""
        vulnerabilities = []
        
        try:
            import tomli
            
            with open(pyproject_path, 'rb') as f:
                pyproject = tomli.load(f)
            
            # Extract dependencies
            dependencies = {}
            
            # Main dependencies
            if 'project' in pyproject and 'dependencies' in pyproject['project']:
                for dep in pyproject['project']['dependencies']:
                    req = Requirement(dep)
                    dependencies[req.name] = str(req.specifier)
            
            # Optional dependencies
            if 'project' in pyproject and 'optional-dependencies' in pyproject['project']:
                for group, deps in pyproject['project']['optional-dependencies'].items():
                    for dep in deps:
                        req = Requirement(dep)
                        dependencies[req.name] = str(req.specifier)
            
            # Scan dependencies
            for package, version_spec in dependencies.items():
                package_vulns = await self._scan_package(package, version_spec)
                vulnerabilities.extend(package_vulns)
                
        except Exception as e:
            self.logger.error(f"Error scanning pyproject.toml: {e}")
        
        return vulnerabilities
    
    async def _scan_package(self, package: str, version_spec: str) -> List[Vulnerability]:
        """Scan a specific package for vulnerabilities."""
        vulnerabilities = []
        
        try:
            # Query OSV database
            osv_vulns = await self._query_osv(package, version_spec)
            vulnerabilities.extend(osv_vulns)
            
            # Query GitHub advisories
            github_vulns = await self._query_github_advisories(package)
            vulnerabilities.extend(github_vulns)
            
        except Exception as e:
            self.logger.error(f"Error scanning package {package}: {e}")
        
        return vulnerabilities
    
    async def _query_osv(self, package: str, version_spec: str) -> List[Vulnerability]:
        """Query OSV database for vulnerabilities."""
        vulnerabilities = []
        
        try:
            query = {
                "package": {
                    "name": package,
                    "ecosystem": "PyPI"
                }
            }
            
            async with self.session.post(
                self.vuln_sources['osv'],
                json=query
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    for vuln in data.get('vulns', []):
                        vulnerability = Vulnerability(
                            id=vuln['id'],
                            package=package,
                            affected_versions=str(vuln.get('affected', [])),
                            severity=self._parse_severity(vuln.get('severity')),
                            description=vuln.get('summary', ''),
                            published_date=self._parse_date(vuln.get('published')),
                            source='osv'
                        )
                        vulnerabilities.append(vulnerability)
                        
        except Exception as e:
            self.logger.error(f"Error querying OSV for {package}: {e}")
        
        return vulnerabilities
    
    async def _query_github_advisories(self, package: str) -> List[Vulnerability]:
        """Query GitHub security advisories."""
        vulnerabilities = []
        
        try:
            headers = {
                'Accept': 'application/vnd.github+json',
                'X-GitHub-Api-Version': '2022-11-28'
            }
            
            params = {
                'ecosystem': 'pip',
                'package': package,
                'per_page': 100
            }
            
            async with self.session.get(
                self.vuln_sources['github'],
                headers=headers,
                params=params
            ) as response:
                if response.status == 200:
                    advisories = await response.json()
                    
                    for advisory in advisories:
                        vulnerability = Vulnerability(
                            id=advisory['ghsa_id'],
                            package=package,
                            affected_versions=str(advisory.get('vulnerable_version_range', '')),
                            severity=VulnerabilitySeverity(advisory.get('severity', 'medium').lower()),
                            description=advisory.get('summary', ''),
                            cve_id=advisory.get('cve_id'),
                            published_date=self._parse_date(advisory.get('published_at')),
                            source='github'
                        )
                        vulnerabilities.append(vulnerability)
                        
        except Exception as e:
            self.logger.error(f"Error querying GitHub advisories for {package}: {e}")
        
        return vulnerabilities
    
    def _parse_requirements(self, requirements_text: str) -> Dict[str, str]:
        """Parse requirements file into package-version mapping."""
        dependencies = {}
        
        for line in requirements_text.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('-'):
                try:
                    req = Requirement(line)
                    dependencies[req.name] = str(req.specifier)
                except Exception:
                    # Skip invalid requirements
                    continue
        
        return dependencies
    
    def _parse_severity(self, severity_data) -> VulnerabilitySeverity:
        """Parse severity from various formats."""
        if not severity_data:
            return VulnerabilitySeverity.MEDIUM
        
        if isinstance(severity_data, str):
            severity = severity_data.lower()
        elif isinstance(severity_data, list) and severity_data:
            severity = severity_data[0].get('score', 'medium').lower()
        else:
            return VulnerabilitySeverity.MEDIUM
        
        # Map various severity formats
        if 'critical' in severity or 'high' in severity:
            return VulnerabilitySeverity.CRITICAL
        elif 'medium' in severity or 'moderate' in severity:
            return VulnerabilitySeverity.MEDIUM
        elif 'low' in severity:
            return VulnerabilitySeverity.LOW
        else:
            return VulnerabilitySeverity.MEDIUM
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse date string into datetime object."""
        if not date_str:
            return None
        
        try:
            # Handle various date formats
            if 'T' in date_str:
                return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
            else:
                return datetime.fromisoformat(date_str)
        except Exception:
            return None


class DependencyAnalyzer:
    """Analyzes dependency updates and provides recommendations."""
    
    def __init__(self, update_strategy: UpdateStrategy = UpdateStrategy.MODERATE):
        """Initialize dependency analyzer."""
        self.update_strategy = update_strategy
        self.logger = logging.getLogger(__name__)
    
    async def analyze_updates(
        self, 
        requirements_path: str,
        vulnerabilities: List[Vulnerability]
    ) -> List[DependencyUpdate]:
        """Analyze available dependency updates."""
        updates = []
        
        try:
            with open(requirements_path, 'r') as f:
                requirements = f.read()
            
            dependencies = self._parse_requirements(requirements)
            
            for package, version_spec in dependencies.items():
                update = await self._analyze_package_update(package, version_spec, vulnerabilities)
                if update:
                    updates.append(update)
                    
        except Exception as e:
            self.logger.error(f"Error analyzing updates: {e}")
        
        return updates
    
    async def _analyze_package_update(
        self,
        package: str,
        current_version_spec: str,
        vulnerabilities: List[Vulnerability]
    ) -> Optional[DependencyUpdate]:
        """Analyze update for a specific package."""
        try:
            # Get current version
            current_version = self._extract_version(current_version_spec)
            if not current_version:
                return None
            
            # Get latest version from PyPI
            latest_version = await self._get_latest_version(package)
            if not latest_version:
                return None
            
            # Check if update is needed
            if version.parse(current_version) >= version.parse(latest_version):
                return None
            
            # Determine update type
            update_type = self._get_update_type(current_version, latest_version)
            
            # Check if update aligns with strategy
            if not self._should_update(update_type):
                return None
            
            # Find security vulnerabilities for this package
            package_vulns = [v for v in vulnerabilities if v.package.lower() == package.lower()]
            
            # Get release notes
            release_notes = await self._get_release_notes(package, latest_version)
            
            # Assess breaking changes
            breaking_changes = self._assess_breaking_changes(update_type, release_notes)
            
            # Calculate confidence and risk
            confidence = self._calculate_confidence(update_type, package_vulns, breaking_changes)
            risk_level = self._assess_risk_level(update_type, breaking_changes, package_vulns)
            
            return DependencyUpdate(
                package=package,
                current_version=current_version,
                recommended_version=latest_version,
                update_type=update_type,
                breaking_changes=breaking_changes,
                security_fixes=package_vulns,
                release_notes=release_notes,
                confidence=confidence,
                risk_level=risk_level
            )
            
        except Exception as e:
            self.logger.error(f"Error analyzing update for {package}: {e}")
            return None
    
    async def _get_latest_version(self, package: str) -> Optional[str]:
        """Get latest version from PyPI."""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f'https://pypi.org/pypi/{package}/json') as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['info']['version']
        except Exception as e:
            self.logger.error(f"Error getting latest version for {package}: {e}")
        
        return None
    
    async def _get_release_notes(self, package: str, version: str) -> Optional[str]:
        """Get release notes for a package version."""
        try:
            # Try to get from PyPI first
            async with aiohttp.ClientSession() as session:
                async with session.get(f'https://pypi.org/pypi/{package}/{version}/json') as response:
                    if response.status == 200:
                        data = await response.json()
                        return data['info'].get('description', '')[:1000]  # Limit length
        except Exception:
            pass
        
        return None
    
    def _parse_requirements(self, requirements_text: str) -> Dict[str, str]:
        """Parse requirements file."""
        dependencies = {}
        
        for line in requirements_text.split('\n'):
            line = line.strip()
            if line and not line.startswith('#') and not line.startswith('-'):
                try:
                    req = Requirement(line)
                    dependencies[req.name] = str(req.specifier)
                except Exception:
                    continue
        
        return dependencies
    
    def _extract_version(self, version_spec: str) -> Optional[str]:
        """Extract version number from version specification."""
        # Remove operators like >=, ==, ~=
        version_match = re.search(r'[\d.]+', version_spec)
        return version_match.group(0) if version_match else None
    
    def _get_update_type(self, current: str, latest: str) -> str:
        """Determine update type (major, minor, patch)."""
        try:
            current_parts = [int(x) for x in current.split('.')]
            latest_parts = [int(x) for x in latest.split('.')]
            
            # Pad with zeros if needed
            while len(current_parts) < 3:
                current_parts.append(0)
            while len(latest_parts) < 3:
                latest_parts.append(0)
            
            if latest_parts[0] > current_parts[0]:
                return 'major'
            elif latest_parts[1] > current_parts[1]:
                return 'minor'
            else:
                return 'patch'
                
        except Exception:
            return 'unknown'
    
    def _should_update(self, update_type: str) -> bool:
        """Check if update aligns with strategy."""
        if self.update_strategy == UpdateStrategy.CONSERVATIVE:
            return update_type == 'patch'
        elif self.update_strategy == UpdateStrategy.MODERATE:
            return update_type in ['patch', 'minor']
        elif self.update_strategy == UpdateStrategy.AGGRESSIVE:
            return True
        
        return False
    
    def _assess_breaking_changes(self, update_type: str, release_notes: Optional[str]) -> bool:
        """Assess likelihood of breaking changes."""
        if update_type == 'major':
            return True
        
        if release_notes:
            breaking_keywords = ['breaking', 'incompatible', 'removed', 'deprecated']
            return any(keyword in release_notes.lower() for keyword in breaking_keywords)
        
        return False
    
    def _calculate_confidence(
        self,
        update_type: str,
        vulnerabilities: List[Vulnerability],
        breaking_changes: bool
    ) -> float:
        """Calculate confidence score for update recommendation."""
        base_confidence = {
            'patch': 0.9,
            'minor': 0.7,
            'major': 0.4
        }.get(update_type, 0.5)
        
        # Increase confidence for security fixes
        if vulnerabilities:
            base_confidence += 0.2
        
        # Decrease confidence for breaking changes
        if breaking_changes:
            base_confidence -= 0.3
        
        return max(0.1, min(1.0, base_confidence))
    
    def _assess_risk_level(
        self,
        update_type: str,
        breaking_changes: bool,
        vulnerabilities: List[Vulnerability]
    ) -> str:
        """Assess risk level for update."""
        if breaking_changes or update_type == 'major':
            return 'high'
        
        if vulnerabilities and any(v.severity == VulnerabilitySeverity.CRITICAL for v in vulnerabilities):
            return 'low'  # Low risk to update due to critical security fixes
        
        if update_type == 'minor':
            return 'medium'
        
        return 'low'


class DependencyManager:
    """Main dependency management orchestrator."""
    
    def __init__(self, config_path: Optional[str] = None):
        """Initialize dependency manager."""
        self.logger = logging.getLogger(__name__)
        self.config = self._load_config(config_path)
        
        self.scanner = SecurityScanner()
        self.analyzer = DependencyAnalyzer(
            UpdateStrategy(self.config.get('update_strategy', 'moderate'))
        )
    
    async def audit_dependencies(self, project_path: str) -> Dict:
        """Perform comprehensive dependency audit."""
        project_path = Path(project_path)
        
        # Find dependency files
        requirements_files = list(project_path.glob('*requirements*.txt'))
        pyproject_files = list(project_path.glob('pyproject.toml'))
        
        all_vulnerabilities = []
        all_updates = []
        
        async with self.scanner:
            # Scan requirements files
            for req_file in requirements_files:
                vulns = await self.scanner.scan_requirements(str(req_file))
                all_vulnerabilities.extend(vulns)
                
                updates = await self.analyzer.analyze_updates(str(req_file), vulns)
                all_updates.extend(updates)
            
            # Scan pyproject.toml files
            for pyproject_file in pyproject_files:
                vulns = await self.scanner.scan_pyproject_toml(str(pyproject_file))
                all_vulnerabilities.extend(vulns)
        
        # Generate report
        report = self._generate_audit_report(all_vulnerabilities, all_updates)
        
        return report
    
    def _generate_audit_report(
        self,
        vulnerabilities: List[Vulnerability],
        updates: List[DependencyUpdate]
    ) -> Dict:
        """Generate comprehensive audit report."""
        # Categorize vulnerabilities by severity
        vuln_by_severity = {}
        for severity in VulnerabilitySeverity:
            vuln_by_severity[severity.value] = [
                v for v in vulnerabilities if v.severity == severity
            ]
        
        # Categorize updates by type
        updates_by_type = {}
        for update_type in ['major', 'minor', 'patch']:
            updates_by_type[update_type] = [
                u for u in updates if u.update_type == update_type
            ]
        
        # Calculate summary statistics
        total_vulns = len(vulnerabilities)
        critical_vulns = len(vuln_by_severity.get('critical', []))
        high_vulns = len(vuln_by_severity.get('high', []))
        
        security_updates = [u for u in updates if u.security_fixes]
        recommended_updates = [u for u in updates if u.confidence > 0.7]
        
        report = {
            'summary': {
                'total_vulnerabilities': total_vulns,
                'critical_vulnerabilities': critical_vulns,
                'high_vulnerabilities': high_vulns,
                'total_available_updates': len(updates),
                'security_updates': len(security_updates),
                'recommended_updates': len(recommended_updates),
                'audit_timestamp': datetime.utcnow().isoformat()
            },
            'vulnerabilities': {
                'by_severity': {
                    severity: [asdict(v) for v in vulns]
                    for severity, vulns in vuln_by_severity.items()
                },
                'total': [asdict(v) for v in vulnerabilities]
            },
            'updates': {
                'by_type': {
                    update_type: [asdict(u) for u in updates]
                    for update_type, updates in updates_by_type.items()
                },
                'security_related': [asdict(u) for u in security_updates],
                'recommended': [asdict(u) for u in recommended_updates],
                'total': [asdict(u) for u in updates]
            },
            'recommendations': self._generate_recommendations(vulnerabilities, updates)
        }
        
        return report
    
    def _generate_recommendations(
        self,
        vulnerabilities: List[Vulnerability],
        updates: List[DependencyUpdate]
    ) -> List[Dict]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Critical security updates
        critical_security = [
            u for u in updates 
            if any(v.severity == VulnerabilitySeverity.CRITICAL for v in u.security_fixes)
        ]
        
        if critical_security:
            recommendations.append({
                'priority': 'critical',
                'action': 'immediate_security_updates',
                'description': f'Update {len(critical_security)} packages with critical security vulnerabilities',
                'packages': [u.package for u in critical_security],
                'timeline': 'Within 24 hours'
            })
        
        # High-confidence updates
        high_confidence = [u for u in updates if u.confidence > 0.8 and u.risk_level == 'low']
        
        if high_confidence:
            recommendations.append({
                'priority': 'high',
                'action': 'safe_updates',
                'description': f'Apply {len(high_confidence)} low-risk, high-confidence updates',
                'packages': [u.package for u in high_confidence],
                'timeline': 'Within 1 week'
            })
        
        # Review needed updates
        review_needed = [u for u in updates if u.breaking_changes or u.risk_level == 'high']
        
        if review_needed:
            recommendations.append({
                'priority': 'medium',
                'action': 'manual_review',
                'description': f'Review {len(review_needed)} updates that may have breaking changes',
                'packages': [u.package for u in review_needed],
                'timeline': 'Within 2 weeks'
            })
        
        return recommendations
    
    def _load_config(self, config_path: Optional[str]) -> Dict:
        """Load configuration."""
        default_config = {
            'update_strategy': 'moderate',
            'auto_update_patch': True,
            'auto_update_security': True,
            'exclude_packages': [],
            'notification_channels': ['email', 'slack']
        }
        
        if config_path and Path(config_path).exists():
            try:
                with open(config_path, 'r') as f:
                    user_config = json.load(f)
                default_config.update(user_config)
            except Exception as e:
                self.logger.warning(f"Could not load config: {e}")
        
        return default_config


async def main():
    """Main entry point for dependency management."""
    manager = DependencyManager()
    
    print("🔍 Performing dependency audit...")
    
    # Audit current project
    audit_report = await manager.audit_dependencies('.')
    
    # Display summary
    summary = audit_report['summary']
    print(f"\n📊 Audit Summary:")
    print(f"   Total Vulnerabilities: {summary['total_vulnerabilities']}")
    print(f"   Critical: {summary['critical_vulnerabilities']}")
    print(f"   High: {summary['high_vulnerabilities']}")
    print(f"   Available Updates: {summary['total_available_updates']}")
    print(f"   Security Updates: {summary['security_updates']}")
    print(f"   Recommended Updates: {summary['recommended_updates']}")
    
    # Display recommendations
    recommendations = audit_report['recommendations']
    if recommendations:
        print(f"\n🎯 Recommendations:")
        for i, rec in enumerate(recommendations, 1):
            print(f"{i}. {rec['description']} ({rec['priority']} priority)")
            print(f"   Timeline: {rec['timeline']}")
            print(f"   Packages: {', '.join(rec['packages'][:5])}")
            if len(rec['packages']) > 5:
                print(f"   ... and {len(rec['packages']) - 5} more")
            print()
    
    # Save detailed report
    with open('dependency_audit_report.json', 'w') as f:
        json.dump(audit_report, f, indent=2, default=str)
    
    print("📄 Detailed report saved to dependency_audit_report.json")


if __name__ == "__main__":
    asyncio.run(main())