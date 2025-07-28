#!/usr/bin/env python3
"""
Generate Software Bill of Materials (SBOM) for the project.

This script generates SBOM in multiple formats (SPDX, CycloneDX) to track
all dependencies and their security status.
"""

import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

import pkg_resources
import yaml


class SBOMGenerator:
    """Generate Software Bill of Materials."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.project_name = "agent-orchestrated-etl"
        self.project_version = "0.0.1"
        self.timestamp = datetime.utcnow().isoformat() + "Z"
    
    def get_python_dependencies(self) -> List[Dict[str, Any]]:
        """Get Python package dependencies."""
        dependencies = []
        
        try:
            # Get installed packages
            installed_packages = [d for d in pkg_resources.working_set]
            
            for package in installed_packages:
                dep_info = {
                    "name": package.project_name,
                    "version": package.version,
                    "type": "python-package",
                    "location": package.location,
                    "dependencies": []
                }
                
                # Get package dependencies
                try:
                    requires = package.requires()
                    dep_info["dependencies"] = [str(req) for req in requires]
                except Exception:
                    pass
                
                dependencies.append(dep_info)
        
        except Exception as e:
            print(f"Error getting Python dependencies: {e}")
        
        return dependencies
    
    def get_system_dependencies(self) -> List[Dict[str, Any]]:
        """Get system-level dependencies."""
        dependencies = []
        
        # Get from Dockerfile
        dockerfile_path = self.project_root / "Dockerfile"
        if dockerfile_path.exists():
            try:
                with open(dockerfile_path, 'r') as f:
                    content = f.read()
                
                # Parse base images
                for line in content.split('\n'):
                    if line.strip().startswith('FROM '):
                        image = line.strip().split()[1]
                        dependencies.append({
                            "name": image.split(':')[0],
                            "version": image.split(':')[1] if ':' in image else "latest",
                            "type": "container-image"
                        })
            except Exception as e:
                print(f"Error parsing Dockerfile: {e}")
        
        # Get from docker-compose.yml
        compose_path = self.project_root / "docker-compose.yml"
        if compose_path.exists():
            try:
                with open(compose_path, 'r') as f:
                    compose_data = yaml.safe_load(f)
                
                services = compose_data.get('services', {})
                for service_name, service_config in services.items():
                    if 'image' in service_config:
                        image = service_config['image']
                        dependencies.append({
                            "name": image.split(':')[0],
                            "version": image.split(':')[1] if ':' in image else "latest",
                            "type": "container-image",
                            "service": service_name
                        })
            except Exception as e:
                print(f"Error parsing docker-compose.yml: {e}")
        
        return dependencies
    
    def check_vulnerabilities(self, dependencies: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Check for known vulnerabilities in dependencies."""
        vulnerabilities = {}
        
        # Run safety check for Python packages
        try:
            result = subprocess.run(
                ["safety", "check", "--json"],
                capture_output=True,
                text=True,
                cwd=self.project_root
            )
            
            if result.returncode == 0:
                safety_data = json.loads(result.stdout)
                for vuln in safety_data:
                    package_name = vuln.get("package")
                    if package_name not in vulnerabilities:
                        vulnerabilities[package_name] = []
                    
                    vulnerabilities[package_name].append({
                        "id": vuln.get("id"),
                        "advisory": vuln.get("advisory"),
                        "cve": vuln.get("cve"),
                        "affected_version": vuln.get("affected_version"),
                        "analyzed_version": vuln.get("analyzed_version")
                    })
        except Exception as e:
            print(f"Error running safety check: {e}")
        
        return vulnerabilities
    
    def generate_spdx_format(self, dependencies: List[Dict[str, Any]], 
                           vulnerabilities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Generate SBOM in SPDX format."""
        spdx_doc = {
            "spdxVersion": "SPDX-2.3",
            "creationInfo": {
                "created": self.timestamp,
                "creators": ["Tool: agent-orchestrated-etl-sbom-generator"],
                "licenseListVersion": "3.19"
            },
            "name": f"{self.project_name}-sbom",
            "documentNamespace": f"https://terragon-labs.com/{self.project_name}/sbom/{self.timestamp}",
            "packages": []
        }
        
        # Add main package
        main_package = {
            "SPDXID": "SPDXRef-Package-root",
            "name": self.project_name,
            "versionInfo": self.project_version,
            "downloadLocation": "NOASSERTION",
            "filesAnalyzed": False,
            "licenseConcluded": "MIT",
            "copyrightText": "Copyright 2025 Terragon Labs"
        }
        spdx_doc["packages"].append(main_package)
        
        # Add dependencies
        for i, dep in enumerate(dependencies, 1):
            package = {
                "SPDXID": f"SPDXRef-Package-{i}",
                "name": dep["name"],
                "versionInfo": dep["version"],
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "licenseConcluded": "NOASSERTION"
            }
            
            # Add vulnerability information
            if dep["name"] in vulnerabilities:
                package["vulnerabilities"] = vulnerabilities[dep["name"]]
            
            spdx_doc["packages"].append(package)
        
        return spdx_doc
    
    def generate_cyclonedx_format(self, dependencies: List[Dict[str, Any]], 
                                vulnerabilities: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Generate SBOM in CycloneDX format."""
        cyclone_doc = {
            "bomFormat": "CycloneDX",
            "specVersion": "1.4",
            "serialNumber": f"urn:uuid:{self.project_name}-{self.timestamp}",
            "version": 1,
            "metadata": {
                "timestamp": self.timestamp,
                "tools": [
                    {
                        "vendor": "Terragon Labs",
                        "name": "agent-orchestrated-etl-sbom-generator",
                        "version": "1.0.0"
                    }
                ],
                "component": {
                    "type": "application",
                    "bom-ref": "root-component",
                    "name": self.project_name,
                    "version": self.project_version,
                    "licenses": [{"license": {"id": "MIT"}}]
                }
            },
            "components": []
        }
        
        # Add dependencies as components
        for dep in dependencies:
            component = {
                "type": "library" if dep["type"] == "python-package" else "container",
                "bom-ref": f"{dep['name']}@{dep['version']}",
                "name": dep["name"],
                "version": dep["version"],
                "purl": self._generate_purl(dep)
            }
            
            # Add vulnerability information
            if dep["name"] in vulnerabilities:
                component["vulnerabilities"] = [
                    {
                        "id": vuln["id"],
                        "source": {"name": "PyUp.io Safety"},
                        "description": vuln["advisory"],
                        "severity": "unknown"  # Safety doesn't provide severity
                    }
                    for vuln in vulnerabilities[dep["name"]]
                ]
            
            cyclone_doc["components"].append(component)
        
        return cyclone_doc
    
    def _generate_purl(self, dependency: Dict[str, Any]) -> str:
        """Generate Package URL (PURL) for dependency."""
        if dependency["type"] == "python-package":
            return f"pkg:pypi/{dependency['name']}@{dependency['version']}"
        elif dependency["type"] == "container-image":
            return f"pkg:docker/{dependency['name']}@{dependency['version']}"
        else:
            return f"pkg:generic/{dependency['name']}@{dependency['version']}"
    
    def generate(self, output_dir: Optional[Path] = None) -> None:
        """Generate SBOM files."""
        if output_dir is None:
            output_dir = self.project_root / "sbom"
        
        output_dir.mkdir(exist_ok=True)
        
        print("Collecting dependencies...")
        python_deps = self.get_python_dependencies()
        system_deps = self.get_system_dependencies()
        all_deps = python_deps + system_deps
        
        print(f"Found {len(all_deps)} dependencies")
        
        print("Checking for vulnerabilities...")
        vulnerabilities = self.check_vulnerabilities(all_deps)
        
        if vulnerabilities:
            print(f"Found vulnerabilities in {len(vulnerabilities)} packages")
        else:
            print("No known vulnerabilities found")
        
        # Generate SPDX format
        print("Generating SPDX SBOM...")
        spdx_sbom = self.generate_spdx_format(all_deps, vulnerabilities)
        spdx_path = output_dir / f"{self.project_name}-sbom.spdx.json"
        with open(spdx_path, 'w') as f:
            json.dump(spdx_sbom, f, indent=2)
        print(f"SPDX SBOM written to {spdx_path}")
        
        # Generate CycloneDX format
        print("Generating CycloneDX SBOM...")
        cyclonedx_sbom = self.generate_cyclonedx_format(all_deps, vulnerabilities)
        cyclonedx_path = output_dir / f"{self.project_name}-sbom.cyclonedx.json"
        with open(cyclonedx_path, 'w') as f:
            json.dump(cyclonedx_sbom, f, indent=2)
        print(f"CycloneDX SBOM written to {cyclonedx_path}")
        
        # Generate summary report
        print("Generating summary report...")
        summary = {
            "project": self.project_name,
            "version": self.project_version,
            "generated": self.timestamp,
            "total_dependencies": len(all_deps),
            "python_packages": len(python_deps),
            "system_dependencies": len(system_deps),
            "vulnerabilities": len(vulnerabilities),
            "vulnerable_packages": list(vulnerabilities.keys())
        }
        
        summary_path = output_dir / f"{self.project_name}-sbom-summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary report written to {summary_path}")


def main():
    """Main entry point."""
    project_root = Path(__file__).parent.parent
    
    # Parse command line arguments
    output_dir = None
    if len(sys.argv) > 1:
        output_dir = Path(sys.argv[1])
    
    generator = SBOMGenerator(project_root)
    generator.generate(output_dir)


if __name__ == "__main__":
    main()