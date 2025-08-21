"""Intelligent Data Lineage Tracking with AI-powered impact analysis and automated documentation."""

from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import networkx as nx
import numpy as np

from .exceptions import DataProcessingException
from .logging_config import get_logger


@dataclass
class DataAsset:
    """Represents a data asset in the lineage graph."""
    asset_id: str
    name: str
    asset_type: str  # table, file, api, model, etc.
    schema_hash: str
    location: str
    created_at: datetime
    updated_at: datetime
    metadata: Dict[str, Any]
    quality_score: float = 0.0


@dataclass
class LineageEdge:
    """Represents a relationship between data assets."""
    source_asset_id: str
    target_asset_id: str
    transformation_type: str
    transformation_code: Optional[str]
    created_at: datetime
    metadata: Dict[str, Any]
    confidence_score: float = 1.0


@dataclass
class ImpactAnalysis:
    """Results of impact analysis for a data asset change."""
    asset_id: str
    affected_assets: List[str]
    impact_severity: str  # low, medium, high, critical
    estimated_processing_time: float
    required_actions: List[str]
    risk_factors: List[str]


class LineageGraph:
    """Advanced graph structure for data lineage with AI-powered analysis."""

    def __init__(self):
        self.graph = nx.DiGraph()
        self.assets: Dict[str, DataAsset] = {}
        self.edges: Dict[Tuple[str, str], LineageEdge] = {}
        self.logger = get_logger("lineage.graph")

    def add_asset(self, asset: DataAsset) -> None:
        """Add a data asset to the lineage graph."""
        self.assets[asset.asset_id] = asset
        self.graph.add_node(
            asset.asset_id,
            name=asset.name,
            asset_type=asset.asset_type,
            quality_score=asset.quality_score,
            created_at=asset.created_at,
            updated_at=asset.updated_at
        )
        self.logger.info(f"Added asset {asset.name} ({asset.asset_id}) to lineage graph")

    def add_relationship(self, edge: LineageEdge) -> None:
        """Add a lineage relationship between assets."""
        edge_key = (edge.source_asset_id, edge.target_asset_id)
        self.edges[edge_key] = edge

        self.graph.add_edge(
            edge.source_asset_id,
            edge.target_asset_id,
            transformation_type=edge.transformation_type,
            confidence_score=edge.confidence_score,
            created_at=edge.created_at
        )
        self.logger.info(f"Added lineage edge: {edge.source_asset_id} -> {edge.target_asset_id}")

    def get_upstream_assets(self, asset_id: str, max_depth: int = 10) -> List[str]:
        """Get all upstream assets that contribute to the given asset."""
        if asset_id not in self.graph:
            return []

        upstream = set()
        visited = set()
        queue = [(asset_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth or current_id in visited:
                continue

            visited.add(current_id)
            predecessors = list(self.graph.predecessors(current_id))
            upstream.update(predecessors)

            for pred in predecessors:
                queue.append((pred, depth + 1))

        return list(upstream)

    def get_downstream_assets(self, asset_id: str, max_depth: int = 10) -> List[str]:
        """Get all downstream assets that depend on the given asset."""
        if asset_id not in self.graph:
            return []

        downstream = set()
        visited = set()
        queue = [(asset_id, 0)]

        while queue:
            current_id, depth = queue.pop(0)
            if depth >= max_depth or current_id in visited:
                continue

            visited.add(current_id)
            successors = list(self.graph.successors(current_id))
            downstream.update(successors)

            for succ in successors:
                queue.append((succ, depth + 1))

        return list(downstream)

    def calculate_criticality_score(self, asset_id: str) -> float:
        """Calculate how critical an asset is based on its position in the graph."""
        if asset_id not in self.graph:
            return 0.0

        # Factors: downstream count, quality score, frequency of access
        downstream_count = len(self.get_downstream_assets(asset_id))
        upstream_count = len(self.get_upstream_assets(asset_id))

        # More downstream dependencies = higher criticality
        downstream_factor = min(downstream_count / 10.0, 1.0)

        # Assets that aggregate from many sources are critical
        upstream_factor = min(upstream_count / 20.0, 0.5)

        # Quality score contributes to criticality
        asset = self.assets.get(asset_id)
        quality_factor = asset.quality_score if asset else 0.5

        # Centrality in the graph
        try:
            betweenness = nx.betweenness_centrality(self.graph)[asset_id]
        except:
            betweenness = 0.0

        criticality = (downstream_factor * 0.4 +
                      upstream_factor * 0.2 +
                      quality_factor * 0.2 +
                      betweenness * 0.2)

        return min(criticality, 1.0)


class IntelligentLineageTracker:
    """AI-powered data lineage tracking with impact analysis and automated documentation."""

    def __init__(self, storage_path: Optional[str] = None):
        self.logger = get_logger("intelligent_lineage")
        self.lineage_graph = LineageGraph()
        self.storage_path = Path(storage_path) if storage_path else Path("lineage_data")
        self.storage_path.mkdir(exist_ok=True)

        # Machine learning models for prediction
        self.impact_predictor = None
        self.quality_predictor = None
        self._initialize_ml_models()

    def _initialize_ml_models(self) -> None:
        """Initialize machine learning models for lineage analysis."""
        # In a production system, these would be pre-trained models
        # For this research implementation, we'll use statistical approximations
        self.logger.info("Initializing AI models for lineage analysis")

    async def track_transformation(
        self,
        source_assets: List[str],
        target_asset: str,
        transformation_code: str,
        transformation_type: str = "etl"
    ) -> None:
        """Track a data transformation and update lineage."""
        try:
            # Create transformation hash for change detection
            import hashlib
            transformation_hash = hashlib.md5(transformation_code.encode()).hexdigest()

            # Record the transformation
            for source_id in source_assets:
                edge = LineageEdge(
                    source_asset_id=source_id,
                    target_asset_id=target_asset,
                    transformation_type=transformation_type,
                    transformation_code=transformation_code,
                    created_at=datetime.now(),
                    metadata={
                        "transformation_hash": transformation_hash,
                        "tracked_at": time.time()
                    },
                    confidence_score=self._calculate_transformation_confidence(
                        transformation_code, transformation_type
                    )
                )

                self.lineage_graph.add_relationship(edge)

            # Analyze impact of this transformation
            await self._analyze_transformation_impact(target_asset, transformation_code)

            # Update quality predictions
            await self._update_quality_predictions(target_asset)

            self.logger.info(f"Tracked transformation: {source_assets} -> {target_asset}")

        except Exception as e:
            self.logger.error(f"Failed to track transformation: {e}")
            raise DataProcessingException(f"Lineage tracking failed: {e}")

    def _calculate_transformation_confidence(
        self,
        transformation_code: str,
        transformation_type: str
    ) -> float:
        """Calculate confidence score for a transformation based on code analysis."""
        confidence = 0.8  # Base confidence

        # Analyze code complexity and patterns
        code_lines = len(transformation_code.split('\n'))

        # Simple transformations get higher confidence
        if code_lines < 10:
            confidence += 0.1
        elif code_lines > 50:
            confidence -= 0.1

        # Known transformation types get higher confidence
        reliable_types = ["filter", "select", "join", "aggregate"]
        if transformation_type in reliable_types:
            confidence += 0.1

        # Check for error handling
        if "try:" in transformation_code and "except:" in transformation_code:
            confidence += 0.05

        # Check for validation
        if any(word in transformation_code.lower() for word in ["validate", "check", "assert"]):
            confidence += 0.05

        return max(0.1, min(1.0, confidence))

    async def _analyze_transformation_impact(
        self,
        target_asset: str,
        transformation_code: str
    ) -> None:
        """Analyze the potential impact of a transformation."""
        try:
            # Get all downstream assets that could be affected
            affected_assets = self.lineage_graph.get_downstream_assets(target_asset)

            # Calculate impact severity based on multiple factors
            severity_score = 0.0

            # Number of affected assets
            if len(affected_assets) > 10:
                severity_score += 0.4
            elif len(affected_assets) > 5:
                severity_score += 0.2

            # Criticality of affected assets
            for asset_id in affected_assets:
                criticality = self.lineage_graph.calculate_criticality_score(asset_id)
                severity_score += criticality * 0.1

            # Complexity of transformation
            code_complexity = len(transformation_code.split('\n')) / 100.0
            severity_score += min(code_complexity, 0.3)

            # Determine severity level
            if severity_score > 0.7:
                severity = "critical"
            elif severity_score > 0.5:
                severity = "high"
            elif severity_score > 0.3:
                severity = "medium"
            else:
                severity = "low"

            # Generate recommended actions
            actions = self._generate_impact_actions(severity, affected_assets)

            impact_analysis = ImpactAnalysis(
                asset_id=target_asset,
                affected_assets=affected_assets,
                impact_severity=severity,
                estimated_processing_time=len(affected_assets) * 2.5,
                required_actions=actions,
                risk_factors=self._identify_risk_factors(transformation_code)
            )

            # Store impact analysis
            await self._store_impact_analysis(impact_analysis)

            self.logger.info(f"Impact analysis completed for {target_asset}: {severity} severity")

        except Exception as e:
            self.logger.error(f"Impact analysis failed: {e}")

    def _generate_impact_actions(self, severity: str, affected_assets: List[str]) -> List[str]:
        """Generate recommended actions based on impact severity."""
        actions = []

        if severity in ["critical", "high"]:
            actions.extend([
                "Schedule maintenance window for affected systems",
                "Notify downstream system owners",
                "Prepare rollback procedures",
                "Monitor data quality metrics closely"
            ])

        if severity == "critical":
            actions.extend([
                "Execute in staged rollout",
                "Implement circuit breakers for affected pipelines",
                "Set up real-time alerting"
            ])

        if len(affected_assets) > 5:
            actions.append("Consider batch processing to reduce system load")

        return actions

    def _identify_risk_factors(self, transformation_code: str) -> List[str]:
        """Identify potential risk factors in transformation code."""
        risks = []

        code_lower = transformation_code.lower()

        # Check for risky operations
        if "drop" in code_lower:
            risks.append("Data deletion operation detected")

        if "delete" in code_lower:
            risks.append("Record deletion operation detected")

        if "update" in code_lower and "where" not in code_lower:
            risks.append("Bulk update without WHERE clause")

        # Check for performance risks
        if code_lower.count("join") > 3:
            risks.append("Multiple joins may impact performance")

        if "distinct" in code_lower and "order by" in code_lower:
            risks.append("DISTINCT with ORDER BY can be expensive")

        # Check for error handling
        if "try:" not in transformation_code:
            risks.append("No error handling implemented")

        return risks

    async def _update_quality_predictions(self, asset_id: str) -> None:
        """Update quality predictions for an asset based on lineage."""
        try:
            # Get historical quality data
            upstream_assets = self.lineage_graph.get_upstream_assets(asset_id)

            # Calculate predicted quality based on upstream quality
            if upstream_assets:
                upstream_qualities = []
                for upstream_id in upstream_assets:
                    if upstream_id in self.lineage_graph.assets:
                        upstream_qualities.append(
                            self.lineage_graph.assets[upstream_id].quality_score
                        )

                if upstream_qualities:
                    # Quality prediction: weighted average with decay for distance
                    predicted_quality = np.mean(upstream_qualities) * 0.95  # Small quality loss

                    # Update asset quality prediction
                    if asset_id in self.lineage_graph.assets:
                        self.lineage_graph.assets[asset_id].quality_score = predicted_quality

                    self.logger.debug(f"Updated quality prediction for {asset_id}: {predicted_quality:.3f}")

        except Exception as e:
            self.logger.error(f"Quality prediction update failed: {e}")

    async def _store_impact_analysis(self, analysis: ImpactAnalysis) -> None:
        """Store impact analysis results for historical tracking."""
        try:
            analysis_file = self.storage_path / f"impact_analysis_{analysis.asset_id}_{int(time.time())}.json"

            with open(analysis_file, 'w') as f:
                json.dump(asdict(analysis), f, indent=2, default=str)

            self.logger.debug(f"Stored impact analysis: {analysis_file}")

        except Exception as e:
            self.logger.error(f"Failed to store impact analysis: {e}")

    async def generate_lineage_report(self, asset_id: str) -> Dict[str, Any]:
        """Generate a comprehensive lineage report for an asset."""
        try:
            if asset_id not in self.lineage_graph.assets:
                raise ValueError(f"Asset {asset_id} not found in lineage graph")

            asset = self.lineage_graph.assets[asset_id]
            upstream = self.lineage_graph.get_upstream_assets(asset_id)
            downstream = self.lineage_graph.get_downstream_assets(asset_id)
            criticality = self.lineage_graph.calculate_criticality_score(asset_id)

            report = {
                "asset_info": asdict(asset),
                "lineage_summary": {
                    "upstream_count": len(upstream),
                    "downstream_count": len(downstream),
                    "criticality_score": criticality,
                    "graph_position": "source" if not upstream else "intermediate" if downstream else "sink"
                },
                "upstream_assets": [
                    {
                        "asset_id": uid,
                        "name": self.lineage_graph.assets[uid].name if uid in self.lineage_graph.assets else "Unknown",
                        "asset_type": self.lineage_graph.assets[uid].asset_type if uid in self.lineage_graph.assets else "Unknown"
                    }
                    for uid in upstream
                ],
                "downstream_assets": [
                    {
                        "asset_id": did,
                        "name": self.lineage_graph.assets[did].name if did in self.lineage_graph.assets else "Unknown",
                        "asset_type": self.lineage_graph.assets[did].asset_type if did in self.lineage_graph.assets else "Unknown"
                    }
                    for did in downstream
                ],
                "risk_assessment": {
                    "impact_radius": len(downstream),
                    "dependency_depth": len(upstream),
                    "criticality_level": "critical" if criticality > 0.8 else "high" if criticality > 0.6 else "medium" if criticality > 0.4 else "low"
                },
                "generated_at": datetime.now().isoformat()
            }

            self.logger.info(f"Generated lineage report for {asset_id}")
            return report

        except Exception as e:
            self.logger.error(f"Failed to generate lineage report: {e}")
            raise DataProcessingException(f"Lineage report generation failed: {e}")

    async def detect_circular_dependencies(self) -> List[List[str]]:
        """Detect circular dependencies in the lineage graph."""
        try:
            cycles = list(nx.simple_cycles(self.lineage_graph.graph))

            if cycles:
                self.logger.warning(f"Detected {len(cycles)} circular dependencies in lineage graph")
                for i, cycle in enumerate(cycles):
                    cycle_names = [
                        self.lineage_graph.assets[asset_id].name
                        if asset_id in self.lineage_graph.assets else asset_id
                        for asset_id in cycle
                    ]
                    self.logger.warning(f"Cycle {i+1}: {' -> '.join(cycle_names)} -> {cycle_names[0]}")

            return cycles

        except Exception as e:
            self.logger.error(f"Circular dependency detection failed: {e}")
            return []

    async def suggest_optimization_opportunities(self) -> List[Dict[str, Any]]:
        """Suggest optimization opportunities based on lineage analysis."""
        opportunities = []

        try:
            # Find assets with many upstream dependencies (potential bottlenecks)
            for asset_id, asset in self.lineage_graph.assets.items():
                upstream_count = len(self.lineage_graph.get_upstream_assets(asset_id))
                downstream_count = len(self.lineage_graph.get_downstream_assets(asset_id))

                if upstream_count > 10:
                    opportunities.append({
                        "type": "consolidation",
                        "asset_id": asset_id,
                        "asset_name": asset.name,
                        "description": f"Asset has {upstream_count} upstream dependencies - consider consolidation",
                        "priority": "high" if upstream_count > 20 else "medium",
                        "estimated_benefit": "Reduced complexity and improved maintainability"
                    })

                if downstream_count > 15:
                    opportunities.append({
                        "type": "caching",
                        "asset_id": asset_id,
                        "asset_name": asset.name,
                        "description": f"Asset serves {downstream_count} downstream consumers - consider caching",
                        "priority": "high",
                        "estimated_benefit": "Improved performance and reduced load"
                    })

                # Low quality assets with high criticality
                criticality = self.lineage_graph.calculate_criticality_score(asset_id)
                if asset.quality_score < 0.7 and criticality > 0.6:
                    opportunities.append({
                        "type": "quality_improvement",
                        "asset_id": asset_id,
                        "asset_name": asset.name,
                        "description": f"Critical asset with low quality score ({asset.quality_score:.2f})",
                        "priority": "critical",
                        "estimated_benefit": "Improved downstream data quality"
                    })

            self.logger.info(f"Identified {len(opportunities)} optimization opportunities")
            return opportunities

        except Exception as e:
            self.logger.error(f"Optimization analysis failed: {e}")
            return []

    def export_lineage_graph(self, format: str = "json") -> str:
        """Export the lineage graph in various formats."""
        try:
            if format == "json":
                export_data = {
                    "assets": {aid: asdict(asset) for aid, asset in self.lineage_graph.assets.items()},
                    "edges": {f"{e[0]}->{e[1]}": asdict(edge) for e, edge in self.lineage_graph.edges.items()},
                    "metadata": {
                        "total_assets": len(self.lineage_graph.assets),
                        "total_edges": len(self.lineage_graph.edges),
                        "exported_at": datetime.now().isoformat()
                    }
                }
                return json.dumps(export_data, indent=2, default=str)

            elif format == "dot":
                # GraphViz DOT format for visualization
                dot_lines = ["digraph lineage {", "  rankdir=LR;"]

                for asset_id, asset in self.lineage_graph.assets.items():
                    label = f"{asset.name}\\n({asset.asset_type})"
                    color = "red" if asset.quality_score < 0.5 else "yellow" if asset.quality_score < 0.8 else "green"
                    dot_lines.append(f'  "{asset_id}" [label="{label}", color={color}];')

                for edge_key, edge in self.lineage_graph.edges.items():
                    source, target = edge_key
                    dot_lines.append(f'  "{source}" -> "{target}" [label="{edge.transformation_type}"];')

                dot_lines.append("}")
                return "\n".join(dot_lines)

            else:
                raise ValueError(f"Unsupported export format: {format}")

        except Exception as e:
            self.logger.error(f"Graph export failed: {e}")
            raise DataProcessingException(f"Graph export failed: {e}")


# Integration with ETL system
async def track_etl_lineage(
    tracker: IntelligentLineageTracker,
    pipeline_id: str,
    source_configs: List[Dict[str, Any]],
    target_config: Dict[str, Any],
    transformation_code: str
) -> None:
    """Track lineage for an ETL pipeline execution."""
    try:
        # Register source assets
        source_asset_ids = []
        for source_config in source_configs:
            source_asset = DataAsset(
                asset_id=f"source_{source_config.get('name', str(uuid4()))}",
                name=source_config.get('name', 'Unknown Source'),
                asset_type=source_config.get('type', 'unknown'),
                schema_hash=str(hash(str(source_config))),
                location=source_config.get('location', ''),
                created_at=datetime.now(),
                updated_at=datetime.now(),
                metadata=source_config
            )
            tracker.lineage_graph.add_asset(source_asset)
            source_asset_ids.append(source_asset.asset_id)

        # Register target asset
        target_asset = DataAsset(
            asset_id=f"target_{target_config.get('name', str(uuid4()))}",
            name=target_config.get('name', 'Unknown Target'),
            asset_type=target_config.get('type', 'unknown'),
            schema_hash=str(hash(str(target_config))),
            location=target_config.get('location', ''),
            created_at=datetime.now(),
            updated_at=datetime.now(),
            metadata=target_config
        )
        tracker.lineage_graph.add_asset(target_asset)

        # Track the transformation
        await tracker.track_transformation(
            source_assets=source_asset_ids,
            target_asset=target_asset.asset_id,
            transformation_code=transformation_code,
            transformation_type="etl_pipeline"
        )

    except Exception as e:
        get_logger("etl_lineage").error(f"ETL lineage tracking failed: {e}")
        raise DataProcessingException(f"ETL lineage tracking failed: {e}")
