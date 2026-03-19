"""
PipelineConfig: YAML-based pipeline definition.

A pipeline is a sequence of stages. Each stage names an agent role,
carries optional config, and may declare source/destination paths.

Example YAML
------------
pipeline:
  name: csv-to-json
  stages:
    - name: extract
      role: extractor
      source: data/input.csv

    - name: transform
      role: transformer
      operations:
        - normalize
        - deduplicate

    - name: load
      role: loader
      destination: output/result.json
"""

from __future__ import annotations

import io
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


# Optional YAML support — falls back gracefully when PyYAML absent
try:
    import yaml as _yaml
    _YAML_AVAILABLE = True
except ImportError:
    _yaml = None  # type: ignore[assignment]
    _YAML_AVAILABLE = False


@dataclass
class PipelineStage:
    """A single stage in an ETL pipeline."""
    name: str
    role: str                                       # AgentRole value
    config: Dict[str, Any] = field(default_factory=dict)

    # Convenience shortcuts surfaced from config
    @property
    def source(self) -> Optional[str]:
        return self.config.get("source")

    @property
    def destination(self) -> Optional[str]:
        return self.config.get("destination")

    @property
    def operations(self) -> List[str]:
        return self.config.get("operations", [])


@dataclass
class PipelineConfig:
    """
    Full pipeline configuration loaded from YAML or built programmatically.

    Parameters
    ----------
    name:
        Human-readable pipeline name.
    stages:
        Ordered list of PipelineStage objects.
    max_retries:
        How many times to retry a failing stage before giving up.
    stop_on_failure:
        If True, halt the pipeline on the first stage failure.
    metadata:
        Arbitrary user-defined key/value pairs stored alongside the config.
    """

    name: str = "unnamed-pipeline"
    stages: List[PipelineStage] = field(default_factory=list)
    max_retries: int = 2
    stop_on_failure: bool = True
    metadata: Dict[str, Any] = field(default_factory=dict)

    # ------------------------------------------------------------------
    # Constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml(cls, source: Union[str, Path]) -> "PipelineConfig":
        """
        Load a PipelineConfig from a YAML file path or YAML string.

        The YAML document must contain a top-level ``pipeline`` key.
        """
        if not _YAML_AVAILABLE:
            raise RuntimeError(
                "PyYAML is required to load pipeline configs from YAML. "
                "Install it with: pip install pyyaml"
            )

        if isinstance(source, Path) or (isinstance(source, str) and "\n" not in source):
            path = Path(source)
            with path.open("r", encoding="utf-8") as fh:
                doc = _yaml.safe_load(fh)
        else:
            doc = _yaml.safe_load(io.StringIO(source))

        return cls._from_dict(doc)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """Build a PipelineConfig from a plain Python dictionary."""
        return cls._from_dict(data)

    # ------------------------------------------------------------------
    # Internal parsing
    # ------------------------------------------------------------------

    @classmethod
    def _from_dict(cls, doc: Dict[str, Any]) -> "PipelineConfig":
        pipeline_doc = doc.get("pipeline", doc)

        stages_raw: List[Dict[str, Any]] = pipeline_doc.get("stages", [])
        stages = []
        for s in stages_raw:
            # Everything except name/role goes into config
            name = s.pop("name", "unnamed-stage")
            role = s.pop("role", "extractor")
            stages.append(PipelineStage(name=name, role=role, config=dict(s)))

        return cls(
            name=pipeline_doc.get("name", "unnamed-pipeline"),
            stages=stages,
            max_retries=int(pipeline_doc.get("max_retries", 2)),
            stop_on_failure=bool(pipeline_doc.get("stop_on_failure", True)),
            metadata=pipeline_doc.get("metadata", {}),
        )

    # ------------------------------------------------------------------
    # Serialization
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        return {
            "pipeline": {
                "name": self.name,
                "max_retries": self.max_retries,
                "stop_on_failure": self.stop_on_failure,
                "metadata": self.metadata,
                "stages": [
                    {"name": s.name, "role": s.role, **s.config}
                    for s in self.stages
                ],
            }
        }

    def to_yaml(self) -> str:
        if not _YAML_AVAILABLE:
            raise RuntimeError("PyYAML is required. Install with: pip install pyyaml")
        return _yaml.dump(self.to_dict(), default_flow_style=False)

    def __repr__(self) -> str:
        return (
            f"PipelineConfig(name={self.name!r}, "
            f"stages={[s.name for s in self.stages]!r})"
        )
