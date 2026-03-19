"""
ETLAgent: Specialized, self-contained ETL agent.

Each agent has a role (extractor, transformer, loader, validator) and
executes its stage of a pipeline independently. No LangChain or
external ML dependencies — pure Python standard library.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class AgentRole(str, Enum):
    """The specialized role of an ETL agent."""
    EXTRACTOR = "extractor"
    TRANSFORMER = "transformer"
    LOADER = "loader"
    VALIDATOR = "validator"


@dataclass
class AgentResult:
    """The outcome of a single agent execution."""
    agent_id: str
    role: AgentRole
    status: str          # "success" | "failure" | "skipped"
    records_in: int
    records_out: int
    duration_seconds: float
    data: Any = None
    error: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


class ETLAgent:
    """
    A specialized ETL agent with a single role in a pipeline.

    Roles
    -----
    - **extractor**: reads raw data from a source (CSV file, dict list, …)
    - **transformer**: applies normalization, deduplication, and custom rules
    - **loader**: writes processed data to a destination (JSON file, dict store, …)
    - **validator**: checks data quality and emits a quality report

    Example
    -------
    >>> agent = ETLAgent(role=AgentRole.EXTRACTOR, name="csv-reader")
    >>> result = agent.run({"source": "data.csv"})
    """

    def __init__(
        self,
        role: AgentRole,
        name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
        custom_handler: Optional[Callable[[Dict[str, Any]], Any]] = None,
    ):
        self.role = AgentRole(role)
        self.agent_id = str(uuid.uuid4())
        self.name = name or f"{self.role.value}-{self.agent_id[:8]}"
        self.config = config or {}
        self.custom_handler = custom_handler
        self._log = logging.getLogger(f"etl.agent.{self.name}")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self, inputs: Dict[str, Any]) -> AgentResult:
        """Execute the agent's role and return an AgentResult."""
        t0 = time.monotonic()
        records_in = self._count_records(inputs.get("data"))

        self._log.info("Starting %s agent '%s'", self.role.value, self.name)
        try:
            if self.custom_handler:
                data = self.custom_handler(inputs)
            elif self.role == AgentRole.EXTRACTOR:
                data = self._extract(inputs)
            elif self.role == AgentRole.TRANSFORMER:
                data = self._transform(inputs)
            elif self.role == AgentRole.LOADER:
                data = self._load(inputs)
            elif self.role == AgentRole.VALIDATOR:
                data = self._validate(inputs)
            else:
                raise ValueError(f"Unknown agent role: {self.role}")

            records_out = self._count_records(data)
            duration = time.monotonic() - t0
            self._log.info(
                "Agent '%s' completed: %d -> %d records in %.3fs",
                self.name, records_in, records_out, duration,
            )
            return AgentResult(
                agent_id=self.agent_id,
                role=self.role,
                status="success",
                records_in=records_in,
                records_out=records_out,
                duration_seconds=duration,
                data=data,
            )

        except Exception as exc:
            duration = time.monotonic() - t0
            self._log.error("Agent '%s' failed: %s", self.name, exc, exc_info=True)
            return AgentResult(
                agent_id=self.agent_id,
                role=self.role,
                status="failure",
                records_in=records_in,
                records_out=0,
                duration_seconds=duration,
                error=str(exc),
            )

    # ------------------------------------------------------------------
    # Built-in role handlers
    # ------------------------------------------------------------------

    def _extract(self, inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract records from a CSV file or an inline data list."""
        source = inputs.get("source")

        # Inline list of dicts
        if isinstance(source, list):
            return list(source)

        # CSV file path
        if isinstance(source, (str, Path)):
            path = Path(source)
            if not path.exists():
                raise FileNotFoundError(f"Source file not found: {path}")
            with path.open(newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                records = [dict(row) for row in reader]
            self._log.info("Extracted %d records from %s", len(records), path)
            return records

        # CSV string
        if isinstance(source, str) and "\n" in source:
            reader = csv.DictReader(io.StringIO(source))
            return [dict(row) for row in reader]

        raise ValueError(
            f"Unsupported source type: {type(source).__name__}. "
            "Expected a file path, list of dicts, or CSV string."
        )

    def _transform(self, inputs: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Apply normalization and deduplication to records."""
        records: List[Dict[str, Any]] = inputs.get("data", [])
        if not isinstance(records, list):
            raise TypeError("Transformer expects 'data' to be a list of dicts.")

        operations: List[str] = self.config.get(
            "operations",
            inputs.get("operations", ["normalize", "deduplicate"]),
        )

        for op in operations:
            if op == "normalize":
                records = self._normalize(records)
            elif op == "deduplicate":
                records = self._deduplicate(records)
            elif op == "drop_nulls":
                records = [r for r in records if all(v not in (None, "", "NULL") for v in r.values())]
            elif op == "lowercase_keys":
                records = [{k.lower(): v for k, v in r.items()} for r in records]
            elif op == "strip_whitespace":
                records = [
                    {k: v.strip() if isinstance(v, str) else v for k, v in r.items()}
                    for r in records
                ]
            else:
                self._log.warning("Unknown transform operation '%s' — skipping", op)

        return records

    def _load(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Write records to a JSON file or return them as an in-memory dict."""
        records: List[Dict[str, Any]] = inputs.get("data", [])
        destination = inputs.get("destination") or self.config.get("destination")

        if destination:
            dest_path = Path(destination)
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            with dest_path.open("w", encoding="utf-8") as fh:
                json.dump(records, fh, indent=2, default=str)
            self._log.info("Loaded %d records to %s", len(records), dest_path)
            return {"destination": str(dest_path), "records_loaded": len(records)}

        # In-memory store
        return {"records": records, "records_loaded": len(records)}

    def _validate(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """Run basic quality checks and return a quality report."""
        records: List[Dict[str, Any]] = inputs.get("data", [])
        rules: Dict[str, Any] = self.config.get("rules", inputs.get("rules", {}))

        total = len(records)
        issues: List[str] = []
        null_counts: Dict[str, int] = {}

        required_fields: List[str] = rules.get("required_fields", [])
        max_null_pct: float = float(rules.get("max_null_pct", 20.0))

        for field_name in required_fields:
            missing = sum(1 for r in records if not r.get(field_name))
            null_counts[field_name] = missing
            pct = (missing / total * 100) if total else 0.0
            if pct > max_null_pct:
                issues.append(
                    f"Field '{field_name}' has {pct:.1f}% null values (threshold {max_null_pct}%)"
                )

        passed = len(issues) == 0
        report = {
            "total_records": total,
            "passed": passed,
            "issues": issues,
            "null_counts": null_counts,
            "quality_score": max(0.0, 1.0 - len(issues) / max(len(required_fields), 1)),
        }
        self._log.info(
            "Validation %s: %d records, %d issues",
            "PASSED" if passed else "FAILED", total, len(issues)
        )
        return report

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Strip whitespace from all string values and unify key casing."""
        normalized = []
        for record in records:
            normalized.append({
                k.strip().lower().replace(" ", "_"): (
                    v.strip() if isinstance(v, str) else v
                )
                for k, v in record.items()
            })
        return normalized

    @staticmethod
    def _deduplicate(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove exact duplicate records (order-preserving)."""
        seen: set = set()
        unique: List[Dict[str, Any]] = []
        for record in records:
            key = hashlib.md5(
                json.dumps(record, sort_keys=True, default=str).encode()
            ).hexdigest()
            if key not in seen:
                seen.add(key)
                unique.append(record)
        return unique

    @staticmethod
    def _count_records(data: Any) -> int:
        """Return the number of records in a data object."""
        if data is None:
            return 0
        if isinstance(data, list):
            return len(data)
        if isinstance(data, dict):
            return data.get("records_loaded", data.get("total_records", 1))
        return 1

    def __repr__(self) -> str:
        return f"ETLAgent(name={self.name!r}, role={self.role.value!r})"
