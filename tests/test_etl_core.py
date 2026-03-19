"""
Tests for the self-contained ETL core (etl_core package).

Covers:
- ETLAgent extraction from CSV file, list, and CSV string
- ETLAgent transformation (normalize, deduplicate, drop_nulls)
- ETLAgent loading to JSON file and in-memory
- ETLAgent validation
- PipelineConfig from YAML string and dict
- ETLOrchestrator happy-path and failure/retry handling
"""

from __future__ import annotations

import json
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

# Allow running tests without installing the package
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from agent_orchestrated_etl.etl_core.agent import AgentRole, ETLAgent
from agent_orchestrated_etl.etl_core.orchestrator import ETLOrchestrator
from agent_orchestrated_etl.etl_core.pipeline import PipelineConfig, PipelineStage


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_RECORDS = [
    {"id": "1", "name": "Alice", "score": "95"},
    {"id": "2", "name": "Bob", "score": "80"},
    {"id": "2", "name": "Bob", "score": "80"},   # duplicate
    {"id": "3", "name": " Carol ", "score": "88"},  # whitespace
]


@pytest.fixture
def csv_file(tmp_path: Path) -> Path:
    """Write SAMPLE_RECORDS to a temporary CSV file."""
    path = tmp_path / "test.csv"
    import csv
    with path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=["id", "name", "score"])
        writer.writeheader()
        writer.writerows(SAMPLE_RECORDS)
    return path


# ---------------------------------------------------------------------------
# ETLAgent — extractor
# ---------------------------------------------------------------------------

class TestExtractor:
    def test_extract_from_list(self):
        agent = ETLAgent(role=AgentRole.EXTRACTOR)
        result = agent.run({"source": SAMPLE_RECORDS})
        assert result.status == "success"
        assert result.records_out == 4

    def test_extract_from_csv_file(self, csv_file):
        agent = ETLAgent(role=AgentRole.EXTRACTOR)
        result = agent.run({"source": csv_file})
        assert result.status == "success"
        assert result.records_out == 4

    def test_extract_from_csv_string(self):
        # Write CSV to a temp file since the extractor expects a file path
        csv_str = "id,name\n1,Alice\n2,Bob\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(csv_str)
            tmp_path = f.name
        agent = ETLAgent(role=AgentRole.EXTRACTOR)
        result = agent.run({"source": tmp_path})
        assert result.status == "success"
        assert result.records_out == 2

    def test_extract_missing_file_returns_failure(self):
        agent = ETLAgent(role=AgentRole.EXTRACTOR)
        result = agent.run({"source": "/nonexistent/path/data.csv"})
        assert result.status == "failure"
        assert result.error is not None

    def test_extract_unsupported_source_returns_failure(self):
        agent = ETLAgent(role=AgentRole.EXTRACTOR)
        result = agent.run({"source": 12345})
        assert result.status == "failure"


# ---------------------------------------------------------------------------
# ETLAgent — transformer
# ---------------------------------------------------------------------------

class TestTransformer:
    def test_deduplicate(self):
        agent = ETLAgent(role=AgentRole.TRANSFORMER, config={"operations": ["deduplicate"]})
        result = agent.run({"data": SAMPLE_RECORDS})
        assert result.status == "success"
        assert result.records_out == 3   # "Bob" deduped

    def test_normalize_strips_whitespace_and_lowercases_keys(self):
        records = [{"Name": " Alice ", "Score": "95"}]
        agent = ETLAgent(role=AgentRole.TRANSFORMER, config={"operations": ["normalize"]})
        result = agent.run({"data": records})
        assert result.status == "success"
        assert result.data[0]["name"] == "Alice"   # stripped + lower
        assert "Name" not in result.data[0]

    def test_drop_nulls(self):
        records = [
            {"id": "1", "name": "Alice"},
            {"id": "2", "name": ""},      # empty → dropped
            {"id": "3", "name": "Bob"},
        ]
        agent = ETLAgent(role=AgentRole.TRANSFORMER, config={"operations": ["drop_nulls"]})
        result = agent.run({"data": records})
        assert result.status == "success"
        assert result.records_out == 2

    def test_combined_operations(self):
        agent = ETLAgent(
            role=AgentRole.TRANSFORMER,
            config={"operations": ["normalize", "deduplicate"]},
        )
        result = agent.run({"data": SAMPLE_RECORDS})
        assert result.status == "success"
        assert result.records_out == 3

    def test_operations_from_inputs(self):
        """Operations may be passed via inputs dict, not just config."""
        agent = ETLAgent(role=AgentRole.TRANSFORMER)
        result = agent.run({"data": SAMPLE_RECORDS, "operations": ["deduplicate"]})
        assert result.status == "success"
        assert result.records_out == 3

    def test_unknown_operation_is_skipped(self):
        agent = ETLAgent(role=AgentRole.TRANSFORMER, config={"operations": ["unknown_op"]})
        result = agent.run({"data": SAMPLE_RECORDS})
        assert result.status == "success"   # should not crash


# ---------------------------------------------------------------------------
# ETLAgent — loader
# ---------------------------------------------------------------------------

class TestLoader:
    def test_load_to_json_file(self, tmp_path):
        dest = tmp_path / "out.json"
        agent = ETLAgent(role=AgentRole.LOADER)
        result = agent.run({"data": SAMPLE_RECORDS, "destination": str(dest)})
        assert result.status == "success"
        with dest.open() as fh:
            saved = json.load(fh)
        assert len(saved) == 4

    def test_load_in_memory(self):
        agent = ETLAgent(role=AgentRole.LOADER)
        result = agent.run({"data": SAMPLE_RECORDS})
        assert result.status == "success"
        assert result.data["records_loaded"] == 4

    def test_load_creates_parent_dirs(self, tmp_path):
        dest = tmp_path / "deep" / "nested" / "out.json"
        agent = ETLAgent(role=AgentRole.LOADER)
        result = agent.run({"data": SAMPLE_RECORDS, "destination": str(dest)})
        assert result.status == "success"
        assert dest.exists()


# ---------------------------------------------------------------------------
# ETLAgent — validator
# ---------------------------------------------------------------------------

class TestValidator:
    def test_validation_passes(self):
        agent = ETLAgent(
            role=AgentRole.VALIDATOR,
            config={"rules": {"required_fields": ["id", "name"]}},
        )
        result = agent.run({"data": SAMPLE_RECORDS})
        assert result.status == "success"
        assert result.data["passed"] is True

    def test_validation_fails_on_nulls(self):
        records = [
            {"id": "1", "name": ""},
            {"id": "2", "name": ""},
            {"id": "3", "name": "Bob"},
        ]
        agent = ETLAgent(
            role=AgentRole.VALIDATOR,
            config={"rules": {"required_fields": ["name"], "max_null_pct": 10.0}},
        )
        result = agent.run({"data": records})
        assert result.status == "success"        # validator itself succeeds
        assert result.data["passed"] is False    # but data quality failed
        assert len(result.data["issues"]) > 0

    def test_quality_score_present(self):
        agent = ETLAgent(role=AgentRole.VALIDATOR)
        result = agent.run({"data": SAMPLE_RECORDS})
        assert "quality_score" in result.data


# ---------------------------------------------------------------------------
# PipelineConfig
# ---------------------------------------------------------------------------

class TestPipelineConfig:
    def test_from_dict(self):
        cfg = PipelineConfig.from_dict({
            "pipeline": {
                "name": "test-pipe",
                "stages": [
                    {"name": "extract", "role": "extractor", "source": "file.csv"},
                    {"name": "transform", "role": "transformer", "operations": ["normalize"]},
                ],
            }
        })
        assert cfg.name == "test-pipe"
        assert len(cfg.stages) == 2
        assert cfg.stages[0].source == "file.csv"
        assert cfg.stages[1].operations == ["normalize"]

    def test_from_yaml_string(self):
        yaml_str = textwrap.dedent("""
            pipeline:
              name: yaml-pipe
              stages:
                - name: extract
                  role: extractor
                  source: data.csv
                - name: load
                  role: loader
                  destination: out.json
        """)
        cfg = PipelineConfig.from_yaml(yaml_str)
        assert cfg.name == "yaml-pipe"
        assert cfg.stages[0].role == "extractor"
        assert cfg.stages[1].destination == "out.json"

    def test_round_trip_dict(self):
        original = PipelineConfig(
            name="rt-test",
            stages=[PipelineStage("s1", "extractor", {"source": "f.csv"})],
            max_retries=3,
        )
        d = original.to_dict()
        restored = PipelineConfig.from_dict(d)
        assert restored.name == "rt-test"
        assert restored.max_retries == 3
        assert restored.stages[0].source == "f.csv"

    def test_defaults(self):
        cfg = PipelineConfig()
        assert cfg.max_retries == 2
        assert cfg.stop_on_failure is True


# ---------------------------------------------------------------------------
# ETLOrchestrator
# ---------------------------------------------------------------------------

class TestOrchestrator:
    def _simple_config(self, csv_path: str, json_path: str) -> PipelineConfig:
        return PipelineConfig.from_dict({
            "pipeline": {
                "name": "test-pipeline",
                "max_retries": 0,
                "stop_on_failure": True,
                "stages": [
                    {"name": "extract", "role": "extractor", "source": csv_path},
                    {
                        "name": "transform",
                        "role": "transformer",
                        "operations": ["normalize", "deduplicate"],
                    },
                    {"name": "load", "role": "loader", "destination": json_path},
                ],
            }
        })

    def test_full_pipeline_success(self, csv_file, tmp_path):
        out = tmp_path / "out.json"
        config = self._simple_config(str(csv_file), str(out))
        orch = ETLOrchestrator(config)
        run = orch.run()

        assert run.status == "success"
        assert len(run.stages) == 3
        assert all(sr.result.status == "success" for sr in run.stages)
        assert out.exists()

    def test_pipeline_summary_string(self, csv_file, tmp_path):
        config = self._simple_config(str(csv_file), str(tmp_path / "out.json"))
        orch = ETLOrchestrator(config)
        run = orch.run()
        summary = run.summary()
        assert "SUCCESS" in summary
        assert "extract" in summary

    def test_pipeline_stops_on_failure_by_default(self):
        config = PipelineConfig.from_dict({
            "pipeline": {
                "name": "fail-fast",
                "max_retries": 0,
                "stop_on_failure": True,
                "stages": [
                    {
                        "name": "extract",
                        "role": "extractor",
                        "source": "/does/not/exist.csv",
                    },
                    {"name": "load", "role": "loader"},
                ],
            }
        })
        orch = ETLOrchestrator(config)
        run = orch.run()
        assert run.status == "failure"
        assert len(run.stages) == 1   # stopped after first failure

    def test_pipeline_continues_on_failure_when_configured(self):
        config = PipelineConfig.from_dict({
            "pipeline": {
                "name": "keep-going",
                "max_retries": 0,
                "stop_on_failure": False,
                "stages": [
                    {
                        "name": "extract",
                        "role": "extractor",
                        "source": "/does/not/exist.csv",
                    },
                    {"name": "load", "role": "loader"},
                ],
            }
        })
        orch = ETLOrchestrator(config)
        run = orch.run()
        assert run.status in ("failure", "partial")
        assert len(run.stages) == 2   # both stages attempted

    def test_retry_succeeds_eventually(self, tmp_path):
        """
        Custom agent that fails on the first attempt then succeeds.
        Verifies the orchestrator respects max_retries.
        """
        attempts = {"count": 0}

        def flaky_extractor(inputs):
            attempts["count"] += 1
            if attempts["count"] < 2:
                raise RuntimeError("Temporary failure")
            return [{"id": "1", "name": "OK"}]

        stage = PipelineStage("extract", "extractor", {})
        config = PipelineConfig(name="retry-test", stages=[stage], max_retries=2)

        def factory(s):
            return ETLAgent(
                role=AgentRole.EXTRACTOR,
                name=s.name,
                custom_handler=flaky_extractor,
            )

        orch = ETLOrchestrator(config, agent_factory=factory)
        run = orch.run()
        assert run.status == "success"
        assert run.stages[0].attempts == 2

    def test_custom_agent_factory(self, csv_file, tmp_path):
        """Verify a custom factory replaces the default agent creation."""
        created = []

        def tracking_factory(stage: PipelineStage) -> ETLAgent:
            agent = ETLOrchestrator._default_agent_factory(stage)
            created.append(stage.name)
            return agent

        config = self._simple_config(str(csv_file), str(tmp_path / "out.json"))
        orch = ETLOrchestrator(config, agent_factory=tracking_factory)
        run = orch.run()
        assert run.status == "success"
        assert set(created) == {"extract", "transform", "load"}

    def test_invalid_role_raises(self):
        config = PipelineConfig.from_dict({
            "pipeline": {
                "name": "bad-role",
                "stages": [{"name": "x", "role": "unknown_role"}],
            }
        })
        orch = ETLOrchestrator(config)
        with pytest.raises(ValueError, match="Unknown agent role"):
            orch.run()

    def test_total_records_processed(self, csv_file, tmp_path):
        config = self._simple_config(str(csv_file), str(tmp_path / "out.json"))
        orch = ETLOrchestrator(config)
        run = orch.run()
        # total_records_processed sums records_out of successful stages
        assert run.total_records_processed > 0

    def test_pipeline_duration_positive(self, csv_file, tmp_path):
        config = self._simple_config(str(csv_file), str(tmp_path / "out.json"))
        orch = ETLOrchestrator(config)
        run = orch.run()
        assert run.duration_seconds >= 0
