from agent_orchestrated_etl import cli, orchestrator

import pytest


def test_run_pipeline_cmd(capsys):
    """run_pipeline command executes the pipeline and prints JSON results."""
    exit_code = cli.run_pipeline_cmd(["s3"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out
    assert '"extract"' in out  # nosec B101
    assert '"load"' in out  # nosec B101


def test_run_pipeline_cmd_api(capsys):
    """run_pipeline works for the new 'api' source."""
    exit_code = cli.run_pipeline_cmd(["api"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out
    assert '"extract"' in out  # nosec B101


def test_run_pipeline_cmd_writes_files(tmp_path):
    """Results and Airflow DAG are written when paths are provided."""
    out_json = tmp_path / "results.json"
    out_dag = tmp_path / "dag.py"
    exit_code = cli.run_pipeline_cmd(
        [
            "postgresql",
            "--output",
            str(out_json),
            "--airflow",
            str(out_dag),
            "--dag-id",
            "custom_dag",
        ]
    )
    assert exit_code == 0  # nosec B101
    assert out_json.exists()  # nosec B101
    assert "custom_dag" in out_dag.read_text()  # nosec B101


def test_run_pipeline_cmd_list_tasks(capsys):
    """The --list-tasks option prints task order without execution."""
    exit_code = cli.run_pipeline_cmd(["s3", "--list-tasks"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out.strip().splitlines()
    assert out[0] == "extract"  # nosec B101
    assert "load" in out[-1]  # nosec B101


def test_run_pipeline_cmd_invalid_source(capsys):
    """Invalid source results in non-zero exit code and error message."""
    with pytest.raises(SystemExit):
        cli.run_pipeline_cmd(["mongodb"])


def test_run_pipeline_list_sources(capsys):
    """--list-sources prints supported data sources."""
    exit_code = cli.run_pipeline_cmd(["--list-sources"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out
    for src in ("s3", "postgresql", "api"):
        assert src in out  # nosec B101


def test_run_pipeline_monitor_log(tmp_path):
    """Events are written to the monitor log file."""
    log_file = tmp_path / "events.log"
    exit_code = cli.run_pipeline_cmd(["s3", "--monitor", str(log_file)])
    assert exit_code == 0  # nosec B101
    content = log_file.read_text()
    assert "starting extract" in content  # nosec B101
    assert "completed load" in content  # nosec B101


def test_run_pipeline_monitor_log_on_failure(tmp_path, monkeypatch):
    """Monitor file is written even if the pipeline fails."""
    def bad_load(_data):
        raise RuntimeError("boom")

    log_file = tmp_path / "events.log"

    orig = orchestrator.DataOrchestrator.create_pipeline

    def fake_create_pipeline(self, source: str, dag_id: str = "generated"):
        # Disable graceful degradation to ensure failures are logged
        pipeline = orig(self, source, dag_id=dag_id, enable_graceful_degradation=False)
        pipeline.operations["load"] = bad_load
        return pipeline

    monkeypatch.setattr(
        orchestrator.DataOrchestrator,
        "create_pipeline",
        fake_create_pipeline,
    )

    exit_code = cli.run_pipeline_cmd(["s3", "--monitor", str(log_file)])
    # With graceful degradation disabled, pipeline should fail and log errors
    content = log_file.read_text()
    assert "ERROR:" in content  # nosec B101
    # Pipeline should fail due to disabled graceful degradation
    assert exit_code == 1  # nosec B101


def test_run_pipeline_monitor_invalid_source(tmp_path):
    """Invalid source causes argparse error before monitor file creation."""
    log_file = tmp_path / "events.log"
    with pytest.raises(SystemExit):
        cli.run_pipeline_cmd(["mongodb", "--monitor", str(log_file)])
    # Monitor file is not created when argparse validation fails
    assert not log_file.exists()  # nosec B101

