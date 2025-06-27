from agent_orchestrated_etl import cli


def test_run_pipeline_cmd(capsys):
    """run_pipeline command executes the pipeline and prints JSON results."""
    exit_code = cli.run_pipeline_cmd(["s3"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out
    assert '"extract"' in out  # nosec B101
    assert '"load"' in out  # nosec B101


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
    exit_code = cli.run_pipeline_cmd(["mongodb"])
    assert exit_code == 1  # nosec B101
    err = capsys.readouterr().err
    assert "Unsupported source type" in err  # nosec B101

