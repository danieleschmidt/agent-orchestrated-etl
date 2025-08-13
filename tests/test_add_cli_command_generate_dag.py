import os
import subprocess  # nosec B404
import sys
from pathlib import Path

import pytest

from agent_orchestrated_etl import cli


def test_cli_outputs_dag_file(tmp_path):
    out_file = tmp_path / "dag.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_orchestrated_etl.cli",
            "s3",
            str(out_file),
        ],
        capture_output=True,
        text=True,
        env=env,
    )  # nosec B603
    assert result.returncode == 0  # nosec B101
    assert out_file.exists() and out_file.read_text() != ""  # nosec B101


def test_cli_outputs_dag_file_api(tmp_path):
    """generate_dag supports the 'api' source."""
    out_file = tmp_path / "dag.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_orchestrated_etl.cli",
            "api",
            str(out_file),
        ],
        capture_output=True,
        text=True,
        env=env,
    )  # nosec B603
    assert result.returncode == 0  # nosec B101
    assert out_file.exists() and out_file.read_text() != ""  # nosec B101


def test_cli_custom_dag_id(tmp_path):
    """CLI writes the provided DAG ID into the file."""
    out_file = tmp_path / "dag.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_orchestrated_etl.cli",
            "s3",
            str(out_file),
            "--dag-id",
            "custom",
        ],
        capture_output=True,
        text=True,
        env=env,
    )  # nosec B603
    assert result.returncode == 0  # nosec B101
    assert "custom" in out_file.read_text()  # nosec B101


def test_cli_invalid_source(tmp_path):
    out_file = tmp_path / "dag.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_orchestrated_etl.cli",
            "mongodb",
            str(out_file),
        ],
        capture_output=True,
        text=True,
        env=env,
    )  # nosec B603
    assert result.returncode != 0  # nosec B101
    assert "Unsupported source type" in result.stderr  # nosec B101


def test_cli_invalid_output_path():
    """Invalid path characters should cause argument parsing to fail."""
    with pytest.raises(SystemExit):
        cli.generate_dag_cmd(["s3", "bad\npath"])


def test_cli_list_tasks(tmp_path):
    """--list-tasks prints tasks instead of writing a file."""
    out_file = tmp_path / "dag.py"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1] / "src")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "agent_orchestrated_etl.cli",
            "s3",
            str(out_file),
            "--list-tasks",
        ],
        capture_output=True,
        text=True,
        env=env,
    )  # nosec B603
    assert result.returncode == 0  # nosec B101
    assert "extract" in result.stdout  # nosec B101
    assert not out_file.exists()  # nosec B101


def test_cli_list_sources(capsys):
    """--list-sources prints supported data sources."""
    exit_code = cli.generate_dag_cmd(["--list-sources"])
    assert exit_code == 0  # nosec B101
    out = capsys.readouterr().out
    for src in ("s3", "postgresql", "api"):
        assert src in out  # nosec B101
