from __future__ import annotations

import argparse
import sys
from pathlib import Path

import json

from .data_source_analysis import analyze_source, SUPPORTED_SOURCES
from . import data_source_analysis
from .dag_generator import generate_dag, dag_to_airflow_code
from . import orchestrator


def _source_type(value: str) -> str:
    """Return a normalized source type or raise an error."""
    normalized = value.lower()
    if normalized not in SUPPORTED_SOURCES:
        raise argparse.ArgumentTypeError(f"Unsupported source type: {value}")
    return normalized


def _safe_path(value: str) -> str:
    """Basic sanity checks for file paths provided via CLI."""
    if any(ch in value for ch in ("\n", "\r")) or value.startswith("-"):
        raise argparse.ArgumentTypeError("invalid path")
    return value


def generate_dag_cmd(args: list[str] | None = None) -> int:
    """Generate a DAG from a data source and output to a Python file.

    Parameters
    ----------
    args:
        Optional list of CLI arguments. If ``--list-sources`` is supplied,
        the positional ``source`` and ``output`` arguments are ignored.
    """
    parser = argparse.ArgumentParser(prog="generate_dag")
    parser.add_argument(
        "source",
        nargs="?",
        help="Data source type, e.g. s3, postgresql, or api",
    )
    parser.add_argument(
        "output",
        nargs="?",
        type=_safe_path,
        help="Output DAG file path",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print supported source types and exit",
    )
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="Print tasks in execution order and exit",
    )
    parser.add_argument(
        "--dag-id",
        default="generated",
        help="Airflow DAG ID to use in the generated file",
    )
    ns = parser.parse_args(args)

    if ns.list_sources:
        print(data_source_analysis.supported_sources_text())
        return 0

    if not ns.source or not ns.output:
        parser.error("the following arguments are required: source output")

    metadata = analyze_source(ns.source)

    dag = generate_dag(metadata)

    if ns.list_tasks:
        for task_id in dag.topological_sort():
            print(task_id)
        return 0

    out_path = Path(ns.output)
    out_path.write_text(dag_to_airflow_code(dag, dag_id=ns.dag_id))
    return 0


def run_pipeline_cmd(args: list[str] | None = None) -> int:
    """Create and execute a pipeline from the given source.

    Parameters
    ----------
    args:
        Optional list of CLI arguments. ``--list-sources`` can be used to
        display supported sources without providing the ``source`` argument.
        ``--monitor`` writes task events to the specified file.
        Events are appended as they occur and recorded even if pipeline
        creation fails.
    """
    parser = argparse.ArgumentParser(prog="run_pipeline")
    parser.add_argument(
        "source",
        nargs="?",
        help="Data source type, e.g. s3, postgresql, or api",
    )
    parser.add_argument(
        "--output",
        type=_safe_path,
        help=(
            "Optional path to write JSON results; prints to stdout "
            "if omitted"
        ),
    )
    parser.add_argument(
        "--dag-id",
        default="generated",
        help="DAG ID to use if also emitting Airflow code",
    )
    parser.add_argument(
        "--airflow",
        type=_safe_path,
        help="Optional path to also write the generated Airflow DAG file",
    )
    parser.add_argument(
        "--monitor",
        type=_safe_path,
        help="Optional path to write pipeline event log",
    )
    parser.add_argument(
        "--list-sources",
        action="store_true",
        help="Print supported source types and exit",
    )
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="Print tasks in execution order and exit",
    )
    ns = parser.parse_args(args)

    if ns.list_sources:
        print(data_source_analysis.supported_sources_text())
        return 0

    if not ns.source:
        parser.error("the following arguments are required: source")

    monitor = None
    if ns.monitor:
        monitor = orchestrator.MonitorAgent(ns.monitor)

    try:
        ns.source = _source_type(ns.source)
    except argparse.ArgumentTypeError as exc:
        if monitor:
            monitor.error(str(exc))
        parser.exit(2, f"argument source: {exc}\n")
    orch = orchestrator.DataOrchestrator()
    try:
        pipeline = orch.create_pipeline(ns.source, dag_id=ns.dag_id)
    except ValueError as exc:
        if monitor:
            monitor.error(str(exc))
        print(exc, file=sys.stderr)
        return 1

    if ns.list_tasks:
        for task_id in pipeline.dag.topological_sort():
            print(task_id)
        return 0

    try:
        results = pipeline.execute(monitor=monitor)
    except Exception as exc:  # pragma: no cover - defensive
        if monitor:
            monitor.error(str(exc))
        print(exc, file=sys.stderr)
        return 1

    output_text = json.dumps(results, indent=2)

    if ns.output:
        Path(ns.output).write_text(output_text)
    else:
        print(output_text)

    if ns.airflow:
        Path(ns.airflow).write_text(orch.dag_to_airflow(pipeline))

    return 0


def main() -> int:
    return generate_dag_cmd()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
