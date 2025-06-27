from __future__ import annotations

import argparse
import sys
from pathlib import Path

import json

from .data_source_analysis import analyze_source
from .dag_generator import generate_dag, dag_to_airflow_code
from . import orchestrator


def generate_dag_cmd(args: list[str] | None = None) -> int:
    """Generate a DAG from a data source and output to a Python file."""
    parser = argparse.ArgumentParser(prog="generate_dag")
    parser.add_argument("source", help="Data source type, e.g. s3 or postgresql")
    parser.add_argument("output", help="Output DAG file path")
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

    try:
        metadata = analyze_source(ns.source)
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")

    dag = generate_dag(metadata)

    if ns.list_tasks:
        for task_id in dag.topological_sort():
            print(task_id)
        return 0

    out_path = Path(ns.output)
    out_path.write_text(dag_to_airflow_code(dag, dag_id=ns.dag_id))
    return 0


def run_pipeline_cmd(args: list[str] | None = None) -> int:
    """Create and execute a pipeline from the given source."""
    parser = argparse.ArgumentParser(prog="run_pipeline")
    parser.add_argument("source", help="Data source type, e.g. s3 or postgresql")
    parser.add_argument(
        "--output",
        help="Optional path to write JSON results; prints to stdout if omitted",
    )
    parser.add_argument(
        "--dag-id",
        default="generated",
        help="DAG ID to use if also emitting Airflow code",
    )
    parser.add_argument(
        "--airflow",
        help="Optional path to also write the generated Airflow DAG file",
    )
    parser.add_argument(
        "--list-tasks",
        action="store_true",
        help="Print tasks in execution order and exit",
    )
    ns = parser.parse_args(args)

    orch = orchestrator.DataOrchestrator()
    try:
        pipeline = orch.create_pipeline(ns.source, dag_id=ns.dag_id)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1


    if ns.list_tasks:
        for task_id in pipeline.dag.topological_sort():
            print(task_id)
        return 0

    results = pipeline.execute()
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
