from __future__ import annotations

import argparse
from pathlib import Path

from .data_source_analysis import analyze_source
from .dag_generator import generate_dag


def generate_dag_cmd(args: list[str] | None = None) -> int:
    """Generate a DAG from a data source and output to a Python file."""
    parser = argparse.ArgumentParser(prog="generate_dag")
    parser.add_argument("source", help="Data source type, e.g. s3 or postgresql")
    parser.add_argument("output", help="Output DAG file path")
    ns = parser.parse_args(args)

    try:
        metadata = analyze_source(ns.source)
    except ValueError as exc:
        parser.exit(1, f"{exc}\n")

    dag = generate_dag(metadata)
    out_path = Path(ns.output)
    lines = ["# Auto-generated DAG\n", "tasks = {\n"]
    for task_id, task in dag.tasks.items():
        lines.append(f"    '{task_id}': {sorted(task.downstream)!r},\n")
    lines.append("}\n")
    out_path.write_text("".join(lines))
    return 0


def main() -> int:
    return generate_dag_cmd()


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
