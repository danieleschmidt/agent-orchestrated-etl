from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set

from .validation import ValidationError, validate_dag_id, validate_task_name


@dataclass
class Task:
    """Simplified representation of a DAG task."""

    task_id: str
    upstream: Set[str] = field(default_factory=set)
    downstream: Set[str] = field(default_factory=set)


class SimpleDAG:
    """Minimal DAG object tracking task dependencies."""

    def __init__(self) -> None:
        self.tasks: Dict[str, Task] = {}

    def add_task(self, task_id: str) -> Task:
        """Add a task to the DAG if it doesn't already exist."""
        # Validate task ID for security
        try:
            validated_task_id = validate_task_name(task_id)
        except ValidationError as e:
            raise ValueError(f"Invalid task ID '{task_id}': {e}")

        if validated_task_id in self.tasks:
            return self.tasks[validated_task_id]

        task = Task(validated_task_id)
        self.tasks[validated_task_id] = task
        return task

    def topological_sort(self) -> list[str]:
        """Return task IDs in topological execution order."""
        order: list[str] = []
        incoming = {tid: len(t.upstream) for tid, t in self.tasks.items()}
        ready = sorted([tid for tid, count in incoming.items() if count == 0])

        while ready:
            task_id = ready.pop(0)
            order.append(task_id)
            for downstream in sorted(self.tasks[task_id].downstream):
                incoming[downstream] -= 1
                if incoming[downstream] == 0:
                    ready.append(downstream)

        if len(order) != len(self.tasks):
            raise ValueError("Cyclic dependency detected")

        return order

    def set_dependency(self, upstream_id: str, downstream_id: str) -> None:
        upstream = self.tasks[upstream_id]
        downstream = self.tasks[downstream_id]
        upstream.downstream.add(downstream_id)
        downstream.upstream.add(upstream_id)


def generate_dag(metadata: Dict) -> SimpleDAG:
    """Generate a simple DAG from metadata.

    Parameters
    ----------
    metadata:
        Dictionary containing at least ``tables`` and ``fields`` keys. The
        ``tables`` list is used to create per-table extract, transform, and load
        tasks.

    Returns
    -------
    SimpleDAG
        A DAG with extract -> transform -> load tasks and additional per-table
        tasks derived from the provided metadata.
    """
    if not isinstance(metadata, dict) or not {
        "tables",
        "fields",
    } <= set(metadata):
        raise ValueError("Invalid metadata")

    dag = SimpleDAG()
    dag.add_task("extract")
    dag.add_task("transform")
    dag.add_task("load")

    dag.set_dependency("extract", "transform")
    dag.set_dependency("transform", "load")

    for table in metadata["tables"]:
        # Validate table name to prevent injection
        try:
            validated_table = validate_task_name(str(table))
        except ValidationError as e:
            raise ValueError(f"Invalid table name '{table}': {e}")

        extract_id = f"extract_{validated_table}"
        transform_id = f"transform_{validated_table}"
        load_id = f"load_{validated_table}"

        dag.add_task(extract_id)
        dag.add_task(transform_id)
        dag.add_task(load_id)

        dag.set_dependency("extract", extract_id)
        dag.set_dependency(extract_id, transform_id)
        dag.set_dependency(transform_id, load_id)
        dag.set_dependency(load_id, "load")

    return dag


def dag_to_airflow_code(dag: SimpleDAG, dag_id: str = "generated") -> str:
    """Return Python code defining an Airflow DAG equivalent to ``dag``.

    Parameters
    ----------
    dag:
        The :class:`SimpleDAG` to convert to Airflow syntax.
    dag_id:
        Airflow DAG ID to use in the generated code. Defaults to ``"generated"``.

    Returns
    -------
    str
        Python source code for an Airflow DAG using ``EmptyOperator`` tasks.
    """
    # Validate DAG ID for security
    try:
        validated_dag_id = validate_dag_id(dag_id)
    except ValidationError as e:
        raise ValueError(f"Invalid DAG ID '{dag_id}': {e}")

    def _var(name: str) -> str:
        # Additional sanitization for Python variable names
        sanitized = name.replace("-", "_").replace(".", "_")
        # Ensure it starts with a letter or underscore
        if sanitized and not (sanitized[0].isalpha() or sanitized[0] == '_'):
            sanitized = f"task_{sanitized}"
        return sanitized

    lines = [
        "from airflow import DAG",
        "from airflow.operators.empty import EmptyOperator",
        "from datetime import datetime",
        "",
        f"with DAG('{validated_dag_id}', start_date=datetime(2024, 1, 1), schedule_interval=None) as dag:",
    ]

    for task_id in sorted(dag.tasks):
        lines.append(f"    {_var(task_id)} = EmptyOperator(task_id='{task_id}')")

    for task_id in sorted(dag.tasks):
        for downstream in sorted(dag.tasks[task_id].downstream):
            lines.append(f"    {_var(task_id)} >> {_var(downstream)}")

    return "\n".join(lines) + "\n"
