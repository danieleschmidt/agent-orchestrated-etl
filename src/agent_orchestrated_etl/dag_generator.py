from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Set


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
        task = Task(task_id)
        self.tasks[task_id] = task
        return task

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
        Currently unused placeholder for future logic.

    Returns
    -------
    SimpleDAG
        A DAG with extract -> transform -> load tasks.
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
    return dag
