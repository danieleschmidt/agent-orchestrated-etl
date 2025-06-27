"""Simple orchestrator that builds and executes ETL pipelines."""

from __future__ import annotations

from typing import Callable, Dict, Any

from . import data_source_analysis, dag_generator
from .core import primary_data_extraction, transform_data


# Placeholder load function for demonstration purposes.
def load_data(data: Any) -> bool:
    """Pretend to load data and return success."""
    return True


class Pipeline:
    """Executable pipeline based on a :class:`dag_generator.SimpleDAG`."""

    def __init__(
        self,
        dag: dag_generator.SimpleDAG,
        operations: Dict[str, Callable[..., Any]],
        *,
        dag_id: str = "generated",
    ) -> None:
        self.dag = dag
        self.operations = operations
        self.dag_id = dag_id

    def execute(self) -> Dict[str, Any]:
        """Run tasks in topological order and return their results."""
        results: Dict[str, Any] = {}
        for task_id in self.dag.topological_sort():
            func = self.operations.get(task_id)
            if func is None:
                results[task_id] = None
                continue

            if task_id.startswith("transform"):
                src = results.get(task_id.replace("transform", "extract"))
                results[task_id] = func(src)
            elif task_id.startswith("load"):
                src = results.get(task_id.replace("load", "transform"))
                results[task_id] = func(src)
            else:
                results[task_id] = func()
        return results


class DataOrchestrator:
    """High-level interface to build and run ETL pipelines."""

    def create_pipeline(
        self,
        source: str,
        *,
        dag_id: str = "generated",
        operations: Dict[str, Callable[..., Any]] | None = None,
    ) -> Pipeline:
        metadata = data_source_analysis.analyze_source(source)
        dag = dag_generator.generate_dag(metadata)
        ops: Dict[str, Callable[..., Any]] = {
            "extract": primary_data_extraction,
            "transform": transform_data,
            "load": load_data,
        }
        for table in metadata["tables"]:
            ops[f"extract_{table}"] = primary_data_extraction
            ops[f"transform_{table}"] = transform_data
            ops[f"load_{table}"] = load_data

        if operations:
            ops.update(operations)

        return Pipeline(dag, ops, dag_id=dag_id)

    def dag_to_airflow(self, pipeline: Pipeline, dag_id: str | None = None) -> str:
        return dag_generator.dag_to_airflow_code(
            pipeline.dag, dag_id=dag_id or pipeline.dag_id
        )
