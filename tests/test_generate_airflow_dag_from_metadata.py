
from agent_orchestrated_etl import dag_generator, data_source_analysis


def test_dag_contains_required_tasks():
    """DAG object contains extract, transform, and load tasks"""
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())  # nosec B101


def test_task_dependencies():
    """Tasks have correct upstream/downstream dependencies"""
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    assert 'transform' in dag.tasks['extract'].downstream  # nosec B101
    assert 'load' in dag.tasks['transform'].downstream  # nosec B101
    assert 'extract' in dag.tasks['transform'].upstream  # nosec B101
    assert 'transform' in dag.tasks['load'].upstream  # nosec B101
