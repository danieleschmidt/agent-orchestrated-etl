import pytest
from agent_orchestrated_etl import data_source_analysis, dag_generator


def test_dag_structure_for_s3():
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())  # nosec B101
    assert 'transform' in dag.tasks['extract'].downstream  # nosec B101
    assert 'load' in dag.tasks['transform'].downstream  # nosec B101


def test_dag_structure_for_postgresql():
    metadata = data_source_analysis.analyze_source('postgresql')
    dag = dag_generator.generate_dag(metadata)
    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())  # nosec B101
    assert 'transform' in dag.tasks['extract'].downstream  # nosec B101
    assert 'load' in dag.tasks['transform'].downstream  # nosec B101


def test_generate_dag_invalid_metadata():
    with pytest.raises(ValueError):
        dag_generator.generate_dag({'foo': 'bar'})
