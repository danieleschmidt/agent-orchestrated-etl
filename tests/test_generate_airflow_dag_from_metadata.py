
from agent_orchestrated_etl import dag_generator, data_source_analysis


def test_dag_contains_required_tasks():
    """DAG object contains extract, transform, and load tasks"""
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())  # nosec B101


def test_dag_generated_for_api_source():
    """API sources produce a DAG with standard tasks."""
    metadata = data_source_analysis.analyze_source('api')
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


def test_per_table_tasks_created():
    """Per-table extract, transform, and load tasks are generated."""
    metadata = data_source_analysis.analyze_source('postgresql')
    dag = dag_generator.generate_dag(metadata)
    for table in metadata['tables']:
        assert f'extract_{table}' in dag.tasks  # nosec B101
        assert f'transform_{table}' in dag.tasks  # nosec B101
        assert f'load_{table}' in dag.tasks  # nosec B101
        assert f'extract_{table}' in dag.tasks['extract'].downstream  # nosec B101
        assert f'transform_{table}' in dag.tasks[f'extract_{table}'].downstream  # nosec B101
        assert f'load_{table}' in dag.tasks[f'transform_{table}'].downstream  # nosec B101
        assert 'load' in dag.tasks[f'load_{table}'].downstream  # nosec B101


def test_dag_to_airflow_code():
    """Airflow code string contains basic structure and dependencies."""
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    code = dag_generator.dag_to_airflow_code(dag)
    assert 'with DAG(' in code  # nosec B101
    assert 'extract >> transform' in code  # nosec B101
    assert 'transform >> load' in code  # nosec B101


def test_dag_to_airflow_code_custom_id():
    """The DAG ID parameter is reflected in the generated code."""
    metadata = data_source_analysis.analyze_source('s3')
    dag = dag_generator.generate_dag(metadata)
    code = dag_generator.dag_to_airflow_code(dag, dag_id='custom_id')
    assert "with DAG('custom_id'" in code  # nosec B101


def test_topological_sort_returns_valid_order():
    """Topological sort orders tasks according to dependencies."""
    metadata = data_source_analysis.analyze_source('postgresql')
    dag = dag_generator.generate_dag(metadata)
    order = dag.topological_sort()

    assert set(order) == set(dag.tasks)  # nosec B101
    assert order.index('extract') < order.index('transform')  # nosec B101
    assert order.index('transform') < order.index('load')  # nosec B101

    for table in metadata['tables']:
        assert order.index(f'extract_{table}') > order.index('extract')  # nosec B101
        assert order.index(f'transform_{table}') > order.index(f'extract_{table}')  # nosec B101
        assert order.index(f'load_{table}') > order.index(f'transform_{table}')  # nosec B101
        assert order.index(f'load_{table}') < order.index('load')  # nosec B101
