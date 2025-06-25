import json
from pathlib import Path

criteria = json.loads(Path('tests/sprint_acceptance_criteria.json').read_text())

# find task 4 (not used but demonstrates parsing)
_ = next(t for t in criteria['tasks'] if t['id'] == 4)

lines = [
    "import pytest",
    "from agent_orchestrated_etl import data_source_analysis, dag_generator",
    "",
    "",
    "def test_dag_structure_for_s3():",
    "    metadata = data_source_analysis.analyze_source('s3')",
    "    dag = dag_generator.generate_dag(metadata)",
    "    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())",
    "    assert 'transform' in dag.tasks['extract'].downstream",
    "    assert 'load' in dag.tasks['transform'].downstream",
    "",
    "",
    "def test_dag_structure_for_postgresql():",
    "    metadata = data_source_analysis.analyze_source('postgresql')",
    "    dag = dag_generator.generate_dag(metadata)",
    "    assert {'extract', 'transform', 'load'} <= set(dag.tasks.keys())",
    "    assert 'transform' in dag.tasks['extract'].downstream",
    "    assert 'load' in dag.tasks['transform'].downstream",
    "",
    "",
    "def test_generate_dag_invalid_metadata():",
    "    with pytest.raises(ValueError):",
    "        dag_generator.generate_dag({'foo': 'bar'})",
]

Path('tests/test_write_unit_tests_for_dag_generation.py').write_text("\n".join(lines) + "\n")
