import pytest
from agent_orchestrated_etl.core import transform_data

def test_transform_data():
    input_data = [1, 2, 3]
    assert transform_data(input_data) == [1, 4, 9]
