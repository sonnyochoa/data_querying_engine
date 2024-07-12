# tests/conftest.py
import pytest
import pandas as pd
import os

@pytest.fixture(scope="session")
def test_data_dir(tmp_path_factory):
    return tmp_path_factory.mktemp("test_data")

@pytest.fixture
def sample_csv(test_data_dir):
    file_path = test_data_dir / "sample.csv"
    df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
    df.to_csv(file_path, index=False)
    yield file_path
    os.remove(file_path)

# Add more fixtures for other file types as needed