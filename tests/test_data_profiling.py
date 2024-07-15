# tests/test_data_profiling.py

import pytest
import pandas as pd
import numpy as np
from src.data_profiling.profiling import DataProfiler

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        'A': [1, 2, 3, 4, 5],
        'B': [1.1, 2.2, 3.3, 4.4, np.nan],
        'C': ['a', 'b', 'c', 'a', 'b'],
        'D': [True, False, True, True, False]
    })

class TestDataProfiler:
    def test_basic_info(self, sample_df):
        profiler = DataProfiler(sample_df)
        basic_info = profiler._get_basic_info()
        assert basic_info['num_rows'] == 5
        assert basic_info['num_columns'] == 4
        assert set(basic_info['column_names']) == {'A', 'B', 'C', 'D'}

    def test_summary_stats(self, sample_df):
        profiler = DataProfiler(sample_df)
        summary_stats = profiler._get_summary_stats()
        assert 'A' in summary_stats
        assert 'B' in summary_stats
        assert 'C' not in summary_stats  # Non-numeric column
        assert 'D' not in summary_stats  # Boolean column

    def test_missing_values(self, sample_df):
        profiler = DataProfiler(sample_df)
        missing_values = profiler._get_missing_values()
        assert missing_values['A'] == 0
        assert missing_values['B'] == 20.0
        assert missing_values['C'] == 0
        assert missing_values['D'] == 0

    def test_unique_values(self, sample_df):
        profiler = DataProfiler(sample_df)
        unique_values = profiler._get_unique_values()
        assert unique_values['A'] == 5
        assert unique_values['B'] == 4
        assert unique_values['C'] == 3
        assert unique_values['D'] == 2

    def test_data_types(self, sample_df):
        profiler = DataProfiler(sample_df)
        data_types = profiler._get_data_types()
        assert data_types['A'] == 'int64'
        assert data_types['B'] == 'float64'
        assert data_types['C'] == 'object'
        assert data_types['D'] == 'bool'

    def test_correlations(self, sample_df):
        profiler = DataProfiler(sample_df)
        correlations = profiler._get_correlations()
        assert 'A' in correlations
        assert 'B' in correlations
        assert 'C' not in correlations  # Non-numeric column
        assert 'D' not in correlations  # Boolean column

    def test_generate_profile(self, sample_df):
        profiler = DataProfiler(sample_df)
        profile = profiler.generate_profile()
        assert set(profile.keys()) == {'basic_info', 'summary_stats', 'missing_values', 'unique_values', 'data_types', 'correlations'}