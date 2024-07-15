# src/data_profiling/profiling.py

import pandas as pd
import numpy as np
from typing import Dict, Any

class DataProfiler:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def generate_profile(self) -> Dict[str, Any]:
        """
        Generate a comprehensive profile of the DataFrame.
        """
        profile = {
            "basic_info": self._get_basic_info(),
            "summary_stats": self._get_summary_stats(),
            "missing_values": self._get_missing_values(),
            "unique_values": self._get_unique_values(),
            "data_types": self._get_data_types(),
            "correlations": self._get_correlations(),
        }
        return profile

    def _get_basic_info(self) -> Dict[str, Any]:
        """Get basic information about the DataFrame."""
        return {
            "num_rows": len(self.df),
            "num_columns": len(self.df.columns),
            "column_names": list(self.df.columns),
            "memory_usage": self.df.memory_usage(deep=True).sum(),
        }

    def _get_summary_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get summary statistics for numeric columns."""
        return self.df.describe().to_dict()

    def _get_missing_values(self) -> Dict[str, float]:
        """Get percentage of missing values for each column."""
        return (self.df.isnull().sum() / len(self.df) * 100).to_dict()

    def _get_unique_values(self) -> Dict[str, int]:
        """Get number of unique values for each column."""
        return self.df.nunique().to_dict()

    def _get_data_types(self) -> Dict[str, str]:
        """Get data types for each column."""
        return self.df.dtypes.astype(str).to_dict()

    def _get_correlations(self) -> Dict[str, Dict[str, float]]:
        """Get correlation matrix for numeric columns."""
        numeric_df = self.df.select_dtypes(include=[np.number])
        return numeric_df.corr().to_dict()