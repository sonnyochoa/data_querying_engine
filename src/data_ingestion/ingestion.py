# src/data_ingestion/ingestion.py

import pandas as pd
import os
from typing import Optional

class DataIngestion:
    def __init__(self):
        self.supported_formats = {
            'csv': pd.read_csv,
            'xlsx': pd.read_excel,
            'json': pd.read_json,
            'parquet': pd.read_parquet,
            # For SQL, we'll need a separate method
        }

    def ingest_and_validate(self, file_path: str) -> pd.DataFrame:
        """
        Ingest data from the given file path and perform basic validation.
        
        Args:
            file_path (str): Path to the data file.
        
        Returns:
            pd.DataFrame: Ingested and validated DataFrame.
        
        Raises:
            ValueError: If the file format is unsupported or if validation fails.
        """
        file_format = self._detect_file_format(file_path)
        df = self._read_file(file_path, file_format)
        self._validate_data(df)
        return df

    def _detect_file_format(self, file_path: str) -> str:
        """Detect file format based on file extension."""
        _, extension = os.path.splitext(file_path)
        file_format = extension[1:].lower()  # Remove the dot and convert to lowercase
        if file_format not in self.supported_formats:
            raise ValueError(f"Unsupported file format: {file_format}")
        return file_format

    def _read_file(self, file_path: str, file_format: str) -> pd.DataFrame:
        """Read the file using the appropriate pandas method."""
        try:
            if file_format == 'sql':
                # Implement SQL reading logic here
                raise NotImplementedError("SQL ingestion not yet implemented")
            else:
                return self.supported_formats[file_format](file_path)
        except Exception as e:
            raise ValueError(f"Error reading file: {str(e)}")

    def _validate_data(self, df: pd.DataFrame) -> None:
        """Perform basic data validation checks."""
        if df.empty:
            raise ValueError("The DataFrame is empty")
        
        if df.isnull().values.any():
            print("Warning: The DataFrame contains missing values")
        
        if df.duplicated().sum() > 0:
            print(f"Warning: The DataFrame contains {df.duplicated().sum()} duplicate rows")

    def read_sql(self, query: str, connection: str) -> pd.DataFrame:
        """
        Read data from a SQL database.
        
        Args:
            query (str): SQL query to execute.
            connection (str): Database connection string.
        
        Returns:
            pd.DataFrame: Data read from the SQL database.
        """
        try:
            return pd.read_sql(query, connection)
        except Exception as e:
            raise ValueError(f"Error reading from SQL database: {str(e)}")