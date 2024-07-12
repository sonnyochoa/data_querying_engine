# tests/test_data_ingestion.py
import pytest
import pandas as pd
from src.data_ingestion.ingestion import DataIngestion

class TestDataIngestion:
    @pytest.fixture(autouse=True)
    def setup(self):
        self.ingestion = DataIngestion()

    def test_csv_ingestion(self, sample_csv):
        df = self.ingestion.ingest_and_validate(str(sample_csv))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 2)

    def test_excel_ingestion(self, test_data_dir):
        test_excel = test_data_dir / 'test.xlsx'
        pd.DataFrame({'A': [1, 2], 'B': [3, 4]}).to_excel(test_excel, index=False)
        df = self.ingestion.ingest_and_validate(str(test_excel))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 2)

    def test_json_ingestion(self, test_data_dir):
        test_json = test_data_dir / 'test.json'
        pd.DataFrame({'A': [1, 2], 'B': [3, 4]}).to_json(test_json)
        df = self.ingestion.ingest_and_validate(str(test_json))
        assert isinstance(df, pd.DataFrame)
        assert df.shape == (2, 2)

    def test_unsupported_format(self, test_data_dir):
        with pytest.raises(ValueError):
            self.ingestion.ingest_and_validate(str(test_data_dir / 'test.unsupported'))

    def test_empty_dataframe(self, test_data_dir):
        empty_csv = test_data_dir / 'empty.csv'
        pd.DataFrame().to_csv(empty_csv, index=False)
        with pytest.raises(ValueError):
            self.ingestion.ingest_and_validate(str(empty_csv))

    def test_missing_values(self, test_data_dir):
        missing_csv = test_data_dir / 'missing.csv'
        pd.DataFrame({'A': [1, None], 'B': [3, 4]}).to_csv(missing_csv, index=False)
        with pytest.warns(UserWarning, match="The DataFrame contains missing values"):
            self.ingestion.ingest_and_validate(str(missing_csv))

    def test_duplicate_rows(self, test_data_dir):
        duplicate_csv = test_data_dir / 'duplicate.csv'
        pd.DataFrame({'A': [1, 1], 'B': [3, 3]}).to_csv(duplicate_csv, index=False)
        with pytest.warns(UserWarning, match="The DataFrame contains 1 duplicate rows"):
            self.ingestion.ingest_and_validate(str(duplicate_csv))