# tests/test_my_data.py

from src.data_ingestion.ingestion import DataIngestion
from src.data_profiling.profiling import DataProfiler

# Create an instance of DataIngestion
ingestion = DataIngestion()

# Path to your CSV file
file_path = "./data/coffee-sales.csv"

try:
    # Ingest and validate the data
    df = ingestion.ingest_and_validate(file_path)
    print("Data ingested successfully!")
    print(f"Shape of the DataFrame: {df.shape}")
    print("\nFirst few rows of the data:")
    print(df.head())

    # Generate a profile of the data
    profiler = DataProfiler(df)
    profile = profiler.generate_profile()

    print("\nData Profile:")
    for key, value in profile.items():
        print(f"\n{key.upper()}:")
        print(value)

except Exception as e:
    print(f"An error occurred: {str(e)}")