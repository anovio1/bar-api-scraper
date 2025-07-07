# my_project/src/data_processing/data_loader.py

import pandas as pd
import os
from typing import Optional
from src import config  # Updated import


def load_csv_to_dataframe(file_path: str) -> Optional[pd.DataFrame]:
    """
    Loads a CSV file into a pandas DataFrame with basic error handling.
    """
    try:
        print(f"Attempting to load '{os.path.basename(file_path)}'...")
        df = pd.read_csv(file_path)
        print(f"Successfully loaded {len(df):,} rows.")
        return df
    except FileNotFoundError:
        print(f"Error: The file was not found at '{file_path}'")
        return None
    except Exception as e:
        print(f"An unexpected error occurred while loading '{file_path}': {e}")
        return None


def get_row_count(df: pd.DataFrame) -> int:
    """Gets the number of rows from a pandas DataFrame."""
    return df.shape[0]
