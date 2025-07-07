# my_project/src/data_processing/data_preprocessor.py

import pandas as pd
import os
from typing import Tuple, Optional

from src import config  # Updated import
from src.data_processing import data_loader  # Updated import


def preprocess_data() -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Loads raw data, performs initial cleaning and type conversions.
    Prints initial loading summaries.

    Returns:
        Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
            (players_df, replays_df) - processed DataFrames, or None if loading fails.
    """
    print("\n[Phase 1: Loading Data & Initial Preprocessing]")

    # Use config.DATA_DIR for file paths
    players_file_path = os.path.join(config.DATA_DIR, config.PLAYERS_CSV_FILE)
    replays_file_path = os.path.join(config.DATA_DIR, config.REPLAYS_CSV_FILE)

    players_df = data_loader.load_csv_to_dataframe(players_file_path)
    replays_df = data_loader.load_csv_to_dataframe(replays_file_path)

    if players_df is not None:
        print(
            f"-> Summary for '{config.PLAYERS_CSV_FILE}': {data_loader.get_row_count(players_df):,} rows"
        )
    if replays_df is not None:
        print(
            f"-> Summary for '{config.REPLAYS_CSV_FILE}': {data_loader.get_row_count(replays_df):,} rows"
        )

    # --- Preprocess replays_df (date column) ---
    if replays_df is not None and config.DATE_ANALYSIS_COLUMN in replays_df.columns:
        print(f"\nPreprocessing '{config.REPLAYS_CSV_FILE}'...")
        # Convert to datetime and clean
        replays_df[config.DATE_ANALYSIS_COLUMN] = pd.to_datetime(
            replays_df[config.DATE_ANALYSIS_COLUMN], errors="coerce", utc=True
        )
        initial_count = len(replays_df)
        replays_df.dropna(subset=[config.DATE_ANALYSIS_COLUMN], inplace=True)
        if initial_count > len(replays_df):
            print(
                f"Note: Dropped {initial_count - len(replays_df)} rows with invalid date formats in '{config.DATE_ANALYSIS_COLUMN}'."
            )
        else:
            print("No rows dropped due to invalid date formats.")

    return players_df, replays_df
