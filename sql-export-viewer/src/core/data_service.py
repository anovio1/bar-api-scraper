import pandas as pd
import os
from typing import Optional, Tuple, cast
from src import config


class DataService:
    """Manages loading, merging, and caching of the primary DataFrame."""

    def __init__(self):
        self._merged_df: Optional[pd.DataFrame] = None

    def get_data(self) -> Optional[pd.DataFrame]:
        """
        Public method to get the merged DataFrame.
        Handles loading from cache or rebuilding from source if necessary.
        """
        if self._merged_df is not None:
            return self._merged_df

        print(
            f"\nChecking cache: '{os.path.basename(config.CACHED_MERGED_DF_FILE)}'..."
        )
        if self._is_cache_valid():
            try:
                print("Cache valid. Loading merged data from cache...")
                self._merged_df = pd.read_parquet(config.CACHED_MERGED_DF_FILE)
                print(f"Successfully loaded {len(self._merged_df):,} rows from cache.")
                return self._merged_df
            except Exception as e:
                print(f"Error loading from cache: {e}. Rebuilding...")

        else:
            print("Cache invalid or not found. Rebuilding merged DataFrame...")

        players_df, replays_df = self._load_and_preprocess_raw_data()
        if players_df is None or replays_df is None:
            print("Error: Could not load raw data. Aborting.")
            return None

        self._merged_df = self._merge_dataframes(players_df, replays_df)

        if self._merged_df is not None and not self._merged_df.empty:
            self._save_to_cache(self._merged_df)

        return self._merged_df

    def _is_cache_valid(self) -> bool:
        cache_path = config.CACHED_MERGED_DF_FILE
        if not os.path.exists(cache_path):
            return False

        players_path = os.path.join(config.DATA_DIR, config.PLAYERS_CSV_FILE)
        replays_path = os.path.join(config.DATA_DIR, config.REPLAYS_CSV_FILE)

        if not os.path.exists(players_path) or not os.path.exists(replays_path):
            print("Warning: Source CSV file(s) missing. Cannot validate cache.")
            return False  # Rebuild if sources are gone

        cache_mtime = os.path.getmtime(cache_path)
        players_mtime = os.path.getmtime(players_path)
        replays_mtime = os.path.getmtime(replays_path)

        return cache_mtime >= players_mtime and cache_mtime >= replays_mtime

    def _load_and_preprocess_raw_data(
        self,
    ) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        print("\n[Phase 1: Loading & Preprocessing Raw Data]")
        players_path = os.path.join(config.DATA_DIR, config.PLAYERS_CSV_FILE)
        replays_path = os.path.join(config.DATA_DIR, config.REPLAYS_CSV_FILE)

        try:
            players_df = pd.read_csv(players_path)
            print(f"-> Loaded '{config.PLAYERS_CSV_FILE}': {len(players_df):,} rows")
            replays_df = pd.read_csv(replays_path)
            print(f"-> Loaded '{config.REPLAYS_CSV_FILE}': {len(replays_df):,} rows")

            # Preprocess replays date column
            date_col = config.DATE_ANALYSIS_COLUMN
            initial_count = len(replays_df)
            replays_df[date_col] = pd.to_datetime(
                replays_df[date_col], errors="coerce", utc=True
            )
            replays_df.dropna(subset=[date_col], inplace=True)
            if len(replays_df) < initial_count:
                print(
                    f"Note: Dropped {initial_count - len(replays_df)} rows with invalid dates."
                )

            return players_df, replays_df

        except FileNotFoundError as e:
            print(f"Error: Raw data file not found - {e}")
            return None, None
        except Exception as e:
            print(f"An unexpected error occurred during raw data loading: {e}")
            return None, None

    def _merge_dataframes(
        self, players_df: pd.DataFrame, replays_df: pd.DataFrame
    ) -> Optional[pd.DataFrame]:
        print("\n[Phase 2: Merging DataFrames]")
        merged = pd.merge(
            left=players_df, right=replays_df, on=config.REPLAY_ID_COLUMN, how="left"
        )
        print(f"Successfully merged data. New DataFrame has {len(merged):,} rows.")
        return merged

    def _save_to_cache(self, df: pd.DataFrame):
        try:
            os.makedirs(config.CACHE_DIR, exist_ok=True)
            print(
                f"Saving merged DataFrame to cache: '{os.path.basename(config.CACHED_MERGED_DF_FILE)}'..."
            )

            # Ensure compatibility with Parquet
            df_to_save = df.copy()
            for col in df_to_save.select_dtypes(include=["object"]).columns:
                df_to_save[col] = df_to_save[col].astype("string")

            df_to_save.to_parquet(config.CACHED_MERGED_DF_FILE)
            print("Cache saved successfully.")
        except Exception as e:
            print(f"Warning: Could not save merged DataFrame to cache: {e}")
