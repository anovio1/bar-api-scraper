# workflow.py

import pandas as pd
import os
from typing import Optional, List, cast
from datetime import datetime

from src import config
from src.data_processing import data_loader
from src.data_processing import data_preprocessor
from src.reporting import report_generator


class DataAnalysisWorkflow:
    """
    Orchestrates the entire data analysis pipeline from loading to reporting.
    Manages the state of DataFrames throughout the workflow.
    """

    def __init__(self):
        self.players_df: Optional[pd.DataFrame] = None
        self.replays_df: Optional[pd.DataFrame] = None
        self.merged_df: Optional[pd.DataFrame] = None

    def _display_phase_header(self, phase_name: str):
        """Helper to display consistent phase headers."""
        print(f"\n[{phase_name}]")

    def _load_and_preprocess_data_step(self):
        """Loads and preprocesses the raw data files."""
        self._display_phase_header("Data Loading & Preprocessing")
        self.players_df, self.replays_df = data_preprocessor.preprocess_data()

    def _merge_dataframes_step(self):
        """Merges the players and replays DataFrames."""
        self._display_phase_header("Data Merging")

        if self.players_df is None or self.replays_df is None or self.replays_df.empty:
            print(
                "Cannot proceed with merge: one or more source DataFrames are not available or empty after preprocessing."
            )
            return

        print(
            f"Joining '{config.PLAYERS_CSV_FILE}' and '{config.REPLAYS_CSV_FILE}' on '{config.REPLAY_ID_COLUMN}'..."
        )
        self.merged_df = pd.merge(
            left=self.players_df,
            right=self.replays_df,
            on=config.REPLAY_ID_COLUMN,
            how="left",
        )
        print(
            f"Successfully merged data. New DataFrame has {data_loader.get_row_count(self.merged_df):,} rows."
        )
        print("\n--- First 5 rows of merged data (after join) ---")
        print(self.merged_df.head())

    def _is_cache_valid(self) -> bool:
        """
        Checks if the cached merged DataFrame file exists and is newer than its source CSVs.
        """
        if not os.path.exists(config.CACHED_MERGED_DF_FILE):
            return False

        cache_mtime = os.path.getmtime(config.CACHED_MERGED_DF_FILE)

        # Get actual paths for source CSVs
        players_csv_path = os.path.join(config.DATA_DIR, config.PLAYERS_CSV_FILE)
        replays_csv_path = os.path.join(config.DATA_DIR, config.REPLAYS_CSV_FILE)

        # Basic check if source files exist before getting their mtime
        if not os.path.exists(players_csv_path) or not os.path.exists(replays_csv_path):
            print(
                f"Warning: One or both source CSV files are missing. Cannot validate cache against them. Rebuilding cache."
            )
            return False  # Source files missing, cache can't be validated as up-to-date

        players_csv_mtime = os.path.getmtime(players_csv_path)
        replays_csv_mtime = os.path.getmtime(replays_csv_path)

        return cache_mtime >= players_csv_mtime and cache_mtime >= replays_csv_mtime

    def _ensure_merged_df_loaded(self) -> bool:
        """
        Ensures that merged_df is loaded, preferentially from cache.
        If not, attempts to load, preprocess, merge, and then cache.
        Returns True if merged_df is available, False otherwise.
        """
        if self.merged_df is not None:
            return True

        if self._is_cache_valid():
            try:
                print(
                    f"\nAttempting to load merged data from cache: '{config.CACHED_MERGED_DF_FILE}'..."
                )
                self.merged_df = pd.read_parquet(config.CACHED_MERGED_DF_FILE)
                print(f"Successfully loaded {len(self.merged_df):,} rows from cache.")
                return True
            except Exception as e:
                print(f"Error loading from cache: {e}. Rebuilding merged DataFrame...")
                self.merged_df = None

        print("\nRebuilding merged DataFrame from source files...")
        self._load_and_preprocess_data_step()
        self._merge_dataframes_step()

        if self.merged_df is not None and not self.merged_df.empty:
            try:
                os.makedirs(config.CACHE_DIR, exist_ok=True)
                print(
                    f"Saving merged DataFrame to cache: '{config.CACHED_MERGED_DF_FILE}'..."
                )
                assert isinstance(self.merged_df, pd.DataFrame)
                merged_df = cast(pd.DataFrame, self.merged_df)

                for col in merged_df.columns:
                    if self.merged_df[col].dtype == "object":
                        self.merged_df[col] = self.merged_df[col].astype("string")

                self.merged_df.to_parquet(config.CACHED_MERGED_DF_FILE)
                print("Cache saved successfully.")
            except Exception as e:
                print(f"Warning: Could not save merged DataFrame to cache: {e}")

        return self.merged_df is not None and not self.merged_df.empty

    def run_full_analysis_workflow(self, filter_maps: Optional[List[str]] = None):
        """
        Main method to execute the end-to-end data analysis workflow.
        This orchestrates the high-level steps.
        """
        print("--- Starting Full Analysis Workflow ---")

        self._load_and_preprocess_data_step()
        self._run_initial_diagnostics_step()
        self._merge_dataframes_step()
        self._run_advanced_analysis_and_reports_step(filter_maps)

        print("\n--- Workflow Finished ---")

    def _run_initial_diagnostics_step(self):
        """Runs and reports initial diagnostics on the preprocessed data."""
        report_generator.generate_date_diagnostic_report(
            self.replays_df, config.REPLAYS_CSV_FILE
        )

    def _run_advanced_analysis_and_reports_step(
        self, filter_maps: Optional[List[str]] = None
    ):
        """Runs specific data analyses and generates their reports."""
        self._display_phase_header("Advanced Analysis & Reporting")

        if self.merged_df is None or self.merged_df.empty:
            print(
                "Skipping advanced analysis as merged DataFrame is not available or empty."
            )
            return

        report_generator.generate_player_avg_lobby_skill_report(
            self.merged_df, config.LAST_PLAYED_DATE_THRESHOLD
        )
        report_generator.generate_top_maps_report(self.merged_df)

        report_generator.generate_overall_win_rate_report(self.merged_df)
        report_generator.generate_faction_win_rate_report(self.merged_df)
        report_generator.generate_top_players_by_win_rate_report(self.merged_df, n=10)
        report_generator.generate_ranked_unranked_stats_report(self.merged_df)

        maps_to_filter = (
            filter_maps if filter_maps is not None else config.DEFAULT_FILTER_MAPS
        )
        report_generator.generate_filtered_maps_report(self.merged_df, maps_to_filter)

    def run_player_name_filter(self, player_name: str):
        """Loads data, merges it (potentially from cache), then filters and reports on a specific player name."""
        print(f"--- Running Player Name Filter for '{player_name}' ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_filtered_players_report(
                self.merged_df, player_name
            )
        else:
            print("Could not load data to filter players.")
        print("--- Filter Query Finished ---")

    def run_top_maps_report(self):
        """Loads data, merges it (potentially from cache), then reports on the top maps."""
        print("--- Running Top Maps Report ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_top_maps_report(self.merged_df)
        else:
            print("Could not load data to generate top maps report.")
        print("--- Top Maps Report Finished ---")

    def run_player_skill_report(self, since_date: Optional[datetime] = None):
        """Loads data, merges it (potentially from cache), then reports on player average lobby skill,
        optionally filtered by a last played date."""
        print("--- Running Player Skill Report ---")
        if self._ensure_merged_df_loaded():
            threshold_to_use = (
                since_date
                if since_date is not None
                else config.LAST_PLAYED_DATE_THRESHOLD
            )
            report_generator.generate_player_avg_lobby_skill_report(
                self.merged_df, threshold_to_use
            )
        else:
            print("Could not load data for player skill report.")
        print("--- Player Skill Report Finished ---")

    def run_overall_win_rate_report(self):
        """Loads data, merges it, and reports on the overall win rate."""
        print("--- Running Overall Win Rate Report ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_overall_win_rate_report(self.merged_df)
        else:
            print("Could not load data to generate overall win rate report.")
        print("--- Overall Win Rate Report Finished ---")

    def run_faction_win_rate_report(self):
        """Loads data, merges it, and reports on win rates by faction."""
        print("--- Running Faction Win Rate Report ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_faction_win_rate_report(self.merged_df)
        else:
            print("Could not load data to generate faction win rate report.")
        print("--- Faction Win Rate Report Finished ---")

    def run_top_players_by_win_rate_report(self, n: int = 10):
        """Loads data, merges it, and reports on top players by win rate."""
        print(f"--- Running Top {n} Players by Win Rate Report ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_top_players_by_win_rate_report(
                self.merged_df, n
            )  # Corrected to call report_generator
        else:
            print("Could not load data to generate top players by win rate report.")
        print("--- Top Players by Win Rate Report Finished ---")

    def run_ranked_unranked_stats_report(self):
        """Loads data, merges it, and reports on ranked vs. unranked game statistics."""
        print("--- Running Ranked vs. Unranked Stats Report ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_ranked_unranked_stats_report(self.merged_df)
        else:
            print("Could not load data to generate ranked vs. unranked stats report.")
        print("--- Ranked vs. Unranked Stats Report Finished ---")

    def run_filtered_maps_report(self, map_names: List[str]):
        """
        Loads data, merges it (potentially from cache), and then filters and reports on specific map names.
        """
        print(f"--- Running Filtered Maps Report for {len(map_names)} maps ---")
        if self._ensure_merged_df_loaded():
            report_generator.generate_filtered_maps_report(self.merged_df, map_names)
        else:
            print("Could not load data to filter maps.")
        print("--- Filtered Maps Report Finished ---")
