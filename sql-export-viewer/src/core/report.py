import argparse
from abc import ABC, abstractmethod
import pandas as pd


class Report(ABC):
    """Abstract base class for all analysis reports."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The command-line name of the report (e.g., 'top-maps')."""
        pass

    @property
    @abstractmethod
    def description(self) -> str:
        """A short description for the CLI help text."""
        pass

    def add_arguments(self, parser: argparse.ArgumentParser):
        """
        Hook to add report-specific arguments to the CLI subparser.
        Default implementation does nothing.
        """
        pass

    @abstractmethod
    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        """
        The main logic of the report. Takes the merged DataFrame and
        parsed CLI arguments, performs analysis, and prints the result.
        """
        pass

    def _get_latest_date(self, df: pd.DataFrame):
        """Helper to get the latest date from the dataframe for time-based reports."""
        from datetime import datetime
        import pytz
        from src import config

        if (
            config.DATE_ANALYSIS_COLUMN in df.columns
            and pd.api.types.is_datetime64_any_dtype(df[config.DATE_ANALYSIS_COLUMN])
        ):
            latest_date = df[config.DATE_ANALYSIS_COLUMN].max()
            if pd.notna(latest_date):
                return latest_date

        print(
            "Warning: Could not determine latest data date. Using current UTC time as a fallback."
        )
        return datetime.now(pytz.utc)

    def _format_df_for_display(self, df: pd.DataFrame, title: str, max_rows: int = 20):
        """Helper to print a dataframe with a title and row limits."""
        print(f"\n--- {title} ---")
        if df is None or df.empty:
            print("No data to display for this report.")
            return

        print(f"Showing {min(len(df), max_rows)} of {len(df)} total rows.")
        print(df.head(max_rows).to_string(index=False))
