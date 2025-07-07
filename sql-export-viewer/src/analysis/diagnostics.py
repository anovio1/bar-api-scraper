# my_project/src/analysis/diagnostics.py

import pandas as pd
from typing import List, Tuple, Optional
from src import config  # Updated import


def analyze_date_range(
    df: pd.DataFrame, column_name: str
) -> Tuple[Optional[pd.Timestamp], Optional[pd.Timestamp]]:
    """
    Finds the oldest and latest dates in a specified datetime column.
    Returns (None, None) if the column is empty or all NaT.
    """
    valid_dates = df[column_name].dropna()
    if valid_dates.empty:
        return (None, None)

    min_date = valid_dates.min()
    max_date = valid_dates.max()
    return min_date, max_date


def find_date_gaps(df: pd.DataFrame, column_name: str) -> List[pd.Timestamp]:
    """
    Identifies full days missing from a date series.
    Returns an empty list if the column is not datetime-like, empty, or all NaT.
    """
    if df.empty or not pd.api.types.is_datetime64_any_dtype(df[column_name]):
        return []

    actual_dates = df[column_name].dt.normalize().dropna().unique()
    if actual_dates.size == 0:
        return []

    min_date = pd.Timestamp(actual_dates.min())
    max_date = pd.Timestamp(actual_dates.max())

    full_date_range = pd.date_range(start=min_date, end=max_date, freq="D")
    missing_dates = set(full_date_range) - set(actual_dates)

    return sorted(list(missing_dates))
