import pandas as pd
import pytz
from typing import Optional, List
from src import config


def validate_and_prepare_df(
    df: pd.DataFrame, function_name: str, specific_required_cols: List[str]
) -> Optional[pd.DataFrame]:
    for col in specific_required_cols:
        if col not in df.columns:
            print(
                f"Warning: Column '{col}' not found in DataFrame for '{function_name}'. Cannot proceed."
            )
            return None
    if df.empty:
        print(f"Warning: DataFrame is empty for '{function_name}'.")
        return None

    processed_df = df.copy()
    date_col = config.DATE_ANALYSIS_COLUMN
    if date_col in processed_df.columns and not pd.api.types.is_datetime64_any_dtype(
        processed_df[date_col]
    ):
        initial_count = len(processed_df)
        processed_df[date_col] = pd.to_datetime(
            processed_df[date_col], errors="coerce", utc=True
        )
        processed_df.dropna(subset=[date_col], inplace=True)
        if initial_count > len(processed_df):
            print(
                f"Note: Dropped {initial_count - len(processed_df)} rows with invalid dates in '{date_col}' for '{function_name}'."
            )
        if processed_df.empty:
            print(
                f"Warning: DataFrame became empty after cleaning dates for '{function_name}'."
            )
            return None
    return processed_df


def get_latest_player_names(df_processed: pd.DataFrame) -> pd.DataFrame:
    required = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        config.DATE_ANALYSIS_COLUMN,
    ]
    if not all(col in df_processed.columns for col in required):
        print("Error: Missing required columns for get_latest_player_names helper.")
        return pd.DataFrame(columns=[config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN])

    df_sorted = df_processed.sort_values(
        by=[config.USER_ID_COLUMN, config.DATE_ANALYSIS_COLUMN], ascending=[True, False]
    )
    return df_sorted.drop_duplicates(subset=[config.USER_ID_COLUMN])[
        [config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN]
    ]
