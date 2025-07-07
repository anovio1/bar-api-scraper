# my_project/src/analysis/data_analyzer.py

import pandas as pd
import pytz
from typing import Optional, List, Union
from datetime import datetime
from src import config  # Updated import


def filter_by_map_name(
    df: pd.DataFrame, map_names: Union[str, List[str]]
) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the map_name matches the given string(s).
    """
    if config.MAP_NAME_COLUMN not in df.columns:
        print(
            f"Warning: Column '{config.MAP_NAME_COLUMN}' not found in DataFrame. Cannot filter by map name."
        )
        return pd.DataFrame(columns=df.columns)

    filtered_df = pd.DataFrame(columns=df.columns)

    if isinstance(map_names, str):
        print_message = f"map '{map_names}' (partial match)"
        filtered_df = df[
            df[config.MAP_NAME_COLUMN]
            .astype(str)
            .str.contains(map_names, case=False, na=False)
        ]
    elif isinstance(map_names, list):
        print_message = f"maps in list ({len(map_names)} maps)"
        filtered_df = df[df[config.MAP_NAME_COLUMN].isin(map_names)]
    else:
        print("Error: 'map_names' must be a string or a list of strings.")
        return pd.DataFrame(columns=df.columns)

    print(f"Filtered for {print_message}: {len(filtered_df):,} rows found.")
    return filtered_df


def filter_by_player_name(df: pd.DataFrame, player_name: str) -> pd.DataFrame:
    """
    Filters a DataFrame to include only rows where the player name matches the given string.
    Performs a case-insensitive partial match.
    """
    if config.PLAYER_NAME_COLUMN not in df.columns:
        print(
            f"Warning: Column '{config.PLAYER_NAME_COLUMN}' not found in DataFrame. Cannot filter by player name."
        )
        return pd.DataFrame(columns=df.columns)

    filtered_df = df[
        df[config.PLAYER_NAME_COLUMN]
        .astype(str)
        .str.contains(player_name, case=False, na=False)
    ]

    print(
        f"Filtered for player name '{player_name}' (partial match): {len(filtered_df):,} rows found."
    )
    return filtered_df


def get_top_n_map_names(df: pd.DataFrame, n: int = 10) -> Optional[pd.Series]:
    """
    Returns the top N most frequently played map names from a DataFrame.
    """
    if config.MAP_NAME_COLUMN not in df.columns:
        print(
            f"Warning: Column '{config.MAP_NAME_COLUMN}' not found in DataFrame. Cannot get top map names."
        )
        return None

    if df.empty:
        print("Warning: DataFrame is empty. No map names to analyze.")
        return None

    map_counts = df[config.MAP_NAME_COLUMN].value_counts()

    return map_counts.head(n)


def get_player_avg_lobby_skill(
    df: pd.DataFrame, last_played_date_threshold: Optional[datetime] = None
) -> Optional[pd.DataFrame]:
    """
    Calculates the overall average lobby skill, last played date, latest name,
    and various game counts for each unique player (user_id).
    Optimized to filter by date threshold early and use vectorized operations for game counts.

    Args:
        df (pd.DataFrame): The merged DataFrame containing player and replay info.
                           Expected to have 'user_id', 'name', 'avg_lobby_skill',
                           'start_time', and 'map_name' columns.
        last_played_date_threshold (Optional[datetime]): If provided, only players who played
                                                        on or after this date will be included.

    Returns:
        Optional[pd.DataFrame]: A DataFrame with aggregated player statistics.
                                Returns None if required columns are missing or data is empty.
    """
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        config.AVG_LOBBY_SKILL_COLUMN,
        config.DATE_ANALYSIS_COLUMN,
        config.MAP_NAME_COLUMN,
        "won",
    ]
    if not all(col in df.columns for col in required_cols):
        print(
            f"Warning: Missing one or more required columns ({required_cols}) for player skill and game count analysis."
        )
        return None

    if df.empty:
        print("Warning: DataFrame is empty. No player data to analyze.")
        return None

    # Ensure 'start_time' is datetime and timezone-aware
    if not pd.api.types.is_datetime64_any_dtype(df[config.DATE_ANALYSIS_COLUMN]):
        df[config.DATE_ANALYSIS_COLUMN] = pd.to_datetime(
            df[config.DATE_ANALYSIS_COLUMN], errors="coerce", utc=True
        )
        df.dropna(subset=[config.DATE_ANALYSIS_COLUMN], inplace=True)
        if df.empty:
            print(
                "Warning: DataFrame became empty after cleaning invalid date formats for player skill analysis."
            )
            return None

    # --- OPTIMIZATION 1: Filter by date threshold *EARLY* ---
    processed_df = df.copy()

    if last_played_date_threshold:
        if last_played_date_threshold.tzinfo is None:
            print(
                "Warning: last_played_date_threshold is not timezone-aware. Assuming UTC."
            )
            last_played_date_threshold = last_played_date_threshold.replace(
                tzinfo=pytz.utc
            )

        initial_rows = len(processed_df)
        processed_df = processed_df[
            processed_df[config.DATE_ANALYSIS_COLUMN] >= last_played_date_threshold
        ]

        if processed_df.empty:
            print(
                f"No player data found on or after {last_played_date_threshold.strftime('%Y-%m-%d %H:%M:%S %Z')} after initial filtering."
            )
            return pd.DataFrame(
                columns=[
                    config.USER_ID_COLUMN,
                    config.PLAYER_NAME_COLUMN,
                    "overall_avg_lobby_skill",
                    "last_played",
                    "total_games_played",
                    "games_played_glitters",
                    "games_played_smolders",
                    "games_played_simmers",
                    "win_rate",
                ]
            )

        print(
            f"Initial date filter applied: Data reduced from {initial_rows:,} to {len(processed_df):,} rows."
        )

    processed_df[config.MAP_NAME_COLUMN] = processed_df[config.MAP_NAME_COLUMN].astype(
        str
    )

    # --- OPTIMIZATION 2: Pre-compute boolean flags for map series using vectorized operations ---
    processed_df["is_glitters"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_GLITTERS_PATTERN, case=False, na=False
    )
    processed_df["is_smolders"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_SMOLDERS_PATTERN, case=False, na=False
    )
    processed_df["is_simmers"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_SIMMERS_PATTERN, case=False, na=False
    )

    # Step 1: Get the latest name and last played date per user_id from the (potentially filtered) data
    df_sorted_for_latest = processed_df.sort_values(
        by=[config.USER_ID_COLUMN, config.DATE_ANALYSIS_COLUMN], ascending=[True, False]
    )
    latest_user_info = df_sorted_for_latest.drop_duplicates(
        subset=[config.USER_ID_COLUMN]
    )[[config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN, config.DATE_ANALYSIS_COLUMN]]
    latest_user_info = latest_user_info.rename(
        columns={config.DATE_ANALYSIS_COLUMN: "last_played"}
    )

    # Step 2: Perform comprehensive aggregation on the (now filtered and pre-computed) DataFrame grouped by user_id
    player_stats_aggregated = (
        processed_df.groupby(config.USER_ID_COLUMN)
        .agg(
            overall_avg_lobby_skill=(config.AVG_LOBBY_SKILL_COLUMN, "mean"),
            total_games_played=(config.USER_ID_COLUMN, "size"),
            games_played_glitters=("is_glitters", "sum"),
            games_played_smolders=("is_smolders", "sum"),
            games_played_simmers=("is_simmers", "sum"),
            total_wins=("won", "sum"),
        )
        .reset_index()
    )

    player_stats_aggregated["win_rate"] = (
        (
            player_stats_aggregated["total_wins"]
            / player_stats_aggregated["total_games_played"]
            * 100
        )
        .fillna(0)
        .round(2)
    )

    # Step 3: Merge the aggregated statistics with the latest user info (name and last_played)
    player_stats = pd.merge(
        player_stats_aggregated, latest_user_info, on=config.USER_ID_COLUMN, how="left"
    )

    # Step 4: Sort final results
    player_stats = player_stats.sort_values(
        by="overall_avg_lobby_skill", ascending=False
    )

    return player_stats


# --- NEW ANALYTICS FUNCTIONS ---


def get_overall_win_rate(df: pd.DataFrame) -> Optional[float]:
    """Calculates the overall win rate across all player games."""
    if "won" not in df.columns:
        print("Warning: 'won' column not found for win rate calculation.")
        return None
    if df.empty:
        print("Warning: DataFrame is empty. Cannot calculate overall win rate.")
        return None

    win_rate = df["won"].mean() * 100
    return win_rate


def get_win_rate_by_faction(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Calculates win rate and games played for each faction."""
    required_cols = ["faction", "won"]
    if not all(col in df.columns for col in required_cols):
        print(
            f"Warning: Missing one or more required columns ({required_cols}) for faction win rate calculation."
        )
        return None
    if df.empty:
        print("Warning: DataFrame is empty. No faction data to analyze.")
        return None

    faction_stats = (
        df.groupby("faction")
        .agg(games_played=("won", "size"), win_rate=("won", lambda x: x.mean() * 100))
        .reset_index()
    )

    faction_stats["win_rate"] = faction_stats["win_rate"].round(2)
    faction_stats = faction_stats.sort_values(by="games_played", ascending=False)

    return faction_stats


def get_top_n_players_by_win_rate(
    df: pd.DataFrame, n: int = 10
) -> Optional[pd.DataFrame]:
    """
    Calculates top N players by win rate (min 5 games played) from the provided DataFrame.
    Picks the latest name for each user_id.
    """
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        "won",
        config.DATE_ANALYSIS_COLUMN,
    ]
    if not all(col in df.columns for col in required_cols):
        print(
            f"Warning: Missing one or more required columns ({required_cols}) for top players by win rate."
        )
        return None
    if df.empty:
        print("Warning: DataFrame is empty. No player data to analyze for win rate.")
        return None

    if not pd.api.types.is_datetime64_any_dtype(df[config.DATE_ANALYSIS_COLUMN]):
        df[config.DATE_ANALYSIS_COLUMN] = pd.to_datetime(
            df[config.DATE_ANALYSIS_COLUMN], errors="coerce", utc=True
        )
        df.dropna(subset=[config.DATE_ANALYSIS_COLUMN], inplace=True)
        if df.empty:
            return None

    # Get latest name per user_id
    df_sorted_for_latest = df.sort_values(
        by=[config.USER_ID_COLUMN, config.DATE_ANALYSIS_COLUMN], ascending=[True, False]
    )
    latest_user_names = df_sorted_for_latest.drop_duplicates(
        subset=[config.USER_ID_COLUMN]
    )[[config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN]]

    player_win_stats = (
        df.groupby(config.USER_ID_COLUMN)
        .agg(total_games_played=("won", "size"), wins=("won", "sum"))
        .reset_index()
    )

    min_games_threshold = 5
    player_win_stats = player_win_stats[
        player_win_stats["total_games_played"] >= min_games_threshold
    ]

    if player_win_stats.empty:
        print(f"No players found with at least {min_games_threshold} games played.")
        return pd.DataFrame(
            columns=[
                config.USER_ID_COLUMN,
                config.PLAYER_NAME_COLUMN,
                "win_rate",
                "total_games_played",
            ]
        )

    player_win_stats["win_rate"] = (
        player_win_stats["wins"] / player_win_stats["total_games_played"] * 100
    ).round(2)

    player_win_stats = pd.merge(
        player_win_stats, latest_user_names, on=config.USER_ID_COLUMN, how="left"
    )

    player_win_stats = player_win_stats.sort_values(by="win_rate", ascending=False)

    return player_win_stats.head(n)


def get_ranked_unranked_stats(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    Compares key statistics between ranked and unranked games.
    """
    required_cols = [
        "ranked",
        config.AVG_LOBBY_SKILL_COLUMN,
        "duration_ms",
        "player_count",
    ]
    if not all(col in df.columns for col in required_cols):
        print(
            f"Warning: Missing one or more required columns ({required_cols}) for ranked/unranked stats."
        )
        return None
    if df.empty:
        print("Warning: DataFrame is empty. No ranked/unranked data to analyze.")
        return None

    ranked_stats = (
        df.groupby("ranked")
        .agg(
            total_games=("ranked", "size"),
            avg_lobby_skill=(config.AVG_LOBBY_SKILL_COLUMN, "mean"),
            avg_duration_min=("duration_ms", lambda x: (x / 60000).mean()),
            avg_player_count=("player_count", "mean"),
        )
        .reset_index()
    )

    ranked_stats["avg_lobby_skill"] = ranked_stats["avg_lobby_skill"].round(2)
    ranked_stats["avg_duration_min"] = ranked_stats["avg_duration_min"].round(2)
    ranked_stats["avg_player_count"] = ranked_stats["avg_player_count"].round(1)

    return ranked_stats
