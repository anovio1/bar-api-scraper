import pandas as pd
import pytz
from typing import Optional, List
from datetime import datetime
from src import config
from src.analysis import filters, utils


def get_player_avg_lobby_skill(
    df: pd.DataFrame, last_played_date_threshold: Optional[datetime] = None
) -> Optional[pd.DataFrame]:
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        config.AVG_LOBBY_SKILL_COLUMN,
        config.DATE_ANALYSIS_COLUMN,
        config.MAP_NAME_COLUMN,
        "won",
    ]
    processed_df = utils.validate_and_prepare_df(
        df, "player_avg_lobby_skill", required_cols
    )
    if processed_df is None:
        return None

    if last_played_date_threshold:
        if last_played_date_threshold.tzinfo is None:
            last_played_date_threshold = last_played_date_threshold.replace(
                tzinfo=pytz.utc
            )
        initial_rows = len(processed_df)
        processed_df = processed_df[
            processed_df[config.DATE_ANALYSIS_COLUMN] >= last_played_date_threshold
        ]
        if processed_df.empty:
            print(
                f"No player data found on or after {last_played_date_threshold.strftime('%Y-%m-%d')}."
            )
            return pd.DataFrame()
        print(
            f"Date filter applied: Data reduced from {initial_rows:,} to {len(processed_df):,} rows."
        )

    processed_df[config.MAP_NAME_COLUMN] = processed_df[config.MAP_NAME_COLUMN].astype(
        str
    )
    processed_df["is_glitters"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_GLITTERS_PATTERN, case=False, na=False
    )
    processed_df["is_smolders"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_SMOLDERS_PATTERN, case=False, na=False
    )
    processed_df["is_simmers"] = processed_df[config.MAP_NAME_COLUMN].str.contains(
        config.MAP_SIMMERS_PATTERN, case=False, na=False
    )

    agg_dict = {
        "overall_avg_lobby_skill": (config.AVG_LOBBY_SKILL_COLUMN, "mean"),
        "total_games_played": (config.USER_ID_COLUMN, "size"),
        "games_played_glitters": ("is_glitters", "sum"),
        "games_played_smolders": ("is_smolders", "sum"),
        "games_played_simmers": ("is_simmers", "sum"),
        "total_wins": ("won", "sum"),
    }
    player_stats_agg = (
        processed_df.groupby(config.USER_ID_COLUMN).agg(**agg_dict).reset_index()
    )
    player_stats_agg["win_rate"] = (
        (player_stats_agg["total_wins"] / player_stats_agg["total_games_played"] * 100)
        .fillna(0)
        .round(2)
    )

    df_sorted = processed_df.sort_values(
        by=[config.USER_ID_COLUMN, config.DATE_ANALYSIS_COLUMN], ascending=[True, False]
    )
    latest_info = df_sorted.drop_duplicates(subset=[config.USER_ID_COLUMN])[
        [config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN, config.DATE_ANALYSIS_COLUMN]
    ]
    latest_info = latest_info.rename(
        columns={config.DATE_ANALYSIS_COLUMN: "last_played"}
    )

    player_stats = pd.merge(
        player_stats_agg, latest_info, on=config.USER_ID_COLUMN, how="left"
    )
    return player_stats.sort_values(by="overall_avg_lobby_skill", ascending=False)


def get_top_n_players_by_win_rate(
    df: pd.DataFrame, n: int = 10
) -> Optional[pd.DataFrame]:
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        "won",
        config.DATE_ANALYSIS_COLUMN,
    ]
    df_prepared = utils.validate_and_prepare_df(
        df, "top_n_players_by_win_rate", required_cols
    )
    if df_prepared is None:
        return None

    player_win_stats = (
        df_prepared.groupby(config.USER_ID_COLUMN)
        .agg(total_games_played=("won", "size"), wins=("won", "sum"))
        .reset_index()
    )

    min_games = 5
    player_win_stats = player_win_stats[
        player_win_stats["total_games_played"] >= min_games
    ]
    if player_win_stats.empty:
        print(f"No players found with at least {min_games} games played.")
        return pd.DataFrame()

    player_win_stats["win_rate"] = (
        (player_win_stats["wins"] / player_win_stats["total_games_played"] * 100)
        .fillna(0)
        .round(2)
    )
    latest_names = utils.get_latest_player_names(df_prepared)
    player_win_stats = pd.merge(
        player_win_stats, latest_names, on=config.USER_ID_COLUMN, how="left"
    )
    return player_win_stats.sort_values(by="win_rate", ascending=False).head(n)


def get_wins_above_os_leaderboard(
    df: pd.DataFrame,
    os_thresholds: Optional[List[int]] = None,
    since_date: Optional[datetime] = None,
    map_names: Optional[List[str]] = None,
) -> Optional[pd.DataFrame]:
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        config.AVG_LOBBY_SKILL_COLUMN,
        "won",
        config.DATE_ANALYSIS_COLUMN,
    ]
    df_prepared = utils.validate_and_prepare_df(
        df, "wins_above_os_leaderboard", required_cols
    )
    if df_prepared is None:
        return None

    df_filtered = df_prepared.copy()
    if since_date:
        if since_date.tzinfo is None:
            since_date = since_date.replace(tzinfo=pytz.utc)
        df_filtered = df_filtered[
            df_filtered[config.DATE_ANALYSIS_COLUMN] >= since_date
        ]
        if df_filtered.empty:
            return pd.DataFrame()

    if map_names:
        df_filtered = filters.filter_by_map_name(df_filtered, map_names)
        if df_filtered.empty:
            return pd.DataFrame()

    thresholds = sorted(
        os_thresholds if os_thresholds is not None else config.DEFAULT_OS_THRESHOLDS,
        reverse=True,
    )
    agg_dict = {}
    for t in thresholds:
        col_name = f"wins_above_{t}os"
        df_filtered[f"won_at_os_{t}"] = (
            df_filtered["won"].astype(bool)
            & (df_filtered[config.AVG_LOBBY_SKILL_COLUMN] >= t)
        ).astype(int)
        agg_dict[col_name] = (f"won_at_os_{t}", "sum")

    df_agg = df_filtered.groupby(config.USER_ID_COLUMN).agg(**agg_dict).reset_index()

    latest_names = utils.get_latest_player_names(df_filtered)
    total_games = (
        df_filtered.groupby(config.USER_ID_COLUMN)
        .size()
        .reset_index(name="total_games_played")
    )
    df_enriched = pd.merge(df_agg, latest_names, on=config.USER_ID_COLUMN, how="left")
    df_enriched = pd.merge(
        df_enriched, total_games, on=config.USER_ID_COLUMN, how="left"
    )

    final_cols = (
        [config.USER_ID_COLUMN, config.PLAYER_NAME_COLUMN]
        + [f"wins_above_{t}os" for t in thresholds]
        + ["total_games_played"]
    )
    return df_enriched[final_cols].sort_values(
        by=f"wins_above_{thresholds[0]}os", ascending=False
    )
