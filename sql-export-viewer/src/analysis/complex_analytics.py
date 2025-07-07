import pandas as pd
import pytz
from typing import Optional, List, Tuple
from datetime import datetime
from src import config
from src.analysis import utils
from src.analysis import filters


def _prepare_and_bucket_df(
    df: pd.DataFrame,
    reference_date: datetime,
    map_names: Optional[List[str]],
    os_buckets: List[Tuple[int, int]],
    date_periods_days: List[int],
) -> Optional[pd.DataFrame]:
    processed_df = df.copy()

    if map_names:
        processed_df = filters.filter_by_map_name(processed_df, map_names)
        if processed_df.empty:
            print(f"No data found for specified maps: {', '.join(map_names)}.")
            return None

    bins_os = [bucket[0] for bucket in os_buckets] + [os_buckets[-1][1], float("inf")]
    labels_os = [f"{bucket[0]}-{bucket[1]}" for bucket in os_buckets] + [
        f"{os_buckets[-1][1]}+"
    ]
    processed_df["OS_Bucket"] = pd.cut(
        processed_df[config.AVG_LOBBY_SKILL_COLUMN],
        bins=bins_os,
        labels=labels_os,
        right=False,
        include_lowest=True,
    )
    processed_df["OS_Bucket"] = pd.Categorical(
        processed_df["OS_Bucket"], categories=labels_os, ordered=True
    )

    if (
        reference_date.tzinfo is None
        and processed_df[config.DATE_ANALYSIS_COLUMN].dt.tz is not None
    ):
        reference_date = reference_date.replace(tzinfo=pytz.utc)

    processed_df["days_ago"] = processed_df[config.DATE_ANALYSIS_COLUMN].apply(
        lambda x: (reference_date - x).days
    )

    bins_date = [-1] + date_periods_days + [float("inf")]
    labels_date = [
        f"{date_periods_days[i-1] if i > 0 else 0}-{date_periods_days[i]} days ago"
        for i in range(len(date_periods_days))
    ] + [f"{date_periods_days[-1]}+ days ago"]
    processed_df["Date_Period_Bucket"] = pd.cut(
        processed_df["days_ago"],
        bins=bins_date,
        labels=labels_date,
        right=False,
        include_lowest=True,
    )
    processed_df["Date_Period_Bucket"] = pd.Categorical(
        processed_df["Date_Period_Bucket"], categories=labels_date, ordered=True
    )

    processed_df.dropna(subset=["OS_Bucket", "Date_Period_Bucket"], inplace=True)
    if processed_df.empty:
        print("No data left after bucketing for OS and Date Period.")
        return None
    return processed_df


def get_player_skill_period_performance(
    df: pd.DataFrame,
    reference_date: datetime,
    map_names: Optional[List[str]] = None,
    os_buckets: Optional[List[Tuple[int, int]]] = None,
    date_periods_days: Optional[List[int]] = None,
) -> Optional[pd.DataFrame]:
    required_cols = [
        config.USER_ID_COLUMN,
        config.PLAYER_NAME_COLUMN,
        config.DATE_ANALYSIS_COLUMN,
        config.MAP_NAME_COLUMN,
        config.AVG_LOBBY_SKILL_COLUMN,
        "won",
    ]
    df_prepared = utils.validate_and_prepare_df(
        df, "player_skill_period_performance", required_cols
    )
    if df_prepared is None:
        return None

    df_bucketed = _prepare_and_bucket_df(
        df_prepared,
        reference_date,
        map_names,
        os_buckets or [(0, 10), (10, 20), (20, 30), (30, 40)],
        date_periods_days or [7, 14, 21],
    )
    if df_bucketed is None:
        return None

    player_stats = (
        df_bucketed.groupby(
            ["OS_Bucket", "Date_Period_Bucket", config.USER_ID_COLUMN], observed=True
        )
        .agg(Total_Games=("won", "size"), Wins=("won", "sum"))
        .reset_index()
    )
    player_stats["Win_Rate"] = (
        (player_stats["Wins"] / player_stats["Total_Games"] * 100).fillna(0).round(2)
    )
    latest_player_names = utils.get_latest_player_names(df_bucketed)
    player_stats = pd.merge(
        player_stats, latest_player_names, on=config.USER_ID_COLUMN, how="left"
    )
    player_stats = player_stats.sort_values(
        by=["OS_Bucket", "Date_Period_Bucket", config.PLAYER_NAME_COLUMN]
    )
    return player_stats


def get_overall_skill_period_performance(
    df: pd.DataFrame,
    reference_date: datetime,
    map_names: Optional[List[str]] = None,
    os_buckets: Optional[List[Tuple[int, int]]] = None,
    date_periods_days: Optional[List[int]] = None,
) -> Optional[pd.DataFrame]:
    required_cols = [
        config.DATE_ANALYSIS_COLUMN,
        config.MAP_NAME_COLUMN,
        config.AVG_LOBBY_SKILL_COLUMN,
        "won",
    ]
    df_prepared = utils.validate_and_prepare_df(
        df, "overall_skill_period_performance", required_cols
    )
    if df_prepared is None:
        return None

    df_bucketed = _prepare_and_bucket_df(
        df_prepared,
        reference_date,
        map_names,
        os_buckets or [(0, 10), (10, 20), (20, 30), (30, 40)],
        date_periods_days or [7, 14, 21],
    )
    if df_bucketed is None:
        return None

    overall_stats = (
        df_bucketed.groupby(["OS_Bucket", "Date_Period_Bucket"], observed=True)
        .agg(Total_Games=("won", "size"), Wins=("won", "sum"))
        .reset_index()
    )
    overall_stats["Win_Rate"] = (
        (overall_stats["Wins"] / overall_stats["Total_Games"] * 100).fillna(0).round(2)
    )
    overall_stats = overall_stats.sort_values(by=["OS_Bucket", "Date_Period_Bucket"])
    return overall_stats
