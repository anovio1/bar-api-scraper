# my_project/src/reporting/report_generator.py

import pandas as pd
from typing import List, Optional
from datetime import datetime

from src import config  # Updated import
from src.analysis import diagnostics  # Updated import
from src.analysis import data_analyzer  # Updated import


def generate_date_diagnostic_report(df: Optional[pd.DataFrame], file_name: str):
    """Generates and prints a summary report for date diagnostics."""
    print("\n[Phase: Date Diagnostic Report]")

    if df is None:
        print(
            f"Skipping date diagnostics because the DataFrame for '{file_name}' is not available."
        )
        return

    if df.empty:
        print(f"Warning: '{file_name}' is empty. No date diagnostics to report.")
        return

    if config.DATE_ANALYSIS_COLUMN not in df.columns:
        print(
            f"Skipping date diagnostics because column '{config.DATE_ANALYSIS_COLUMN}' not found in '{file_name}'."
        )
        return

    if not pd.api.types.is_datetime64_any_dtype(df[config.DATE_ANALYSIS_COLUMN]):
        print(
            f"Skipping date diagnostics because column '{config.DATE_ANALYSIS_COLUMN}' is not a datetime type."
        )
        return

    print("\n--- Date Diagnostic Summary ---")
    print(f"File Analyzed: '{file_name}'")
    print(f"Date Column:   '{config.DATE_ANALYSIS_COLUMN}'")
    print("-" * 30)

    oldest_date, latest_date = diagnostics.analyze_date_range(
        df, config.DATE_ANALYSIS_COLUMN
    )
    date_gaps = diagnostics.find_date_gaps(df, config.DATE_ANALYSIS_COLUMN)

    if oldest_date is None or latest_date is None:
        print(
            "Could not determine date range. The date column may be empty or contain only invalid dates after cleaning."
        )
        return

    print(f"Oldest Replay: {oldest_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"Latest Replay: {latest_date.strftime('%Y-%m-%d %H:%M:%S %Z')}")

    total_days = (latest_date.date() - oldest_date.date()).days + 1
    print(f"Total Timespan: {total_days} days")

    print("\n--- Daily Data Gaps ---")
    if not date_gaps:
        print("✅ No full-day gaps found. Data exists for every day in the span.")
    else:
        print(f"⚠️ Found {len(date_gaps)} day(s) with no replay data:")
        for gap_date in date_gaps[:5]:
            print(f"  - {gap_date.strftime('%Y-%m-%d')}")
        if len(date_gaps) > 5:
            print(f"  ... and {len(date_gaps) - 5} more.")


def generate_top_maps_report(df: Optional[pd.DataFrame]):
    """Generates and prints a report for the top N map names."""
    print("\n--- Top 10 Map Names Played ---")
    if df is None or df.empty:
        print("No data available to determine top maps.")
        return

    top_maps = data_analyzer.get_top_n_map_names(df, n=10)
    if top_maps is not None:
        print(top_maps.to_string())
    else:
        print("Could not retrieve top maps.")


def generate_filtered_maps_report(
    df: Optional[pd.DataFrame], target_map_names: List[str]
):
    """Generates and prints a report for maps filtered by a specific list."""
    print(
        f"\n--- Filtering by a specific list of {len(target_map_names)} map names ---"
    )
    if df is None or df.empty:
        print("No data available to filter maps.")
        return

    filtered_maps_df = data_analyzer.filter_by_map_name(df, target_map_names)

    if not filtered_maps_df.empty:
        print(
            f"\nFirst 5 rows for the filtered map list (Total: {len(filtered_maps_df):,} rows):"
        )
        print(filtered_maps_df.head())
    else:
        print("No replays found for the specified list of map names.")


def generate_filtered_players_report(df: Optional[pd.DataFrame], player_name: str):
    """
    Generates and prints a report for players filtered by a specific name.
    """
    print(f"\n--- Filtering by Player Name: '{player_name}' ---")
    if df is None or df.empty:
        print("No data available to filter players.")
        return

    filtered_players_df = data_analyzer.filter_by_player_name(df, player_name)

    if not filtered_players_df.empty:
        print(
            f"\nFirst 5 rows for players matching '{player_name}' (Total: {len(filtered_players_df):,} rows):"
        )
        print(filtered_players_df.head())
    else:
        print(f"No players found matching '{player_name}'.")


def generate_player_avg_lobby_skill_report(
    df: Optional[pd.DataFrame], last_played_date_threshold: Optional[datetime] = None
):
    """
    Generates and prints a report for each player's overall average lobby skill.
    Includes username, last played date, and various game counts, with optional filtering.
    """
    print("\n--- Player Overall Average Lobby Skill ---")
    if last_played_date_threshold:
        print(
            f" (Filtering for players active on/after: {last_played_date_threshold.strftime('%Y-%m-%d %H:%M:%S %Z')})"
        )

    if df is None or df.empty:
        print("No data available to analyze player skills.")
        return

    player_skills_df = data_analyzer.get_player_avg_lobby_skill(
        df, last_played_date_threshold
    )

    if player_skills_df is not None and not player_skills_df.empty:
        print(
            f"Overall average lobby skill for {len(player_skills_df):,} unique player(s):"
        )

        if "last_played" in player_skills_df.columns:
            player_skills_df["last_played"] = player_skills_df[
                "last_played"
            ].dt.strftime("%Y-%m-%d %H:%M:%S %Z")

        cols_to_display = [
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
        cols_to_display = [
            col for col in cols_to_display if col in player_skills_df.columns
        ]

        print(player_skills_df[cols_to_display].head(10).to_string(index=False))
        if len(player_skills_df) > 10:
            print("...")
            print(player_skills_df[cols_to_display].tail(5).to_string(index=False))
    else:
        print(
            "Could not retrieve player average lobby skills (possibly due to missing columns or no players matching filter criteria)."
        )


def generate_overall_win_rate_report(df: Optional[pd.DataFrame]):
    """Generates and prints the overall win rate."""
    print("\n--- Overall Win Rate ---")
    if df is None or df.empty:
        print("No data available to calculate overall win rate.")
        return

    overall_win_rate = data_analyzer.get_overall_win_rate(df)
    if overall_win_rate is not None:
        print(f"Overall Win Rate: {overall_win_rate:.2f}%")
    else:
        print("Could not calculate overall win rate.")


def generate_faction_win_rate_report(df: Optional[pd.DataFrame]):
    """Generates and prints win rate and games played for each faction."""
    print("\n--- Faction Performance (Win Rate & Games Played) ---")
    if df is None or df.empty:
        print("No data available to analyze faction performance.")
        return

    faction_stats_df = data_analyzer.get_win_rate_by_faction(df)
    if faction_stats_df is not None and not faction_stats_df.empty:
        print(faction_stats_df.to_string(index=False))
    else:
        print("Could not retrieve faction performance data.")


def generate_top_players_by_win_rate_report(df: Optional[pd.DataFrame], n: int = 10):
    """Generates and prints top N players by win rate."""
    print(f"\n--- Top {n} Players by Win Rate (min 5 games) ---")
    if df is None or df.empty:
        print("No data available to determine top players by win rate.")
        return

    top_players_df = data_analyzer.get_top_n_players_by_win_rate(df, n)
    if top_players_df is not None and not top_players_df.empty:
        print(
            top_players_df[
                [
                    config.USER_ID_COLUMN,
                    config.PLAYER_NAME_COLUMN,
                    "win_rate",
                    "total_games_played",
                ]
            ].to_string(index=False)
        )
    else:
        print(
            "Could not retrieve top players by win rate (or no players meet min games threshold)."
        )


def generate_ranked_unranked_stats_report(df: Optional[pd.DataFrame]):
    """Generates and prints comparison statistics for ranked vs. unranked games."""
    print("\n--- Ranked vs. Unranked Game Statistics ---")
    if df is None or df.empty:
        print("No data available to compare ranked vs. unranked games.")
        return

    ranked_unranked_df = data_analyzer.get_ranked_unranked_stats(df)
    if ranked_unranked_df is not None and not ranked_unranked_df.empty:
        print(ranked_unranked_df.to_string(index=False))
    else:
        print("Could not retrieve ranked vs. unranked statistics.")
