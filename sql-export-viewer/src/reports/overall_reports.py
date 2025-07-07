import argparse
import pandas as pd
from src.core.report import Report
from src.analysis import game_analytics, complex_analytics
from src import config


class OverallWinRateReport(Report):
    @property
    def name(self) -> str:
        return "overall-win-rate"

    @property
    def description(self) -> str:
        return "Calculates the overall win rate across all games."

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        print("\n--- Overall Win Rate ---")
        win_rate = game_analytics.get_overall_win_rate(df)
        if win_rate is not None:
            print(f"Overall Win Rate: {win_rate:.2f}%")
        else:
            print("Could not calculate overall win rate.")


class FactionWinRateReport(Report):
    @property
    def name(self) -> str:
        return "faction-win-rate"

    @property
    def description(self) -> str:
        return "Shows win rates and games played per faction."

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        faction_stats = game_analytics.get_win_rate_by_faction(df)
        self._format_df_for_display(
            faction_stats, "Faction Performance (Win Rate & Games Played)"
        )


class RankedUnrankedStatsReport(Report):
    @property
    def name(self) -> str:
        return "ranked-unranked-stats"

    @property
    def description(self) -> str:
        return "Compares stats between ranked and unranked games."

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        ranked_stats = game_analytics.get_ranked_unranked_stats(df)
        self._format_df_for_display(ranked_stats, "Ranked vs. Unranked Game Statistics")


class OverallSkillPeriodPerformanceReport(Report):
    @property
    def name(self) -> str:
        return "overall-skill-period-performance"

    @property
    def description(self) -> str:
        return "Aggregated win/loss by skill categories and time periods (overall)."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--maps", type=str, help="Optional: Comma-separated list of map names."
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        map_names = (
            [m.strip() for m in args.maps.split(",")]
            if args.maps
            else config.DEFAULT_FILTER_MAPS
        )
        ref_date = self._get_latest_date(df)

        print("\n--- Overall Skill Period Performance Report ---")
        print(f"Reference Date for Periods: {ref_date.strftime('%Y-%m-%d')}")
        print(
            f"Filtering by Maps: {', '.join(map_names) if map_names else 'Default Maps'}"
        )

        result_df = complex_analytics.get_overall_skill_period_performance(
            df, ref_date, map_names
        )
        self._format_df_for_display(
            result_df, "Aggregated Win/Loss by OS and Date Period (Overall)"
        )
