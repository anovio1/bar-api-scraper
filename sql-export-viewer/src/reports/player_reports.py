import argparse
from datetime import datetime
import pytz
import pandas as pd
from src.core.report import Report
from src.analysis import player_analytics, filters, complex_analytics
from src import config


class FilterPlayerReport(Report):
    @property
    def name(self) -> str:
        return "filter-player"

    @property
    def description(self) -> str:
        return "Filters and shows details for a specific player name."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "player_name",
            type=str,
            help="The name (or part of the name) of the player to filter.",
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        print(f"\n--- Filtering by Player Name: '{args.player_name}' ---")
        filtered_df = filters.filter_by_player_name(df, args.player_name)
        self._format_df_for_display(
            filtered_df,
            f"Found {len(filtered_df):,} Games for Player '{args.player_name}'",
            15,
        )


class PlayerSkillReport(Report):
    @property
    def name(self) -> str:
        return "player-skill"

    @property
    def description(self) -> str:
        return "Shows overall average lobby skill for players."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--since-date",
            type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.utc),
            help="Optional: Filter for players active on/after this date (YYYY-MM-DD).",
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        date_filter = args.since_date or config.LAST_PLAYED_DATE_THRESHOLD
        title = f"Player Overall Average Lobby Skill (Active since {date_filter.strftime('%Y-%m-%d')})"

        result_df = player_analytics.get_player_avg_lobby_skill(df, date_filter)

        if (
            result_df is not None
            and not result_df.empty
            and "last_played" in result_df.columns
        ):
            result_df["last_played"] = result_df["last_played"].dt.strftime(
                "%Y-%m-%d %H:%M"
            )

        self._format_df_for_display(result_df, title)


class TopWinRatePlayersReport(Report):
    @property
    def name(self) -> str:
        return "top-win-rate-players"

    @property
    def description(self) -> str:
        return "Shows top N players by win rate (min 5 games)."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "-n",
            "--num-players",
            type=int,
            default=10,
            help="Number of top players to show.",
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        result_df = player_analytics.get_top_n_players_by_win_rate(
            df, n=args.num_players
        )
        self._format_df_for_display(
            result_df, f"Top {args.num_players} Players by Win Rate (min 5 games)"
        )


class PlayerSkillPeriodPerformanceReport(Report):
    @property
    def name(self) -> str:
        return "player-skill-period-performance"

    @property
    def description(self) -> str:
        return "Win/loss by skill categories and time periods per player."

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

        print("\n--- Player-Level Skill Period Performance Report ---")
        print(f"Reference Date for Periods: {ref_date.strftime('%Y-%m-%d')}")
        print(
            f"Filtering by Maps: {', '.join(map_names) if map_names else 'Default Maps'}"
        )

        result_df = complex_analytics.get_player_skill_period_performance(
            df, ref_date, map_names
        )
        self._format_df_for_display(
            result_df, "Aggregated Win/Loss by OS, Date Period, and Player", max_rows=50
        )


class WinsAboveOsLeaderboardReport(Report):
    @property
    def name(self) -> str:
        return "wins-above-os-leaderboard"

    @property
    def description(self) -> str:
        return "Leaderboard of wins above specific OS thresholds."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--os-thresholds",
            type=lambda s: [int(x.strip()) for x in s.split(",")],
            help="Comma-separated OS thresholds (e.g., '50,45,40').",
        )
        parser.add_argument(
            "--since-date",
            type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.utc),
            help="Optional: Filter games on/after this date (YYYY-MM-DD).",
        )
        parser.add_argument(
            "--maps", type=str, help="Optional: Comma-separated list of map names."
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        map_names = [m.strip() for m in args.maps.split(",")] if args.maps else None
        thresholds = args.os_thresholds or config.DEFAULT_OS_THRESHOLDS

        print("\n--- Wins Above Overall Skill (OS) Leaderboard ---")
        if args.since_date:
            print(
                f"Filtered for games on or after: {args.since_date.strftime('%Y-%m-%d')}"
            )
        if map_names:
            print(f"Filtered for maps: {', '.join(map_names)}")

        result_df = player_analytics.get_wins_above_os_leaderboard(
            df,
            os_thresholds=thresholds,
            since_date=args.since_date,
            map_names=map_names,
        )
        self._format_df_for_display(
            result_df, f"Leaderboard (Thresholds: {thresholds})", max_rows=50
        )
