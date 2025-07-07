import argparse
import pandas as pd
from src.core.report import Report
from src.analysis import game_analytics, filters
from src import config


class TopMapsReport(Report):
    @property
    def name(self) -> str:
        return "top-maps"

    @property
    def description(self) -> str:
        return "Shows the top N most played maps."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "-n", "--num-maps", type=int, default=10, help="Number of top maps to show."
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        print(f"\n--- Top {args.num_maps} Most Played Maps ---")
        top_maps = game_analytics.get_top_n_map_names(df, n=args.num_maps)
        if top_maps is not None and not top_maps.empty:
            print(top_maps.to_string())
        else:
            print("Could not retrieve top maps data.")


class FilteredMapsReport(Report):
    @property
    def name(self) -> str:
        return "filter-maps"

    @property
    def description(self) -> str:
        return "Filters and shows replays for specific map names."

    def add_arguments(self, parser: argparse.ArgumentParser):
        parser.add_argument(
            "--maps",
            type=str,
            required=True,
            help='Comma-separated list of map names (e.g., "map1,map2").',
        )

    def execute(self, df: pd.DataFrame, args: argparse.Namespace):
        map_list = [m.strip() for m in args.maps.split(",")]
        print(f"\n--- Filtering for {len(map_list)} Map(s) ---")
        filtered_df = filters.filter_by_map_name(df, map_list)
        self._format_df_for_display(
            filtered_df, f"Found {len(filtered_df):,} Replays", max_rows=10
        )
