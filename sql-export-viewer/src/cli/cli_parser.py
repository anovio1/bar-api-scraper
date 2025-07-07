# my_project/src/cli/cli_parser.py

import argparse
from datetime import datetime
import pytz
from src import config  # Updated import


def get_parser() -> argparse.ArgumentParser:
    """
    Configures and returns the argument parser for the application.
    """
    parser = argparse.ArgumentParser(
        description="Run various data analysis workflows for replay data.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    parser.add_argument(
        "--mode",
        choices=[
            "full_analysis",
            "filter_player",
            "top_maps",
            "player_skill_report",
            "overall_win_rate",
            "faction_win_rate",
            "top_win_rate_players",
            "ranked_unranked_stats",
            "filter_maps",
        ],
        default="full_analysis",
        help=(
            "Choose the analysis mode:\n"
            "- 'full_analysis': Runs the complete data loading, diagnostics, merging, and advanced reports.\n"
            "- 'filter_player': Filters and shows details for a specific player name. Requires --player-name.\n"
            "- 'top_maps': Shows the top N most played maps.\n"
            "- 'player_skill_report': Shows overall average lobby skill for players, with optional date filter.\n"
            "- 'overall_win_rate': Shows the overall win rate across all games.\n"
            "- 'faction_win_rate': Shows win rates and games played per faction.\n"
            "- 'top_win_rate_players': Shows top N players by win rate (requires --num-players, defaults to 10).\n"
            "- 'ranked_unranked_stats': Compares average stats between ranked and unranked games.\n"
            "- 'filter_maps': Filters and shows replays for specific map names. Requires --maps."
        ),
    )
    parser.add_argument(
        "--player-name",
        type=str,
        help="Required for 'filter_player' mode. The name (or part of the name) of the player to filter.",
    )
    parser.add_argument(
        "--since-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=pytz.utc),
        help=(
            "Optional for 'player_skill_report' mode. "
            "Filter players active on/after this date (YYYY-MM-DD format, e.g., 2024-01-01)."
        ),
    )
    parser.add_argument(
        "--num-players",
        type=int,
        default=10,
        help="Optional for 'top_win_rate_players' mode. The number of top players to show. Defaults to 10.",
    )
    parser.add_argument(
        "--maps",
        type=str,
        help=(
            "Required for 'filter_maps' mode. "
            'Comma-separated list of map names to filter by (e.g., "map1,map2,map3").\n'
            "For 'full_analysis', defaults to config.DEFAULT_FILTER_MAPS if not provided here."
        ),
    )

    return parser


def parse_cli_arguments() -> argparse.Namespace:
    """
    Parses command-line arguments and returns the parsed namespace.
    Handles mode-specific argument requirements.
    """
    parser = get_parser()
    args = parser.parse_args()

    if args.mode == "filter_player" and not args.player_name:
        parser.error("--player-name is required for 'filter_player' mode.")

    if args.mode == "filter_maps" and not args.maps:
        parser.error("--maps is required for 'filter_maps' mode.")

    # Convert comma-separated string of maps into a list
    if args.maps:
        args.maps = [m.strip() for m in args.maps.split(",")]
    else:
        # If no --maps argument provided, use the default from config
        # This is for 'full_analysis' mode when --maps is not explicitly set.
        args.maps = config.DEFAULT_FILTER_MAPS

    return args
