# my_project/src/cli/app_runner.py

import argparse
from typing import List
from src import workflow  # Updated import
from src import config  # Updated import


def run_app(args: argparse.Namespace):
    """
    Executes the appropriate workflow method based on parsed arguments.
    """
    analysis_workflow = workflow.DataAnalysisWorkflow()

    if args.mode == "full_analysis":
        analysis_workflow.run_full_analysis_workflow(args.maps)
    elif args.mode == "filter_player":
        analysis_workflow.run_player_name_filter(args.player_name)
    elif args.mode == "top_maps":
        analysis_workflow.run_top_maps_report()
    elif args.mode == "player_skill_report":
        analysis_workflow.run_player_skill_report(args.since_date)
    elif args.mode == "overall_win_rate":
        analysis_workflow.run_overall_win_rate_report()
    elif args.mode == "faction_win_rate":
        analysis_workflow.run_faction_win_rate_report()
    elif args.mode == "top_win_rate_players":
        analysis_workflow.run_top_players_by_win_rate_report(args.num_players)
    elif args.mode == "ranked_unranked_stats":
        analysis_workflow.run_ranked_unranked_stats_report()
    elif args.mode == "filter_maps":
        analysis_workflow.run_filtered_maps_report(args.maps)
    else:
        print(
            f"Error: Unknown mode '{args.mode}'. Please use --help for available modes."
        )
