# This file makes the 'reports' directory a Python package.
# We can perform auto-discovery of report classes here.

import pkgutil
import inspect
from . import map_reports, overall_reports, player_reports
from src.core.report import Report

# List of all report classes to be registered with the CLI
# This is the single place to add a new report to the application
ALL_REPORTS = [
    # Map Reports
    map_reports.TopMapsReport,
    map_reports.FilteredMapsReport,

    # Overall Stat Reports
    overall_reports.OverallWinRateReport,
    overall_reports.FactionWinRateReport,
    overall_reports.RankedUnrankedStatsReport,
    overall_reports.OverallSkillPeriodPerformanceReport,

    # Player-Specific Reports
    player_reports.FilterPlayerReport,
    player_reports.PlayerSkillReport,
    player_reports.TopWinRatePlayersReport,
    player_reports.PlayerSkillPeriodPerformanceReport,
    player_reports.WinsAboveOsLeaderboardReport,
]

def get_all_report_classes():
    return ALL_REPORTS