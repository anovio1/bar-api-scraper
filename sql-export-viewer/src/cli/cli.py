import argparse
from typing import List, Dict
from src.core.report import Report
from src.core.data_service import DataService

class ReportRegistry:
    """Holds and provides access to all registered report classes."""
    def __init__(self, report_classes: List[type[Report]]):
        self._reports: Dict[str, Report] = {
            report().name: report() for report in report_classes
        }

    def get_report(self, name: str) -> Report:
        return self._reports.get(name)

    def get_all_reports(self) -> List[Report]:
        return sorted(self._reports.values(), key=lambda r: r.name)

def setup_parser(registry: ReportRegistry) -> argparse.ArgumentParser:
    """Dynamically constructs the argument parser from registered reports."""
    parser = argparse.ArgumentParser(
        description="Run various data analysis workflows for replay data.",
        formatter_class=argparse.RawTextHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="report_name", required=True, help="Available reports")

    for report in registry.get_all_reports():
        subparser = subparsers.add_parser(
            report.name,
            help=report.description,
            description=report.description,
            formatter_class=argparse.RawTextHelpFormatter,
        )
        report.add_arguments(subparser)

    return parser

def run_cli(report_classes: List[type[Report]]):
    """The main entry point for the command-line interface."""
    registry = ReportRegistry(report_classes)
    parser = setup_parser(registry)
    args = parser.parse_args()

    report_to_run = registry.get_report(args.report_name)
    if not report_to_run:
        print(f"Error: Unknown report '{args.report_name}'.")
        parser.print_help()
        return

    print("--- Starting Analysis ---")
    # 1. Get Data
    data_service = DataService()
    merged_df = data_service.get_data()

    if merged_df is None or merged_df.empty:
        print("\nError: Merged data could not be loaded or is empty. Cannot run report.")
        return

    # 2. Execute Report
    try:
        report_to_run.execute(merged_df, args)
        print("\n--- Report Finished ---")
    except Exception as e:
        print(f"\n--- An error occurred during report execution: {e} ---")
        import traceback
        traceback.print_exc()