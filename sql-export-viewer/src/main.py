from src.cli.cli import run_cli
from src.reports import get_all_report_classes


def main():
    """Main application entry point."""
    # Discover all report classes from the reports package
    all_reports = get_all_report_classes()

    # Run the command-line interface with the discovered reports
    run_cli(all_reports)


if __name__ == "__main__":
    main()
