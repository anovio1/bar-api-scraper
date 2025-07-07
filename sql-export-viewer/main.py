# my_project/main.py

from src.cli import cli_parser  # Updated import
from src.cli import app_runner  # Updated import


def main():
    """
    Main entry point for the data analysis application.
    1. Parses command-line arguments.
    2. Executes the appropriate application logic.
    """
    # Command 1: Argument Parsing
    args = cli_parser.parse_cli_arguments()

    # Command 2: Business Logic Execution
    app_runner.run_app(args)


if __name__ == "__main__":
    main()
