# Modular Dataframes Viewer for CSVs exported from PostgreSQL

Script that displays aggregate data from csv's. CSV's are from PostgreSQL derived.replays and derived.match_players.

## Quickstart

`python main.py`

## Table of Contents

1.  [Project Overview](#1-project-overview)
2.  [Features](#2-features)
3.  [Directory Structure](#3-directory-structure)
4.  [Setup and Installation](#4-setup-and-installation)
5.  [Usage](#5-usage)
    - [Default Run (Full Analysis)](#default-run-full-analysis)
    - [Specific Analysis Modes](#specific-analysis-modes)
6.  [Project Structure and Design Principles](#6-project-structure-and-design-principles)
7.  [Future Enhancements](#7-future-enhancements)

## 1. Project Overview

This pipeline processes two primary CSV datasets: `match_players_unlogged` (player-specific data per match) and `replays` (match metadata). It loads, cleans, merges, and analyzes this data to provide various insights into game performance, player behavior, and map popularity. A key feature is its intelligent caching system, which speeds up subsequent analysis runs by storing the merged dataset.

## 2. Features

- **Modular Architecture**: Clear separation of concerns across multiple Python files and directories.
- **Data Loading & Preprocessing**: Loads CSV files, performs initial data type conversions (e.g., `timestamptz` to `datetime`), and handles missing/invalid data.
- **Data Caching**: Automatically saves the merged DataFrame to a Parquet cache file (`data_cache/merged_data_cache.parquet`). Subsequent runs load from cache if source CSVs haven't changed, significantly speeding up queries.
- **Date Diagnostics**: Analyzes replay `start_time` for overall date range (oldest/latest replay) and identifies daily gaps with no data.
- **Player Analytics**:
  - **Overall Average Lobby Skill**: Calculates each player's (identified by `user_id`, showing their `latest_name`) average lobby skill across all games.
  - **Games Played by Map Series**: Counts how many games each player has played on "Glitters", "Smolders", and "Simmers" map series (supports various versions via pattern matching).
  - **Player Win Rate**: Calculates individual player win rates.
  - **Filtered Player Data**: Allows filtering and displaying data for specific players by name (partial, case-insensitive match).
  - **Top Players by Win Rate**: Identifies top N players based on their win rate (with a configurable minimum games played threshold).
- **Map Analytics**:
  - **Top N Maps Played**: Shows the most frequently played maps.
  - **Filtered Map Data**: Allows filtering and displaying replays for specific map names (supports multiple exact map names or a single partial map name).
- **Game Type Analytics**:
  - **Overall Win Rate**: Calculates the overall win rate across all recorded games.
  - **Faction Performance**: Displays win rates and total games played for each faction.
  - **Ranked vs. Unranked Stats**: Compares average lobby skill, duration, and player count between ranked and unranked games.
- **Command-Line Interface (CLI)**: Enables running specific analysis modes or the full workflow via command-line arguments.

## 3. Directory Structure

```
my_project/
├── main.py                     # The single, minimal entry point
├── data/                       # Contains your raw CSV files
│   ├── match_players_unlogged_202507071306.csv
│   └── replays_202507071307.csv
├── data_cache/                 # Contains your merged_data_cache.parquet cache file
│   └── merged_data_cache.parquet
├── src/                        # All Python source code
│   ├── __init__.py             # Makes 'src' a Python package
│   ├── config.py               # Application-wide constants and configuration
│   ├── workflow.py             # Orchestrates the core analysis pipeline (class-based)
│   ├── cli/                    # Command-line interface logic
│   │   ├── __init__.py         # Makes 'cli' a package
│   │   ├── cli_parser.py       # Argument parsing logic
│   │   └── app_runner.py       # Dispatches parsed arguments to workflow methods
│   ├── data_processing/        # Data loading and preprocessing components
│   │   ├── __init__.py
│   │   ├── data_loader.py      # Handles reading raw CSVs
│   │   └── data_preprocessor.py# Handles initial cleaning and type conversion
│   ├── analysis/               # Core data analysis and calculations
│   │   ├── __init__.py
│   │   ├── diagnostics.py      # Calculates date-related diagnostics
│   │   └── data_analyzer.py    # Performs all data transformations and aggregations
│   └── reporting/              # Output generation and presentation
│       ├── __init__.py
│       └── report_generator.py # Formats and prints reports to console
├── .gitignore                  # (Optional) Git ignore file for cache, venv, etc.
└── README.md                   # This file
```

## 4. Setup and Installation

1.  **Clone the Repository (or create the directory structure):**

    ```bash
    git clone <repository_url>
    cd my_project
    ```

    If not using Git, manually create the `my_project` directory and the subdirectories `data/`, `data_cache/`, and `src/` (with its subfolders: `cli/`, `data_processing/`, `analysis/`, `reporting/`).

2.  **Place your CSV Data:**
    Move your exported PostgreSQL CSV files (`match_players_unlogged_202507071306.csv` and `replays_202507071307.csv`) into the `my_project/data/` directory.

3.  **Create `__init__.py` files:**
    Ensure an empty `__init__.py` file exists in each of the following directories to make them Python packages:

    - `my_project/src/__init__.py`
    - `my_project/src/cli/__init__.py`
    - `my_project/src/data_processing/__init__.py`
    - `my_project/src/analysis/__init__.py`
    - `my_project/src/reporting/__init__.py`

4.  **Install Dependencies:**
    It's recommended to use a virtual environment.

    ```bash
    python -m venv venv
    # On Windows:
    # venv\Scripts\activate
    # On macOS/Linux:
    source venv/bin/activate

    pip install pandas pyarrow pytz
    ```

5.  **Copy Python Files:**
    Place the respective `.py` files into their designated `src/` subdirectories as shown in the [Directory Structure](#3-directory-structure).

## 5. Usage

Navigate to the `my_project/` directory in your terminal (the one containing `main.py`).

### Default Run (Full Analysis)

By default, if no command-line arguments are provided, the script will run a comprehensive `full_analysis` workflow. This includes loading, preprocessing, merging, initial diagnostics, and all advanced reports (player skills, map stats, win rates, etc.).

```bash
python main.py
```

### Specific Analysis Modes

You can specify a `--mode` argument to run a particular analysis. Use `--help` to see all available modes and arguments.

```bash
python main.py --help
```

**Examples:**

- **Full Analysis (explicitly):**

  ```bash
  python main.py --mode full_analysis
  ```

- **Player Overall Average Lobby Skill (default mode if no args):**
  This mode shows average lobby skill, total games, games per map series, win rate, and last played date for each player.

  ```bash
  python main.py --mode player_skill_report
  ```

  _With a custom "since date" filter (e.g., only players active after January 1, 2024):_

  ```bash
  python main.py --mode player_skill_report --since-date "2024-01-01"
  ```

- **Filter by Player Name:**
  Filters all merged data and shows records related to a specific player name (case-insensitive, partial match).

  ```bash
  python main.py --mode filter_player --player-name "YourPlayerName"
  # Example: python main.py --mode filter_player --player-name "Ace"
  ```

- **Top 10 Most Played Maps:**

  ```bash
  python main.py --mode top_maps
  ```

- **Overall Win Rate:**

  ```bash
  python main.py --mode overall_win_rate
  ```

- **Faction Win Rate and Games Played:**

  ```bash
  python main.py --mode faction_win_rate
  ```

- **Top Players by Win Rate (e.g., Top 5):**
  Requires at least 5 games played by default (configurable in `data_analyzer.py`).

  ```bash
  python main.py --mode top_win_rate_players --num-players 5
  ```

- **Ranked vs. Unranked Game Statistics:**

  ```bash
  python main.py --mode ranked_unranked_stats
  ```

- **Filter by Specific Map Names:**
  Filters all merged data and shows records related to a list of exact map names.
  ```bash
  python main.py --mode filter_maps --maps "All That Glitters v3.0,All That Smolders v1.2"
  ```

## 6. Project Structure and Design Principles

This project adheres to strong software engineering principles, primarily **Separation of Concerns (SoC)** and **Modularity**, to ensure code quality, maintainability, and extensibility.

- **`main.py`**: The minimalist entry point. Its _sole_ responsibility is to initiate the CLI parsing and then dispatch the application's core logic.
- **`src/` (Python Package)**: Contains all application source code, organized into functional sub-packages.
  - **`src/cli/`**: Dedicated to command-line interface logic.
    - `cli_parser.py`: Defines all command-line arguments and handles their parsing and basic validation.
    - `app_runner.py`: Acts as the central dispatcher, taking parsed arguments and calling the appropriate method within the `workflow`.
  - **`src/config.py`**: A central place for all configuration constants, file paths, and default values. This makes it easy to modify settings without digging into logic files.
  - **`src/data_processing/`**: Manages data input and initial cleaning.
    - `data_loader.py`: Handles reading raw CSV files into pandas DataFrames, with error handling.
    - `data_preprocessor.py`: Performs initial data cleaning, type conversions (e.g., ensuring `datetime` objects), and basic data quality checks upon loading.
  - **`src/analysis/`**: Contains the core computational logic.
    - `diagnostics.py`: Calculates diagnostic metrics (e.g., date ranges, gaps) without any presentation logic.
    - `data_analyzer.py`: Performs all data transformations, aggregations (e.g., calculating average lobby skill, game counts, win rates), and complex filtering. It returns processed DataFrames.
  - **`src/reporting/`**: Dedicated to presenting results.
    - `report_generator.py`: Takes processed DataFrames from `data_analyzer` and `diagnostics` and formats them for console output. It contains all `print()` statements related to reports.
  - **`src/workflow.py`**: Encapsulates the `DataAnalysisWorkflow` class. This class is the orchestrator, defining the sequence of high-level steps for each analysis mode. It manages the state of DataFrames (e.g., `players_df`, `replays_df`, `merged_df`) throughout the pipeline, including a robust **caching mechanism** to speed up repeated queries. It delegates specific tasks to the other specialized modules.

This modular design offers:

- **Clarity**: Each file/module has a single, well-defined purpose.
- **Maintainability**: Changes to one area (e.g., a calculation method) are unlikely to break others.
- **Extensibility**: Adding new features (e.g., more analysis metrics, new report types, different data sources) is straightforward, requiring modifications only to relevant modules.
- **Testability**: Individual functions and methods can be tested in isolation.

## 7. Future Enhancements

- **More Advanced Analytics**: Implement additional statistical analysis, anomaly detection, time-series forecasting, etc.
- **Interactive Visualization**: Integrate with libraries like Matplotlib, Seaborn, or Plotly for generating graphs and charts.
- **Configuration File**: Move more dynamic thresholds and parameters (like `LAST_PLAYED_DATE_THRESHOLD` or `min_games_threshold` for win rates) from `config.py` into a separate `.ini`, `.json`, or `.yaml` file that can be easily modified by users without changing code.
- **Output Formats**: Allow reports to be outputted to files (e.g., Excel, JSON, PDF) instead of just the console.
- **Error Handling**: Enhance error handling and logging for robustness in production environments.
- **Database Integration**: Implement direct connections to PostgreSQL (or other databases) in `data_loader.py` as an alternative to CSVs.
- **User Management/Authentication**: (For larger web-based applications)

---
