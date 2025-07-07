
### Scraper

## VSC/Cmd
1. Python Virtual Environment
2. Install dependencies
```bash
    # Windows Cmd
    python -m venv ./.venv
    .\.venv\Scripts\activate.bat
    pip install -r requirements.txt
    python listener.py --skip-download  --maps "All That Glitters v3.0" --maps "All That Smolders v1.2" --maps "All That Glitters v1.0" --maps "All That Glitters v1.1" --maps "All That Glitters v1.2" --maps "All That Glitters v2.0" --maps "All That Glitters v2.1" --maps "All That Glitters v2.2" --maps "All That Smolders v1.1.1" --maps "All That Simmers v1.0.2" --maps "All That Simmers v1.1" --maps "All That Simmers v1.1.1"
```

### Note: --skip-download skips downloading the replay

# BAR Replay Listener

This Python script is a refactored and **Prefect-free** listener for Beyond All Reason (BAR) replays. It automates the process of discovering new replays, fetching their detailed metadata, and downloading the replay files (`.sdz`) from the BAR API. It leverages the Command Pattern for clear separation of concerns and uses a thread pool for parallel fetching and downloading.

## Table of Contents

*   [Features](#features)
*   [Prerequisites](#prerequisites)
*   [Installation](#installation)
*   [Usage](#usage)
    *   [Basic Run](#basic-run)
    *   [Command-Line Arguments](#command-line-arguments)
    *   [Examples](#examples)
*   [Output Structure](#output-structure)
*   [How It Works (Briefly)](#how-it-works-briefly)
*   [Stopping the Script](#stopping-the-script)
*   [Important Considerations](#important-considerations)
*   [License](#license)

## Features

*   **Automated Replay Discovery:** Continuously checks the BAR API for new replays within a specified date range.
*   **Metadata Fetching:** Retrieves detailed JSON metadata for each new replay.
*   **Replay File Download:** Downloads the `.sdz` replay files.
*   **Parallel Processing:** Uses `ThreadPoolExecutor` for efficient concurrent metadata fetching and downloading.
*   **Intelligent Skipping:** Avoids re-downloading replays or refetching metadata for already seen replays (unless `force-meta` is used).
*   **Configurable Paths:** Allows specifying custom folders for replays and metadata.
*   **Flexible Date Ranges:** Define the period for replay searches.
*   **Continuous Listening:** `--listen` mode for endless operation.
*   **Sandbox Mode:** `--sandbox` flag for testing, writing data to staging folders to avoid polluting your main collection.
*   **Robust Logging:** Detailed logging to both console and a file.
*   **Graceful Shutdown:** Handles `Ctrl+C` cleanly, attempting to stop ongoing operations.

## Prerequisites

*   Python 3.7+

## Installation

1.  **Save the script:** Save the provided Python code as `bar_replay_listener.py` (or any other `.py` name you prefer).
2.  **Install dependencies:** Open your terminal or command prompt and run:
    ```bash
    pip install requests tqdm
    ```

## Usage

Navigate to the directory where you saved `bar_replay_listener.py` in your terminal.

### Basic Run

To start the listener with default settings, simply run:

```bash
python bar_replay_listener.py
```

**By default, this will:**
*   Look for replays from **yesterday** up to **two days from now**.
*   Create `Replays/` and `metas/` folders in the script's directory.
*   Download replay files (`.sdz`) into date-specific subfolders within `Replays/`.
*   Save metadata (`.json`) into `metas/`.
*   Log activity to `listener.log` and the console.
*   Stop after `5` consecutive pages of the API yield no new replays.
*   Wait `60` seconds between checks if a page is empty or no new replays are found.

### Command-Line Arguments

Customize the script's behavior using these arguments:

*   **`--download-folder PATH`**
    *   Type: `Path`
    *   Default: `Replays`
    *   Description: Specifies the base directory where replay `.sdz` files will be downloaded. Subfolders by date will be created within this path.
    *   Example: `python bar_replay_listener.py --download-folder /media/bar_data/replays`

*   **`--metas-folder PATH`**
    *   Type: `Path`
    *   Default: `metas`
    *   Description: Specifies the base directory where replay metadata `.json` files will be saved.
    *   Example: `python bar_replay_listener.py --metas-folder ~/bar_meta_collection`

*   **`--from-date YYYY-MM-DD`**
    *   Type: `str`
    *   Default: `(datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")` (yesterday)
    *   Description: Defines the start date (inclusive) for fetching replays.
    *   Example: `python bar_replay_listener.py --from-date 2023-01-01`

*   **`--to-date YYYY-MM-DD`**
    *   Type: `str`
    *   Default: `(datetime.today() + timedelta(days=2)).strftime("%Y-%m-%d")` (two days from now)
    *   Description: Defines the end date (inclusive) for fetching replays.
    *   Example: `python bar_replay_listener.py --to-date 2023-01-31`

*   **`--listen-interval SECONDS`**
    *   Type: `int`
    *   Default: `60`
    *   Description: The waiting period in seconds between checks when no new replays are found on a page. Also used between full cycles in `--listen` mode.
    *   Example: `python bar_replay_listener.py --listen-interval 300` (wait 5 minutes)

*   **`--results-per-page-limit N`**
    *   Type: `int`
    *   Default: `1000`
    *   Description: The maximum number of replays to request per API page. The BAR API typically has a maximum of 500.
    *   Example: `python bar_replay_listener.py --results-per-page-limit 250`

*   **`--listen-max-empty-pages N`**
    *   Type: `int`
    *   Default: `5`
    *   Description: The number of consecutive empty API pages (no new replays found) after which the script will exit. This is ignored if `--listen` is active.
    *   Example: `python bar_replay_listener.py --listen-max-empty-pages 10`

*   **`--skip-download`**
    *   Type: `action="store_true"` (a boolean flag)
    *   Default: `False`
    *   Description: If this flag is present, the script will only fetch and save metadata (`.json` files) but will skip downloading the actual `.sdz` replay files.
    *   Example: `python bar_replay_listener.py --skip-download`

*   **`--force-meta`**
    *   Type: `action="store_true"` (a boolean flag)
    *   Default: `False`
    *   Description: If this flag is present, the script will attempt to fetch metadata for *all* replays found within the date range, even if their `gameId` is already recorded in the `downloaded.jsonl` log. This is useful for updating potentially outdated metadata. It does not force re-downloads of `.sdz` files if they exist.
    *   Example: `python bar_replay_listener.py --force-meta --from-date 2023-01-01 --to-date 2023-01-31`

*   **`--listen`**
    *   Type: `action="store_true"` (a boolean flag)
    *   Default: `False`
    *   Description: Runs the script in an endless loop. It will continuously check for new replays, ignoring the `--listen-max-empty-pages` limit. Suitable for long-running processes or background tasks.
    *   Example: `python bar_replay_listener.py --listen`

*   **`--maps`**
    *   Type: `str`
    *   Default: `False`
    *   Description: Filter replays by map name.Can specify multiple times
    *   Example: `python bar_replay_listener.py  --maps 'All That Glitters v3.0' --maps 'All That Smolders v1.2`

*   **`--sandbox`**
    *   Type: `action="store_true"` (a boolean flag)
    *   Default: `False`
    *   Description: Activates "sandbox mode" for testing. When enabled:
        *   `--download-folder` will be suffixed with `_staging` (e.g., `Replays_staging/`).
        *   `--metas-folder` will be suffixed with `_staging` (e.g., `metas_staging/`).
        *   The log file will be named `listener_sandbox.log`.
        *   This ensures that any files downloaded or metadata fetched during a sandbox run do not interfere with your main collection.
    *   Example: `python bar_replay_listener.py --sandbox`

### Examples

1.  **Run in sandbox mode to test without affecting your main data:**
    ```bash
    python bar_replay_listener.py --sandbox
    ```

2.  **Run continuously, checking every 5 minutes, and only fetching metadata:**
    ```bash
    python bar_replay_listener.py --listen --listen-interval 300 --skip-download
    ```

3.  **Download replays and metadata for a specific single day (October 26, 2023):**
    ```bash
    python bar_replay_listener.py --from-date 2023-10-26 --to-date 2023-10-26
    ```

4.  **Update metadata for all replays from the past week, without re-downloading replay files:**
    ```bash
    python bar_replay_listener.py --force-meta --skip-download --from-date 2023-10-20 --to-date 2023-10-27
    ```

## Output Structure

The script organizes downloaded data into distinct folders:

*   **`Replays/`** (or `Replays_staging/` in sandbox mode):
    *   Contains date-specific subfolders, e.g., `L2023-10-27Replays/`.
    *   Each date subfolder contains:
        *   The actual BAR replay files (`.sdz`).
        *   A `downloaded.jsonl` file: A JSON Lines file that logs the `gameId` of every replay successfully processed (metadata fetched or replay downloaded) for that specific day. This file is crucial for the script to avoid reprocessing known replays.

*   **`metas/`** (or `metas_staging/` in sandbox mode):
    *   Contains individual JSON files for each replay's metadata, named by their `gameId` (e.g., `550e8400-e29b-41d4-a716-446655440000.json`).

*   **`listener.log`** (or `listener_sandbox.log` in sandbox mode):
    *   A detailed log file containing all script activity, including debug messages and error traces.

## How It Works (Briefly)

The script operates in a continuous loop, performing the following steps for each page of the BAR API:

1.  **Search:** Queries the BAR API for replays within the specified date range and page limit.
2.  **Filter:** Compares the newly found replays against a set of previously "seen" replay IDs (loaded from all `downloaded.jsonl` files) to identify genuinely new or "forced" metadata entries.
3.  **Parallel Fetch Metadata:** For each new replay, it concurrently fetches the detailed metadata JSON. This metadata is saved to the `metas/` folder.
4.  **Parallel Download Replay:** If not skipping downloads, it concurrently downloads the `.sdz` replay files into date-specific folders.
5.  **Log Seen IDs:** For every successfully processed replay (metadata fetched or file downloaded), its `gameId` is appended to the relevant `downloaded.jsonl` file to prevent future re-processing.
6.  **Sleep:** Waits for the specified `listen-interval` before checking the next page or starting a new cycle.

## Stopping the Script

To gracefully stop the script at any time, press `Ctrl+C` in your terminal. The script is designed to catch this interruption, shut down its thread pools, and exit cleanly.

## Important Considerations

*   **Disk Space:** Replay files can consume significant disk space over time. Ensure your chosen `download-folder` has ample storage.
*   **API Rate Limits:** While the script includes delays, be mindful of potential API rate limits if you run it excessively frequently or request very broad date ranges continuously.
*   **Network Connectivity:** An active internet connection is required to communicate with the BAR API and download servers.
*   **`downloaded.jsonl` files:** These files are critical for tracking which replays have already been processed. Do not manually modify or delete them while the script is running, as this could lead to re-downloading or re-processing content. If you *do* want to re-process everything, you can delete these files, or use `--force-meta` to re-fetch metadata.

---

This README should provide users with all the necessary information to understand, install, configure, and run your BAR Replay Listener script effectively.
