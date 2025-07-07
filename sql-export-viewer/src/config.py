import os
from datetime import datetime
import pytz

# --- Project Structure ---
# Assumes config.py is in my_project/src/
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
CACHE_DIR = os.path.join(PROJECT_ROOT, "data_cache")

# --- Source Files ---
PLAYERS_CSV_FILE = "match_players_unlogged_202507071306.csv"
REPLAYS_CSV_FILE = "replays_202507071307.csv"
CACHED_MERGED_DF_FILE = os.path.join(CACHE_DIR, "merged_data_cache.parquet")

# --- Core Column Names ---
DATE_ANALYSIS_COLUMN = "start_time"
MAP_NAME_COLUMN = "map_name"
REPLAY_ID_COLUMN = "replay_id"
USER_ID_COLUMN = "user_id"
PLAYER_NAME_COLUMN = "name"
SKILL_COLUMN = "skill"
AVG_LOBBY_SKILL_COLUMN = "avg_lobby_skill"

# --- Analysis Parameters ---
# Map series patterns for filtering
MAP_GLITTERS_PATTERN = "All That Glitters"
MAP_SMOLDERS_PATTERN = "All That Smolders"
MAP_SIMMERS_PATTERN = "All That Simmers"

# Default list of maps for filtering (if not provided via CLI)
DEFAULT_FILTER_MAPS = [
    "All That Glitters v3.0",
    "All That Smolders v1.2",
    "All That Glitters v1.0",
    "All That Glitters v1.1",
    "All That Glitters v1.2",
    "All That Glitters v2.0",
    "All That Glitters v2.1",
    "All That Glitters v2.2",
    "All That Smolders v1.1.1",
    "All That Simmers v1.0.2",
    "All That Simmers v1.1",
    "All That Simmers v1.1.1",
]

# Default threshold for "last played" analysis
LAST_PLAYED_DATE_THRESHOLD = datetime(2025, 1, 1, 0, 0, 0, tzinfo=pytz.utc)

# Default OS thresholds for wins_above_os_leaderboard
DEFAULT_OS_THRESHOLDS = [40, 35, 30, 25]
