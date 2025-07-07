import pandas as pd
from typing import Optional
from src import config


def get_top_n_map_names(df: pd.DataFrame, n: int = 10) -> Optional[pd.Series]:
    if config.MAP_NAME_COLUMN not in df.columns:
        print(f"Warning: Column '{config.MAP_NAME_COLUMN}' not found.")
        return None
    if df.empty:
        return None
    return df[config.MAP_NAME_COLUMN].value_counts().head(n)


def get_overall_win_rate(df: pd.DataFrame) -> Optional[float]:
    if "won" not in df.columns or df.empty:
        return None
    return df["won"].mean() * 100


def get_win_rate_by_faction(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    required_cols = ["faction", "won"]
    if not all(col in df.columns for col in required_cols) or df.empty:
        return None
    faction_stats = (
        df.groupby("faction")
        .agg(games_played=("won", "size"), win_rate=("won", lambda x: x.mean() * 100))
        .reset_index()
    )
    faction_stats["win_rate"] = faction_stats["win_rate"].round(2)
    return faction_stats.sort_values(by="games_played", ascending=False)


def get_ranked_unranked_stats(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    required_cols = [
        "ranked",
        config.AVG_LOBBY_SKILL_COLUMN,
        "duration_ms",
        "player_count",
    ]
    if not all(col in df.columns for col in required_cols) or df.empty:
        return None
    ranked_stats = (
        df.groupby("ranked")
        .agg(
            total_games=("ranked", "size"),
            avg_lobby_skill=(config.AVG_LOBBY_SKILL_COLUMN, "mean"),
            avg_duration_min=("duration_ms", lambda x: (x / 60000).mean()),
            avg_player_count=("player_count", "mean"),
        )
        .reset_index()
    )
    ranked_stats["avg_lobby_skill"] = ranked_stats["avg_lobby_skill"].round(2)
    ranked_stats["avg_duration_min"] = ranked_stats["avg_duration_min"].round(2)
    ranked_stats["avg_player_count"] = ranked_stats["avg_player_count"].round(1)
    return ranked_stats
