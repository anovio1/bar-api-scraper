import pandas as pd
from typing import List, Union
from src import config


def filter_by_map_name(
    df: pd.DataFrame, map_names: Union[str, List[str]]
) -> pd.DataFrame:
    if config.MAP_NAME_COLUMN not in df.columns:
        print(
            f"Warning: Column '{config.MAP_NAME_COLUMN}' not found. Cannot filter by map name."
        )
        return pd.DataFrame(columns=df.columns)

    if isinstance(map_names, str):
        print_message = f"map '{map_names}' (partial match)"
        filtered_df = df[
            df[config.MAP_NAME_COLUMN]
            .astype(str)
            .str.contains(map_names, case=False, na=False)
        ]
    elif isinstance(map_names, list):
        print_message = f"maps in list ({len(map_names)} maps)"
        filtered_df = df[df[config.MAP_NAME_COLUMN].isin(map_names)]
    else:
        print("Error: 'map_names' must be a string or a list of strings.")
        return pd.DataFrame(columns=df.columns)

    print(f"Filtered for {print_message}: {len(filtered_df):,} rows found.")
    return filtered_df


def filter_by_player_name(df: pd.DataFrame, player_name: str) -> pd.DataFrame:
    if config.PLAYER_NAME_COLUMN not in df.columns:
        print(
            f"Warning: Column '{config.PLAYER_NAME_COLUMN}' not found. Cannot filter by player name."
        )
        return pd.DataFrame(columns=df.columns)

    filtered_df = df[
        df[config.PLAYER_NAME_COLUMN]
        .astype(str)
        .str.contains(player_name, case=False, na=False)
    ]
    print(
        f"Filtered for player name '{player_name}' (partial match): {len(filtered_df):,} rows found."
    )
    return filtered_df
