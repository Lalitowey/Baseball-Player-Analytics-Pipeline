import pandas as pd
from pybaseball import statcast_pitcher, statcast_batter, playerid_lookup
import os
import sys
import logging


# --- Configuration ---
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'raw')

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- User Input with Defaults ---
PLAYER_FIRST_NAME = os.getenv('PLAYER_FIRST_NAME', 'Shohei')
PLAYER_LAST_NAME = os.getenv('PLAYER_LAST_NAME', 'Ohtani')
PLAYER_TYPE = os.getenv('PLAYER_TYPE', 'batter').lower()
START_DATE = os.getenv('START_DATE', '2023-01-01')
END_DATE = os.getenv('END_DATE', '2023-12-31')
OUTPUT_FILENAME = f"{PLAYER_FIRST_NAME.lower()}_{PLAYER_LAST_NAME.lower()}_{PLAYER_TYPE}_statcast_{START_DATE}_to_{END_DATE}.csv"

COLUMNS_TO_KEEP = [
    'pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',
    'player_name', 'batter', 'pitcher', 'events', 'description', 'zone',
    'des', 'game_type', 'stand', 'p_throws', 'home_team', 'away_team',
    'type', 'hit_location', 'bb_type', 'balls', 'strikes', 'game_year',
    'pfx_x', 'pfx_z', 'plate_x', 'plate_z', 'on_3b', 'on_2b', 'on_1b',
    'outs_when_up', 'inning', 'inning_topbot', 'hc_x', 'hc_y',
    'sv_id', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az',
    'sz_top', 'sz_bot', 'hit_distance_sc', 'launch_speed', 'launch_angle',
    'effective_speed', 'release_spin_rate', 'release_extension', 'game_pk',
    'fielder_2', 'fielder_3', 'fielder_4', 'fielder_5',
    'fielder_6', 'fielder_7', 'fielder_8', 'fielder_9',
    'estimated_ba_using_speedangle', 'estimated_woba_using_speedangle',
    'woba_value', 'woba_denom', 'babip_value', 'iso_value',
    'launch_speed_angle', 'at_bat_number', 'pitch_number', 'pitch_name',
    'home_score', 'away_score', 'bat_score', 'fld_score', 'post_away_score',
    'post_home_score', 'post_bat_score', 'post_fld_score'
]

def find_player_id(last_name, first_name):
    """Looks up MLBAM player ID, handles errors and multiple results."""
    logging.info(f"Looking up player ID for {first_name} {last_name}...")
    try:
        player_info = playerid_lookup(last_name, first_name, fuzzy=True)
        if player_info.empty:
            logging.error(f"Player '{first_name} {last_name}' not found.")
            return None
        if len(player_info) > 1:
            logging.warning(f"Multiple players found for '{first_name} {last_name}'. Using the first result.")
        player_id = player_info['key_mlbam'].iloc[0]
        logging.info(f"Found MLBAM ID: {player_id}")
        return player_id
    except Exception as e:
        logging.error(f"An unexpected error occurred during player ID lookup: {e}")
        return None

def get_statcast_data(player_id, start_dt, end_dt, player_type):
    """Fetches Statcast data using the appropriate pybaseball function."""
    logging.info(f"Fetching Statcast data for player {player_id} ({player_type}) from {start_dt} to {end_dt}...")
    try:
        if player_type == 'pitcher':
            data = statcast_pitcher(start_dt=start_dt, end_dt=end_dt, player_id=player_id)
        else:
            data = statcast_batter(start_dt=start_dt, end_dt=end_dt, player_id=player_id)
        if data is None or data.empty:
            logging.warning(f"No Statcast data found for player {player_id} in the specified date range.")
            return None
        logging.info(f"Fetched {len(data)} rows of Statcast data.")
        return data
    except Exception as e:
        logging.error(f"An error occurred fetching Statcast data: {e}")
        return None

def clean_and_select_columns(df, columns_to_keep):
    """Selects relevant columns and handles potential missing columns."""
    logging.info("Selecting relevant columns...")
    existing_columns = [col for col in columns_to_keep if col in df.columns]
    missing_columns = [col for col in columns_to_keep if col not in df.columns]
    if missing_columns:
        logging.warning(f"The following requested columns were not found in the data: {missing_columns}")
    return df[existing_columns]

if __name__ == "__main__":
    # Find player id
    mlbam_id = find_player_id(PLAYER_LAST_NAME, PLAYER_FIRST_NAME)
    if mlbam_id is None: # exit if player not found
        sys.exit(1)

    # Fetch Statcast data
    statcast_df = get_statcast_data(mlbam_id, START_DATE, END_DATE, PLAYER_TYPE)
    if statcast_df is not None:
        cleaned_df = clean_and_select_columns(statcast_df, COLUMNS_TO_KEEP)
        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True) # Create output directory if it doesn't exist
            logging.info(f"Ensured output directory exists: {OUTPUT_DIR}")
        except OSError as e:
            logging.error(f"Error creating directory {OUTPUT_DIR}: {e}")
            sys.exit(1)
        output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)
        try:
            cleaned_df.to_csv(output_path, index=False)
            logging.info(f"Successfully saved data to: {output_path}")
        except Exception as e:
            logging.error(f"Error saving data to CSV: {e}")
            sys.exit(1)
    else:
        logging.info("No data to save.")
        sys.exit(1)
    logging.info("Script finished successfully.")