import pandas as pd
from sqlalchemy import create_engine, text # Added 'text' for potential raw SQL later
import os
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine, text # Added 'text' for potential raw SQL later
from sqlalchemy.exc import IntegrityError


# --- Configuration ---

# Determine directories relative to the script location
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# Assume the CSV is in ../data/raw relative to this script
DATA_DIR = os.path.join(SCRIPT_DIR, '..', 'data', 'raw')
# Load environment variables from .env file located in the parent directory
dotenv_path = os.path.join(SCRIPT_DIR, '..', '.env')
load_dotenv(dotenv_path=dotenv_path)

# Get DB credentials from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# --- Check if credentials are loaded ---
if not all([DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME]):
    print("Error: Database credentials not found. Ensure .env file exists in project root and is populated.")
    sys.exit(1)

# --- Construct Database Connection URL ---
# Format: postgresql://user:password@host:port/database
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# --- Define Target Table ---
TABLE_NAME = "statcast_data" # Must match the table created in SQL

# --- Helper Function ---
def find_latest_csv(directory, prefix=""):
    """Finds the most recently modified CSV file in a directory, optionally matching a prefix."""
    try:
        files = [
            os.path.join(directory, f)
            for f in os.listdir(directory)
            if f.lower().endswith(".csv") and (not prefix or f.lower().startswith(prefix.lower()))
        ]
        if not files:
            return None
        # Return the file with the latest modification time
        return max(files, key=os.path.getmtime)
    except FileNotFoundError:
        print(f"Error: Data directory not found at {directory}")
        return None
    except Exception as e:
        print(f"Error finding CSV file: {e}")
        return None

# --- Main Execution ---
if __name__ == "__main__":
    print("Starting ETL process: Load CSV to RDS...")

    # 1. Find the most recent Statcast CSV file
    # You might want to make the prefix dynamic if you run fetch script for many players
    # For now, it finds the newest CSV in the data/raw directory.
    csv_file_path = find_latest_csv(DATA_DIR)

    if not csv_file_path:
        print(f"Error: No CSV files found in {DATA_DIR}. Run the fetch script first.")
        sys.exit(1)

    print(f"Found data file: {os.path.basename(csv_file_path)}")

    # 2. Read the CSV data using Pandas
    try:
        print("Reading CSV file...")
        df = pd.read_csv(csv_file_path, low_memory=False) # low_memory=False can help with mixed types
        print(f"Read {len(df)} rows from CSV.")

        # --- Basic Data Cleaning/Preparation (IMPORTANT!) ---
        # Ensure column names match DB table (lowercase is good practice)
        df.columns = [col.lower() for col in df.columns]

        # Convert 'game_date' to datetime objects if it's not already
        if 'game_date' in df.columns:
            df['game_date'] = pd.to_datetime(df['game_date'])

        # Add any other necessary type conversions or cleaning here
        # Example: Ensure numeric columns are numeric, fill specific NaNs if needed
        numeric_cols = ['release_speed', 'release_pos_x', 'release_pos_z', 'pfx_x', 'pfx_z',
                        'plate_x', 'plate_z', 'effective_speed', 'release_spin_rate',
                        'release_extension', 'spin_axis', 'hit_distance_sc', 'launch_speed',
                        'launch_angle', 'estimated_ba_using_speedangle', 'woba_value',
                        'estimated_woba_using_speedangle', 'hc_x', 'hc_y', 'sz_top', 'sz_bot']
        for col in numeric_cols:
            if col in df.columns:
                # errors='coerce' turns unparseable values into NaN
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # Ensure integer columns are integers (handle potential NaNs first)
        int_cols = ['balls', 'strikes', 'zone', 'outs_when_up', 'inning',
                    'at_bat_number', 'pitch_number', 'game_pk', 'batter', 'pitcher',
                    'on_1b', 'on_2b', 'on_3b', 'hit_location', 'woba_denom',
                    'babip_value', 'iso_value', 'launch_speed_angle',
                    'fielder_2', 'fielder_3', 'fielder_4', 'fielder_5',
                    'fielder_6', 'fielder_7', 'fielder_8', 'fielder_9',
                    'home_score','away_score', 'bat_score', 'fld_score',
                    'post_away_score', 'post_home_score', 'post_bat_score', 'post_fld_score']
        for col in int_cols:
             if col in df.columns:
                # Convert to nullable integer type Int64Dtype to handle NaNs
                df[col] = df[col].astype(pd.Int64Dtype())

        print("Performing pre-load validation checks...")

        # --- Validation Check 1: Nulls in Primary Key Columns ---
        pk_cols = ['game_pk', 'at_bat_number', 'pitch_number']
        null_pk_rows = df[pk_cols].isnull().any(axis=1)
        num_null_pk_rows = null_pk_rows.sum()
        if num_null_pk_rows > 0:
            print(f"ERROR: Found {num_null_pk_rows} rows with NULL values in primary key columns {pk_cols}.")
            print("Sample rows with NULL PKs:")
            print(df[null_pk_rows].head())
            # Decide how to handle: exit, drop rows, fill values?
            # For now, let's exit
            print("Exiting due to NULL values in primary key columns.")
            sys.exit(1)
        else:
            print("Validation Check 1 Passed: No NULL values found in primary key columns.")

        # --- Validation Check 2: Duplicate Primary Keys ---
        duplicates = df.duplicated(subset=pk_cols, keep=False)  # keep=False marks ALL duplicates
        num_duplicates = duplicates.sum()
        if num_duplicates > 0:
            print(f"ERROR: Found {num_duplicates} rows that are part of duplicate primary key combinations {pk_cols}.")
            print("Sample duplicate rows (showing all occurrences):")
            print(df[duplicates].sort_values(by=pk_cols).head(10))
            # Decide how to handle: exit, drop duplicates?
            # Option 1: Exit (safer)
            print("Exiting due to duplicate primary keys found in the data.")
            sys.exit(1)
            # Option 2: Drop duplicates, keeping the first occurrence (use with caution)
            # print("Attempting to drop duplicate rows, keeping the first occurrence...")
            # df = df.drop_duplicates(subset=pk_cols, keep='first')
            # print(f"DataFrame size after dropping duplicates: {len(df)} rows.")
        else:
            print("Validation Check 2 Passed: No duplicate primary keys found.")

        # --- Validation Check 3: String Lengths (Optional but good) ---
        max_desc_len = df['description'].astype(str).str.len().max()
        db_desc_limit = 100  # From CREATE TABLE statement
        if max_desc_len > db_desc_limit:
            print(
                f"WARNING: Longest 'description' has length {max_desc_len}, exceeds DB limit of {db_desc_limit}. This might cause errors if not handled.")
            # Consider truncating: df['description'] = df['description'].str.slice(0, db_desc_limit)
        else:
            print(
                f"Validation Check 3 Passed: Max 'description' length ({max_desc_len}) within limit ({db_desc_limit}).")

        # Select only columns that exist in the database table schema
        # Get table columns from schema (or define the list explicitly)
        # For now, let's assume df columns already match the CREATE TABLE statement
        # Filter df to only include columns that are expected in the DB table.
        # This requires knowing the target table columns accurately.

    except Exception as e:
        print(f"Error reading or processing CSV file {csv_file_path}: {e}")
        sys.exit(1)

    # 3. Connect to the Database
    try:
        print(f"Connecting to database {DB_NAME} at {DB_HOST}...")
        # `create_engine` sets up the connection pool
        engine = create_engine(DATABASE_URL, echo=False) # Set echo=True for verbose SQL logging

        # Optional: Test connection
        with engine.connect() as connection:
            print("Database connection successful.")

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        sys.exit(1)

    # 4. Load Data into PostgreSQL
    try:
        print(f"Loading data into table '{TABLE_NAME}'...")
        # Use pandas `to_sql` function
        # - `name`: Name of the SQL table
        # - `con`: SQLAlchemy engine or connection
        # - `if_exists`: {'fail', 'replace', 'append'}
        #     - 'fail': Raise ValueError if table exists.
        #     - 'replace': Drop the table before inserting new values. (Use with caution!)
        #     - 'append': Insert new values to the existing table.
        # - `index`: Write DataFrame index as a column? (Usually False)
        # - `method`: {'multi', None, callable} Controls SQL INSERT clause.
        #             'multi' is often faster for larger datasets.
        # - `chunksize`: Process data in chunks to manage memory.
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='append', # Append data. Use 'replace' only if you want to overwrite!
            index=False,
            method='multi', # Efficient for inserting many rows
            chunksize=1000 # Adjust based on memory/performance
        )
        print(f"Successfully loaded {len(df)} rows into '{TABLE_NAME}'.")

        # --- Handle Potential Duplicates (if using 'append') ---
        # The PRIMARY KEY constraint (game_pk, at_bat_number, pitch_number)
        # should prevent duplicate rows from being inserted if you run this
        # script twice on the exact same CSV. Pandas 'to_sql' might raise an
        # IntegrityError in that case.
        # A more robust approach uses SQL's ON CONFLICT clause, but requires
        # executing raw SQL or using more advanced SQLAlchemy features.
        # For this project, 'append' with the PK handling errors is a start.
        # Example advanced way (Not implemented here fully):
        # from sqlalchemy.dialects.postgresql import insert
        # stmt = insert(YourTableModel).values(df.to_dict(orient='records'))
        # stmt = stmt.on_conflict_do_nothing(index_elements=['game_pk', 'at_bat_number', 'pitch_number'])
        # engine.execute(stmt)


    except IntegrityError as e:
         print(f"Integrity Error: Likely tried to insert duplicate primary keys. Details: {e}")
         # Decide how to handle: Maybe log it, maybe try an update, etc.
         # For now, we just report it.
    except Exception as e:
        print(f"Error loading data into table '{TABLE_NAME}': {e}")
        # Provide more context if possible, e.g., which row failed.
        sys.exit(1)
    finally:
        # Dispose of the engine connections (good practice)
        if 'engine' in locals() and engine:
            engine.dispose()
            print("Database engine connections closed.")

    print("ETL process finished.")