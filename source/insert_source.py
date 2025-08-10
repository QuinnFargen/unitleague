import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get Postgres settings from env
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DB   = os.getenv("PG_DB")
PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")

# Connect to Postgres
conn = psycopg2.connect(
    host=PG_HOST,
    port=PG_PORT,
    dbname=PG_DB,
    user=PG_USER,
    password=PG_PASS
)
cur = conn.cursor()

def clean_value(val):
    """Convert pandas NaN/None to None for Postgres."""
    if pd.isna(val) or (isinstance(val, str) and val.strip().lower() == "nan"):
        return None
    return val

def insert_from_csv(folder_path, filename, table_name, league_id):
    """
    Inserts data from a CSV file in folder_path into a Postgres table.
    Adds league_id as the first column.
    """
    csv_path = os.path.join(folder_path, filename)
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Build the column list with league_id as the first column
    cols = "league_id, " + ", ".join(df.columns)
    placeholders = "%s, " + ", ".join(["%s"] * len(df.columns))
    insert_sql = f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders});"

    for _, row in df.iterrows():
        values = [league_id] + [clean_value(v) for v in row]
        cur.execute(insert_sql, values)

    conn.commit()
    print(f"Inserted {len(df)} rows into {table_name} with league_id={league_id}")



# Insert each CSV into its corresponding table
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_defense.csv", "src.foot_defensive_box",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_offense.csv", "src.foot_offensive_box",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_special.csv", "src.foot_special_teams_box",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_games.csv", "src.foot_game_team_summary",2)

cur.close()
conn.close()