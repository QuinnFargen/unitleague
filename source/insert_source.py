import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
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


def insert_from_csv(folder_path, filename, table_name, league_id="NULL"):
    csv_path = os.path.join(folder_path, filename)
    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    cols = list(df.columns)
    data = [[clean_value(v) for v in row] for row in df.to_numpy()]
    if league_id != 'NULL':
        cols = ["league_id"] + cols
        data = [[league_id] + [clean_value(v) for v in row] for row in df.to_numpy()]
    insert_sql = f"INSERT INTO {table_name} ({', '.join(cols)}) VALUES %s"
    

    execute_values(cur, insert_sql, data, page_size=1000)
    conn.commit()
    print(f"Inserted {len(data)} rows into {table_name} with league_id={league_id}")


# Insert each CSV into its corresponding table
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_defense.csv", "src.foot_defensive_box",2)  #less than a min
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_offense.csv", "src.foot_offensive_box",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_special.csv", "src.foot_special_teams_box",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_games.csv", "src.foot_game_team_summary",2)

insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/5_CFB", "cfb_defense.csv", "src.foot_defensive_box",5)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/5_CFB", "cfb_offense.csv", "src.foot_offensive_box",5)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/5_CFB", "cfb_special.csv", "src.foot_special_teams_box",5)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/5_CFB", "cfb_games.csv", "src.foot_game_team_summary",5)

insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL", "nfl_schedule.csv", "src.foot_schedule",2)
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/5_CFB", "cfb_schedule.csv", "src.foot_schedule",5)

insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source", "ball_team.csv", 'ball.team')
insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source", "ball_season.csv", 'ball.season')

insert_from_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source", "the_odds_api_teamname.csv", 'ball.team_name')


cur.close()
conn.close()

