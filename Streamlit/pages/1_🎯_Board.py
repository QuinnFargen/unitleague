import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras

##############################################
st.title(f"The Board")
st.logo("logo.png")

##############################################
league = st.sidebar.radio("Choose League", ["ALL", "🪖NFL", "🎓CFB"], index=0)
league_map = {"🪖NFL": 2, "🎓CFB": 5}
conf = "ALL"
subconf = ""
if league != "ALL":
    if league == "🪖NFL":
        conf = st.sidebar.radio("Choose Conference", ["ALL", "AFC", "NFC"], index=0)
    else: # CFB
        conf = st.sidebar.radio("Choose Conference", ["ALL", "B10", "SEC", "ACC", "B12", "(I)ND"], index=0)
conf_map = {"AFC": "AFC", "NFC": "NFC", "B10": "B10", "SEC": "SEC", "ACC": "ACC", "B12": "B12", "(I)ND": "IND"}

book = st.sidebar.selectbox("Choose Bookie", ("Best Odds","FanDuel","DraftKings"))

if st.sidebar.button("🔄 Refresh Results"):
    st.cache_resource.clear()  # clears cached connection



##############################################
# Database connection
load_dotenv()
@st.cache_resource
def init_connection():
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        st.error("DATABASE_URL environment variable not set")
        st.stop()
    conn = psycopg2.connect(db_url)
    return conn

@st.cache_data(ttl=300)
def run_query(query: str, params: tuple = None):
    conn = init_connection()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query, params or ())
        rows = cur.fetchall()
        return pd.DataFrame(rows)

##############################################
# Queries
games_query = """
    SELECT *
    FROM ball.game_team
"""
detail_query = """
    SELECT *
    FROM ball.game_team
    WHERE game_id = %s
"""

games_df = run_query(games_query)

if league != "ALL":   # Filter by Radio Sidebar League Selected
    if conf != "ALL": 
        subconf = f" - {conf}"
        games_df = games_df[
            (games_df["league_id"] == league_map[league]) &
            ((games_df["home_conf"] == conf_map[conf]) | (games_df["away_conf"] == conf_map[conf]))
        ]
    else:
        games_df = games_df[games_df["league_id"] == league_map[league]]
else:   
    games_df = games_df


##############################################
# Top Board

selected_game_id = None

with st.expander(label=f"{league}  Games {subconf}", width="stretch"):

    # We'll create a column of buttons
    for idx, row in games_df.iterrows():
        col1, col2, col3 = st.columns([3, 3, 1])
        col1.write(f"{row['away_team']} @ {row['home_team']}")
        col2.write(f"{row['game_dt']} {row['game_time']}")
        if col3.button("Details", key=row['game_id']):
            selected_game_id = row['game_id']


##############################################
# Single Game

with st.expander(label=f"Selected Game", width="stretch"):

    if selected_game_id:
        details_df = run_query(detail_query, (selected_game_id,))
        column_order = ["home_team", "away_team", "game_dt", "h", "a"]
        filtered_df = details_df[column_order]
        st.dataframe(filtered_df, use_container_width=True)
