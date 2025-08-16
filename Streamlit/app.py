import os
from dotenv import load_dotenv
import streamlit as st
import pandas as pd
import psycopg2
import psycopg2.extras

# -------------------------
# Database connection
# -------------------------
# Load environment variables from .env file
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


# -------------------------
# Sidebar navigation
# -------------------------
st.sidebar.title("UnitLeague")
page = st.sidebar.radio("Choose League", ["NFL", "CFB"])

league_map = {"NFL": 2, "CFB": 5}
league_id = league_map[page]

# -------------------------
# Queries
# -------------------------
games_query = """
    SELECT *
    FROM ball.game_team
    WHERE league_id = %s
    ORDER BY game_dt, game_time
"""

odds_query = """
    SELECT *
    FROM odd.bet_active
    WHERE game_id = %s
"""

# -------------------------
# Main Page
# -------------------------
st.title(f"🏈 UnitLeague - {page}")

games_df = run_query(games_query, (league_id,))

if games_df.empty:
    st.info("No games available right now.")
else:
    for _, game in games_df.iterrows():
        header = f"{game['game_dt']} - {game['away_team']} @ {game['home_team']}"
        with st.expander(header, expanded=False):
            st.write(f"**Date:** {game['game_dt']} | **Time:** {game['game_time']}")
            st.write(f"**Matchup:** {game['away_team']} @ {game['home_team']}")

            # Query odds for this game by game_id
            odds_df = run_query(odds_query, (game["game_id"],))

            if odds_df.empty:
                st.write("_No active odds available_")
            else:
                # Show top odds per bet type
                st.subheader("Top Odds")
                top_odds = (
                    odds_df.groupby("bet_type")
                    .apply(lambda x: x.sort_values("price", ascending=False).head(1))
                    .reset_index(drop=True)
                )
                st.dataframe(top_odds[["bet_type", "bet_team", "bookmaker", "price", "points"]])

                st.subheader("All Odds")
                st.dataframe(odds_df)