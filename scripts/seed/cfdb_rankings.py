import requests
import pandas as pd
import time
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path=".env")

CFBD_API_KEY = os.getenv("CFBD_API_KEY")
BASE_URL = "https://api.collegefootballdata.com"

START_YEAR = 2015
END_YEAR = datetime.now().year

SEASON_TYPES = ["regular", "postseason"]
TOTAL_WEEKS = 16   # safe upper bound
REQUEST_SLEEP = 1


def get_request(api_key, year, season_type, week, type):
    if type == 'rank':
        url = f"{BASE_URL}/rankings"
    else:
        url = f"{BASE_URL}/ratings/{type}"
    params = {"year": year, "seasonType": season_type, "week": week}
    headers = {"Authorization": f"Bearer {api_key}"}
    r = requests.get(url, params=params, headers=headers)
    r.raise_for_status()
    return r.json()


def normalize_rankings(data):
    rows = []
    for item in data:
        for poll in item.get("polls", []):
            poll_name = poll.get("poll")
            if poll_name in ['Coaches Poll','AP Top 25','Playoff Committee Rankings']:
                for r in poll.get("ranks", []):
                    rows.append({
                        "season": item["season"],
                        "season_type": item["seasonType"],
                        "week": item["week"],
                        "system": poll_name,
                        "value": r.get("rank"),
                        "school": r.get("school"),
                        "conference": r.get("conference"),
                        "first": r.get("firstPlaceVotes"),
                        "points": r.get("points"),
                    })
    return pd.DataFrame(rows)

def normalize_elo(data, season, season_type, week):
    rows = []
    for item in data:
        rows.append({
            "season": season,
            "season_type": season_type,
            "week": week,
            "system": "elo",
            "value": item.get("elo"),
            "school": item.get("team"),
            "conference": item.get("conference"),
            "first": "",
            "points": ""
        })
    return pd.DataFrame(rows)



def collect_weekly():
    frames = []
    for year in range(START_YEAR, END_YEAR + 1):
        for season_type in SEASON_TYPES:
            for week in range(1, TOTAL_WEEKS + 1):
                print(f"Rankings {year} {season_type} wk {week}")
                rank_data = get_request(CFBD_API_KEY,year,season_type,week,type="rank")
                df_rank = normalize_rankings(rank_data)
                if not df_rank.empty:
                    frames.append(df_rank)
                time.sleep(REQUEST_SLEEP)
                print(f"ELO {year} {season_type} wk {week}")
                elo_data = get_request(CFBD_API_KEY,year,season_type,week,type="elo")
                df_elo = normalize_elo(elo_data,season=year,season_type=season_type,week=week)
                if not df_elo.empty:
                    frames.append(df_elo)
                time.sleep(REQUEST_SLEEP)

    df = pd.concat(frames, ignore_index=True)
    return df


df = collect_weekly()
df.to_csv("cfbd_all_years.csv", index=False)


rank_data = get_request(CFBD_API_KEY,2024,'regular',1,type="rank")
