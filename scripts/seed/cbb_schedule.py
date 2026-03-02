import requests
import pandas as pd
import time
from datetime import datetime

HEADERS = {"User-Agent": "Mozilla/5.0"}

# ✅ CHANGED ENDPOINT
BASE_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"


def is_list_or_dict(x):
    return isinstance(x, (list, dict))


def merge_data(data):
    game_df = pd.json_normalize(data)
    game_df = game_df.drop(['uid'], axis=1, errors='ignore')

    team_df = pd.json_normalize(
        data,
        record_path=['competitions', 'competitors'],
        meta=['id'],
        meta_prefix='game.'
    )
    team_df = team_df.drop(['id', 'uid'], axis=1, errors='ignore')

    columns_to_keep = game_df.map(is_list_or_dict).all(axis=0) == False
    game_df = game_df.loc[:, columns_to_keep]

    df = pd.merge(
        game_df,
        team_df,
        how='outer',
        left_on=['id'],
        right_on=['game.id']
    ).drop(['id'], axis=1, errors='ignore')

    # ✅ College basketball = 2 halves + OT
    df['H1'] = 0
    df['H2'] = 0
    for i in range(1, 6):
        df[f'OT{i}'] = 0

    if 'linescores' in df.columns:
        for idx, row in df.iterrows():
            scores = row['linescores'] if isinstance(row['linescores'], list) else []
            for s in scores:
                period = s.get('period', 0)
                value = s.get('value', 0)

                if period == 1:
                    df.at[idx, 'H1'] = value
                elif period == 2:
                    df.at[idx, 'H2'] = value
                elif period >= 3:
                    ot_number = period - 2
                    if 1 <= ot_number <= 5:
                        df.at[idx, f'OT{ot_number}'] = value

    final_columns = [
        'date','name','shortName',
        'season.year','season.type','season.slug','status.period',
        'homeAway','score','winner','linescores',
        'team.displayName','team.abbreviation','team.shortDisplayName',
        'team.id','game.id',
        'H1','H2','OT1','OT2','OT3','OT4','OT5'
    ]

    return df.reindex(columns=final_columns)


def collapse_games(df):
    home_df = df[df['homeAway'] == 'home'].copy()
    away_df = df[df['homeAway'] == 'away'].copy()

    home_df.drop(['homeAway','winner','linescores'], axis=1, inplace=True)
    away_df.drop(['homeAway','winner','linescores'], axis=1, inplace=True)

    home_df = home_df.rename(columns={
        'score': 'home_score',
        'team.displayName': 'home_team',
        'team.abbreviation': 'home_abbr',
        'team.shortDisplayName': 'home_short',
        'team.id': 'home_team_id'
    })

    away_df = away_df.rename(columns={
        'score': 'away_score',
        'team.displayName': 'away_team',
        'team.abbreviation': 'away_abbr',
        'team.shortDisplayName': 'away_short',
        'team.id': 'away_team_id'
    })

    home_df = home_df.rename(columns={
        'H1': 'H1_home',
        'H2': 'H2_home',
        **{f'OT{i}': f'OT{i}_home' for i in range(1, 6)}
    })

    away_df = away_df.rename(columns={
        'H1': 'H1_away',
        'H2': 'H2_away',
        **{f'OT{i}': f'OT{i}_away' for i in range(1, 6)}
    })

    merge_keys = [
        'game.id','date','name','shortName',
        'season.year','season.type','season.slug','status.period'
    ]

    return pd.merge(home_df, away_df, on=merge_keys)


def get_season_calendar(year):
    anchor_date = f"{year}1015"  # mid-October anchor
    url = f"{BASE_URL}?dates={anchor_date}"

    print(f"Initializing season {year} with anchor {anchor_date}")
    r = requests.get(url, headers=HEADERS)
    data = r.json()

    league = data["leagues"][0]

    calendar_dates = league["calendar"]
    start_date = league["calendarStartDate"]
    end_date = league["calendarEndDate"]

    parsed_dates = [
        datetime.fromisoformat(d.replace("Z", "+00:00")).strftime("%Y%m%d")
        for d in calendar_dates
    ]

    return parsed_dates, start_date, end_date


def pull_full_season_from_calendar(year):
    calendar_days, start_date, end_date = get_season_calendar(year)

    print(f"Season {year}")
    print(f"Start: {start_date}")
    print(f"End:   {end_date}")
    print(f"Total valid game days: {len(calendar_days)}")

    all_games = []

    for day in calendar_days:
        url = f"{BASE_URL}?dates={day}"
        print(f"Fetching {day}")

        r = requests.get(url, headers=HEADERS)
        data = r.json()

        games_data = data.get("events", [])

        if games_data:
            df = merge_data(games_data)
            all_games.append(df)

        time.sleep(1)

    if not all_games:
        return pd.DataFrame()

    results = pd.concat(all_games, ignore_index=True)
    games = collapse_games(results)
    games.columns = games.columns.str.replace('.', '_', regex=False)

    return games


START_YEAR = 2014
END_YEAR   = 2026

season_frames = []

for year in range(START_YEAR, END_YEAR + 1):
    print(year)
    season_df = pull_full_season_from_calendar(year)
    season_frames.append(season_df)

final_df = pd.concat(season_frames, ignore_index=True)
final_df.drop_duplicates(subset="game_id", inplace=True)

final_df.to_csv("cbb_schedule.csv", index=False)

print("Complete historical pull finished.")