

import requests
import pandas as pd
import time

# Function to check if an element is a list or dictionary
def is_list_or_dict(x):
    return isinstance(x, (list, dict))

def merge_data(data):
    game_df = pd.json_normalize(data)
    game_df = game_df.drop(['uid'], axis=1)

    team_df = pd.json_normalize(data,
                            record_path=['competitions', 'competitors'],
                            meta=['id'],
                            meta_prefix='game.')
    team_df = team_df.drop(['id', 'uid'], axis=1)    
    
    columns_to_keep = game_df.map(is_list_or_dict).all(axis=0) == False

    # Filter the DataFrame to keep only the desired columns
    game_df = game_df.loc[:, columns_to_keep]
    
    df = pd.merge(game_df, team_df, how='outer', left_on=['id'], right_on=['game.id']).drop(['id'], axis=1)

    # --- Split linescores into Q1-Q5 ---
    # Initialize Q1-Q5 columns to 0 if linescores column is missing
    for i in range(1, 6):
        df[f'Q{i}'] = 0
    if 'linescores' in df.columns:
        for idx, row in df.iterrows():
            scores = row['linescores'] if isinstance(row['linescores'], list) else []
            for s in scores:
                period = s.get('period', 0)
                value = s.get('value', 0)
                if 1 <= period <= 5:
                    df.at[idx, f'Q{period}'] = value

    final_columns = [ 'date','name','shortName','week.number','season.year','season.type','season.slug','status.period'
                    ,'homeAway','score','winner','linescores','team.displayName','team.abbreviation','team.shortDisplayName'
                    ,'team.id','game.id','seasontype','seasontypeLabel','week','weekLabel','Q1','Q2','Q3','Q4','Q5']
    df = df.reindex(columns=final_columns)

    return df

def collapse_games(df):

    # Split into home/away
    home_df = df[df['homeAway'] == 'home'].copy()
    away_df = df[df['homeAway'] == 'away'].copy()

    home_df.drop(['homeAway', 'winner', 'linescores'], axis=1, inplace=True)
    away_df.drop(['homeAway', 'winner', 'linescores'], axis=1, inplace=True)

    # Rename home/away columns
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

    home_df = home_df.rename(columns={f'Q{i}': f'Q{i}_home' for i in range(1, 6)})
    away_df = away_df.rename(columns={f'Q{i}': f'Q{i}_away' for i in range(1, 6)})

    # Merge on game.id and other shared columns
    merge_keys = ['game.id', 'date', 'name', 'shortName', 'week.number', 
                  'season.year', 'season.type', 'season.slug', 'status.period', 
                  'seasontype', 'seasontypeLabel', 'week', 'weekLabel']
    return pd.merge(home_df, away_df, on=merge_keys)

    
headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko)  Chrome/58.0.3029.110 Safari/537.3"}
dfs = []

for year in range(2010, 2026):

    url = f'https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}'
    jsonData = requests.get(url, headers=headers).json()
    calendar = jsonData['content']['calendar']

    for each in calendar:
        seasontype = each['value']
        seasontypeLabel = each['label']
        if seasontype != '4':
            weeks = each['entries']
            for eachWeek in weeks:
                weekNo = eachWeek['value']
                weekLabel = eachWeek['label']
            
                url = f'https://cdn.espn.com/core/nfl/schedule?xhr=1&year={year}&seasontype={seasontype}&week={weekNo}'
                jsonData = requests.get(url, headers=headers).json()
                schedules = jsonData['content']['schedule']
                
                print(f'Aquiring {year} {seasontypeLabel}: {weekLabel}')

                for k,v in schedules.items():
                    games = v['games']
                    
                    df = merge_data(games)
                    df['seasontype'] = seasontype
                    df['seasontypeLabel'] = seasontypeLabel
                    df['week'] = weekNo
                    df['weekLabel'] = weekLabel
                    
                    dfs.append(df)

                time.sleep(.5)


results = pd.concat(dfs)
games = collapse_games(results)

games = pd.read_csv("/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL/nfl_schedule.csv")

games = games[~(games['name'] == 'TBD TBD at TBD TBD')]
games.columns = games.columns.str.replace('.', '_', regex=False)
games = games.rename(columns={'date': 'gamedate', 'week': 'gameweek'})
games = games.drop_duplicates(subset='game_id', keep='first')

games.to_csv("nfl_schedule2.csv", index=False)


