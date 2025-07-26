
##############################################

# pip install nhl-api-py
# https://github.com/coreyjs/nhl-api-py

from nhlpy import NHLClient
client = NHLClient()

import pandas as pd
import time

##############################################

# client.schedule.get_schedule(date="2025-03-14")
# client.game_center.boxscore(2024021040)
# client.teams.teams_info()
# client.schedule.get_season_schedule(team_abbr="BUF", season="20212022")


# Assume you already have `client` set up
# List of NHL team abbreviations
team_abbrs = [
    'ANA', 'ARI', 'BOS', 'BUF', 'CGY', 'CAR', 'CHI', 'COL', 'CBJ', 'DAL', 
    'DET', 'EDM', 'FLA', 'LAK', 'MIN', 'MTL', 'NSH', 'NJD', 'NYI', 'NYR', 
    'OTT', 'PHI', 'PIT', 'SJS', 'SEA', 'STL', 'TBL', 'TOR', 'VAN', 'VGK', 'WSH', 'WPG', 'UTA'
]

# Seasons from 20112012 to 20232024
seasons = [f"{year}{year+1}" for year in range(2011, 2024)]

# Collect all rows
rows = []

for team in team_abbrs:
    for season in seasons:
        try:
            response = client.schedule.get_season_schedule(team_abbr=team, season=season)
            games = response.get('games', [])
            
            for game in games:
                row = {
                    'id': game.get('id'),
                    'season': game.get('season'),
                    'gameDate': game.get('gameDate'),
                    'startTimeUTC': game.get('startTimeUTC'),
                    'awayTeamAbbr': game['awayTeam'].get('abbrev'),
                    'awayTeamScore': game['awayTeam'].get('score'),
                    'homeTeamAbbr': game['homeTeam'].get('abbrev'),
                    'homeTeamScore': game['homeTeam'].get('score'),
                    'REG_SO': game['gameOutcome'].get('lastPeriodType')
                }
                rows.append(row)
            time.sleep(2)
            print(season)
            print(team)
        
        except Exception as e:
            print(f"Error fetching schedule for {team} in {season}: {e}")

# Build dataframe
games_df = pd.DataFrame(rows)
games_df.to_csv('nhl_schedule.csv', index=False)

# Error fetching schedule for CAR in 20222023: 'gameOutcome'
# Error fetching schedule for LAK in 20192020: The read operation timed out
# Error fetching schedule for TBL in 20222023: 'gameOutcome'




##########################################################

def player_goalie_boxscore(game_data):
    boxscore = []
    boxscore_goalie = []
    game_id = game_data['id']
    date = game_data['gameDate']
    
    for team_key in ['homeTeam', 'awayTeam']:
        team = game_data[team_key]
        team_abbrev = team['abbrev']
        
        for position_group in ['forwards', 'defense']:
            for player in game_data['playerByGameStats'][team_key].get(position_group, []):
                boxscore.append({
                    'GameID': game_id,
                    'Date': date,
                    'Team': team_abbrev,
                    'PlayerID': player['playerId'],
                    'PlayerName': player['name']['default'],
                    'Position': player['position'],
                    'Goals': player.get('goals', 0),
                    'Assists': player.get('assists', 0),
                    'Points': player.get('points', 0),
                    'plusMinus': player.get('plusMinus', 0),
                    'PIM': player.get('pim', 0),
                    'Hits': player.get('hits', 0),
                    'SOG': player.get('sog', 0),
                    'blockedShots': player.get('blockedShots', 0),
                    'Giveaways': player.get('giveaways', 0),
                    'Takeaways': player.get('takeaways', 0),
                    'TimeonIce': player.get('toi', '00:00'),
                    'shifts': player.get('shifts', 0),
                    'faceoffWinningPctg': player.get('faceoffWinningPctg', 0.0)
                })

        for position_group in ['goalies']:
            for player in game_data['playerByGameStats'][team_key].get(position_group, []):
                boxscore_goalie.append({
                    'GameID': game_id,
                    'Date': date,
                    'Team': team_abbrev,
                    'PlayerID': player['playerId'],
                    'PlayerName': player['name']['default'],
                    'Position': player['position'],
                    'evenStrengthShotsAgainst': player.get('evenStrengthShotsAgainst', 0),
                    'powerPlayShotsAgainst': player.get('powerPlayShotsAgainst', 0),
                    'shorthandedShotsAgainst': player.get('shorthandedShotsAgainst', 0),
                    'saveShotsAgainst': player.get('saveShotsAgainst', 0),
                    'savePctg': player.get('savePctg', 0),
                    'evenStrengthGoalsAgainst': player.get('evenStrengthGoalsAgainst', 0),
                    'powerPlayGoalsAgainst': player.get('powerPlayGoalsAgainst', 0),
                    'shorthandedGoalsAgainst': player.get('shorthandedGoalsAgainst', 0),
                    'pim': player.get('pim', 0),
                    'goalsAgainst': player.get('goalsAgainst', 0),
                    'TimeonIce': player.get('toi', '00:00'),
                    'shotsAgainst': player.get('shotsAgainst', 0),
                    'saves': player.get('saves', 0.0)
                })
    
    df = pd.DataFrame(boxscore)
    df2 = pd.DataFrame(boxscore_goalie)
    return df, df2



def trad_boxscore(land):
    # Extract basic game info
    game_info = {
        'id': land['id'],
        'season': land['season'],
        'gameDate': land['gameDate'],
        'startTimeUTC': land['startTimeUTC'],
        'gamePeriod': land['periodDescriptor'].get('periodType'),
        'awayTeamAbbr': land['awayTeam'].get('abbrev'),
        'awayTeamScore': land['awayTeam'].get('score'),
        'homeTeamAbbr': land['homeTeam'].get('abbrev'),
        'homeTeamScore': land['homeTeam'].get('score')
    }

    # Initialize goals summary
    goals_summary = {
        'H1': 0, 'H2': 0, 'H3': 0, 'H4': 0, 'H5': 0,
        'A1': 0, 'A2': 0, 'A3': 0, 'A4': 0, 'A5': 0,
    }

    # Process goals
    for period in land.get('summary', {}).get('scoring', []):
        period_num = period['periodDescriptor']['number']
        # Map regulation periods (1-3), OT (4), SO (5)
        if period['periodDescriptor']['periodType'] == 'OVERTIME':
            period_num = 4
        if period['periodDescriptor']['periodType'] == 'SHOOTOUT':
            period_num = 5

        for goal in period.get('goals', []):
            is_home = goal.get('isHome')
            if is_home:
                goals_summary[f'H{period_num}'] += 1
            else:
                goals_summary[f'A{period_num}'] += 1

    SO_H = 0
    SO_A = 0
    # Process shootout goals if any
    for shootout_goal in land.get('summary', {}).get('shootout', []):
        is_home = shootout_goal.get('isHome')
        if is_home:
            SO_H += 1
        else:
            SO_A += 1

    if SO_H > SO_A:
        SO_H = 1
        SO_A = 0
    else:
        SO_H = 0
        SO_A = 1

    goals_summary['H5'] = SO_H
    goals_summary['A5'] = SO_A

    return {**game_info, **goals_summary}




##############################################

# ['', ''
# DONE: 20112012, 20232024, 20222023, 20212022, 20202021, 20192020, 20182019, 20172018, 20162017, 20152016, 20142015, 20132014, 20122013

import os
os.chdir('/Users/quinnfargen/Documents/GitHub/OddDB/API/sources/historic')

sched_df = pd.read_csv('nhl_schedule.csv')
game_ids = sched_df[sched_df['season'] == 20182019]['id'].tolist()
len(game_ids) #34801
game_ids = list(set(game_ids)) #Dedup
#17660


# Initialize an empty list to store box score data
all_players = []
all_goalies = []
all_box = []
i = 0

# Loop through each game ID and fetch box score data
for game_id in game_ids:
    try:
        game_data = client.game_center.boxscore(game_id)
        player_score, goalie_score = player_goalie_boxscore(game_data)
        land = client.game_center.match_up(game_id)
        box_trad = trad_boxscore(land)
        all_players.append(player_score)
        all_goalies.append(goalie_score)
        all_box.append(box_trad)
        time.sleep(1)
        print(str(game_id) + ' ' + str(i))    
        i += 1
    except Exception as e:
        print(f"Error fetching box for {game_id}: {e}")


# Combine all DataFrames into a single DataFrame
final_player = pd.concat(all_players, ignore_index=True)
final_goalie = pd.concat(all_goalies, ignore_index=True)
final_box = pd.DataFrame(all_box)

# Optional: Save to CSV
final_player.to_csv("nhl_player_box_.csv", index=False)
final_goalie.to_csv("nhl_goalie_box_.csv", index=False)
final_box.to_csv(     "nhl_trad_box_.csv", index=False)





# NON time out
# Error fetching box for 2011030213: 'A6'
# Error fetching box for 2022030311: 'A7'
# Error fetching box for 2021030141: 'A6'
# Error fetching box for 2020030184: 'H6'
# Error fetching box for 2019030121: 'H8'
# Error fetching box for 2015030244: 'H6'

# Error fetching box for 2014030164: 'H6'
# Error fetching box for 2014030322: 'A6'
# Error fetching box for 2013030161: 'H6'
# Error fetching box for 2012030411: 'H6'
game_ids = [2011030213, 2022030311, 2021030141, 2020030184, 2019030121, 2015030244, 2014030164, 2014030322, 2013030161, 2012030411]

# TIME OUT
game_ids = [
2021010096,2021020595,2020020225,2020020235,2020020236,2020020238,2020020239,2020020312,
2020020316,2020020381,2020020462,2020020473,2020020474,2020020475,2020020505,2020020506,
2020020583,2020020667,2020020704,2020020705,2020020760,2020020761,2020020764,2020020765,
2020020831,2020030152,2020030315,2020030316,2020020043,2020020146,2020020147,2020020144,
2020020153,2020020154,2020020155,2020020156,2019010049,2019010082,2019010083,2019010084,
2019010085,2019010091,2019020028,2019020030,2019020031,2019020103,2019020137,2019020200,
2019020201,2019020357,2019020462,2019020668,2019020869,2019020917,2019020919,2019021017,
2018020016,2018020153,2018020154,2018020155,2018020156,2018020193,2015020132,2014020075,
2014020232,2014020269
]










import pandas as pd
import glob
import os

folder_path = '/Users/quinnfargen/Documents/GitHub/OddDB/API/sources/historic'
# pattern = os.path.join(folder_path, "nhl_goalie_box*.csv")
# pattern = os.path.join(folder_path, "nhl_player_box*.csv")
pattern = os.path.join(folder_path, "nhl_trad_box*.csv")
csv_files = glob.glob(pattern)

df_list = []
for file in csv_files:
    try:
        df = pd.read_csv(file)
        df_list.append(df)
    except Exception as e:
        print(f"Skipping {file} due to error: {e}")

# Concatenate all into one
combined_df = pd.concat(df_list, ignore_index=True)

# combined_df.drop_duplicates(inplace=True)
combined_df.reset_index(drop=True, inplace=True)

# combined_df.to_csv("nhl_goalie_box.csv", index=False)
# combined_df.to_csv("nhl_player_box.csv", index=False)
combined_df.to_csv("nhl_trad_box.csv", index=False)




