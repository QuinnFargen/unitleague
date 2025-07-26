
from nba_api.stats.endpoints import ScoreboardV2, BoxScoreTraditionalV2
import pandas as pd
from datetime import datetime

input_df = pd.read_csv('input.csv')
dates = input_df['GAME_DT'].tolist()
box = []
for dt in dates:
    day_dt = datetime.strptime(dt, '%Y-%m-%d')
    day = day_dt.strftime('%Y-%m-%d')
    print(day)

    scoreboard = ScoreboardV2(game_date=day)
    games = scoreboard.get_dict()['resultSets'][0]['rowSet']

    game_ids = [game[2] for game in games]

    for id in game_ids:
        box_score = BoxScoreTraditionalV2(game_id=id)
        df = box_score.get_data_frames()[0]
        box.append(df)

box_all = pd.concat(box, ignore_index=True)
box_all.to_csv('box.csv', index=False)     

dt = '2019-02-07'
for dt in dates:
    day_dt = datetime.strptime(dt, '%Y-%m-%d')
    day = day_dt.strftime('%Y-%m-%d')
    print(day)

    scoreboard = ScoreboardV2(game_date=day)

for id in game_ids:
    box_score = BoxScoreTraditionalV2(game_id=id)
    print(box_score.get_data_frames()[0].columns)
