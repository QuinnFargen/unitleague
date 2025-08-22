from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
import time
from tqdm import tqdm

SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/football/nfl/summary?event={event_id}"

# ---- helpers -----------------------------------------------------------------

def safe_get(d: Dict, *path, default=None):
    cur = d
    for p in path:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return default
    return cur


def request_summary(event_id: str, timeout: float = 20.0) -> Optional[Dict[str, Any]]:
    url = SUMMARY_URL.format(event_id=event_id)
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[WARN] event {event_id}: {e}")
        return None


def build_row_from_athlete_block(
    event_id: str,
    team: Dict[str, Any],
    block: Dict[str, Any],
    athlete: Dict[str, Any],
    category: str,
) -> Dict[str, Any]:
    """
    Map the parallel arrays (block['keys'] and athlete['stats']) into columns.
    Keeps values as strings; you can cast later if desired.
    """
    row = {
        "event_id": event_id,
        "team_id": safe_get(team, "id"),
        "team_abbr": safe_get(team, "abbreviation"),
        "team_name": safe_get(team, "displayName"),
        "athlete_id": safe_get(athlete, "athlete", "id"),
        "athlete_name": safe_get(athlete, "athlete", "displayName"),
        "athlete_jersey": safe_get(athlete, "athlete", "jersey"),
    }
    keys = block.get("keys", [])
    stats_vals = athlete.get("stats", [])
    for k, v in zip(keys, stats_vals):
        # normalize key names a bit (remove slashes)
        col = k.replace("/", "_").replace("-", "_")
        row[col] = v
    return row


def extract_stat_category(
    event_id: str,
    team_node: Dict[str, Any],
    wanted_blocks: Iterable[str],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    team = team_node.get("team", {})
    for block in team_node.get("statistics", []):
        name = block.get("name")
        if name not in wanted_blocks:
            continue
        for athlete in block.get("athletes", []):
            out.append(
                build_row_from_athlete_block(
                    event_id=event_id,
                    team=team,
                    block=block,
                    athlete=athlete,
                    category=name,
                )
            )
    return out


def extract_all_stat_rows(event_id: str, js: Dict[str, Any]):
    """
    Returns three lists of dicts:
      offense_rows, defense_rows, st_rows
    """
    offense_blocks = {"passing", "rushing", "receiving"}
    defense_blocks = {"defensive", "interceptions"}  # interceptions could be empty often
    special_blocks = {"kickReturns", "puntReturns", "kicking", "punting"}

    offense_rows, defense_rows, st_rows = [], [], []

    for team_node in safe_get(js, "boxscore", "players", default=[]):
        offense_rows.extend(extract_stat_category(event_id, team_node, offense_blocks))
        defense_rows.extend(extract_stat_category(event_id, team_node, defense_blocks))
        st_rows.extend(extract_stat_category(event_id, team_node, special_blocks))

    return offense_rows, defense_rows, st_rows


def extract_game_meta(event_id: str, js: Dict[str, Any]) -> List[Dict[str, Any]]:
    header = safe_get(js, "header", default=[]) or []
    competitions = safe_get(js, "header", "competitions", default=[]) or []
    comp = competitions[0] if competitions else {}

    meta_rows = []
    teams = safe_get(comp, "competitors", default=[]) or []
    team_stats = safe_get(js, "boxscore", "teams", default=[]) or []

    # Map team_id -> stats for quick lookup
    stats_map = {t.get("team", {}).get("id"): t for t in team_stats}

    for t in teams:
        team = t.get("team", {})
        team_id = team.get("id")
        team_stat_block = stats_map.get(team_id, {})

        row = {
            "event_id": event_id,
            # "game_id": comp.get("id"),
            "date": comp.get("date"),
            "season": safe_get(header, "season", "year"),
            "season_type": safe_get(header, "season", "type"),
            "week": safe_get(header, "week"),
            "venue_full_name": safe_get(js, "gameInfo", "venue", "fullName"),
            "venue_city": safe_get(js, "gameInfo", "venue", "address", "city"),
            "venue_state": safe_get(js, "gameInfo", "venue", "address", "state"),
            "venue_zip": safe_get(js, "gameInfo", "venue", "address", "zipCode"),
            "venue_country": safe_get(js, "gameInfo", "venue", "address", "country"),
            "venue_grass": safe_get(js, "gameInfo", "venue", "grass"),
            "attendance": safe_get(js, "gameInfo", "attendance"),
            "status_type": safe_get(comp, "status", "type", "name"),
            "status_detail": safe_get(comp, "status", "type", "detail"),
            "neutral_site": safe_get(comp, "neutralSite"),
            "conference_competition": safe_get(comp, "conferenceCompetition"),
            "team_id": team_id,
            "team_abbr": team.get("abbreviation"),
            "team_name": team.get("displayName"),
            "home_away": t.get("homeAway"),
            "team_score": t.get("score"),
            "spread": safe_get(js, "pickcenter", 0, "spread"),
            "over_under": safe_get(js, "pickcenter", 0, "overUnder"),
        }

        # Add all statistics for this team
        for stat in team_stat_block.get("statistics", []):
            label = stat.get("name")
            display_val = stat.get("displayValue")
            if label:
                row[label] = display_val

        meta_rows.append(row)

    return meta_rows


def split_fraction_columns(df: pd.DataFrame):
    # Define mapping of columns to split -> [new_column_1, new_column_2]
    fraction_map = {
        "completions_passingAttempts": ["passingCompletions", "passingAttempts"],   #offense
        "sacks_sackYardsLost": ["sacks", "sackYardsLost"],                          #offense
        "completionAttempts": ["passingCompletions", "passingAttempts"],            #games
        "thirdDownEff": ["thirddownSuccess", "thirddownAttempts"],                  #games
        "fourthDownEff": ["fourthdownSuccess", "fourthdownAttempts"],               #games
        "sacksYardsLost": ["sacks", "sackYardsLost"],                               #games
        "redZoneAttempts": ["redZoneSuccess", "redZoneAttempts"],                   #games
        "totalPenaltiesYards": ["Penalties", "PenaltyYards"],                       #games
        "extraPointsMade_extraPointAttempts": ["xpSuccess", "xpAttempts"],          #special
        "fieldGoalsMade_fieldGoalAttempts": ["fgSuccess", "fgAttempts"]             #special     
    }
    for col, new_cols in fraction_map.items():
        if col in df.columns:
            # Ensure correct delimiter: "/" or "-"
            df[col] = df[col].astype(str).str.replace("-", "/", regex=False)
            df[new_cols] = df[col].str.split("/", expand=True)
            df.drop(columns=[col], inplace=True)
    return df


def merge_duplicate_athletes(df: pd.DataFrame):
    merge_cols = ['event_id', 'team_id', 'team_abbr', 'team_name', 'athlete_id', 'athlete_name', 'athlete_jersey']
    return df.groupby(merge_cols, dropna=False, as_index=False).first()



nfl_sched = pd.read_csv('/Users/quinnfargen/Documents/GitHub/unitleague/source/2_NFL/nfl_schedule.csv')
event_ids = nfl_sched["game.id"].tolist()
# event_ids = [401671937,401671878,401326315]
# len(event_ids) # 5003

all_offense: List[Dict[str, Any]] = []
all_defense: List[Dict[str, Any]] = []
all_special: List[Dict[str, Any]] = []
all_games: List[Dict[str, Any]] = []

for eid in tqdm(event_ids, desc="Processing events"):
    js = request_summary(eid)
    if js is None:
        continue

    time.sleep(.5)

    offense_rows, defense_rows, special_rows = extract_all_stat_rows(eid, js)
    all_offense.extend(offense_rows)
    all_defense.extend(defense_rows)
    all_special.extend(special_rows)
    all_games.extend(extract_game_meta(eid, js))


df_off = pd.DataFrame(all_offense)
df_def = pd.DataFrame(all_defense)
df_spe = pd.DataFrame(all_special)
df_gam = pd.DataFrame(all_games)


df_off = merge_duplicate_athletes(df_off)
df_def = merge_duplicate_athletes(df_def)
df_spe = merge_duplicate_athletes(df_spe)

df_off = split_fraction_columns(df_off)
df_spe = split_fraction_columns(df_spe)
df_gam = split_fraction_columns(df_gam)

# avoid issue on insert into db:
df_off['adjQBR'] = df_off['adjQBR'].replace('--', '')
df_off['sacks'] = df_off['sacks'].replace('None', '')
df_off['passingCompletions'] = df_off['passingCompletions'].replace('None', '')
df_spe['xpSuccess'] = df_spe['xpSuccess'].replace('None', '')
df_spe['fgSuccess'] = df_spe['fgSuccess'].replace('None', '')
df_gam = df_gam.rename(columns={'date': 'gamedate', 'week': 'gameweek'})

df_off.to_csv("nfl_offense.csv", index=False)
df_def.to_csv("nfl_defense.csv", index=False)
df_spe.to_csv("nfl_special.csv", index=False)
df_gam.to_csv("nfl_games2.csv", index=False)

