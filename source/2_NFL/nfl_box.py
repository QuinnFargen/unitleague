# #!/usr/bin/env python3
# import argparse
# import csv
# import json
# import os
from typing import Any, Dict, Iterable, List, Optional

import pandas as pd
import requests
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
        "category": category,
        "block_name": block.get("name"),
        "athlete_id": safe_get(athlete, "athlete", "id"),
        "athlete_name": safe_get(athlete, "athlete", "displayName"),
        "athlete_jersey": safe_get(athlete, "athlete", "jersey"),
        # "athlete_uid": safe_get(athlete, "athlete", "uid"),
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


# def extract_game_meta(event_id: str, js: Dict[str, Any]) -> Dict[str, Any]:
#     competitions = safe_get(js, "header", "competitions", default=[]) or []
#     comp = competitions[0] if competitions else {}
#     situation = safe_get(js, "situation", default={})

#     # Scores/teams
#     teams = safe_get(comp, "competitors", default=[]) or []
#     away, home = None, None
#     for t in teams:
#         if t.get("homeAway") == "home":
#             home = t
#         elif t.get("homeAway") == "away":
#             away = t

#     meta = {
#         "event_id": event_id,
#         "game_id": comp.get("id"),
#         "date": comp.get("date"),
#         "season": safe_get(comp, "season", "year"),
#         "season_type": safe_get(comp, "season", "type"),
#         "week": safe_get(comp, "week", "number"),
#         "venue_full_name": safe_get(comp, "venue", "fullName"),
#         "venue_city": safe_get(comp, "venue", "address", "city"),
#         "venue_state": safe_get(comp, "venue", "address", "state"),
#         "attendance": safe_get(js, "gameInfo", "attendance"),
#         "game_duration": safe_get(js, "gameInfo", "gameDuration"),
#         "status_type": safe_get(comp, "status", "type", "name"),
#         "status_detail": safe_get(comp, "status", "type", "detail"),
#         "neutral_site": safe_get(comp, "neutralSite"),
#         "conference_competition": safe_get(comp, "conferenceCompetition"),
#         "home_team_id": safe_get(home, "team", "id"),
#         "home_team_abbr": safe_get(home, "team", "abbreviation"),
#         "home_team_score": safe_get(home, "score"),
#         "away_team_id": safe_get(away, "team", "id"),
#         "away_team_abbr": safe_get(away, "team", "abbreviation"),
#         "away_team_score": safe_get(away, "score"),
#         "spread": safe_get(js, "pickcenter", 0, "spread"),  # usually first entry
#         "over_under": safe_get(js, "pickcenter", 0, "overUnder"),
#         "last_play_text": safe_get(situation, "lastPlay", "text"),
#     }
#     return meta

def extract_game_meta(event_id: str, js: Dict[str, Any]) -> List[Dict[str, Any]]:
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
            "season": safe_get(comp, "season", "year"),
            "season_type": safe_get(comp, "season", "type"),
            "week": safe_get(comp, "week", "number"),
            "venue_full_name": safe_get(comp, "venue", "fullName"),
            "venue_city": safe_get(comp, "venue", "address", "city"),
            "venue_state": safe_get(comp, "venue", "address", "state"),
            "attendance": safe_get(js, "gameInfo", "attendance"),
            "game_duration": safe_get(js, "gameInfo", "gameDuration"),
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





event_ids = [401671937,401671878,401326315]

all_offense: List[Dict[str, Any]] = []
all_defense: List[Dict[str, Any]] = []
all_special: List[Dict[str, Any]] = []
all_games: List[Dict[str, Any]] = []

for eid in tqdm(event_ids, desc="Processing events"):
    js = request_summary(eid)
    if js is None:
        continue

    offense_rows, defense_rows, special_rows = extract_all_stat_rows(eid, js)
    all_offense.extend(offense_rows)
    all_defense.extend(defense_rows)
    all_special.extend(special_rows)
    all_games.extend(extract_game_meta(eid, js))


pd.DataFrame(all_offense).to_csv("offense.csv", index=False)
pd.DataFrame(all_defense).to_csv("defense.csv", index=False)
pd.DataFrame(all_special).to_csv("special.csv", index=False)
pd.DataFrame(all_games).to_csv("games.csv", index=False)

