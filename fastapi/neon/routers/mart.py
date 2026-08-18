from fastapi import APIRouter, Query
from sqlalchemy import text

from db import engine

router = APIRouter(tags=["mart"])


@router.get("/mart/league")
def get_league():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM mart.league"))
        return [dict(row._mapping) for row in result]

@router.get("/mart/team")
def get_team(league_id: int = Query(None)
            ,team_id: int = Query(None)
            ,conf: str = Query(None)
            ,color: str = Query(None)
            ,region: str = Query(None)
            ,category: str = Query(None)
            ,mascot: str = Query(None)):
    q = "SELECT * FROM mart.team WHERE 1=1"
    query_params = {}

    q += " AND league_id = :league_id"
    query_params["league_id"] = league_id

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if conf:
        q += " AND conf = :conf"
        query_params["conf"] = conf

    if color:
        q += " AND color = :color"
        query_params["color"] = color

    if region:
        q += " AND region = :region"
        query_params["region"] = region

    if category:
        q += " AND category = :category"
        query_params["category"] = category

    if mascot:
        q += " AND mascot = :mascot"
        query_params["mascot"] = mascot

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/game")
def get_game(game_dt: str = Query(None)
            , league_id: int = Query(None)):
    q = "SELECT * FROM mart.game WHERE 1=1"
    query_params = {}

    if game_dt:
        q += " AND game_dt = :game_dt"
        query_params["game_dt"] = game_dt

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/league_game_dates")
def get_league_game_dates(league_id: int = Query(None)):
    q = "SELECT * FROM mart.league_game_dates WHERE 1=1"
    query_params = {}

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/sched")
def get_sched(team_id: str = Query(None)
            , league_id: int = Query(None)
            , yr: int = Query(None)
            , opp_conf: str = Query(None)
            , opp_color: str = Query(None)
            , opp_region: str = Query(None)
            , opp_mascot: str = Query(None)
            , limit: int = Query(None)):
    q = "SELECT * FROM mart.sched WHERE 1=1"
    query_params = {}

    if yr:
        q += " AND yr = :yr"
        query_params["yr"] = yr

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if opp_conf:
        q += " AND opp_conf = :opp_conf"
        query_params["opp_conf"] = opp_conf

    if opp_color:
        q += " AND opp_color = :opp_color"
        query_params["opp_color"] = opp_color

    if opp_region:
        q += " AND opp_region = :opp_region"
        query_params["opp_region"] = opp_region

    if opp_mascot:
        q += " AND opp_mascot = :opp_mascot"
        query_params["opp_mascot"] = opp_mascot

    if limit:
        q += " ORDER BY game_dt DESC LIMIT :limit"
        query_params["limit"] = limit

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/team_season")
def get_team_season(league_id: int = Query(None)
                   , team_id: int = Query(None)
                   , yr: int = Query(None)
                   , conf: str = Query(None)
                   , color: str = Query(None)
                   , region: str = Query(None)
                   , category: str = Query(None)):
    q = "SELECT * FROM mart.team_season WHERE 1=1"
    query_params = {}

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if yr:
        q += " AND yr = :yr"
        query_params["yr"] = yr

    if conf:
        q += " AND conf = :conf"
        query_params["conf"] = conf

    if color:
        q += " AND color = :color"
        query_params["color"] = color

    if region:
        q += " AND region = :region"
        query_params["region"] = region

    if category:
        q += " AND category = :category"
        query_params["category"] = category

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/game_oddbest")
def get_game_oddbest(game_id: int = Query(None)
                   , game_dt: str = Query(None)
                   , league_id: int = Query(None)):
    q = "SELECT * FROM mart.game_oddbest WHERE has_active_bets"
    query_params = {}

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    if game_id:
        q += " AND game_id = :game_id"
        query_params["game_id"] = game_id

    if game_dt:
        q += " AND game_dt = :game_dt"
        query_params["game_dt"] = game_dt

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/game_oddall")
def get_game_oddall(game_id: int = Query(...)):
    q = "SELECT * FROM mart.game_oddall WHERE game_id = :game_id"
    with engine.connect() as conn:
        result = conn.execute(text(q), {"game_id": game_id})
        return [dict(row._mapping) for row in result]

@router.get("/mart/team_odds_recent")
def get_team_odds_recent(league_id: int = Query(None)
                        , team_id: int = Query(None)
                        , yr: int = Query(None)
                        , conf: str = Query(None)
                        , color: str = Query(None)
                        , region: str = Query(None)
                        , category: str = Query(None)):
    q = "SELECT * FROM mart.team_odds_recent WHERE 1=1"
    query_params = {}

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if yr:
        q += " AND yr = :yr"
        query_params["yr"] = yr

    if conf:
        q += " AND conf = :conf"
        query_params["conf"] = conf

    if color:
        q += " AND color = :color"
        query_params["color"] = color

    if region:
        q += " AND region = :region"
        query_params["region"] = region

    if category:
        q += " AND category = :category"
        query_params["category"] = category

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/txn")
def get_mart_txn(bettor_id: int = Query(None), syndicate_id: int = Query(None), game_id: int = Query(None)):
    q = "SELECT * FROM mart.txn WHERE 1=1"
    query_params = {}

    if bettor_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    if syndicate_id:
        q += " AND syndicate_id = :syndicate_id"
        query_params["syndicate_id"] = syndicate_id

    if game_id:
        q += " AND game_id = :game_id"
        query_params["game_id"] = game_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/txn/bets")
def get_mart_txn_bets(bettor_id: int = Query(None), syndicate_id: int = Query(None)):
    q = "SELECT * FROM mart.v_txn_bets WHERE 1=1"
    query_params = {}

    if bettor_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    if syndicate_id:
        q += " AND syndicate_id = :syndicate_id"
        query_params["syndicate_id"] = syndicate_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/syndicate")
def get_syndicate(syndicate_id: int = Query(None), bettor_id: int = Query(None)):
    q = """
        SELECT s.*
        FROM odd.syndicate s
        JOIN odd.runner r ON s.syndicate_id = r.syndicate_id
        WHERE r.active = true
    """
    query_params = {}

    if syndicate_id:
        q += " AND s.syndicate_id = :syndicate_id"
        query_params["syndicate_id"] = syndicate_id

    if bettor_id:
        q += " AND r.bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    q += " ORDER BY s.syndicate_id"

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/syndicate/public")
def get_public_syndicates(bettor_id: int = Query(None), league_id: int = Query(None)):
    q = """
        WITH runner_counts AS (
            SELECT syndicate_id, COUNT(*) FILTER (WHERE active = true) AS active_runner_count
            FROM odd.runner
            GROUP BY syndicate_id
        )
        SELECT s.syndicate_id, s.name, s.description, s.code, s.is_public, s.max_runner,
               s.created_by_bettor_id, s.symbol, s.color, s.start_units, s.config, s.is_started,
               s.syndicate_type, s.league_ids,
               COALESCE(rc.active_runner_count, 0) AS active_runner_count
        FROM odd.syndicate s
        LEFT JOIN runner_counts rc ON rc.syndicate_id = s.syndicate_id
        WHERE s.is_public = true AND s.is_started = false
          AND (s.max_runner IS NULL OR COALESCE(rc.active_runner_count, 0) < s.max_runner)
    """
    query_params = {}

    if bettor_id:
        q += """
            AND NOT EXISTS (
                SELECT 1 FROM odd.runner mr
                WHERE mr.syndicate_id = s.syndicate_id AND mr.bettor_id = :bettor_id AND mr.active = true
            )
        """
        query_params["bettor_id"] = bettor_id

    if league_id:
        q += """
            AND (s.league_ids IS NULL OR s.league_ids = '{}' OR :league_id = ANY(s.league_ids))
        """
        query_params["league_id"] = league_id

    q += " ORDER BY s.syndicate_id"

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/bettor")
def get_bettor(bettor_id: int = Query(None)):
    q = "SELECT * FROM mart.bettor WHERE 1=1"
    query_params = {}

    if bettor_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/bettor_league")
def get_bettor_league(bettor_id: int = Query(None)):
    q = "SELECT * FROM mart.bettor_league WHERE 1=1"
    query_params = {}

    if bettor_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/season")
def get_season(league_id: int = Query(None), active: bool = Query(None)):
    q = "SELECT * FROM mart.season WHERE 1=1"
    query_params = {}

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    if active is not None:
        q += " AND active = :active"
        query_params["active"] = active

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/week")
def get_week(season_id: int = Query(None), league_id: int = Query(None)):
    q = "SELECT * FROM mart.week WHERE 1=1"
    query_params = {}

    if season_id:
        q += " AND season_id = :season_id"
        query_params["season_id"] = season_id

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    q += " ORDER BY week_start_dt"

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@router.get("/mart/runner")
def get_runner(syndicate_id: int = Query(None),
               bettor_id: int = Query(None),
               competition: bool = Query(True)):
    query_params = {}

    if bettor_id and competition:
        q = """
            SELECT r.* FROM mart.runner r
            WHERE r.syndicate_id IN (
                SELECT syndicate_id FROM odd.runner
                WHERE bettor_id = :bettor_id AND active = true
            )
        """
        query_params["bettor_id"] = bettor_id
    else:
        q = "SELECT * FROM mart.runner WHERE 1=1"
        if syndicate_id:
            q += " AND syndicate_id = :syndicate_id"
            query_params["syndicate_id"] = syndicate_id
        if bettor_id:
            q += " AND bettor_id = :bettor_id"
            query_params["bettor_id"] = bettor_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]
