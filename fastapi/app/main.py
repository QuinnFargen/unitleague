# app/main.py
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

@app.get("/mart/league")
def get_league():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM mart.league"))
        return [dict(row._mapping) for row in result]

@app.get("/mart/team")
def get_team(league_id: int = Query(None)
            ,team_id: int = Query(None)):
    q = "SELECT * FROM mart.team WHERE 1=1"
    query_params = {}

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@app.get("/mart/game")
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

@app.get("/mart/sched")
def get_sched(team_id: str = Query(None)
            , league_id: int = Query(None)
            , yr: int = Query(None)):
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

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@app.get("/mart/game_oddbest")
def get_game_oddbest(game_id: int = Query(None)
                   , game_dt: str = Query(None)
                   , league_id: int = Query(None)):
    q = "SELECT * FROM mart.game_oddbest " \
    "   WHERE has_active_bets and game_dt >= current_date"
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

@app.get("/mart/syndicate")
def get_runner(syndicate_id: int = Query(None)):
    q = "SELECT * FROM mart.syndicate WHERE 1=1"
    query_params = {}

    if syndicate_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]
    
@app.get("/mart/runner")
def get_runner(syndicate_id: int = Query(None)):
    q = "SELECT * FROM mart.runner WHERE 1=1"
    query_params = {}

    if syndicate_id:
        q += " AND syndicate_id = :syndicate_id"
        query_params["syndicate_id"] = syndicate_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]


class BettorCreate(BaseModel):
    apple_sub: str
    apple_email: Optional[str] = None
    apple_name: Optional[str] = None
    apple_refresh_token: Optional[str] = None

class BettorUpdate(BaseModel):
    apple_email: Optional[str] = None
    apple_name: Optional[str] = None
    apple_refresh_token: Optional[str] = None
    last_login_ts: Optional[str] = None

class BettorProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None

class SyndicateCreate(BaseModel):
    bettor_id: int
    name: str
    description: Optional[str] = None
    fantasy: Optional[bool] = False
    password: Optional[str] = None
    max_runner: Optional[int] = None

class RunnerCreate(BaseModel):
    bettor_id: int
    password: Optional[str] = None

@app.post("/odd/bettor")
def create_bettor(bettor: BettorCreate):
    q = """
        INSERT INTO odd.bettor (apple_sub, apple_email, apple_name, apple_refresh_token)
        VALUES (:apple_sub, :apple_email, :apple_name, :apple_refresh_token)
        RETURNING *
    """
    with engine.begin() as conn:
        result = conn.execute(text(q), bettor.model_dump())
        row = result.fetchone()
        return dict(row._mapping)

@app.patch("/odd/bettor/{bettor_id}")
def update_bettor(bettor_id: int, bettor: BettorUpdate):
    updates = {k: v for k, v in bettor.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["bettor_id"] = bettor_id

    q = f"UPDATE odd.bettor SET {set_clause} WHERE bettor_id = :bettor_id RETURNING *"

    with engine.begin() as conn:
        result = conn.execute(text(q), updates)
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Bettor not found")
        return dict(row._mapping)

@app.patch("/odd/bettor/{bettor_id}/profile")
def set_bettor_profile(bettor_id: int, profile: BettorProfile):
    updates = {k: v for k, v in profile.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["bettor_id"] = bettor_id

    q = f"UPDATE odd.bettor SET {set_clause} WHERE bettor_id = :bettor_id RETURNING *"

    with engine.begin() as conn:
        result = conn.execute(text(q), updates)
        row = result.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Bettor not found")
        return dict(row._mapping)

@app.post("/odd/syndicate")
def create_syndicate(syndicate: SyndicateCreate):
    with engine.begin() as conn:
        syndicate_row = conn.execute(text("""
            INSERT INTO odd.syndicate (name, description, fantasy, password, max_runner, created_by_bettor_id)
            VALUES (:name, :description, :fantasy, :password, :max_runner, :bettor_id)
            RETURNING *
        """), syndicate.model_dump()).fetchone()

        runner_row = conn.execute(text("""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role)
            VALUES (:bettor_id, :syndicate_id, 'admin')
            RETURNING *
        """), {"bettor_id": syndicate.bettor_id, "syndicate_id": syndicate_row._mapping["syndicate_id"]}).fetchone()

    return {"syndicate": dict(syndicate_row._mapping), "runner": dict(runner_row._mapping)}

@app.post("/odd/syndicate/{syndicate_id}/join")
def join_syndicate(syndicate_id: int, body: RunnerCreate):
    with engine.begin() as conn:
        existing = conn.execute(text("""
            SELECT runner_id FROM odd.runner
            WHERE bettor_id = :bettor_id AND syndicate_id = :syndicate_id AND active = true
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate_id}).fetchone()

        if existing:
            raise HTTPException(status_code=409, detail="Bettor is already in this syndicate")

        syndicate = conn.execute(text("""
            SELECT syndicate_id, password, max_runner FROM odd.syndicate
            WHERE syndicate_id = :syndicate_id
        """), {"syndicate_id": syndicate_id}).fetchone()

        if syndicate is None:
            raise HTTPException(status_code=404, detail="Syndicate not found")

        if syndicate.password and syndicate.password != body.password:
            raise HTTPException(status_code=403, detail="Incorrect password")

        if syndicate.max_runner is not None:
            count = conn.execute(text("""
                SELECT COUNT(*) FROM odd.runner
                WHERE syndicate_id = :syndicate_id AND active = true
            """), {"syndicate_id": syndicate_id}).scalar()
            if count >= syndicate.max_runner:
                raise HTTPException(status_code=409, detail="Syndicate is full")

        runner_row = conn.execute(text("""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role)
            VALUES (:bettor_id, :syndicate_id, 'member')
            RETURNING *
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate_id}).fetchone()

    return dict(runner_row._mapping)
