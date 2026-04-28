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

@app.get("/mart/odd_best")
def get_odd_best(game_id: int = Query(None)
               , game_dt: str = Query(None)
               , has_active_bets: bool = Query(None)):
    q = "SELECT * FROM mart.odd_best WHERE 1=1"
    query_params = {}

    if game_id:
        q += " AND game_id = :game_id"
        query_params["game_id"] = game_id

    if game_dt:
        q += " AND game_dt = :game_dt"
        query_params["game_dt"] = game_dt

    if has_active_bets is not None:
        q += " AND has_active_bets = :has_active_bets"
        query_params["has_active_bets"] = has_active_bets

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

@app.post("/mart/bettor")
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

@app.patch("/mart/bettor/{bettor_id}")
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