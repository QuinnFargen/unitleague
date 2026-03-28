# app/main.py
from fastapi import FastAPI, Query
from sqlalchemy import create_engine, text
import os

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")

engine = create_engine(DATABASE_URL)

@app.get("/ball/league")
def get_league():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT *FROM ball.league"))
        return [dict(row._mapping) for row in result]
    
@app.get("/ball/team")
def get_team(league_id: int = Query(None)
             ,team_id: int = Query(None)):
    # Build base query
    q = "SELECT * FROM ball.team where 1=1 "
    query_params = {}

    if team_id:
        q += " AND team_id = :team_id"
        query_params["team_id"] = team_id

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    query = text(q)

    with engine.connect() as conn:
        result = conn.execute(query, query_params)
        return [dict(row._mapping) for row in result]
    

@app.get("/ball/game")
def get_game(game_dt: str = Query(None)
            , league_id: int = Query(None)):

    # Build base query
    q = "SELECT * FROM ball.v_game WHERE 1=1"
    query_params = {}

    if game_dt:
        q += " AND game_dt = :game_dt"
        query_params["game_dt"] = game_dt

    if league_id:
        q += " AND league_id = :league_id"
        query_params["league_id"] = league_id

    query = text(q)

    with engine.connect() as conn:
        result = conn.execute(query, query_params)
        return [dict(row._mapping) for row in result]
    
@app.get("/ball/sched")
def get_sched(team_id: str = Query(None)
            , league_id: int = Query(None)
            , yr: int = Query(None)):

    # Build base query
    q = "SELECT * FROM ball.v_sched WHERE 1=1"
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

    query = text(q)

    with engine.connect() as conn:
        result = conn.execute(query, query_params)
        return [dict(row._mapping) for row in result]