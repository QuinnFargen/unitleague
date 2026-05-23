# app/main.py
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional, List
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

@app.get("/mart/syndicate")
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

class BettorProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None

class BettorSignin(BaseModel):
    bettor_id: Optional[int] = None
    apple_sub: Optional[str] = None

class SyndicateCreate(BaseModel):
    bettor_id: int
    name: str
    description: Optional[str] = None
    is_public: Optional[bool] = False
    password: Optional[str] = None
    max_runner: Optional[int] = None

class RunnerCreate(BaseModel):
    bettor_id: int
    password: Optional[str] = None
    
class SyndicateUpdate(BaseModel):
    name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None

    
@app.post("/odd/bettor")
def create_bettor(bettor: BettorCreate):
    q = """
        INSERT INTO odd.bettor (apple_sub, apple_email, apple_name)
        VALUES (:apple_sub, :apple_email, :apple_name)
        ON CONFLICT (apple_sub) DO UPDATE
            SET apple_email = COALESCE(EXCLUDED.apple_email, odd.bettor.apple_email),
                apple_name  = COALESCE(EXCLUDED.apple_name,  odd.bettor.apple_name),
                last_login_ts = now()
        RETURNING *
    """
    with engine.begin() as conn:
        result = conn.execute(text(q), bettor.model_dump())
        row = result.fetchone()
        return dict(row._mapping)

@app.post("/odd/bettor/signin")
def signin_bettor(signin: BettorSignin):
    if signin.bettor_id is None and signin.apple_sub is None:
        raise HTTPException(status_code=400, detail="bettor_id or apple_sub required")

    if signin.bettor_id is not None:
        where = "bettor_id = :bettor_id"
        params = {"bettor_id": signin.bettor_id}
    else:
        where = "apple_sub = :apple_sub"
        params = {"apple_sub": signin.apple_sub}

    q = f"UPDATE odd.bettor SET last_login_ts = now() WHERE {where} RETURNING *"

    with engine.begin() as conn:
        row = conn.execute(text(q), params).fetchone()
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
            INSERT INTO odd.syndicate (name, description, is_public, password, max_runner, created_by_bettor_id)
            VALUES (:name, :description, :is_public, :password, :max_runner, :bettor_id)
            RETURNING *
        """), syndicate.model_dump()).fetchone()

        runner_row = conn.execute(text("""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role)
            VALUES (:bettor_id, :syndicate_id, 'admin')
            RETURNING *
        """), {"bettor_id": syndicate.bettor_id, "syndicate_id": syndicate_row._mapping["syndicate_id"]}).fetchone()

    return {"syndicate": dict(syndicate_row._mapping), "runner": dict(runner_row._mapping)}

@app.post("/odd/syndicate/join/{code}")
def join_syndicate(code: str, body: RunnerCreate):
    with engine.begin() as conn:
        syndicate = conn.execute(text("""
            SELECT syndicate_id, password, max_runner FROM odd.syndicate
            WHERE code = :code
        """), {"code": code}).fetchone()

        if syndicate is None:
            raise HTTPException(status_code=404, detail="Syndicate not found")

        existing = conn.execute(text("""
            SELECT runner_id FROM odd.runner
            WHERE bettor_id = :bettor_id AND syndicate_id = :syndicate_id AND active = true
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate.syndicate_id}).fetchone()

        if existing:
            raise HTTPException(status_code=409, detail="Bettor is already in this syndicate")

        if syndicate.password and syndicate.password != body.password:
            raise HTTPException(status_code=403, detail="Incorrect password")

        if syndicate.max_runner is not None:
            count = conn.execute(text("""
                SELECT COUNT(*) FROM odd.runner
                WHERE syndicate_id = :syndicate_id AND active = true
            """), {"syndicate_id": syndicate.syndicate_id}).scalar()
            if count >= syndicate.max_runner:
                raise HTTPException(status_code=409, detail="Syndicate is full")

        runner_row = conn.execute(text("""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role)
            VALUES (:bettor_id, :syndicate_id, 'member')
            RETURNING *
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate.syndicate_id}).fetchone()

    return dict(runner_row._mapping)

@app.patch("/odd/syndicate/{syndicate_id}")
def update_syndicate(syndicate_id: int, body: SyndicateUpdate):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["syndicate_id"] = syndicate_id

    q = f"UPDATE odd.syndicate SET {set_clause} WHERE syndicate_id = :syndicate_id RETURNING *"

    with engine.begin() as conn:
        row = conn.execute(text(q), updates).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Syndicate not found")
        return dict(row._mapping)


class TxnCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    bet_hash: str
    unit: float
    price: float

class ParlayLeg(BaseModel):
    bet_hash: str
    price: float

class ParlayCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    unit: float
    legs: List[ParlayLeg]


@app.post("/odd/txn")
def create_txn(txn: TxnCreate):
    q = """
        INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price)
        VALUES (:bettor_id, :syndicate_id, :bet_hash, :unit, :price)
        RETURNING *
    """
    with engine.begin() as conn:
        row = conn.execute(text(q), txn.model_dump()).fetchone()
        return dict(row._mapping)

@app.post("/odd/parlay")
def create_parlay(parlay: ParlayCreate):
    if not parlay.legs:
        raise HTTPException(status_code=400, detail="At least one leg required")

    price_mult = 1.0
    for leg in parlay.legs:
        price_mult *= leg.price

    with engine.begin() as conn:
        parlay_row = conn.execute(text("""
            INSERT INTO odd.parlay (price_mult)
            VALUES (:price_mult)
            RETURNING *
        """), {"price_mult": price_mult}).fetchone()

        parlay_id = parlay_row._mapping["parlay_id"]

        leg_rows = []
        for leg in parlay.legs:
            leg_row = conn.execute(text("""
                INSERT INTO odd.leg (parlay_id, bet_hash, price)
                VALUES (:parlay_id, :bet_hash, :price)
                RETURNING *
            """), {"parlay_id": parlay_id, "bet_hash": leg.bet_hash, "price": leg.price}).fetchone()
            leg_rows.append(dict(leg_row._mapping))

        txn_row = conn.execute(text("""
            INSERT INTO odd.txn (bettor_id, syndicate_id, parlay_id, unit, price)
            VALUES (:bettor_id, :syndicate_id, :parlay_id, :unit, :price)
            RETURNING *
        """), {
            "bettor_id": parlay.bettor_id,
            "syndicate_id": parlay.syndicate_id,
            "parlay_id": parlay_id,
            "unit": parlay.unit,
            "price": price_mult,
        }).fetchone()

    return {
        "parlay": dict(parlay_row._mapping),
        "legs": leg_rows,
        "txn": dict(txn_row._mapping),
    }

@app.get("/odd/txn")
def get_txn(bettor_id: int = Query(None), syndicate_id: int = Query(None)):
    q = "SELECT * FROM odd.txn WHERE won IS NULL AND canceled = false"
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