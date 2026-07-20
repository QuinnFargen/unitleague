# app/main.py
from fastapi import FastAPI, Query, HTTPException
from sqlalchemy import create_engine, text
from pydantic import BaseModel
from typing import Optional, List
import os
import json

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL", "")
# Railway/Neon supply postgres:// or postgresql:// — rewrite to use psycopg v3
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+psycopg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"sslmode": "require"},
    pool_size=5,
    max_overflow=2,
    pool_timeout=30,
    pool_recycle=300,
    pool_pre_ping=True,
)

@app.get("/mart/league")
def get_league():
    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM mart.league"))
        return [dict(row._mapping) for row in result]

@app.get("/mart/team")
def get_team(league_id: int = Query(None)
            ,team_id: int = Query(None)
            ,conf: str = Query(None)
            ,color: str = Query(None)
            ,region: str = Query(None)
            ,category: str = Query(None)):
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

@app.get("/mart/game_oddall")
def get_game_oddall(game_id: int = Query(...)):
    q = "SELECT * FROM mart.game_oddall WHERE game_id = :game_id"
    with engine.connect() as conn:
        result = conn.execute(text(q), {"game_id": game_id})
        return [dict(row._mapping) for row in result]

@app.get("/mart/txn")
def get_mart_txn(bettor_id: int = Query(None), syndicate_id: int = Query(None)):
    q = "SELECT * FROM mart.txn WHERE 1=1"
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

    q += " ORDER BY s.syndicate_id"

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@app.get("/mart/enhance_options")
def get_enhance_options(syndicate_id: int = Query(None),
                        bettor_id: int = Query(None)):
    q = "SELECT * FROM odd.v_enhance_options WHERE 1=1"
    query_params = {}

    if syndicate_id:
        q += " AND syndicate_id = :syndicate_id"
        query_params["syndicate_id"] = syndicate_id

    if bettor_id:
        q += " AND bettor_id = :bettor_id"
        query_params["bettor_id"] = bettor_id

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@app.get("/mart/runner")
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


class BettorCreate(BaseModel):
    apple_sub: str
    apple_email: Optional[str] = None
    apple_name: Optional[str] = None
    apple_refresh_token: Optional[str] = None

class BettorProfile(BaseModel):
    profile_name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
    apple_email: Optional[str] = None

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
    start_units: Optional[int] = None
    config: Optional[dict] = None

class RunnerCreate(BaseModel):
    bettor_id: int
    password: Optional[str] = None

class EnhancedCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    enhancement_id: int
    team_id: Optional[int] = 0
    level: Optional[int] = 1
    option_hash: str

class SyndicateStart(BaseModel):
    bettor_id: int

class SyndicateUpdate(BaseModel):
    name: Optional[str] = None
    symbol: Optional[str] = None
    color: Optional[str] = None
    config: Optional[dict] = None

class RunnerProfile(BaseModel):
    profile_name: Optional[str] = None
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
        RETURNING *, (xmax = 0) AS is_new
    """
    with engine.begin() as conn:
        result = conn.execute(text(q), bettor.model_dump())
        row = result.fetchone()
        mapping = dict(row._mapping)
        if mapping.pop("is_new"):
            conn.execute(text("""
                INSERT INTO odd.txn (bettor_id, syndicate_id, unit, price, txn_type_id, description)
                VALUES (:bettor_id, 0, 100, 1.0, 3, 'initial units')
            """), {"bettor_id": mapping["bettor_id"]})
        return mapping

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
            INSERT INTO odd.syndicate (name, description, is_public, password, max_runner, created_by_bettor_id, start_units, config)
            VALUES (:name, :description, :is_public, :password, :max_runner, :bettor_id, :start_units, :config)
            RETURNING *
        """), {**syndicate.model_dump(), "config": json.dumps(syndicate.config) if syndicate.config else None}).fetchone()

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

@app.get("/odd/enhancement")
def get_enhancement(enhancement_type: str = Query(None), active: bool = Query(True)):
    q = "SELECT * FROM odd.enhancement WHERE 1=1"
    query_params = {}

    if active is not None:
        q += " AND active = :active"
        query_params["active"] = active

    if enhancement_type:
        q += " AND enhancement_type = :enhancement_type"
        query_params["enhancement_type"] = enhancement_type

    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]

@app.get("/odd/enhanced")
def get_enhanced(bettor_id: int = Query(None), syndicate_id: int = Query(None)):
    q = "SELECT * FROM odd.v_enhanced WHERE 1=1"
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

@app.post("/odd/enhanced")
def choose_enhancement(body: EnhancedCreate):
    with engine.begin() as conn:
        option = conn.execute(text("""
            SELECT option_hash FROM odd.enhance_options
            WHERE bettor_id      = :bettor_id
              AND syndicate_id   = :syndicate_id
              AND enhancement_id = :enhancement_id
            LIMIT 1
        """), {
            "bettor_id":      body.bettor_id,
            "syndicate_id":   body.syndicate_id,
            "enhancement_id": body.enhancement_id,
        }).fetchone()

        if option is None:
            raise HTTPException(status_code=404, detail="Enhancement option not available or already chosen")
        if option.option_hash != body.option_hash:
            raise HTTPException(status_code=400, detail="Invalid option_hash")

        row = conn.execute(text("""
            INSERT INTO odd.enhanced (bettor_id, syndicate_id, enhancement_id, team_id, level, option_hash)
            VALUES (:bettor_id, :syndicate_id, :enhancement_id, :team_id, :level, :option_hash)
            RETURNING *
        """), body.model_dump()).fetchone()

    return dict(row._mapping)

@app.patch("/odd/syndicate/{syndicate_id}/start")
def start_syndicate(syndicate_id: int, body: SyndicateStart):
    with engine.begin() as conn:
        admin = conn.execute(text("""
            SELECT runner_id FROM odd.runner
            WHERE bettor_id = :bettor_id AND syndicate_id = :syndicate_id
              AND role = 'admin' AND active = true
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate_id}).fetchone()

        if admin is None:
            raise HTTPException(status_code=403, detail="Only an active admin can start the syndicate")

        syndicate = conn.execute(text("""
            SELECT syndicate_id, start_units, is_started FROM odd.syndicate
            WHERE syndicate_id = :syndicate_id
        """), {"syndicate_id": syndicate_id}).fetchone()

        if syndicate is None:
            raise HTTPException(status_code=404, detail="Syndicate not found")
        if syndicate.is_started:
            raise HTTPException(status_code=409, detail="Syndicate already started")

        start_units = syndicate.start_units or 0

        runners = conn.execute(text("""
            SELECT runner_id, bettor_id FROM odd.runner
            WHERE syndicate_id = :syndicate_id AND active = true
        """), {"syndicate_id": syndicate_id}).fetchall()

        if start_units > 0:
            shortfall = []
            for runner in runners:
                balance = conn.execute(text("""
                    SELECT COALESCE(SUM(unit), 0) AS balance FROM odd.txn
                    WHERE bettor_id = :bettor_id AND syndicate_id = 0 AND canceled = false
                """), {"bettor_id": runner.bettor_id}).scalar()
                if balance < start_units:
                    shortfall.append({"bettor_id": runner.bettor_id, "balance": balance})
            if shortfall:
                raise HTTPException(status_code=402, detail={"message": "Insufficient balance", "runners": shortfall})

        conn.execute(text("""
            UPDATE odd.syndicate SET is_started = true WHERE syndicate_id = :syndicate_id
        """), {"syndicate_id": syndicate_id})

        txn_pairs = []
        for runner in runners:
            debit = conn.execute(text("""
                INSERT INTO odd.txn (bettor_id, syndicate_id, unit, price, txn_type_id, description)
                VALUES (:bettor_id, 0, :unit, 1.0, 3, :description)
                RETURNING *
            """), {
                "bettor_id":   runner.bettor_id,
                "unit":        -start_units,
                "description": f"syndicate buy-in {syndicate_id}",
            }).fetchone()

            credit = conn.execute(text("""
                INSERT INTO odd.txn (bettor_id, syndicate_id, unit, price, txn_type_id, description)
                VALUES (:bettor_id, :syndicate_id, :unit, 1.0, 3, :description)
                RETURNING *
            """), {
                "bettor_id":    runner.bettor_id,
                "syndicate_id": syndicate_id,
                "unit":         start_units,
                "description":  "syndicate start units",
            }).fetchone()

            txn_pairs.append({"debit": dict(debit._mapping), "credit": dict(credit._mapping)})

        syndicate_row = conn.execute(text("""
            SELECT * FROM odd.syndicate WHERE syndicate_id = :syndicate_id
        """), {"syndicate_id": syndicate_id}).fetchone()

    return {"syndicate": dict(syndicate_row._mapping), "txns": txn_pairs}

@app.patch("/odd/syndicate/{syndicate_id}")
def update_syndicate(syndicate_id: int, body: SyndicateUpdate):
    updates = {k: v for k, v in body.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    if "config" in updates:
        updates["config"] = json.dumps(updates["config"])

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["syndicate_id"] = syndicate_id

    q = f"UPDATE odd.syndicate SET {set_clause} WHERE syndicate_id = :syndicate_id RETURNING *"

    with engine.begin() as conn:
        row = conn.execute(text(q), updates).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Syndicate not found")
        return dict(row._mapping)

@app.patch("/odd/runner/{runner_id}")
def set_runner_profile(runner_id: int, profile: RunnerProfile):
    updates = {k: v for k, v in profile.model_dump().items() if v is not None}
    if not updates:
        raise HTTPException(status_code=400, detail="No fields provided to update")

    set_clause = ", ".join(f"{k} = :{k}" for k in updates)
    updates["runner_id"] = runner_id

    q = f"UPDATE odd.runner SET {set_clause} WHERE runner_id = :runner_id RETURNING *"

    with engine.begin() as conn:
        row = conn.execute(text(q), updates).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Runner not found")
        return dict(row._mapping)


class TxnCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    bet_hash: str
    unit: float
    price: float
    game_dt: Optional[str] = None

class ParlayLeg(BaseModel):
    bet_hash: str
    price: float

class ParlayCreate(BaseModel):
    bettor_id: int
    syndicate_id: int
    unit: float
    legs: List[ParlayLeg]
    game_dt: Optional[str] = None


@app.post("/odd/txn")
def create_txn(txn: TxnCreate):
    q = """
        INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, game_dt, txn_type_id)
        VALUES (:bettor_id, :syndicate_id, :bet_hash, :unit, :price, :game_dt, 1)
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
            INSERT INTO odd.txn (bettor_id, syndicate_id, parlay_id, unit, price, game_dt, txn_type_id)
            VALUES (:bettor_id, :syndicate_id, :parlay_id, :unit, :price, :game_dt, 2)
            RETURNING *
        """), {
            "bettor_id": parlay.bettor_id,
            "syndicate_id": parlay.syndicate_id,
            "parlay_id": parlay_id,
            "unit": parlay.unit,
            "price": price_mult,
            "game_dt": parlay.game_dt,
        }).fetchone()

    return {
        "parlay": dict(parlay_row._mapping),
        "legs": leg_rows,
        "txn": dict(txn_row._mapping),
    }

@app.patch("/odd/txn/{txn_id}/cancel")
def cancel_txn(txn_id: int):
    q = """
        UPDATE odd.txn
        SET canceled = true, cancel_ts = now()
        WHERE txn_id = :txn_id AND canceled = false
        RETURNING *
    """
    with engine.begin() as conn:
        row = conn.execute(text(q), {"txn_id": txn_id}).fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail="Transaction not found or already canceled")
        return dict(row._mapping)

@app.get("/odd/txn")
def get_txn(bettor_id: int = Query(None), syndicate_id: int = Query(None)):
    q = "SELECT * FROM odd.v_txn WHERE game_dt >= CURRENT_DATE AND canceled = false"
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
