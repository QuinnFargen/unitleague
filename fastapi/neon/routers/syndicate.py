import json

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from db import engine
from schemas.syndicate import (
    SyndicateCreate,
    RunnerCreate,
    SyndicateStart,
    SyndicateUpdate,
    RunnerProfile,
    RunnerOrder,
)

router = APIRouter(tags=["syndicate"])

# Season year for a league, via mart.league.current_season_id.
SEASON_YR_SQL = """
    (SELECT se.yr FROM mart.league l
     JOIN ball.season se ON se.season_id = l.current_season_id
     WHERE l.league_id = :league_id)
"""

# New syndicates land at the bottom of the bettor's Syndicate tab.
NEXT_SORT_ORDER_SQL = """
    coalesce((SELECT max(sort_order) FROM odd.runner
              WHERE bettor_id = :bettor_id AND active = true), 0) + 1
"""


@router.post("/odd/syndicate")
def create_syndicate(syndicate: SyndicateCreate):
    with engine.begin() as conn:
        syndicate_row = conn.execute(text(f"""
            INSERT INTO odd.syndicate (name, description, is_public, password, max_runner, created_by_bettor_id, start_units, config, syndicate_type, league_id, yr, symbol, color)
            VALUES (:name, :description, :is_public, :password, :max_runner, :bettor_id, :start_units, :config, :syndicate_type, :league_id, {SEASON_YR_SQL}, :symbol, :color)
            RETURNING *
        """), {**syndicate.model_dump(), "config": json.dumps(syndicate.config) if syndicate.config else None}).fetchone()

        runner_row = conn.execute(text(f"""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role, sort_order)
            VALUES (:bettor_id, :syndicate_id, 'admin', {NEXT_SORT_ORDER_SQL})
            RETURNING *
        """), {"bettor_id": syndicate.bettor_id, "syndicate_id": syndicate_row._mapping["syndicate_id"]}).fetchone()

    return {"syndicate": dict(syndicate_row._mapping), "runner": dict(runner_row._mapping)}

@router.post("/odd/syndicate/join/{code}")
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

        runner_row = conn.execute(text(f"""
            INSERT INTO odd.runner (bettor_id, syndicate_id, role, sort_order)
            VALUES (:bettor_id, :syndicate_id, 'member', {NEXT_SORT_ORDER_SQL})
            RETURNING *
        """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate.syndicate_id}).fetchone()

    return dict(runner_row._mapping)

@router.patch("/odd/syndicate/{syndicate_id}/start")
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

@router.patch("/odd/syndicate/{syndicate_id}")
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

@router.put("/odd/runner/order")
def set_runner_order(body: RunnerOrder):
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE odd.runner SET sort_order = NULL
            WHERE bettor_id = :bettor_id AND active = true
        """), {"bettor_id": body.bettor_id})

        for index, syndicate_id in enumerate(body.syndicate_ids):
            conn.execute(text("""
                UPDATE odd.runner SET sort_order = :sort_order
                WHERE bettor_id = :bettor_id AND syndicate_id = :syndicate_id AND active = true
            """), {"bettor_id": body.bettor_id, "syndicate_id": syndicate_id, "sort_order": index + 1})

        rows = conn.execute(text("""
            SELECT * FROM odd.runner
            WHERE bettor_id = :bettor_id AND active = true
            ORDER BY sort_order NULLS LAST, syndicate_id
        """), {"bettor_id": body.bettor_id}).fetchall()

    return [dict(row._mapping) for row in rows]

@router.patch("/odd/runner/{runner_id}")
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
