from fastapi import APIRouter, HTTPException
from sqlalchemy import text

from db import engine
from schemas.bettor import BettorCreate, BettorProfile, BettorSignin

router = APIRouter(tags=["bettor"])


@router.post("/odd/bettor")
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

@router.post("/odd/bettor/signin")
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

@router.patch("/odd/bettor/{bettor_id}/profile")
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
