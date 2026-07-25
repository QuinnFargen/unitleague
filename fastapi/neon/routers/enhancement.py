from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text

from db import engine
from schemas.enhancement import EnhancedCreate

router = APIRouter(tags=["enhancement"])


@router.get("/odd/enhancement")
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

@router.get("/odd/enhanced")
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

@router.post("/odd/enhanced")
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
