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

@router.get("/odd/enhance_options")
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
            SELECT option_hash, enhancement_type FROM odd.enhance_options
            WHERE bettor_id      = :bettor_id
              AND syndicate_id   = :syndicate_id
              AND runner_id      = :runner_id
              AND enhancement_id = :enhancement_id
            LIMIT 1
        """), {
            "bettor_id":      body.bettor_id,
            "syndicate_id":   body.syndicate_id,
            "runner_id":      body.runner_id,
            "enhancement_id": body.enhancement_id,
        }).fetchone()

        if option is None:
            raise HTTPException(status_code=404, detail="Enhancement option not available or already chosen")
        if option.option_hash != body.option_hash:
            raise HTTPException(status_code=400, detail="Invalid option_hash")

        if option.enhancement_type == "edge":
            syndicate = conn.execute(text("""
                SELECT config::jsonb ->> 'max_active_edges' AS max_active_edges
                FROM odd.syndicate WHERE syndicate_id = :syndicate_id
            """), {"syndicate_id": body.syndicate_id}).fetchone()

            max_active_edges = (
                int(syndicate.max_active_edges)
                if syndicate and syndicate.max_active_edges is not None
                else None
            )

            if max_active_edges is not None:
                active_edge_count = conn.execute(text("""
                    SELECT COUNT(*) FROM odd.enhanced ed
                    JOIN odd.enhancement en ON en.enhancement_id = ed.enhancement_id
                    WHERE ed.runner_id = :runner_id
                      AND ed.active    = true
                      AND en.enhancement_type = 'edge'
                """), {"runner_id": body.runner_id}).scalar()

                if active_edge_count >= max_active_edges:
                    if body.sell_enhanced_id is None:
                        raise HTTPException(status_code=409, detail="Edge limit reached; choose an edge to sell")

                    sellable = conn.execute(text("""
                        SELECT 1 FROM odd.enhanced ed
                        JOIN odd.enhancement en ON en.enhancement_id = ed.enhancement_id
                        WHERE ed.enhanced_id = :sell_enhanced_id
                          AND ed.runner_id   = :runner_id
                          AND ed.active      = true
                          AND en.enhancement_type = 'edge'
                    """), {
                        "sell_enhanced_id": body.sell_enhanced_id,
                        "runner_id":        body.runner_id,
                    }).fetchone()

                    if sellable is None:
                        raise HTTPException(status_code=400, detail="Invalid edge to sell")

                    conn.execute(text("""
                        UPDATE odd.enhanced SET active = false, end_dt = now()
                        WHERE enhanced_id = :sell_enhanced_id
                    """), {"sell_enhanced_id": body.sell_enhanced_id})

        row = conn.execute(text("""
            INSERT INTO odd.enhanced (bettor_id, syndicate_id, runner_id, enhancement_id, team_id, level, option_hash)
            VALUES (:bettor_id, :syndicate_id, :runner_id, :enhancement_id, :team_id, :level, :option_hash)
            RETURNING *
        """), body.model_dump(exclude={"sell_enhanced_id"})).fetchone()

    return dict(row._mapping)

@router.get("/odd/enhanced/runner/{runner_id}")
def get_runner_enhanced(runner_id: int, enhancement_type: str = Query(None)):
    q = """
        SELECT ed.enhanced_id, ed.runner_id, ed.bettor_id, ed.syndicate_id,
               ed.enhancement_id, ed.team_id, ed.level,
               en.enhancement_type, en.edge_type, en.name, en.symbol
        FROM odd.enhanced ed
        JOIN odd.enhancement en ON en.enhancement_id = ed.enhancement_id
        WHERE ed.runner_id = :runner_id AND ed.active = true
    """
    query_params = {"runner_id": runner_id}
    if enhancement_type:
        q += " AND en.enhancement_type = :enhancement_type"
        query_params["enhancement_type"] = enhancement_type
    q += " ORDER BY ed.insert_ts"
    with engine.connect() as conn:
        result = conn.execute(text(q), query_params)
        return [dict(row._mapping) for row in result]
