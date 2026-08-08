from fastapi import APIRouter, Query, HTTPException
from sqlalchemy import text

from db import engine
from schemas.txn import TxnCreate, ParlayLeg, ParlayCreate

router = APIRouter(tags=["txn"])


@router.post("/odd/txn")
def create_txn(txn: TxnCreate):
    q = """
        INSERT INTO odd.txn (bettor_id, syndicate_id, bet_hash, unit, price, unit_enhanced, price_enhanced, game_dt, txn_type_id)
        VALUES (:bettor_id, :syndicate_id, :bet_hash, :unit, :price, :unit_enhanced, :price_enhanced, :game_dt, 1)
        RETURNING *
    """
    with engine.begin() as conn:
        row = conn.execute(text(q), txn.model_dump()).fetchone()
        return dict(row._mapping)

@router.post("/odd/parlay")
def create_parlay(parlay: ParlayCreate):
    if not parlay.legs:
        raise HTTPException(status_code=400, detail="At least one leg required")

    price_mult = 1.0
    for leg in parlay.legs:
        price_mult *= leg.price

    with engine.begin() as conn:
        parlay_row = conn.execute(text("""
            INSERT INTO odd.parlay (price_mult, unit_enhanced)
            VALUES (:price_mult, :unit_enhanced)
            RETURNING *
        """), {"price_mult": price_mult, "unit_enhanced": parlay.unit_enhanced}).fetchone()

        parlay_id = parlay_row._mapping["parlay_id"]

        leg_rows = []
        for leg in parlay.legs:
            leg_row = conn.execute(text("""
                INSERT INTO odd.leg (parlay_id, bet_hash, price, price_enhanced)
                VALUES (:parlay_id, :bet_hash, :price, :price_enhanced)
                RETURNING *
            """), {
                "parlay_id": parlay_id,
                "bet_hash": leg.bet_hash,
                "price": leg.price,
                "price_enhanced": leg.price_enhanced,
            }).fetchone()
            leg_rows.append(dict(leg_row._mapping))

        txn_row = conn.execute(text("""
            INSERT INTO odd.txn (bettor_id, syndicate_id, parlay_id, unit, price, unit_enhanced, game_dt, txn_type_id)
            VALUES (:bettor_id, :syndicate_id, :parlay_id, :unit, :price, :unit_enhanced, :game_dt, 2)
            RETURNING *
        """), {
            "bettor_id": parlay.bettor_id,
            "syndicate_id": parlay.syndicate_id,
            "parlay_id": parlay_id,
            "unit": parlay.unit,
            "price": price_mult,
            "unit_enhanced": parlay.unit_enhanced,
            "game_dt": parlay.game_dt,
        }).fetchone()

    return {
        "parlay": dict(parlay_row._mapping),
        "legs": leg_rows,
        "txn": dict(txn_row._mapping),
    }

@router.patch("/odd/txn/{txn_id}/cancel")
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

@router.get("/odd/txn")
def get_txn(bettor_id: int = Query(None), syndicate_id: int = Query(None), game_id: int = Query(None)):
    q = "SELECT * FROM odd.v_txn WHERE game_dt >= CURRENT_DATE AND canceled = false"
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
