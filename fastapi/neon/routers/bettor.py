import logging

from fastapi import APIRouter, HTTPException
from sqlalchemy import text

import apple
from auth import CurrentBettor, issue_session_token, require_configured, require_self, verify_apple_identity_token
from db import engine
from schemas.bettor import BettorCreate, BettorDelete, BettorProfile

log = logging.getLogger(__name__)

router = APIRouter(tags=["bettor"])


# Sign in with Apple. The bettor is keyed on the `sub` of the verified identity token,
# never on anything else the client sends. Returns the bettor plus a session_token.
@router.post("/odd/bettor")
def create_bettor(bettor: BettorCreate):
    require_configured()  # don't create the bettor if we can't hand back a session
    claims = verify_apple_identity_token(bettor.identity_token)
    params = {
        "apple_sub": claims["sub"],
        "apple_email": bettor.apple_email or claims.get("email"),
        "apple_name": bettor.apple_name,
    }
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
        result = conn.execute(text(q), params)
        row = result.fetchone()
        mapping = dict(row._mapping)
        if mapping.pop("is_new"):
            conn.execute(text("""
                INSERT INTO odd.txn (bettor_id, syndicate_id, unit, price, txn_type_id, description)
                VALUES (:bettor_id, 0, 100, 1.0, 3, 'initial units')
            """), {"bettor_id": mapping["bettor_id"]})
    mapping["session_token"] = issue_session_token(mapping["bettor_id"])
    return mapping

# Called on app launch with the stored session token: records the login and hands back
# a fresh token so active users never hit the expiry.
@router.post("/odd/bettor/signin")
def signin_bettor(current: CurrentBettor):
    q = "UPDATE odd.bettor SET last_login_ts = now() WHERE bettor_id = :bettor_id RETURNING *"
    with engine.begin() as conn:
        row = conn.execute(text(q), {"bettor_id": current}).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail="Bettor not found")
    return {**dict(row._mapping), "session_token": issue_session_token(current)}

@router.patch("/odd/bettor/{bettor_id}/profile")
def set_bettor_profile(bettor_id: int, profile: BettorProfile, current: CurrentBettor):
    require_self(bettor_id, current)
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

# Account deletion (App Store guideline 5.1.1(v)).
#
# Rows are hard-deleted, not anonymized: units are play money, so there is
# nothing to retain. Deleted: odd.txn, odd.parlay/odd.leg for those txns,
# odd.enhanced, odd.runner, odd.bettor. Derived dbt tables (mart.txn,
# mart.txn_enhanced, ...) only carry ids and drop the rows on the next build.
#
# Syndicates the bettor created or admins:
#   - other active runners remain -> admin is handed to the longest-standing
#     remaining runner (an existing admin if there is one) and
#     created_by_bettor_id moves to them, so the league keeps running;
#   - nobody else is active -> the syndicate and all its rows are deleted.
#
# The caller must be signed in as bettor_id. The Apple authorization_code is only
# used to revoke the Apple token (its sub must still match the bettor).
@router.delete("/odd/bettor/{bettor_id}")
def delete_bettor(bettor_id: int, current: CurrentBettor, body: BettorDelete | None = None):
    require_self(bettor_id, current)
    with engine.connect() as conn:
        bettor = conn.execute(text("SELECT bettor_id, apple_sub FROM odd.bettor WHERE bettor_id = :bettor_id"),
                              {"bettor_id": bettor_id}).fetchone()
    if bettor is None:
        raise HTTPException(status_code=404, detail="Bettor not found")

    apple_revoked = _revoke_apple(bettor.apple_sub, body.authorization_code if body else None)

    with engine.begin() as conn:
        syndicates = _release_syndicates(conn, bettor_id)
        deleted = _delete_bettor_rows(conn, bettor_id)

    return {"bettor_id": bettor_id, "apple_token_revoked": apple_revoked, "syndicates": syndicates, "deleted": deleted}


def _revoke_apple(apple_sub: str, authorization_code: str | None) -> bool:
    if not authorization_code:
        return False
    if not apple.is_configured():
        log.warning("Apple Sign in env vars not set; skipping token revoke")
        return False
    try:
        tokens = apple.exchange_code(authorization_code)
    except apple.AppleAuthError as e:
        log.warning("Apple code exchange failed: %s", e)
        return False
    if tokens.get("sub") and tokens["sub"] != apple_sub:
        raise HTTPException(status_code=403, detail="Apple account does not match bettor")
    try:
        if tokens.get("refresh_token"):
            apple.revoke(tokens["refresh_token"], "refresh_token")
        elif tokens.get("access_token"):
            apple.revoke(tokens["access_token"], "access_token")
        else:
            return False
    except apple.AppleAuthError as e:
        log.warning("Apple token revoke failed: %s", e)
        return False
    return True


def _release_syndicates(conn, bettor_id: int) -> list[dict]:
    rows = conn.execute(text("""
        SELECT s.syndicate_id FROM odd.syndicate s
        WHERE s.created_by_bettor_id = :bettor_id
           OR EXISTS (SELECT 1 FROM odd.runner r
                      WHERE r.syndicate_id = s.syndicate_id AND r.bettor_id = :bettor_id
                        AND r.role = 'admin' AND r.active = true)
    """), {"bettor_id": bettor_id}).fetchall()

    results = []
    for (syndicate_id,) in rows:
        params = {"bettor_id": bettor_id, "syndicate_id": syndicate_id}
        successor = conn.execute(text("""
            SELECT runner_id, bettor_id FROM odd.runner
            WHERE syndicate_id = :syndicate_id AND bettor_id <> :bettor_id AND active = true
            ORDER BY (role = 'admin') DESC, coalesce(start_ts, insert_ts), runner_id
            LIMIT 1
        """), params).fetchone()

        if successor:
            conn.execute(text("UPDATE odd.runner SET role = 'admin' WHERE runner_id = :runner_id"),
                         {"runner_id": successor.runner_id})
            conn.execute(text("""
                UPDATE odd.syndicate SET created_by_bettor_id = :new_bettor_id
                WHERE syndicate_id = :syndicate_id AND created_by_bettor_id = :bettor_id
            """), {**params, "new_bettor_id": successor.bettor_id})
            results.append({"syndicate_id": syndicate_id, "action": "transferred", "to_bettor_id": successor.bettor_id})
        else:
            parlay_ids = _parlay_ids(conn, "syndicate_id = :syndicate_id", params)
            conn.execute(text("DELETE FROM odd.txn WHERE syndicate_id = :syndicate_id"), params)
            _delete_orphan_parlays(conn, parlay_ids)
            conn.execute(text("DELETE FROM odd.enhanced WHERE syndicate_id = :syndicate_id"), params)
            conn.execute(text("DELETE FROM odd.runner WHERE syndicate_id = :syndicate_id"), params)
            conn.execute(text("DELETE FROM odd.syndicate WHERE syndicate_id = :syndicate_id"), params)
            results.append({"syndicate_id": syndicate_id, "action": "deleted"})
    return results


def _delete_bettor_rows(conn, bettor_id: int) -> dict:
    params = {"bettor_id": bettor_id}
    parlay_ids = _parlay_ids(conn, "bettor_id = :bettor_id", params)
    counts = {
        "txn": conn.execute(text("DELETE FROM odd.txn WHERE bettor_id = :bettor_id"), params).rowcount,
        "parlay": _delete_orphan_parlays(conn, parlay_ids),
        "enhanced": conn.execute(text("DELETE FROM odd.enhanced WHERE bettor_id = :bettor_id"), params).rowcount,
        "runner": conn.execute(text("DELETE FROM odd.runner WHERE bettor_id = :bettor_id"), params).rowcount,
        "bettor": conn.execute(text("DELETE FROM odd.bettor WHERE bettor_id = :bettor_id"), params).rowcount,
    }
    return counts


def _parlay_ids(conn, where: str, params: dict) -> list[int]:
    return list(conn.execute(text(f"SELECT DISTINCT parlay_id FROM odd.txn WHERE {where} AND parlay_id IS NOT NULL"),
                             params).scalars())


def _delete_orphan_parlays(conn, parlay_ids: list[int]) -> int:
    """Delete parlays (and legs) no longer referenced by any txn. Call after the txns are gone."""
    if not parlay_ids:
        return 0
    params = {"parlay_ids": parlay_ids}
    orphan = "parlay_id = ANY(:parlay_ids) AND NOT EXISTS (SELECT 1 FROM odd.txn t WHERE t.parlay_id = {alias}.parlay_id)"
    conn.execute(text(f"DELETE FROM odd.leg l WHERE {orphan.format(alias='l')}"), params)
    return conn.execute(text(f"DELETE FROM odd.parlay p WHERE {orphan.format(alias='p')}"), params).rowcount
