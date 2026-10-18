"""Request authentication: Sign in with Apple identity tokens in, API session tokens out.

Flow:
    1. The app sends the identityToken from Sign in with Apple to POST /odd/bettor.
    2. verify_apple_identity_token() checks it against Apple's JWKS (signature, iss,
       aud = bundle id, exp) and the bettor is keyed on its `sub`.
    3. The API returns its own session token (HS256 JWT, sub = bettor_id). The app sends
       it as `Authorization: Bearer <token>`, and write routes depend on CurrentBettor.

Env vars:
    SESSION_SECRET    Required. Random secret for signing session tokens, e.g.
                      `python -c "import secrets; print(secrets.token_urlsafe(48))"`.
                      Rotating it signs every user out.
    APPLE_CLIENT_ID   Bundle id the identity token must be issued for (default Quinn.unitleagueios).
"""

import os
import time
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import text

from db import engine

APPLE_ISSUER = "https://appleid.apple.com"
APPLE_JWKS_URL = f"{APPLE_ISSUER}/auth/keys"
DEFAULT_BUNDLE_ID = "Quinn.unitleagueios"

SESSION_ISSUER = "unitleague-api"
SESSION_TTL_SECONDS = 90 * 24 * 3600  # refreshed on every app launch via /odd/bettor/signin

_jwks_client = jwt.PyJWKClient(APPLE_JWKS_URL, cache_keys=True, lifespan=24 * 3600)
_bearer = HTTPBearer(auto_error=False)


def _unauthorized(detail: str) -> HTTPException:
    return HTTPException(status_code=401, detail=detail, headers={"WWW-Authenticate": "Bearer"})


def _session_secret() -> str:
    secret = os.getenv("SESSION_SECRET")
    if not secret:
        # Fail closed: without a secret no token can be issued or trusted.
        raise HTTPException(status_code=503, detail="Server auth is not configured")
    return secret


def verify_apple_identity_token(identity_token: str) -> dict:
    """Return the verified claims of a Sign in with Apple identityToken, or raise 401."""
    try:
        signing_key = _jwks_client.get_signing_key_from_jwt(identity_token)
        return jwt.decode(
            identity_token,
            signing_key.key,
            algorithms=["RS256"],
            audience=os.getenv("APPLE_CLIENT_ID", DEFAULT_BUNDLE_ID),
            issuer=APPLE_ISSUER,
            options={"require": ["sub", "iss", "aud", "exp"]},
        )
    except jwt.PyJWKClientError as e:
        raise HTTPException(status_code=503, detail=f"Could not fetch Apple signing keys: {e}") from e
    except jwt.InvalidTokenError as e:
        raise _unauthorized(f"Invalid Apple identity token: {e}") from e


def issue_session_token(bettor_id: int) -> str:
    now = int(time.time())
    return jwt.encode(
        {"iss": SESSION_ISSUER, "sub": str(bettor_id), "iat": now, "exp": now + SESSION_TTL_SECONDS},
        _session_secret(),
        algorithm="HS256",
    )


def current_bettor_id(creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]) -> int:
    """Resolve the bettor from `Authorization: Bearer <session token>`; 401 if missing or invalid."""
    if creds is None:
        raise _unauthorized("Missing bearer token")
    try:
        claims = jwt.decode(
            creds.credentials,
            _session_secret(),
            algorithms=["HS256"],
            issuer=SESSION_ISSUER,
            options={"require": ["sub", "exp"]},
        )
        bettor_id = int(claims["sub"])
    except (jwt.InvalidTokenError, ValueError) as e:
        raise _unauthorized("Invalid or expired session token") from e

    # Tokens outlive deleted accounts, so confirm the bettor still exists.
    with engine.connect() as conn:
        exists = conn.execute(text("SELECT 1 FROM odd.bettor WHERE bettor_id = :bettor_id"),
                              {"bettor_id": bettor_id}).fetchone()
    if exists is None:
        raise _unauthorized("Bettor no longer exists")
    return bettor_id


CurrentBettor = Annotated[int, Depends(current_bettor_id)]


def optional_bettor_id(creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)]) -> int | None:
    """For public reads that show more to the signed-in bettor: the bettor_id, or None when
    there is no valid token. Never raises, so reads keep working signed out."""
    try:
        return current_bettor_id(creds)
    except HTTPException:
        return None


ViewerBettor = Annotated[int | None, Depends(optional_bettor_id)]


def require_self(bettor_id: int, current: int) -> None:
    """403 unless the bettor_id named in the path or body is the signed-in bettor."""
    if bettor_id != current:
        raise HTTPException(status_code=403, detail="bettor_id does not match the signed-in bettor")
