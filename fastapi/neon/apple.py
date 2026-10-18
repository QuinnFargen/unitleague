"""Sign in with Apple server-side helpers: client-secret JWT, code exchange, token revoke.

Required env vars (never commit them):
    APPLE_TEAM_ID       10-char Apple Developer Team ID
    APPLE_KEY_ID        Key ID of the Sign in with Apple .p8 key
    APPLE_CLIENT_ID     App bundle ID (e.g. Quinn.unitleagueios)
    APPLE_PRIVATE_KEY   Contents of the .p8 file (literal "\\n" escapes are accepted)
      or APPLE_PRIVATE_KEY_PATH  Path to the .p8 file
"""

import json
import logging
import os
import time
import urllib.error
import urllib.parse
import urllib.request

import jwt

log = logging.getLogger(__name__)

APPLE_ISSUER = "https://appleid.apple.com"
TOKEN_URL = f"{APPLE_ISSUER}/auth/token"
REVOKE_URL = f"{APPLE_ISSUER}/auth/revoke"


class AppleAuthError(Exception):
    pass


def _private_key() -> str | None:
    key = os.getenv("APPLE_PRIVATE_KEY")
    if key:
        return key.replace("\\n", "\n")
    path = os.getenv("APPLE_PRIVATE_KEY_PATH")
    if path and os.path.exists(path):
        with open(path) as f:
            return f.read()
    return None


def is_configured() -> bool:
    return all(os.getenv(k) for k in ("APPLE_TEAM_ID", "APPLE_KEY_ID", "APPLE_CLIENT_ID")) and _private_key() is not None


def client_secret() -> str:
    now = int(time.time())
    return jwt.encode(
        {
            "iss": os.environ["APPLE_TEAM_ID"],
            "iat": now,
            "exp": now + 300,
            "aud": APPLE_ISSUER,
            "sub": os.environ["APPLE_CLIENT_ID"],
        },
        _private_key(),
        algorithm="ES256",
        headers={"kid": os.environ["APPLE_KEY_ID"]},
    )


def _post(url: str, data: dict) -> dict:
    body = urllib.parse.urlencode(data).encode()
    req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/x-www-form-urlencoded"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as e:
        raise AppleAuthError(f"{url} -> {e.code}: {e.read().decode(errors='replace')}") from e
    except urllib.error.URLError as e:
        raise AppleAuthError(f"{url} -> {e.reason}") from e


def exchange_code(authorization_code: str) -> dict:
    """Exchange a fresh authorizationCode from the app for Apple tokens.

    Returns Apple's token response plus `sub` decoded from its id_token. The
    id_token comes straight from Apple over TLS, so its signature isn't re-checked.
    """
    tokens = _post(TOKEN_URL, {
        "client_id": os.environ["APPLE_CLIENT_ID"],
        "client_secret": client_secret(),
        "code": authorization_code,
        "grant_type": "authorization_code",
    })
    id_token = tokens.get("id_token")
    if id_token:
        tokens["sub"] = jwt.decode(id_token, options={"verify_signature": False}).get("sub")
    return tokens


def revoke(token: str, token_type_hint: str = "refresh_token") -> None:
    _post(REVOKE_URL, {
        "client_id": os.environ["APPLE_CLIENT_ID"],
        "client_secret": client_secret(),
        "token": token,
        "token_type_hint": token_type_hint,
    })
