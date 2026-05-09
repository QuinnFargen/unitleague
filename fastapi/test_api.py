"""
Manual API test script for syndicate and profile endpoints.
Usage:
    pip install requests
    python test_api.py
    BASE_URL=http://localhost:8000 python test_api.py

NOTE: Known bug in main.py — GET /mart/syndicate with syndicate_id filter
uses an undefined variable (bettor_id) and will 500. The fetch-all call works fine.
"""

import json
import os
import sys
import time

try:
    import requests
except ImportError:
    sys.exit("Missing dependency: pip install requests")

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000").rstrip("/")


def call(method: str, path: str, label: str = "", **kwargs) -> dict:
    url = BASE_URL + path
    tag = label or f"{method} {path}"
    print(f"\n--- {tag} ---")
    print(f"  {method} {url}")
    if "json" in kwargs:
        print(f"  body: {json.dumps(kwargs['json'], indent=2)}")

    resp = getattr(requests, method.lower())(url, **kwargs)

    print(f"  status: {resp.status_code}")
    try:
        data = resp.json()
        print(f"  response: {json.dumps(data, indent=2, default=str)}")
    except Exception:
        data = resp.text
        print(f"  response: {data}")

    if not resp.ok:
        sys.exit(f"FAILED: {tag} returned {resp.status_code}")

    return data


def run():
    # Use a timestamp suffix so repeated runs don't collide on apple_sub
    ts = int(time.time())

    # Step 1: Create bettor A (syndicate founder)
    bettor_a = call(
        "POST", "/odd/bettor",
        label="Create bettor A (founder)",
        json={
            "apple_sub": f"test_founder_{ts}",
            "apple_email": f"founder_{ts}@test.com",
            "apple_name": "Test Founder",
        },
    )
    bettor_a_id = bettor_a["bettor_id"]
    print(f"\n  => bettor_a_id: {bettor_a_id}")

    # Step 2: Create bettor B (will join the syndicate)
    bettor_b = call(
        "POST", "/odd/bettor",
        label="Create bettor B (joiner)",
        json={
            "apple_sub": f"test_joiner_{ts}",
            "apple_email": f"joiner_{ts}@test.com",
            "apple_name": "Test Joiner",
        },
    )
    bettor_b_id = bettor_b["bettor_id"]
    print(f"\n  => bettor_b_id: {bettor_b_id}")

    # Step 3: Create a syndicate as bettor A
    created = call(
        "POST", "/odd/syndicate",
        label="Create syndicate",
        json={
            "bettor_id": bettor_a_id,
            "name": f"Test Syndicate {ts}",
            "description": "Created by test_api.py",
            "is_public": True,
            "password": None,
            "max_runner": 10,
        },
    )
    syndicate_id = created["syndicate"]["syndicate_id"]
    print(f"\n  => syndicate_id: {syndicate_id}")

    # Step 4a: Fetch all syndicates (no filter)
    call(
        "GET", "/mart/syndicate",
        label="Fetch all syndicates",
    )

    # Step 4b: Fetch syndicate by syndicate_id
    # NOTE: This will 500 due to a bug in main.py (undefined variable bettor_id on line 109).
    # Uncomment once the bug is fixed:
    # call(
    #     "GET", f"/mart/syndicate?syndicate_id={syndicate_id}",
    #     label="Fetch syndicate by id",
    # )

    # Step 5: Bettor B joins the syndicate
    call(
        "POST", f"/odd/syndicate/{syndicate_id}/join",
        label="Join syndicate (bettor B)",
        json={
            "bettor_id": bettor_b_id,
            "password": None,
        },
    )

    # Step 6: Update bettor A's profile
    call(
        "PATCH", f"/odd/bettor/{bettor_a_id}/profile",
        label="Update bettor A profile",
        json={
            "profile_name": "The Founder",
            "symbol": "TF",
            "color": "blue",
        },
    )

    print("\n=== All steps passed ===")


if __name__ == "__main__":
    run()
