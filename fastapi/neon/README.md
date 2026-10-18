# UnitLeague FastAPI — Neon

Connects to a remote [Neon](https://neon.tech) serverless PostgreSQL database.

## Setup

Create a `.env` file in this directory:

```
DATABASE_URL=postgresql+psycopg://user:password@ep-xxx-pooler.us-east-2.aws.neon.tech/dbname?sslmode=require
```

Every write route requires `Authorization: Bearer <session token>`. Session tokens are signed with a secret only the API knows; set it (and never commit it):

```
SESSION_SECRET=...   # python -c "import secrets; print(secrets.token_urlsafe(48))"
APPLE_CLIENT_ID=Quinn.unitleagueios   # bundle id Apple identity tokens must be issued for
```

Without `SESSION_SECRET`, sign-in and every write return 503. Rotating it signs everyone out.

For account deletion to revoke Sign in with Apple tokens, also set (values from the Apple Developer portal → Keys; never commit the `.p8`):

```
APPLE_TEAM_ID=XXXXXXXXXX
APPLE_KEY_ID=XXXXXXXXXX
APPLE_CLIENT_ID=Quinn.unitleagueios
APPLE_PRIVATE_KEY_PATH=/run/secrets/AuthKey_XXXXXXXXXX.p8   # or APPLE_PRIVATE_KEY="-----BEGIN PRIVATE KEY-----\n..."
```

Without them, deletion still works but `apple_token_revoked` is `false`.

Use the **pooler** endpoint from your Neon project dashboard (the `-pooler` variant) for best performance.

## Run

```bash
docker compose up --build
```

API available at `http://localhost:8000`.

## Endpoints

### Read (GET)

| Method | Path | Query Params | Description |
|--------|------|-------------|-------------|
| GET | `/mart/league` | — | All leagues with season status (`preseason`, `regular season`, `playoffs`, `offseason`) |
| GET | `/mart/team` | `league_id`, `team_id` | Teams, optionally filtered by league or team |
| GET | `/mart/game` | `game_dt`, `league_id` | Games with scores, winner, margin, and total |
| GET | `/mart/sched` | `team_id`, `league_id`, `yr` | Team schedule with scores, margin, and total |
| GET | `/mart/game_oddbest` | `game_id`, `game_dt`, `league_id` | Best active odds per game (ML, spread, over/under) with post-game win flags |
| GET | `/mart/runner` | `syndicate_id` | Runners (syndicate members) with profile info and balance |

### Auth

The app sends the `identityToken` from Sign in with Apple to `POST /odd/bettor`. The API verifies it against Apple's JWKS (`iss` = `https://appleid.apple.com`, `aud` = `APPLE_CLIENT_ID`, `exp`), keys the bettor on its `sub`, and returns the bettor with a `session_token` (HS256 JWT, 90 days). The app keeps it in the Keychain and sends it on every write; `POST /odd/bettor/signin` returns a fresh one on each launch.

Write routes resolve the caller from the token (`auth.CurrentBettor`). A `bettor_id` in the path or body must match it (403 otherwise), txns and runners can only be changed by their owner, and syndicate edits need an active admin. A missing, invalid, or expired token, or one for a deleted bettor, gets 401. GET routes stay public.

### Write (POST / PATCH) — all except `POST /odd/bettor` need a bearer token

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/odd/bettor` | `identity_token`, `apple_email?`, `apple_name?` | Sign in with Apple: create or update the bettor for the verified `sub`; returns the bettor plus `session_token` |
| POST | `/odd/bettor/signin` | — | Update last login timestamp; returns the bettor plus a refreshed `session_token` |
| PATCH | `/odd/bettor/{bettor_id}/profile` | `profile_name?`, `symbol?`, `color?` | Update bettor display profile |
| DELETE | `/odd/bettor/{bettor_id}` | `authorization_code?` | Delete the bettor and all their txns, parlays, runners, and edges; revoke their Apple token (code is a fresh Sign in with Apple `authorizationCode`, and its `sub` must match). Syndicates they admin pass to the longest-standing remaining member, or are deleted if no one else is active |
| POST | `/odd/syndicate` | `bettor_id`, `name`, `description?`, `is_public?`, `password?`, `max_runner?` | Create a syndicate; creator is added as admin runner |
| POST | `/odd/syndicate/join/{code}` | `bettor_id`, `password?` | Join a syndicate as a member; validates password and max capacity |
| PATCH | `/odd/syndicate/{syndicate_id}` | `name?`, `symbol?`, `color?` | Update syndicate display fields |
