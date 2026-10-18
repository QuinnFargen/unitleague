# UnitLeague FastAPI — Neon

Connects to a remote [Neon](https://neon.tech) serverless PostgreSQL database.

## Setup

Create a `.env` file in this directory:

```
DATABASE_URL=postgresql+psycopg://user:password@ep-xxx-pooler.us-east-2.aws.neon.tech/dbname?sslmode=require
```

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

### Write (POST / PATCH)

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/odd/bettor` | `apple_sub`, `apple_email?`, `apple_name?`, `apple_refresh_token?` | Create a new bettor via Apple Sign-In |
| POST | `/odd/bettor/signin` | `bettor_id?`, `apple_sub?` | Update last login timestamp |
| PATCH | `/odd/bettor/{bettor_id}/profile` | `profile_name?`, `symbol?`, `color?` | Update bettor display profile |
| DELETE | `/odd/bettor/{bettor_id}` | `authorization_code?` | Delete the bettor and all their txns, parlays, runners, and edges; revoke their Apple token (code is a fresh Sign in with Apple `authorizationCode`, and its `sub` must match). Syndicates they admin pass to the longest-standing remaining member, or are deleted if no one else is active |
| POST | `/odd/syndicate` | `bettor_id`, `name`, `description?`, `is_public?`, `password?`, `max_runner?` | Create a syndicate; creator is added as admin runner |
| POST | `/odd/syndicate/join/{code}` | `bettor_id`, `password?` | Join a syndicate as a member; validates password and max capacity |
| PATCH | `/odd/syndicate/{syndicate_id}` | `name?`, `symbol?`, `color?` | Update syndicate display fields |
