# UnitLeague FastAPI

## Endpoints

### Read (GET)

| Method | Path | Query Params | Description |
|--------|------|-------------|-------------|
| GET | `/mart/league` | — | All leagues with season status (`preseason`, `regular season`, `playoffs`, `offseason`) |
| GET | `/mart/team` | `league_id`, `team_id` | Teams, optionally filtered by league or team |
| GET | `/mart/game` | `game_dt`, `league_id` | Games with scores, winner, margin, and total |
| GET | `/mart/sched` | `team_id`, `league_id`, `yr` | Team schedule with scores, margin, and total |
| GET | `/mart/game_oddbest` | `game_id`, `game_dt`, `league_id` | Best active odds per game (ML, spread, over/under) with post-game win flags; defaults to games with active bets on or after today |
| GET | `/mart/runner` | `syndicate_id` | Runners (syndicate members) with profile info and balance |

### Write (POST / PATCH)

| Method | Path | Body | Description |
|--------|------|------|-------------|
| POST | `/odd/bettor` | `apple_sub`, `apple_email?`, `apple_name?`, `apple_refresh_token?` | Create a new bettor via Apple Sign-In |
| PATCH | `/odd/bettor/{bettor_id}` | `apple_email?`, `apple_name?`, `apple_refresh_token?`, `last_login_ts?` | Update bettor account fields |
| PATCH | `/odd/bettor/{bettor_id}/profile` | `profile_name?`, `symbol?`, `color?` | Update bettor display profile |
| POST | `/odd/syndicate` | `bettor_id`, `name`, `description?`, `fantasy?`, `password?`, `max_runner?` | Create a syndicate; creator is added as admin runner |
| POST | `/odd/syndicate/{syndicate_id}/join` | `bettor_id`, `password?` | Join a syndicate as a member; validates password and max capacity |
