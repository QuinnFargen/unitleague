# Database Layout

## Schema

### api
This is the location that kestra will land api and python pulled data into the database.
- stg_ prefixed tables will be the initial tables that don't have PK's or insert dates. These are truncated and loaded with new data each time.
- all_ prefixed tables will retain new unique data that was just landed into the stg_ tables.
If there is a population queried in the kestra job dynamically, there will be a view in this api schema that might query other schema's to know what data is needed.

### src
This is the cleaned & extracted data from the api schema. 
dbt will query from this to populate the ball & odd schema.

### utility
Currently this is similar to src but for ancillary data sources.
This has a calendar table and weather data. 
Weather data will be updated with new values but also will have locationally average values for potential nulls.

### ball
This is the main sports data model that has results and scores.
game table is a one row per game and sched will have two rows with one for each team.
season has one row per league each year and will have many rows per season in the week table.
rank data is the only non-result data flowing into this since it ties to the week data that is in here.

### odd
This is the store of odds and prediction market data.
bet table is the store of all possible bets offered on these platforms.
Other tables will be used to track hypothetical bets places in a future unitleague application.
