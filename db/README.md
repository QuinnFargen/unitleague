# Database Layout
## Schema
### etl
- This is the location that kestra will land api and python pulled data into the database.
- stg_ prefixed tables will be the initial tables that don't have PK's or insert dates. These are truncated and loaded with new data each time.
- all_ prefixed tables will retain new unique data that was just landed into the stg_ tables.
- If there is a population queried in the kestra job dynamically, there will be a view in this etl schema that might query other schema's to know what data is needed.

### src
- This is the cleaned & extracted data from the etl schema. 
- dbt will query from this to populate the ball & odd schema.
- Weather data for historical lat/long regions. Also will have locationally average values for null imputation.
- If i'm able to publish the data in this format, it allows the usage of this db from this point on without needing the prior etl/kestra setup.

### utility
- Calendar table will be the main static table.
- Metadata on dynamically populated table sources.
- Log tables for what ran where and when.

### ball
- This is the main sports data model that has results and scores.
- game table is a one row per game and sched will have two rows with one for each team.
- season has one row per league each year and will have many rows per season in the week table.
- rank data is the only non-result data flowing into this since it ties to the week data that is in here.

### odd
- This is the store of odds and prediction market data.
- bet table is the store of all possible bets offered on these platforms.
- Other tables will be used to track hypothetical bets places in a future unitleague application.

### mart
- This will be features and trends rolled up from the ball and odd schema.