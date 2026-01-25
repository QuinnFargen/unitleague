# Kestra Layout
## Flow Names
- neon_ stands for what pg instance the data is flowing into.
- neon_unit_league stands for the namespace in kestra.
- **flow name structure**
  - {namespace}.{sport}_{source}_{method}_{type}.yaml
  - sport being all or type like foot/base/puck
  - source being where it is provided by espn/kalshi
  - method so far has been api or python
  - type being daily, weekly, monthly or can it backfill missing.
## Secrets
- these need to be added to the kestra docker compose file and encrypted with the secrets.py
## Preferred Steps
- log start into utility.log table
- check if there has been events that need to be pulled. (Game finished/missing data)
  - results in a csv of values to pull
  - bypass rest of logic if no data to be pulled.
- might need an ion to csv to pass to python
- python to pull api or packages of data and export as csv
- truncate the etl.stg_ table the data will be landed into
- copy in the csv into the table
- execute a procedure to load from etl.stg_ to etl.all_ or src table.
- log end into utility.log table
## Preferred job generalizing
- would prefer foot jobs instead of a cfb and nfl semi-duplicate job.
  - can have cfb and nfl logic within seperated, but one kestra flow
- backfill capability and ability to query what to look for
