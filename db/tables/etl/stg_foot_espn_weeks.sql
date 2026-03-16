CREATE TABLE IF NOT EXISTS etl.stg_foot_espn_weeks (
	league varchar,
	yr smallint,
	season_label varchar,
	season_value varchar,
	week_label varchar,
	week_value varchar,
	week_start_date date,
	week_end_date date
);