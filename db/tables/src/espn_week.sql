CREATE TABLE IF NOT EXISTS src.espn_week (
	league_id   smallint NOT NULL,
	yr   smallint NOT NULL,
    week_start_dt date NOT NULL,
    week_end_dt date NOT NULL,
    week_num smallint NOT NULL,
    week_name varchar(50),
    is_pre boolean default (false),
    is_post boolean default (false),
    PRIMARY KEY (league_id, yr, week_start_dt, week_name)
);