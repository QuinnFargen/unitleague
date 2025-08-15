

-- https://medium.com/justdataplease/building-a-dynamic-date-calendar-in-postgresql-a-step-by-step-guide-20c8edfc3bf7

create schema utility;

-- Create date calendar
DROP TABLE if exists utility.calendar;

CREATE TABLE utility.calendar (
    date_id INT NOT NULL,
    thedate DATE NOT NULL,
    start_of_week DATE NOT NULL,
    start_of_month DATE NOT NULL,
    start_of_midmonth DATE NOT NULL,
    start_of_qtr DATE NOT NULL,
    start_of_year DATE NOT NULL,
    end_of_week DATE NOT NULL,
    end_of_month DATE NOT NULL,
    end_of_qtr DATE NOT NULL,
    end_of_year DATE NOT NULL,
    epoch BIGINT NOT NULL,
    day_suffix VARCHAR(4) NOT NULL,
    day_name VARCHAR(9) NOT NULL,
    day_name_abbr VARCHAR(9) NOT NULL,
    day_of_week INT NOT NULL,
    day_of_month INT NOT NULL,
    day_of_qtr INT NOT NULL,
    day_of_year INT NOT NULL,
    week_of_month INT NOT NULL,
    week_of_year INT NOT NULL,
    week_of_year_iso CHAR(10) NOT NULL,
    month_int INT NOT NULL,
    month_name VARCHAR(9) NOT NULL,
    month_name_abbr CHAR(3) NOT NULL,
    qtr_int INT NOT NULL,
    qtr_name VARCHAR(9) NOT NULL,
    year_int INT NOT NULL,
    yyyymm VARCHAR NOT NULL,
    yyyymmdd VARCHAR NOT NULL,
    year_var VARCHAR,
    month_var VARCHAR,
    qtr_var VARCHAR,
    week_monday VARCHAR,
    is_weekend INT2 NOT NULL
);

ALTER TABLE utility.calendar ADD CONSTRAINT date_calendar_date_pk PRIMARY KEY (date_id);
CREATE INDEX date_calendar_date_ac_idx ON utility.calendar(thedate);



INSERT INTO utility.calendar
SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_id,
datum AS date,
DATE_TRUNC('week', datum)::date AS start_of_week,
DATE_TRUNC('month', datum)::date AS start_of_month,
CASE 
        WHEN EXTRACT(DAY FROM datum) < 15 THEN
            DATE_TRUNC('month', datum)::date
        ELSE
            DATE_TRUNC('month', datum)::date + INTERVAL '14 days'
    END AS start_of_midmonth,
DATE_TRUNC('quarter',datum)::DATE AS start_of_quarter,
DATE_TRUNC('YEAR',datum)::DATE AS start_of_year,
(DATE_TRUNC('WEEK',datum) +INTERVAL '1 WEEK - 1 day')::DATE AS end_of_week,
(DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS end_of_month,
(DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS end_of_quarter,
(DATE_TRUNC('YEAR',datum)::DATE +INTERVAL '1 YEAR - 1 day')::DATE AS end_of_year,
EXTRACT(epoch FROM datum) AS epoch,
TO_CHAR(datum,'Dth') AS day_suffix,
TO_CHAR(datum,'Day') AS day_name,
TO_CHAR(datum,'Dy') AS day_name_abbr,
EXTRACT(isodow FROM datum) AS day_of_week,
EXTRACT(DAY FROM datum) AS day_of_month,
datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
EXTRACT(doy FROM datum) AS day_of_year,
TO_CHAR(datum,'W')::INT AS week_of_month,
EXTRACT(week FROM datum) AS week_of_year,
TO_CHAR(datum,'YYYY-WIW-D') AS week_of_year_iso,
EXTRACT(MONTH FROM datum) AS month_,
TO_CHAR(datum,'Month') AS month_name,
TO_CHAR(datum,'Mon') AS month_name_abbr,
EXTRACT(quarter FROM datum) AS quarter_,
CONCAT('Q',EXTRACT(quarter FROM datum)) quarter_name,
EXTRACT(year FROM datum)::int year_,
--EXTRACT(isoyear FROM datum) AS year_,
TO_CHAR(datum,'yyyymm') AS yyyymm,
TO_CHAR(datum,'yyyymmdd') AS yyyymmdd,
EXTRACT(year FROM datum) Year,
CONCAT(EXTRACT(year FROM datum),'-',TO_CHAR(datum,'Mon'))  Month,
CONCAT(EXTRACT(year FROM datum),'-Q',EXTRACT(quarter FROM datum)) Quarter,
DATE_TRUNC('week', datum)::date Week Monday,
CASE WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN 1 ELSE 0 END AS is_weekend 
FROM (SELECT datum::date FROM GENERATE_SERIES (
    DATE '2000-01-01', 
    DATE '2030-12-31', 
    INTERVAL '1 day'
) AS datum) dates_series;
-- 11323

SELECT * FROM utility.calendar;


SELECT A.year_int, COUNT(*) FROM utility.calendar A group by A.year_int order by 1;
SELECT A.year_var, COUNT(*) FROM utility.calendar A group by A.year_var order by 1;


select * 
FROM utility.calendar A 
where A.year_int = 2009 and A.month_int = 1
order by A.thedate ;


SELECT A.month_int, COUNT(*) FROM utility.calendar A where A.year_int = 2009 group by A.month_int order by 1 ;


