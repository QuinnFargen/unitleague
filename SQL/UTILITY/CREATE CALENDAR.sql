

-- https://medium.com/justdataplease/building-a-dynamic-date-calendar-in-postgresql-a-step-by-step-guide-20c8edfc3bf7

create schema "UTILITY";

-- Create date calendar
DROP TABLE if exists "UTILITY"."CALENDAR";

CREATE TABLE "UTILITY"."CALENDAR" (
DATE_ID INT NOT NULL,
THEDATE DATE NOT NULL,
START_OF_WEEK DATE NOT NULL,
START_OF_MONTH DATE NOT NULL,
START_OF_MIDMONTH DATE NOT NULL,
START_OF_QTR DATE NOT NULL,
START_OF_YEAR DATE NOT NULL,
END_OF_WEEK DATE NOT NULL,
END_OF_MONTH DATE NOT NULL,
END_OF_QTR DATE NOT NULL,
END_OF_YEAR DATE NOT NULL,
EPOCH BIGINT NOT NULL,
DAY_SUFFIX VARCHAR(4) NOT NULL,
DAY_NAME VARCHAR(9) NOT NULL,
DAY_NAME_ABBR VARCHAR(9) NOT NULL,
DAY_OF_WEEK INT NOT NULL,
DAY_OF_MONTH INT NOT NULL,
DAY_OF_QTR INT NOT NULL,
DAY_OF_YEAR INT NOT NULL,
WEEK_OF_MONTH INT NOT NULL,
WEEK_OF_YEAR INT NOT NULL,
WEEK_OF_YEAR_ISO CHAR(10) NOT NULL,
MONTH_INT INT NOT NULL,
MONTH_NAME VARCHAR(9) NOT NULL,
MONTH_NAME_ABBR CHAR(3) NOT NULL,
QTR_INT INT NOT NULL,
QTR_NAME VARCHAR(9) NOT NULL,
YEAR_INT INT NOT NULL,
YYYYMM VARCHAR NOT NULL,
YYYYMMDD VARCHAR NOT NULL,
YEAR_VAR VARCHAR,
MONTH_VAR VARCHAR,
QTR_VAR VARCHAR,
WEEK_MONDAY VARCHAR,
IS_WEEKEND INT2 NOT NULL);

ALTER TABLE "UTILITY"."CALENDAR" ADD CONSTRAINT date_calendar_date_pk PRIMARY KEY (DATE_ID);
CREATE INDEX date_calendar_date_ac_idx ON "UTILITY"."CALENDAR"(THEDATE);



INSERT INTO "UTILITY"."CALENDAR"
SELECT TO_CHAR(datum,'yyyymmdd')::INT AS DATE_ID,
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
TO_CHAR(datum,'YYYY"-W"IW-D') AS week_of_year_iso,
EXTRACT(MONTH FROM datum) AS month_,
TO_CHAR(datum,'Month') AS month_name,
TO_CHAR(datum,'Mon') AS month_name_abbr,
EXTRACT(quarter FROM datum) AS quarter_,
CONCAT('Q',EXTRACT(quarter FROM datum)) quarter_name,
EXTRACT(year FROM datum)::int year_,
--EXTRACT(isoyear FROM datum) AS year_,
TO_CHAR(datum,'yyyymm') AS yyyymm,
TO_CHAR(datum,'yyyymmdd') AS yyyymmdd,
EXTRACT(year FROM datum) "Year",
CONCAT(EXTRACT(year FROM datum),'-',TO_CHAR(datum,'Mon'))  "Month",
CONCAT(EXTRACT(year FROM datum),'-Q',EXTRACT(quarter FROM datum)) "Quarter",
DATE_TRUNC('week', datum)::date "Week Monday",
CASE WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN 1 ELSE 0 END AS is_weekend 
FROM (SELECT datum::date FROM GENERATE_SERIES (
    DATE '2000-01-01', 
    DATE '2030-12-31', 
    INTERVAL '1 day'
) AS datum) dates_series;
-- 11323

SELECT * FROM "UTILITY"."CALENDAR";


SELECT A.year_int, COUNT(*) FROM "UTILITY"."CALENDAR" A group by A.year_int order by 1;
SELECT A.year_var, COUNT(*) FROM "UTILITY"."CALENDAR" A group by A.year_var order by 1;


select * 
FROM "UTILITY"."CALENDAR" A 
where A.year_int = 2009 and A.month_int = 1
order by A.thedate ;


SELECT A.month_int, COUNT(*) FROM "UTILITY"."CALENDAR" A where A.year_int = 2009 group by A.month_int order by 1 ;


