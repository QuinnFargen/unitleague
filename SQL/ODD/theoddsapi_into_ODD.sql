


select * from "SPORT"."IMP"."ODD_API" oa ;
select * from "SPORT"."IMP"."ODD_API_ARCHIVE" oa ;

select * from "SPORT"."IMP"."ODD_API_BET" oa ;
select * from "SPORT"."IMP"."ODD_API_BET_ARCHIVE" oa ;


select COUNT(*), COUNT(distinct oa.commence_time), MIN(oa.commence_time), MAX(oa.commence_time), MIN(oa."INSERT_DT"), MAX(oa."INSERT_DT") 
from "SPORT"."IMP"."ODD_API" oa ;
--20819	5599	2020-12-15T19:00:00Z	2025-09-01T23:30:00Z	2024-12-19 00:01:29.256	2025-03-17 12:19:07.803
select COUNT(*), COUNT(distinct oa.commence_time), MIN(oa.commence_time), MAX(oa.commence_time), MIN(oa."INSERT_DT"), MAX(oa."INSERT_DT") 
from "SPORT"."IMP"."ODD_API_ARCHIVE" oa ;
--92230 17925	2021-03-16T23:00:00Z	2025-01-23T19:00:00Z	2024-09-12 19:20:26.914	2024-12-19 17:13:32.828




select * from "SPORT".api.the_odds_api toa ;
select * from "SPORT".api.the_odds_api_bet toa ;

select COUNT(*), COUNT(distinct oa.commence_time), MIN(oa.commence_time), MAX(oa.commence_time), MIN(oa."INSERT_DT"), MAX(oa."INSERT_DT") 
from "SPORT"."api"."the_odds_api" oa ;
--141171	23558	2020-07-18T22:05:00Z	2026-01-04T18:00:00Z	2024-09-12 19:20:26.914	2025-07-21 13:00:12.087



select * from "SPORT"."ODD"."LINE" l ;

select * from "SPORT"."ODD"."TXN" l ;


