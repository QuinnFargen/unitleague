CREATE OR REPLACE FUNCTION "api".fn_convert_odds_money_dec(moneyline integer)
 RETURNS real
 LANGUAGE plpgsql
AS $function$

	BEGIN

	RETURN(SELECT CASE 

						WHEN moneyline < 0 THEN ROUND( ((moneyline*-1)+100) * 1.0 / (moneyline*-1) ,2)

						WHEN moneyline > 100 THEN ROUND( ((moneyline)+100) * 1.0 / (100) ,2)

						WHEN moneyline = 100 THEN 2

						ELSE NULL END

			);

	END;

$function$
;
