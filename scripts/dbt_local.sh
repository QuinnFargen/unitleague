

dbt % export $(cat .env | grep -v '^#' | xargs) && dbt debug 

dbt seed --select ball.team ball.meta
