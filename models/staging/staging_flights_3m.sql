
WITH flights_3m AS (
    SELECT * 
    FROM {{source('flights_data', 'flights_3m')}}
)
SELECT * FROM flights_3m