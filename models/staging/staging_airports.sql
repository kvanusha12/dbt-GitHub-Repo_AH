WITH airports AS (
    SELECT * 
    FROM {{source('flights_data', 'airports')}}
)
SELECT * FROM airports