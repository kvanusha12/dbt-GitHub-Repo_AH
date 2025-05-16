    WITH airports_reorder AS (
        SELECT faa
               , name
        	   , country
               , region
               , city
               , lat
               , lon
               , alt
               , tz
               , dst
        FROM {{ref('staging_airports')}}
    )
    SELECT * FROM airports_reorder