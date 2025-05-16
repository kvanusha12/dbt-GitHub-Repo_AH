WITH departures AS (-- ON crée la 1ère table
		SELECT origin AS airport_code,
			count(DISTINCT dest) nunique_to,
			count(sched_dep_time) dep_planned,
			sum(cancelled) dep_cancelled,
			sum(diverted) dep_diverted,
			count(arr_time) dep_n_flights
		FROM {{ref('prep_flights')}}
		GROUP BY origin
		),
arrivals AS (-- la 2ème
SELECT dest AS airport_code, 
			count(DISTINCT origin) nunique_from,
			count(sched_arr_time) arr_planned,
			sum(cancelled) arr_cancelled,
			sum(cancelled) arr_diverted,
			count(arr_time) arr_n_flights
		FROM {{ref('prep_flights')}}
		GROUP BY dest
		),
total_stats AS (-- ON joint les 2 sur la collone en commun
		SELECT d.AIRPORT_CODE,
		nunique_to, nunique_from,
		(dep_planned + arr_planned) AS total_planned,
		(DEP_CANCELLED + arr_cancelled) AS total_cancelled,
		(dep_diverted + arr_diverted) AS total_diverted,
		(dep_n_flights + arr_n_flights) AS total_flights
		FROM departures d
		JOIN arrivals a
		using(AIRPORT_CODE)
)
SELECT ts.*, 
		a.city, 
		a.country, 
		a.name 
FROM total_stats ts
JOIN {{ref('prep_flights')}}
ON ts.airport_code = faa