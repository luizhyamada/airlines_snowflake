import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()

session = snowpark_session_create()
session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA staging").collect()

df = session.sql(f"""
    with airport_activity as (
        select 
            icao,
            airline_icao,
            sum(case when icao = origin_aerodrome_icao then 1 else 0 end) as departures,
            sum(case when icao = destination_aerodrome_icao then 1 else 0 end) as arrivals
        from (
            select 
                origin_aerodrome_icao,
                destination_aerodrome_icao,
                airline_icao
            from staging.vra
        ) v
        join (
            select icao from staging.airport
        ) a
        on v.origin_aerodrome_icao = a.icao or v.destination_aerodrome_icao = a.icao
        group by icao, airline_icao
    ),
    ranked_activity as (
        select 
            *,
            departures + arrivals as total_movements,
            row_number() over (partition by icao order by (departures + arrivals) DESC) as rn
        from airport_activity
    )
    select 
        ap.name as airport_name,
        ap.icao as airport_icao,
        ac.corporate_name as airline_name,
        ra.departures,
        ra.arrivals,
        ra.total_movements
    from ranked_activity ra
    join staging.airport ap on ap.icao = ra.icao
    join staging.air_cia ac on ac.icao = ra.airline_icao
    where ra.rn = 1
""")

session.sql("USE SCHEMA analytics").collect()

df.write.mode("overwrite").save_as_table("analytics.top_airline_per_airport")
logging.info("Data written to staging.top_airline_per_airport successfully.")