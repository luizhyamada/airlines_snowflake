import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()

session = snowpark_session_create()
session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA staging").collect()

df = session.sql(f"""
    with route_counts as (
        select 
            airline_icao,
            origin_aerodrome_icao,
            destination_aerodrome_icao,
            count(*) as total_flights,
            row_number() over (
                partition by airline_icao 
                order by count(*) desc
            ) as rn
        from staging.vra
        group by airline_icao, origin_aerodrome_icao, destination_aerodrome_icao
    ),
    most_used_routes as (
        select * 
        from route_counts 
        where rn = 1
    )
    select 
        ac.corporate_name,
        ao.name as origin_airport_name,
        ao.icao as origin_airport_icao,
        ao.state as origin_state,
        ad.name as destination_airport_name,
        ad.icao as destination_airport_icao,
        ad.state as destination_state
    from most_used_routes r
    join staging.air_cia ac on ac.icao = r.airline_icao
    left join staging.airport ao on ao.icao = r.origin_aerodrome_icao
    left join  staging.airport ad on ad.icao = r.destination_aerodrome_icao
""")

session.sql("USE SCHEMA analytics").collect()

df.write.mode("overwrite").save_as_table("analytics.top_airline_per_routes")
logging.info("Data written to staging.top_airline_routes successfully.")