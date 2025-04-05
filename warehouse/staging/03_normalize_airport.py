import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()

session = snowpark_session_create()
session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

df = session.sql(f"""
    SELECT
        col1:"icao"::STRING         AS icao,
        col1:"iata"::STRING         AS iata,
        col1:"name"::STRING         AS name,
        col1:"location"::STRING     AS location,
        col1:"latitude"::STRING     AS latitude,
        col1:"longitude"::STRING    AS longitude,
        col1:"country"::STRING      AS country,
        col1:"city"::STRING         AS city,
        col1:"state"::STRING        AS state
    FROM
        raw.airport
    WHERE 
        col1:"error" IS NULL
""")

session.sql("USE SCHEMA staging").collect()

df.write.mode("overwrite").save_as_table("staging.airport")
logging.info("Data written to staging.airport successfully.")