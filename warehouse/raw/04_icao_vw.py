import logging
from config.snowpark_session import snowpark_session_create
from snowflake.snowpark.functions import col, trim
from config.logging_config import setup_logging

setup_logging()
session = snowpark_session_create()

session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

def create_icao_view(session):
    vra_df = session.table("raw.vra")

    destination_df = (vra_df 
        .filter(col("COL1")["ICAOAeródromoDestino"].is_not_null()) 
        .select(trim(col("COL1")["ICAOAeródromoDestino"].cast("string")).alias("icao")))

    origin_df = (vra_df
        .filter(col("COL1")["ICAOAeródromoOrigem"].is_not_null()) \
        .select(trim(col("COL1")["ICAOAeródromoOrigem"].cast("string")).alias("icao")))

    icao_df = destination_df.union(origin_df).distinct()

    icao_df.create_or_replace_view("raw.icao_vw")
    logging.info(f"View successfully created in Snowflake.")

create_icao_view(session)
