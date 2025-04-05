import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()
session = snowpark_session_create()

session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

def load_and_save_table(session, stage_path: str, table_name: str):
    df = (session.read.options({
        "type": "json"
        }).json(stage_path))
    
    df.write.save_as_table(table_name, mode="overwrite")
    logging.info(f"Table '{table_name}' successfully created in Snowflake.")

load_and_save_table(session, "@airlines_raw_stage/airport/", "airport")