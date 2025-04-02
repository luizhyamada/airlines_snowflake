import logging
from snowflake.snowpark.session import Session
from config.settings import (
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD,
    SNOWFLAKE_ROLE, SNOWFLAKE_WAREHOUSE
)
from config.logging_config import setup_logging

setup_logging()

def snowpark_session_create():
    connection_params = {
        "account": SNOWFLAKE_ACCOUNT,
        "user": SNOWFLAKE_USER,
        "password": SNOWFLAKE_PASSWORD,
        "role": SNOWFLAKE_ROLE,
        "warehouse": SNOWFLAKE_WAREHOUSE
    }

    session = Session.builder.configs(connection_params).create()

    logging.info(f"Connected to Snowflake. ")

    return session

snowpark_session_create()