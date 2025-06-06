import logging
import os
from dotenv import load_dotenv
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()
load_dotenv()

db_name = "airlines"
schema_name = "raw"
stage_name = "airlines_raw_stage"
s3_bucket_name = os.environ.get("S3_BUCKET_NAME")
s3_bucket_url = f"s3://{s3_bucket_name}/"

session = snowpark_session_create()

session.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}").collect()
logging.info(f"Database '{db_name}' created or already exists.")

session.sql(f"USE DATABASE {db_name}").collect()

session.sql(f"CREATE SCHEMA IF NOT EXISTS {schema_name}").collect()
logging.info(f"Schema '{schema_name}' created or already exists.")

session.sql(f"""
            CREATE OR REPLACE STAGE {stage_name}
            URL = '{s3_bucket_url}'
""").collect()
logging.info(f"Stage '{stage_name}' created.")

session.close()