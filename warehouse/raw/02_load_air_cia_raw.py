import logging
from config.snowpark_session import snowpark_session_create
from snowflake.snowpark.types import StructType, StructField, StringType
from config.logging_config import setup_logging

setup_logging()
session = snowpark_session_create()

session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

schema_air_cia = StructType([
    StructField("razao_social", StringType()),
    StructField("icao_iata", StringType()),
    StructField("cnpj", StringType()),
    StructField("atividades_aereas", StringType()),
    StructField("endereco_sede", StringType()),
    StructField("telefone", StringType()),
    StructField("email", StringType()),
    StructField("decisao_operacional", StringType()),
    StructField("data_decisao_operacional", StringType()),
    StructField("validade_operacional", StringType())
])

def load_and_save_table(session, stage_path: str, table_name: str, schema: StructType):
    df = (session.read
          .options({"field_delimiter": ";", "skip_header": 1})
          .schema(schema)
          .csv(stage_path))
    
    df.write.save_as_table(table_name, mode="overwrite")
    logging.info(f"Table '{table_name}' successfully created in Snowflake.")


load_and_save_table(session, "@airlines_raw_stage/air_cia/", "air_cia", schema_air_cia)