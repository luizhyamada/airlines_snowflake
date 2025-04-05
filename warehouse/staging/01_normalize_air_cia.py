import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()

session = snowpark_session_create()
session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

df = session.sql(f"""
    SELECT
        razao_social                    as corporate_name,
        split_part(icao_iata, ' ', 1)   as icao,
        split_part(icao_iata, ' ', 2)   as iata,          
        cnpj                            as cnpj,
        atividades_aereas               as air_activities,
        endereco_sede                   as headquarters_address,
        telefone                        as phone,
        email                           as email,
        decisao_operacional             as operational_decision,
        data_decisao_operacional        as operation_decision_date,
        validade_operacional            as operational_validity
    FROM
        raw.air_cia
""")

session.sql("USE SCHEMA staging").collect()

df.write.mode("overwrite").save_as_table("staging.air_cia")
logging.info("Data written to staging.air_cia successfully.")