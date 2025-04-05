import logging
from config.snowpark_session import snowpark_session_create
from config.logging_config import setup_logging

setup_logging()

session = snowpark_session_create()
session.sql("USE DATABASE airlines").collect()
session.sql("USE SCHEMA raw").collect()

df = session.sql(f"""
    SELECT
        TO_TIMESTAMP(col1:"ChegadaPrevista")          AS scheduled_arrival,
        TO_TIMESTAMP(col1:"ChegadaReal")              AS actual_arrival,
        col1:"CódigoAutorização"::STRING              AS authorization_code,
        col1:"CódigoJustificativa"::STRING            AS justification_code,
        col1:"CódigoTipoLinha"::STRING                AS line_type_code,
        col1:"ICAOAeródromoDestino"::STRING           AS destination_aerodrome_icao,
        col1:"ICAOAeródromoOrigem"::STRING            AS origin_aerodrome_icao,
        col1:"ICAOEmpresaAérea"::STRING               AS airline_icao,
        col1:"NúmeroVoo"::STRING                      AS flight_number,
        TO_TIMESTAMP(col1:"PartidaPrevista")          AS scheduled_departure,
        TO_TIMESTAMP(col1:"PartidaReal")              AS actual_departure,
        col1:"SituaçãoVoo"::TEXT                      AS flight_status
    FROM
        raw.vra
""")

session.sql("USE SCHEMA staging").collect()

df.write.mode("overwrite").save_as_table("staging.vra")
logging.info("Data written to staging.vra successfully.")