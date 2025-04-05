import requests
import datetime
import logging
import json
import os
import boto3
import datetime
from config.logging_config import setup_logging
from dotenv import load_dotenv
from config.snowpark_session import snowpark_session_create


setup_logging()
load_dotenv()

class AirportsApi():
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.s3_bucket = os.getenv("S3_BUCKET_NAME")
        self.base_endpoint = "https://airport-info.p.rapidapi.com/airport"
        self.session = snowpark_session_create()
        self.session.use_database("airlines")
        self.session.use_schema("raw") 

    def read_icao_codes(self):
        try:
            df = self.session.sql("SELECT * FROM raw.icao_vw;")
            icao_list = [row["ICAO"] for row in df.collect()]
            return icao_list
        except Exception as e:
            logging.error(f"Error reading ICAO codes from Snowflake: {e}")
            return []

    def get_data(self, icao:str):
        params = {"icao": icao}
        headers = {
            "X-RapidAPI-Key": self.api_key,
            "X-RapidAPI-Host": "airport-info.p.rapidapi.com"
        }

        try:
            logging.info(f"Getting data from endpoint: {self.base_endpoint} with ICAO: {icao}")
            response = requests.get(self.base_endpoint, headers=headers, params=params)
            response.raise_for_status()

            data = response.json()
            if data:
                return data
            else:
                logging.warning(f"No data returned for ICAO: {icao}")
                return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data: {e}")
            return None
        
    def load_to_s3(self, data: dict, icao: str):
        try:
            timestamp = datetime.datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
            file_key = f"airport/airport_{icao}_{timestamp}.json"
            json_data = json.dumps(data, indent=2)

            s3_client = boto3.client("s3")
            s3_client.put_object(
                Bucket=self.s3_bucket,
                Key=file_key,
                Body=json_data,
                ContentType='application/json'
            )

            logging.info(f"Uploaded {icao} data to s3://{self.s3_bucket}/{file_key}")
        except Exception as e:
            logging.error(f"Failed to upload {icao} data to S3: {e}")
    
        
if __name__ == "__main__":
    airports_api = AirportsApi()
    icao_codes = airports_api.read_icao_codes()

    for icao in icao_codes:
        data = airports_api.get_data(icao)
        if data:
            airports_api.load_to_s3(data, icao)