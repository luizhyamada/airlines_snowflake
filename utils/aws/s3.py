import boto3
import os
import logging
from config import logging_config

logger = logging_config()

def upload_to_s3(local_path, bucket, s3_key):
    s3_client = boto3.client('s3')

    if os.path.isdir(local_path):
        for root, _, files in os.walk(local_path):
            for filename in files:
                file_path = os.path.join(root, filename)
                relative_path = os.path.relpath(file_path, local_path)
                s3_file_key = os.path.join(s3_key, relative_path).replace("\\", "/") 

                try:
                    logging.info(f"Uploading {file_path} to s3://{bucket}/{s3_file_key}")
                    s3_client.upload_file(file_path, bucket, s3_file_key)
                    logging.info(f"Successfully uploaded {file_path} to s3://{bucket}/{s3_file_key}")
                except Exception as e:
                    logging.error(f"Failed to upload {file_path} to S3: {e}")
    else:
        try:
            logging.info(f"Uploading {local_path} to s3://{bucket}/{s3_key}")
            s3_client.upload_file(local_path, bucket, s3_key)
            logging.info(f"Successfully uploaded {local_path} to s3://{bucket}/{s3_key}")
        except Exception as e:
            logging.error(f"Failed to upload {local_path} to S3: {e}")

#upload_to_s3("dataset/AIR_CIA", "airlines-snowflake-test", "air_cia")
#upload_to_s3("dataset/VRA", "airlines-snowflake-test", "vra")