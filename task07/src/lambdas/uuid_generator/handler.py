import uuid
import boto3
import datetime
import json
import os

# Assuming `get_logger` and `AbstractLambda` are correctly defined
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda

_LOG = get_logger('UuidGenerator-handler')

# Initialize the S3 client
s3_client = boto3.client('s3')


class UuidGenerator(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        try:
            uuids = [str(uuid.uuid4()) for _ in range(10)]
            _LOG.info(f"Generated UUIDs: {uuids}")
            data = json.dumps({"ids": uuids})
            timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = timestamp[:-3] + 'Z'
            file_name = f"{timestamp}"
            _LOG.info(f"File name for UUID storage: {file_name}")
            bucket_name = os.environ.get("target_table")
            _LOG.info(f"Target S3 bucket: {bucket_name}")
            s3_client.put_object(
                Bucket=bucket_name,
                Key=file_name,
                Body=data,
                ContentType='application/json'
            )
            _LOG.info(f"Successfully stored UUIDs in S3 bucket '{bucket_name}' with filename '{file_name}'")

            return {"statusCode": 200, "body": f"Stored UUIDs in '{file_name}'"}

        except Exception as e:
            _LOG.error(f"Error occurred while handling the request: {str(e)}", exc_info=True)
            return {"statusCode": 500, "body": f"Error occurred: {str(e)}"}


HANDLER = UuidGenerator()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
