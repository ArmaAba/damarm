from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json
import uuid
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from datetime import datetime
import os

_LOG = get_logger("ApiHandler-handler")
dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("target_table", "Events")
table = dynamodb.Table(table_name)

check_status = "/status"
events_path = "/events"


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        _LOG.info("Request event: %s", event)
        try:
            http_method = event.get("httpMethod")
            path = event.get("path")
            if http_method == "GET" and path == check_status:
                response = self.build_response(200, "All good")
            elif http_method == "POST" and path == events_path:
                request_body = json.loads(event["body"])

                _LOG.info("Request body: %s", request_body)
                if "id" not in request_body:
                    request_body["id"] = str(uuid.uuid4())
                if "principalId" not in request_body:
                    raise ValueError("principalId is missing from request body")
                if "content" not in request_body:
                    raise ValueError("content is missing from request body")
                request_body["createdAt"] = datetime.utcnow().isoformat() + "Z"
                _LOG.info("Processed request body: %s", request_body)
                response = self.save_events(request_body)
            else:
                response = self.build_response(404, "404 Not Found")
        except Exception as e:
            print("Error: ", e)
            response = self.build_response(400, "Error procesing request")
        return response

    def save_events(self, request_body):
        try:
            id = request_body.get("id")
            principal_id = request_body.get("principalId")
            created_at = request_body.get("createdAt")
            content = request_body.get("content")
            item = {
                "id": id,
                "principalId": principal_id,
                "createdAt": created_at,
                "body": content
            }
            # Log the item before saving to DynamoDB
            _LOG.info("Item to be saved to DynamoDB: %s", item)
            table.put_item(Item=item)
            # Fetch the item back from DynamoDB to verify it was saved
            response = table.get_item(Key={"id": id})
            saved_item = response.get("Item")
            _LOG.info("Saved item from DynamoDB: %s", saved_item)
            if saved_item:
                response_body = {
                    "statusCode": 201,
                    "event": {
                        "id": saved_item["id"],
                        "principalId": saved_item["principalId"],
                        "createdAt": saved_item["createdAt"],
                        "body": saved_item["body"]
                    }
                }
                return self.build_response(201, response_body)
        except ClientError as e:
            _LOG.error("Saved item not found in DynamoDB")
            print('Error:', e)
            return self.build_response(400, e.response['Error']['Message'])
        except Exception as e:
            _LOG.error("Error saving event: %s", e)
            return self.build_response(400, f"Unexpected error: {str(e)}")

    def build_response(self, status_code, body):
        return {
            'statusCode': status_code,
            'headers': {
                'Content-Type': 'application/json'
            },
            'body': json.dumps(body, cls=DecimalEncoder)
        }


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            # Check if it's an int or a float
            if obj % 1 == 0:
                return int(obj)
            else:
                return float(obj)
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)


HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
