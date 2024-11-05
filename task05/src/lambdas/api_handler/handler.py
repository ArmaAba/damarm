from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import json
import uuid
import boto3
from botocore.exceptions import ClientError
from decimal import Decimal
from datetime import datetime

_LOG = get_logger('ApiHandler-handler')
dynamodb = boto3.resource('dynamodb')
table_name = 'cmtr-f7e4afc6-Events'
table = dynamodb.Table(table_name)

check_status = "/status"
events_path = "/events"


class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        print("Request event: ", event)
        try:
            http_method = event.get("httpMethod")
            path = event.get("path")
            if http_method == "GET" and path == check_status:
                response = self.build_response(200, "All good")
            elif http_method == "POST" and path == events_path:
                request_body = json.loads(event["body"])
                if "id" not in request_body:
                    request_body["id"] = str(uuid.uuid4())
                request_body["createdAt"] = datetime.utcnow().isoformat() + "Z"
                response = self.save_events(request_body)
            else:
                response = self.build_response(404, "404 Not Found")
        except Exception as e:
            print("Error: ", e)
            response = self.build_response(400, "Error procesing request")
        return response

    def save_events(self, request_body):
        try:
            # Generate a new UUID for the event ID if not provided
            id = request_body.get("id") or str(uuid.uuid4())
            principal_id = int(request_body.get("principalId"))  # Ensure principalId is an integer
            created_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"  # ISO 8601 without microseconds
            content = request_body.get("content")  # Expected to be a Map<String, String>

            # Define the item structure to store in DynamoDB
            item = {
                "id": id,  # UUID v4 as a string
                "principalId": principal_id,  # Integer
                "createdAt": created_at,  # ISO 8601 formatted datetime
                "body": content  # Stored as a map
            }

            # Insert the item into the DynamoDB table
            table.put_item(Item=item)

            # Retrieve the saved item to verify it was saved correctly
            response = table.get_item(Key={"id": id})
            saved_item = response.get("Item")

            # Build the response to match the expected format
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
            else:
                # Return a structured error if the item was not found after saving
                return self.build_response(400, {
                    "errors": [
                        "Requested resource not found"
                    ]
                })

        except ClientError as e:
            print("Error:", e)
            return self.build_response(400, {
                "errors": [e.response["Error"]["Message"]]
            })

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
