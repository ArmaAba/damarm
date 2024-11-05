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
            table.put_item(Item=item)
            # Fetch the item back from DynamoDB to verify it was saved
            response = table.get_item(Key={"id": id})
            saved_item = response.get("Item")
            if saved_item:
                response_body = {
                    "statusCode": 201,
                    "event": {
                        "id": saved_item["id"],
                        "principalId": saved_item["principalId"],
                        "createdAt": saved_item["createdAt"],
                        "body": saved_item["body"]  # body as a map (key-value pairs)
                    }
                }
                return self.build_response(201, response_body)
        except ClientError as e:
            print('Error:', e)
            return self.build_response(400, e.response['Error']['Message'])

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
