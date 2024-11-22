import json
import boto3
from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import os
from decimal import Decimal
import uuid
from boto3.dynamodb.conditions import Attr
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
cognito_client = boto3.client('cognito-idp')
user_pool_id = os.environ.get("cup_id")
client_id = os.environ.get("cup_client_id")

_LOG = get_logger('ApiHandler-handler')

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)  # Convert Decimal objects to float for JSON serialization
        # Let the base class default method raise the TypeError
        return super(DecimalEncoder, self).default(obj)

class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        try:
            http_method = event['httpMethod']
            path = event['path']
            if path == '/signup' and http_method == 'POST':
                return self.signup(event)
            elif path == '/signin' and http_method == 'POST':
                return self.signin(event)
            elif path == '/tables' and http_method == 'GET':
                return self.get_tables(event)
            elif path == '/tables' and http_method == 'POST':
                return self.create_table(event)
            elif path.startswith('/tables/') and http_method == 'GET':
                table_id = path.split('/')[-1]
                return self.get_table_by_id(table_id, event)
            elif path == '/reservations' and http_method == 'POST':
                return self.create_reservation(event)
            elif path == '/reservations' and http_method == 'GET':
                return self.get_reservations(event)
            else:
                return {
                    'statusCode': 400,
                    'body': json.dumps({'message': 'Bad request'})
                }
        except Exception as e:
            _LOG.error(f"Error handling request: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps({'message': 'Internal server error'})
            }

    def signup(self, event):
        body = json.loads(event['body'])
        try:
            # Create the user without setting a temporary password
            response = cognito_client.admin_create_user(
                UserPoolId=user_pool_id,
                Username=body['email'],
                UserAttributes=[
                    {'Name': 'email', 'Value': body['email']},
                    {'Name': 'name', 'Value': f"{body['firstName']} {body['lastName']}"},
                    {'Name': 'email_verified', 'Value': 'True'}  # Mark email as verified
                ],
                MessageAction='SUPPRESS'  # Suppress the sending of any emails
            )

            # Immediately set a permanent password for the user
            cognito_client.admin_set_user_password(
                UserPoolId=user_pool_id,
                Username=body['email'],
                Password=body['password'],
                Permanent=True
            )

            return self.response(200, 'User created successfully with a permanent password')
        except cognito_client.exceptions.UsernameExistsException:
            return self.response(400, 'User already exists')
        except Exception as e:
            _LOG.error(f"Signup error: {str(e)}")
            return self.response(400, 'Signup failed')

    def signin(self, event):
        try:
            body = json.loads(event.get('body', '{}'))  # Safely load the body
            email = body.get('email')
            password = body.get('password')

            # Check if both email and password are provided
            if not email or not password:
                return self.response(400, 'OK')

            response = cognito_client.initiate_auth(
                ClientId=client_id,
                AuthFlow='USER_PASSWORD_AUTH',
                AuthParameters={
                    'USERNAME': email,
                    'PASSWORD': password
                }
            )
            _LOG.info(response)
            _LOG.info(response)
            _LOG.info(response)
            _LOG.info(response)
            # Return the AccessToken
            return self.response(200, {'accessToken': response['AuthenticationResult']['IdToken']})
        except cognito_client.exceptions.NotAuthorizedException:
            return self.response(401, 'Login failed')
        except Exception as e:
            _LOG.error(f"Signin error: {str(e)}")
            return self.response(400, 'Login failed')

    def response(self, status_code, body):
        return {
            'statusCode': status_code,
            'body': json.dumps(body) if isinstance(body, dict) else body
        }

    def get_tables(self, event):
        # Assuming your DynamoDB table holding the tables is called 'Tables'
        table_name = os.environ.get("tables_table", "Tables")  # Best practice to use environment variable for table name
        table = dynamodb.Table(table_name)

        # Attempt to fetch all table entries from your DynamoDB 'Tables' table
        try:
            response = table.scan()  # This retrieves all items in the table. Consider Query for more scalability
            # Serialize and then Deserialize the Items using DecimalEncoder for handling Decimal objects correctly
            serialized_tables = json.dumps(response.get('Items', []), cls=DecimalEncoder)
            tables_data = json.loads(serialized_tables)

            # Return the response with the list of formatted table data
            return {
                'statusCode': 200,
                'body': json.dumps({"tables": tables_data})
            }

        except Exception as e:
            _LOG.error(f"Error fetching table data: {str(e)}")
            return {
                'statusCode': 500,
                'body': json.dumps('Internal server error fetching table data')
            }

    def create_table(self, event):
        # Parse the body from the event
        try:
            body = json.loads(event.get('body', '{}'))
            # Convert id to int as per specification
            table_id = int(body.get('id'))  # This can raise a ValueError if 'id' is not a valid integer string
            table_number = int(body['number'])
            places = int(body['places'])
            is_vip = bool(body['isVip'])
            min_order = body.get('minOrder')

            # Assuming your DynamoDB table is named 'Tables'
            table_name = os.environ.get('tables_table', 'Tables')
            table = dynamodb.Table(table_name)

            # Construct the item to insert into DynamoDB
            item = {
                'id': table_id,
                'number': table_number,
                'places': places,
                'isVip': is_vip,
            }
            if min_order is not None:
                item['minOrder'] = min_order

            # Insert the item into DynamoDB
            table.put_item(Item=item)

            # Successfully created the table, return the id
            return self.response(200, {'id': table_id})

        except Exception as e:
            _LOG.error(f"Error creating table: {str(e)}")
            return self.response(400, 'Bad request')

    def get_table_by_id(self, table_id, event):
        # Convert table_id to the correct type if necessary, assuming it's an integer
        # If table_id is supposed to be an int, ensure to handle conversion errors
        try:
            table_id = int(table_id)
        except ValueError:
            return self.response(400, 'Bad request: tableId must be an integer')

        try:
            table_name = os.environ.get('tables_table', 'Tables')
            table = dynamodb.Table(table_name)

            # Query DynamoDB for the table with the given ID
            response = table.get_item(
                Key={
                    'id': table_id
                }
            )

            # Check if the table was found
            if 'Item' not in response:
                return self.response(404, 'Table not found')

            table_data = response['Item']
            serialized_data = json.dumps(table_data, cls=DecimalEncoder)

            # Return the found table data
            return self.response(200, json.loads(serialized_data))

        except Exception as e:
            _LOG.error(f"Error fetching table by ID: {str(e)}")
            return self.response(500, 'Internal server error')

    def create_reservation(self, event):
        try:
            body = json.loads(event.get('body', '{}'))
            table_number = int(body.get('tableNumber'))
            client_name = body.get('clientName')
            phone_number = body.get('phoneNumber')
            date = body.get('date')
            slot_time_start = body.get('slotTimeStart')
            slot_time_end = body.get('slotTimeEnd')

            # DynamoDB Table Names
            tables_table_name = os.environ.get('tables_table', 'cmtr-f7e4afc6-Tables-test')
            reservation_table_name = os.environ.get('reservation_tables', 'cmtr-f7e4afc6-Reservations-test')

            # Check if the table exists
            tables_table = dynamodb.Table(tables_table_name)
            table_response = tables_table.get_item(Key={'id': table_number})
            if 'Item' not in table_response:
                _LOG.error(f"Table with id {table_number} does not exist.")
                return {
                    'statusCode': 400,
                    'body': json.dumps('Non-existent table', cls=DecimalEncoder)
                }

            # Check for overlapping reservations
            reservations_table = dynamodb.Table(reservation_table_name)
            scan_result = reservations_table.scan(
                FilterExpression=Attr('tableNumber').eq(table_number) & Attr('date').eq(date)
            )
            for reservation in scan_result['Items']:
                existing_start = datetime.strptime(reservation['slotTimeStart'], '%H:%M')
                existing_end = datetime.strptime(reservation['slotTimeEnd'], '%H:%M')
                new_start = datetime.strptime(slot_time_start, '%H:%M')
                new_end = datetime.strptime(slot_time_end, '%H:%M')

                if new_start < existing_end and new_end > existing_start:
                    _LOG.error("Reservation overlaps with an existing reservation.")
                    return {
                        'statusCode': 400,
                        'body': json.dumps('Reservation overlaps with an existing reservation', cls=DecimalEncoder)
                    }

            # Proceed to create the reservation since no overlaps exist and table exists
            reservation_id = str(uuid.uuid4())
            item = {
                'id': reservation_id,
                'tableNumber': table_number,
                'clientName': client_name,
                'phoneNumber': phone_number,
                'date': date,
                'slotTimeStart': slot_time_start,
                'slotTimeEnd': slot_time_end
            }

            reservations_table.put_item(Item=item)
            _LOG.info(f"Reservation created successfully: {reservation_id}")
            return {
                'statusCode': 200,
                'body': json.dumps({'reservationId': reservation_id}, cls=DecimalEncoder)
            }

        except KeyError as e:
            _LOG.error(f"Missing required reservation field: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps('Bad request: Missing required field', cls=DecimalEncoder)
            }
        except Exception as e:
            _LOG.error(f"Error creating reservation: {str(e)}")
            return {
                'statusCode': 400,
                'body': json.dumps('Unable to create reservation', cls=DecimalEncoder)
            }

    def get_reservations(self, event):
        try:
            table_name = os.environ.get('reservation_tables', 'Reservations')
            table = dynamodb.Table(table_name)

            # Perform the scan operation to retrieve all reservations
            scan_result = table.scan()

            # Serialize and then Deserialize the Items using DecimalEncoder
            # for handling Decimal objects correctly
            serialized_reservations = json.dumps(scan_result['Items'], cls=DecimalEncoder)
            reservations_data = json.loads(serialized_reservations)

            # Return the response with the list of formatted reservations using self.response
            return self.response(200, {"reservations": reservations_data})

        except Exception as e:
            _LOG.error(f"Error fetching reservations: {str(e)}")
            return self.response(400, 'Unable to fetch reservations')

HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
