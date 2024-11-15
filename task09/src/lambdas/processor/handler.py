from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import os
import requests
import boto3
import uuid
from decimal import Decimal


_LOG = get_logger('Processor-handler')
dynamodb = boto3.resource("dynamodb")
os.environ["target_table"] = "cmtr-f7e4afc6-Weather-test"
table_name = os.environ.get("target_table")
weather_table = dynamodb.Table(table_name)


class OpenMeteoClient:
    def __init__(self):
        self.base_url = 'https://api.open-meteo.com/v1/forecast'

    def get_weather_forecast(self, latitude, longitude):
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'hourly': 'temperature_2m'
        }
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()  # Raise an error for bad status codes
            return response.json()
        except requests.RequestException as e:
            print(f"Error fetching weather data: {e}")
            raise

class Processor(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass

    def handle_request(self, event, context):
        # Extract latitude and longitude from event or use default values
        latitude = event.get('queryStringParameters', {}).get('latitude', '50.4375')
        longitude = event.get('queryStringParameters', {}).get('longitude', '30.5')

        # Instantiate the weather client and fetch weather data
        weather_client = OpenMeteoClient()
        try:
            weather_data = weather_client.get_weather_forecast(latitude, longitude)

            # Convert latitude and longitude to Decimal (not float)
            latitude = Decimal(latitude)
            longitude = Decimal(longitude)

            # Convert float fields to Decimal recursively in the weather data
            def convert_floats(obj):
                if isinstance(obj, list):
                    return [convert_floats(i) for i in obj]
                elif isinstance(obj, dict):
                    return {k: convert_floats(v) for k, v in obj.items()}
                elif isinstance(obj, float):
                    return Decimal(str(obj))  # Convert float to Decimal
                return obj

            weather_data = convert_floats(weather_data)

            # Extract and structure data according to the schema
            item = {
                'id': str(uuid.uuid4()),  # Unique identifier for the entry
                'forecast': {
                    'elevation': weather_data.get('elevation', Decimal(0)),
                    'generationtime_ms': weather_data.get('generationtime_ms', Decimal(0)),
                    'hourly': {
                        'temperature_2m': weather_data.get('hourly', {}).get('temperature_2m', []),
                        'time': weather_data.get('hourly', {}).get('time', [])
                    },
                    'hourly_units': {
                        'temperature_2m': weather_data.get('hourly_units', {}).get('temperature_2m', ''),
                        'time': weather_data.get('hourly_units', {}).get('time', '')
                    },
                    'latitude': latitude,  # Make sure to use Decimal here
                    'longitude': longitude,  # And here
                    'timezone': weather_data.get('timezone', ''),
                    'timezone_abbreviation': weather_data.get('timezone_abbreviation', ''),
                    'utc_offset_seconds': weather_data.get('utc_offset_seconds', Decimal(0))
                }
            }
            # Insert the item into the DynamoDB table
            weather_table.put_item(Item=item)

            # Return a success response
            return {
                "statusCode": 200,
                "body": {
                    "message": "Weather data retrieved and stored successfully",
                    "data": item
                }
            }

        except Exception as e:
            # Handle errors and return a failure response
            error_message = f"Failed to fetch and store weather data: {str(e)}"
            _LOG.error(error_message)
            return {
                "statusCode": 500,
                "body": {
                    "message": "Internal server error",
                    "error": error_message
                }
            }


HANDLER = Processor()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
