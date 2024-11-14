from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import requests
import boto3
import os
import uuid


_LOG = get_logger('Processor-handler')
dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("table", "cmtr-f7e4afc6-Weather-test")
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

            # Extract and structure data according to the schema
            item = {
                'id': str(uuid.uuid4()),  # Unique identifier for the entry
                'forecast': {
                    'elevation': weather_data.get('elevation', 0),
                    'generationtime_ms': weather_data.get('generationtime_ms', 0),
                    'hourly': {
                        'temperature_2m': weather_data.get('hourly', {}).get('temperature_2m', []),
                        'time': weather_data.get('hourly', {}).get('time', [])
                    },
                    'hourly_units': {
                        'temperature_2m': weather_data.get('hourly_units', {}).get('temperature_2m', ''),
                        'time': weather_data.get('hourly_units', {}).get('time', '')
                    },
                    'latitude': float(latitude),
                    'longitude': float(longitude),
                    'timezone': weather_data.get('timezone', ''),
                    'timezone_abbreviation': weather_data.get('timezone_abbreviation', ''),
                    'utc_offset_seconds': weather_data.get('utc_offset_seconds', 0)
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
