from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import requests

_LOG = get_logger('ApiHandler-handler')

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

class ApiHandler(AbstractLambda):

    def validate_request(self, event) -> dict:
        pass
        
    def handle_request(self, event, context):
        # Default coordinates (e.g., Berlin) if not provided in the event
        latitude = event.get('queryStringParameters', {}).get('latitude', '52.52')
        longitude = event.get('queryStringParameters', {}).get('longitude', '13.4050')

        weather_client = OpenMeteoClient()

        try:
            # Fetch the weather data using the OpenMeteoClient
            weather_data = weather_client.get_weather_forecast(latitude, longitude)
            return {
                "statusCode": 200,
                "body": {
                    "message": "Weather data retrieved successfully!",
                    "data": weather_data
                }
            }
        except Exception as e:
            return {
                "statusCode": 500,
                "body": {
                    "message": "Failed to fetch weather data",
                    "error": str(e)
                }
            }
    

HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
