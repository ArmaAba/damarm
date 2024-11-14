from commons.log_helper import get_logger
from commons.abstract_lambda import AbstractLambda
import requests
import json

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
        # Extract the request path and method
        path = event.get("rawPath", "/")
        method = event["requestContext"]["http"]["method"]

        # Check if the method is GET and the path is '/weather'
        if method == "GET" and path == "/weather":
            # Default coordinates (e.g., Kyiv) if not provided in the request
            latitude = event.get('queryStringParameters', {}).get('latitude', '50.4375')
            longitude = event.get('queryStringParameters', {}).get('longitude', '30.5')

            weather_client = OpenMeteoClient()

            try:
                # Fetch the weather data using the OpenMeteoClient
                weather_data = weather_client.get_weather_forecast(latitude, longitude)

                # Construct a response in the specified format
                response_body = {
                    "latitude": latitude,
                    "longitude": longitude,
                    "generationtime_ms": weather_data.get("generationtime_ms", 0),
                    "utc_offset_seconds": weather_data.get("utc_offset_seconds", 0),
                    "timezone": weather_data.get("timezone", ""),
                    "timezone_abbreviation": weather_data.get("timezone_abbreviation", ""),
                    "elevation": weather_data.get("elevation", 0.0),
                    "hourly_units": weather_data.get("hourly_units", {}),
                    "hourly": weather_data.get("hourly", {}),
                    "current_units": {
                        "time": "iso8601",
                        "interval": "seconds",
                        "temperature_2m": "°C",
                        "wind_speed_10m": "km/h"
                    },
                    "current": {
                        "time": "2023-12-04T07:00",  # Example placeholder; replace with dynamic value if needed
                        "interval": 900,  # Example interval in seconds
                        "temperature_2m": weather_data.get("hourly", {}).get("temperature_2m", [0])[0],
                        # Example placeholder value
                        "wind_speed_10m": weather_data.get("hourly", {}).get("wind_speed_10m", [0])[0]
                        # Example placeholder value
                    }
                }

                response = {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps(response_body)
                }
                return response

            except Exception as e:
                error_response = {
                    "statusCode": 500,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "body": json.dumps({
                        "message": "Failed to fetch weather data",
                        "error": str(e)
                    })
                }
                return error_response

        else:
            # Return error response for unsupported paths or methods
            error_message = {
                "statusCode": 400,
                "message": f"Bad request syntax or unsupported method. Request path: {path}. HTTP method: {method}"
            }
            error_response = {
                "statusCode": 400,
                "headers": {
                    "Content-Type": "application/json"
                },
                "body": json.dumps(error_message)
            }
            return error_response


HANDLER = ApiHandler()


def lambda_handler(event, context):
    return HANDLER.lambda_handler(event=event, context=context)
