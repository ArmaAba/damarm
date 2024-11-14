# open_meteo_client.py
import requests

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
