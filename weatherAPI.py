from datetime import datetime

import requests
import time
class Weather_API:
    def __init__(self,city ,API_key):
        self.API_key = API_key
        self.city = city
        # self.parameters = {'q': self.city, 'units': 'metrics', 'appid': self.API_key}

    def get_weather(self,today):
        base = datetime(1979, 1, 1)
        ts = int((today - base).total_seconds())
        print("time",ts)
        # response = requests.get(self.url,self.parameters)
        url = f'https://api.openweathermap.org/data/2.5/weather?q={self.city}&APPID={self.API_key}&units=metrics&dt={ts}'
        response = requests.get(url)
        json_response = response.json()
        weather_description = json_response["weather"][0]['main']
        weather_temp = json_response['main']['temp']
        weather_humidity = json_response['main']['humidity']
        return (weather_temp,weather_description,weather_humidity)
