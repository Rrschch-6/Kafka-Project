import time

import weather_consumer
from weather_producer import WeatherProducer
from weather_consumer import WeatherConsumer
from datetime import datetime
API_key = "69848dcc9764a2eb33d2281e6bf609ef"
city = "Heidelberg"
wp = WeatherProducer(city,API_key)
for month in range(1,13):
	wp.send(datetime(2020, month, 1))
	time.sleep(2)
