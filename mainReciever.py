import weather_consumer
from weather_producer import WeatherProducer
from weather_consumer import WeatherConsumer
from datetime import datetime
API_key = "69848dcc9764a2eb33d2281e6bf609ef"
city = "Heidelberg"
wc = WeatherConsumer()
wc.recieve()

