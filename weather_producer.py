import json
from kafka import KafkaProducer

from kafka import KafkaConsumer
# from config.kafka_config import *
import json
from weatherAPI import Weather_API

class WeatherProducer:
	def __init__(self,city,API_key):
		self.producer = KafkaProducer(
			bootstrap_servers='kafka-ffee791-karampanah927-22f4.aivencloud.com:26471',
			security_protocol="SSL",
			ssl_cafile="ca.pem",
			ssl_certfile="service.cert",
			ssl_keyfile="service.key",
			value_serializer=lambda v: json.dumps(v).encode('utf-8'),
			key_serializer=lambda v: json.dumps(v).encode('utf-8') )

		self.weather = Weather_API(city, API_key)


	def send(self,date):
		data = self.weather.get_weather(date)
		self.producer.send(topic="weather_API", key=str(date), value=data)
		print(f"weather for date {date} was sent")
		self.producer.flush()
