import json
import logging
import os

from dotenv import load_dotenv
from kafka.consumer import KafkaConsumer

from pymongo import MongoClient

cluster=MongoClient("mongodb+srv://Sasha:9eeJEgyGYkZPc6VY@cluster0.i2q1kde.mongodb.net/?retryWrites=true&w=majority")
db=cluster["API_data"]
collection=db["weather"]

load_dotenv(verbose=True)

def weather_key_deserializer(key):
  return key.decode('utf-8')

def weather_value_deserializer(value):
  return json.loads(value.decode('utf-8'))


def main():
  consumer = KafkaConsumer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
                          group_id=os.environ['CONSUMER_GROUP'],
                          key_deserializer=weather_key_deserializer,
                          value_deserializer=weather_value_deserializer,
                          enable_auto_commit=False,
                          security_protocol="SSL",
                           ssl_cafile="ca.pem",
                           ssl_certfile="service.cert",
                           ssl_keyfile="service.key")

  consumer.subscribe([os.environ['TOPICS_NAME']])
  for record in consumer:
      collection.insert_one(record.value)




if __name__ == '__main__':
  main()
