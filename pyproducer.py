
import os
import requests
import json

from dotenv import load_dotenv
from fastapi import FastAPI
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from kafka.producer import KafkaProducer
from commands import CreateCommand

def weather_api_request():
  API_key=
  lat=33.44
  lon=-94.04
  url= f"https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&appid={API_key}"
  payload={}
  response= requests.request("GET", url, data=payload)
  return response.text

load_dotenv(verbose=True)

app = FastAPI()

#@app.on_event('startup')
#async def startup_event():
  #client = KafkaAdminClient(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'])
  #try:
   # topic = NewTopic(name=os.environ['TOPICS_NAME'],
    #                num_partitions=int(os.environ['TOPICS_PARTITIONS']),
     #               replication_factor=int(os.environ['TOPICS_REPLICAS']))
    #client.create_topics([topic])
  #except TopicAlreadyExistsError as e:
   # print(e)
  #finally:
   # client.close()


def make_producer():
  producer = KafkaProducer(bootstrap_servers=os.environ['BOOTSTRAP_SERVERS'],
                           security_protocol="SSL",
                           ssl_cafile="ca.pem",
                           ssl_certfile="service.cert",
                           ssl_keyfile="service.key"
                           )
  return producer

weather=[]
@app.post('/api/weather', status_code=201)
async def create_weather_status(cmd:CreateCommand):
  producer = make_producer()
  for i in range(cmd.count):
    data=weather_api_request()
    weather.append(data)
    producer.send(topic=os.environ['TOPICS_NAME'],
                key=f"{i}".encode('utf-8'),
                value=data.encode('utf-8'))
  producer.flush()

  return weather
