from google.cloud import bigquery
from google.cloud import pubsub_v1
import os
import json

from pymongo import MongoClient

#credentials and cluster infor from Mongo db
cluster=MongoClient("mongodb+srv://Sasha:9eeJEgyGYkZPc6VY@cluster0.i2q1kde.mongodb.net/?retryWrites=true&w=majority")
db=cluster["API_data"]
collection=db["weather"]

#credentials and cluster infor from google cloud
credentials_path="heidelberg-wt-visualization-5ea0dc965455.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]=credentials_path
client = bigquery.Client()

# credentials for pubsub and topic variable define
publisher = pubsub_v1.PublisherClient()
PROJECT_ID = os.getenv('heidelberg-wt-visualization')
topic_path = 'projects/heidelberg-wt-visualization/topics/topicweather'

query_job_bigQuery = client.query(
    """
    SELECT MAX(_id) FROM heidelberg-wt-visualization.myproject.openweathertable;
"""
)
results = query_job_bigQuery.result()
#last timestamp in bigQuery table
last_id=results.__next__()[0]

query_job_mongoDB={"_id":{"$gt":last_id}}
results=collection.find(query_job_mongoDB)

pubsub_message=[]
for result in results:
    id=result['_id']
    sunrise=result['sunrise']
    sunset=result['sunset']
    temp=result['temp']
    feels_like=result['feels_like']
    pressure=result['pressure']
    humidity=result['humidity']
    dew_point=result['dew_point']
    uvi=result['uvi']
    clouds=result['clouds']
    visibility=result['visibility']
    wind_speed=result['wind_speed']
    wind_deg=result['wind_deg']

    message_json = json.dumps({
        "_id": id,
        "sunrise": sunrise,
        "sunset": sunset,
        "temp": temp,
        "feels_like": feels_like,
        "pressure": pressure,
        "humidity": humidity,
        "dew_point": dew_point,
        "uvi": uvi,
        "clouds": clouds,
        "visibility": visibility,
        "wind_speed": wind_speed,
        "wind_deg": wind_deg,
    })

    message_bytes = message_json.encode('utf-8')

    try:
        publish_future = publisher.publish(topic_path, data=message_bytes)
        publish_future.result()
    except Exception as e:
        print(e)

    print(id)
print("latest messages saved in bigQuary")
