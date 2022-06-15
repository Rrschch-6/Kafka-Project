import json
from kafka import KafkaProducer

from kafka import KafkaConsumer
# from config.kafka_config import *
import json
import pymongo as pym
from bson.code import Code
from pymongo import MongoClient
import logging
logging.basicConfig(level=logging.INFO) #,filename='myapp.log'

class Mongodb:
	def __init__(self):
		self.cluster = MongoClient(("mongodb+srv://fatemekaram:fatemehealia@cluster0.b5etoll.mongodb.net/?retryWrites=true&w=majority"))
		self.db = self.cluster["CSV_Pdstrian_data"]
		self.collection = self.db["Heidelberg_pedestrain"]

	def insert(self,record):
		self.collection.insert_one(record)

class Pdstrian_consumer:
	def __init__(self):
		group_id = "pedestrian_csv"

		self.consumer = KafkaConsumer(
			client_id="client1",
			group_id=group_id,
			bootstrap_servers='kafka-ffee791-karampanah927-22f4.aivencloud.com:26471',
			security_protocol="SSL",
			ssl_cafile="ca.pem",
			ssl_certfile="service.cert",
			ssl_keyfile="service.key",
			value_deserializer=lambda v: json.loads(v.decode('ascii')),
			key_deserializer=lambda v: json.loads(v.decode('ascii')),
			max_poll_records=10
		)
		self.consumer.subscribe("pedestrian_csv")
		logging.debug("subscribed")



	def recieve(self):
		mymongo = Mongodb()
		for message in self.consumer:
			logging.info("recieved", message.key,message.value)

			message_fileds = message.value.split(sep = " ")
			logging.info(message_fileds)
			msgdict = {"key":message.key,"location":message_fileds[0],"city":message_fileds[1],"time":" ".join(message_fileds[2:4]),"weekday":message_fileds[5],"pedestrians_count":message_fileds[6],"temperature":message_fileds[7],"condition":message_fileds[8]}

			msg_dump = json.dumps(msgdict)
			msg_json = json.loads(msg_dump)

			mymongo.insert(msg_json)
			logging.info(f"{message} inserted in database successfully\n")

		logging.info("recieving finished")



pd_consumer = Pdstrian_consumer()
pd_consumer.recieve()
