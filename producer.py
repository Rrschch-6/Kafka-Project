import json
from kafka import KafkaProducer

from kafka import KafkaConsumer
# from config.kafka_config import *
import json


def make_producer():
	producer = KafkaProducer(
		bootstrap_servers='kafka-ffee791-karampanah927-22f4.aivencloud.com:26471',
		security_protocol="SSL",
		ssl_cafile="ca.pem",
		ssl_certfile="service.cert",
		ssl_keyfile="service.key",
		value_serializer=lambda v: json.dumps(v).encode('utf-8'),
		key_serializer=lambda v: json.dumps(v).encode('utf-8')
	)
	return producer


producer = make_producer()
message_list = ["Prodigy jim math skills", "Powerline.io gain speed", "RuneScape diverse races",
                "NoBrakes.io boost your speed"]
for idx, msg in enumerate(message_list):
	message = "{} game".format(msg)
	print("sending:{}".format(message))
	producer.send(topic="game", key=idx,
	              value=message)
	print("message sent")
	producer.flush()

group_id = "game"

consumer = KafkaConsumer(
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

consumer.subscribe("game")
print("subscribed")
for message in consumer:
	print("recieved", message.value)
