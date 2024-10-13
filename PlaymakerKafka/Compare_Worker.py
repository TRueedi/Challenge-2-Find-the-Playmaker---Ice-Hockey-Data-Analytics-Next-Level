import json
from time import sleep

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
import sys

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os


confConsumer = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge2',
        'auto.offset.reset': 'earliest'}

confProducer = {'bootstrap.servers': 'kafka-1:19092'}

producer = Producer(confProducer)
consumer = Consumer(confConsumer)
Readtopic = ["Challenge2PlaymakerPasser"]
Writetopic = ["Challenge2PlaymakerEvent"]

running = True

def delivery_report(err, msg):
    """ Callback called once Kafka acknowledges the message """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass



def shutdown():
    running = False


#temp_df = pd.DataFrame({
#    'EventPrimaryPlayerId': ['888052795'],
#    'EventPrimaryPlayerName': ['#69 Lukas Radil'],
#    'PlaymakerValue': ['14']
#})

temp_df2 = pd.DataFrame({
    'EventPrimaryPlayerId': ['888047300'],
    'EventPrimaryPlayerName': ['#90 Robert Kousal'],
    'PlaymakerValue': ['28']
})

#write_data = temp_df.to_json()
write_data2 = temp_df2.to_json()

#producer.produce(Writetopic[0], key="ttel_9", value=write_data, callback=delivery_report)  # Send message to Kafka
#producer.poll(0)  # Trigger the delivery report callback
#producer.flush()  # Ensure all messages are sent before exiting

producer.produce(Writetopic[0], key="ttel_9", value=write_data2, callback=delivery_report)  # Send message to Kafka
producer.poll(0)  # Trigger the delivery report callback
producer.flush()  # Ensure all messages are sent before exiting
