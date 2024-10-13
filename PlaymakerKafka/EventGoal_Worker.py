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
consumer2 = Consumer(confConsumer)
Readtopic = ["Challange2GameDataCSV"]
Readtopic2 = ["Challenge2Situation"]
Writetopic = ["Challenge2PlaymakerMatch"]

running = True

def delivery_report(err, msg):
    """ Callback called once Kafka acknowledges the message """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

def basic_consume_loop(consumer, consumer2, Readtopic, Readtopic2, Writetopic):
    try:
        #Delete the Offset where we are in the topic
        tp = TopicPartition(topic=Readtopic[0], partition=2, offset=0)
        consumer.assign([tp])  # Assign the consumer to the specified partition
        consumer.seek(tp)  # Seek to the specified offset
        sleep(10)


        consumer.subscribe(Readtopic)
        consumer2.subscribe(Readtopic2)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                pass
            #Read the json into usable variable
            value = msg.value().decode("utf-8")
            message_data = json.loads(value)


            if message_data["EventType"] == "Goal":
                msg2 = consumer2.poll(timeout=1.0)
                if msg2 is None:
                    continue

                if msg2.error():
                    if msg2.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg2.topic(), msg2.partition(), msg2.offset()))
                    elif msg2.error():
                        raise KafkaException(msg2.error())
                else:
                    pass
                value2 = msg2.value().decode("utf-8")
                message_data2 = json.loads(value2)




                write_data = json.dumps(message_data)
                
                producer.produce(Writetopic[0], key=msg.key().decode("utf-8"), value=write_data, callback=delivery_report)  # Send message to Kafka
                producer.poll(0)  # Trigger the delivery report callback
                producer.flush()  # Ensure all messages are sent before exiting

            #print(f'key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, consumer2, Readtopic, Readtopic2, Writetopic)
