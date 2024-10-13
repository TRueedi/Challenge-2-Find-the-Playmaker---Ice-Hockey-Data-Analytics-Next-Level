import json
import math
from time import sleep

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
import sys

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os


confConsumer = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge2freespace',
        'auto.offset.reset': 'earliest'}
confConsumer2 = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge2freespace2',
        'auto.offset.reset': 'earliest'}

confProducer = {'bootstrap.servers': 'kafka-1:19092'}

producer = Producer(confProducer)
consumer = Consumer(confConsumer)
consumer2 = Consumer(confConsumer2)
Readtopic = ["Challenge2PlayersPosition"]
Readtopic2 = ["Challenge2MainPlayer"]
Writetopic = ["Challenge2MainPlayerSpace"]

running = True

def mean_distance_to_opponents(X,Y,Team, df: pd.DataFrame):

    distances_to_opponents = []
    main_player_team = ""
    opponent_team = ""

    if Team == 'Home':
        main_player_team = "Home"
        opponent_team = "Away"
    elif Team == 'Away':
        main_player_team = "Away"
        opponent_team = "Home"
    else:
        print("Error: The team is neither home nor away.")

    for i in range(1, 12):

        coords_primary = [X,Y]
        float_coords_primary = [float(coord) for coord in coords_primary]

        if df[f'StartPlayerTeam{i}'] == opponent_team:

            if type(df[f'StartPlayerCoordinates{i}']) == str:
                coords_opponent = df[f'StartPlayerCoordinates{i}'].split(',')

            elif type(df[f'StartPlayerCoordinates{i}']) == tuple:
                coords_opponent = df[f'StartPlayerCoordinates{i}']

            value = math.sqrt((float_coords_primary[0] - float(coords_opponent[0])) ** 2 + (float_coords_primary[1] - float(coords_opponent[1])) ** 2)
            distances_to_opponents.append(value)

    return sum(distances_to_opponents)/len(distances_to_opponents)

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
        tp = TopicPartition(topic=Readtopic[0], partition=0, offset=0)
        consumer.assign([tp])  # Assign the consumer to the specified partition
        consumer.seek(tp)  # Seek to the specified offset
        tp = TopicPartition(topic=Readtopic2[0], partition=0, offset=0)
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
            #Read the json into usable variable
            value = msg.value().decode("utf-8")
            message_data = json.loads(value)
            value2 = msg2.value().decode("utf-8")
            message_data2 = json.loads(value2)

            temp_df = pd.DataFrame({
                'StartPlayerName1': [message_data['StartPlayerName1']],
                'StartPlayerName2': [message_data['StartPlayerName2']],
                'StartPlayerName3': [message_data['StartPlayerName3']],
                'StartPlayerName4': [message_data['StartPlayerName4']],
                'StartPlayerName5': [message_data['StartPlayerName5']],
                'StartPlayerName6': [message_data['StartPlayerName6']],
                'StartPlayerName7': [message_data['StartPlayerName7']],
                'StartPlayerName8': [message_data['StartPlayerName8']],
                'StartPlayerName9': [message_data['StartPlayerName9']],
                'StartPlayerName10': [message_data['StartPlayerName10']],
                'StartPlayerName11': [message_data['StartPlayerName11']],
                'StartPlayerName12': [message_data['StartPlayerName12']]
            })
            mainPlayerN = 0
            c = 0
            for k,v in temp_df.iterrows():
                if v == message_data2['EventPrimaryPlayerName']:
                    mainPlayerN = c
                c += 1
            mainPlayerC = "StartPlayerCoordinates" + mainPlayerN

            dataMainPlayerC = message_data[mainPlayerC]
            print(dataMainPlayerC)
            print('testesttestestestestest')



            #mean_distance_to_opponents()

            if message_data["EventType"] == "Pass":
                write_data = json.dumps(message_data)
                
                #producer.produce(Writetopic[0], key=msg.key().decode("utf-8"), value=write_data, callback=delivery_report)  # Send message to Kafka
                #producer.poll(0)  # Trigger the delivery report callback
                #producer.flush()  # Ensure all messages are sent before exiting

            #print(f'key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, consumer2, Readtopic, Readtopic2, Writetopic)
