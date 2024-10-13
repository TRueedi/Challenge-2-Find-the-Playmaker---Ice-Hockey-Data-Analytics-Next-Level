import json
from time import sleep

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
import sys

import pandas as pd
from concurrent.futures import ThreadPoolExecutor
import time
import os


confConsumer = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge2hasPuck',
        'auto.offset.reset': 'earliest'}
confConsumer2 = {'bootstrap.servers': 'kafka-1:19092',
        'group.id': 'challenge2hasPuck2',
        'auto.offset.reset': 'earliest'}

confProducer = {'bootstrap.servers': 'kafka-1:19092'}

producer = Producer(confProducer)
producer2 = Producer(confProducer)
producer3 = Producer(confProducer)
consumer = Consumer(confConsumer)
consumer2 = Consumer(confConsumer2)
Readtopic = ["Challenge2GameDataCSV"]
Readtopic2 = ["Challenge2Memory"]
Writetopic = ["Challenge2MainPlayer"]
Writetopic2 = ["Challenge2Memory"]
Writetopic3 = ["Challenge2PlayersPosition"]

running = True

def delivery_report(err, msg):
    """ Callback called once Kafka acknowledges the message """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

def basic_consume_loop(consumer, consumer2, Readtopic, Readtopic2, Writetopic, Writetopic2, Writetopic3):
    try:
        #Delete the Offset where we are in the topic

        #for i in (0,1,2):
        tp = TopicPartition(topic=Readtopic[0], partition=0, offset=0)
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
            #df = message_data

            # each row has a column EventStartCoordinate and EventEndCoordinate which if applicable, contains the x and y coordinates of the event.
            # these columns need to be split into two columns, one for x and one for y
            #dfS['EventStartX', 'EventStartY'] = str(df['EventStartCoordinate']).split(',')
            #dfE['EventEndX', 'EventEndY'] = str(df['EventEndCoordinate']).split(',')
            #dff['EventStartX'] = pd.to_numeric(dfS['EventStartX'])
            #dff['EventStartY'] = pd.to_numeric(dfS['EventStartY'])
            #dff['EventEndX'] = pd.to_numeric(dfE['EventEndX'])
            #dff['EventEndY'] = pd.to_numeric(dfE['EventEndY'])

            result_df = pd.DataFrame(
                columns=['EventType', 'PuckControlState', 'EventPrimaryPlayerId', 'EventPrimaryPlayerName',
                         'EventSecondaryPlayerId','EventPrimaryTeam'])

            # Loop through the DataFrame
            i = 0
            success = 0
            fail = 0
            row = message_data


            # Correct the condition to check both 'AwayControl' and 'HomeControl'
            if row['PuckControlState'] in ['AwayControl', 'HomeControl']:
                # Create a temporary DataFrame for the current row
                if row['EventPrimaryPlayerName']:
                    temp_df = pd.DataFrame({
                        'EventType': [row['EventType']],
                        'PuckControlState': [row['PuckControlState']],
                        'EventPrimaryPlayerId': [row['EventPrimaryPlayerId']],
                        'EventPrimaryPlayerName': [row['EventPrimaryPlayerName']],
                        'EventPrimaryTeam': [row['EventPrimaryTeam']]
                    })

                # Use pd.concat() to append the row to result_df
                    result_df = temp_df

                if not row['EventPrimaryPlayerName']:
                    sleep(4)
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
                    # Read the json into usable variable
                    sleep(4)
                    value2 = msg2.value().decode("utf-8")
                    message_data2 = json.loads(value2)

                    row2 = message_data2[0]
                    temp_df = pd.DataFrame({
                        'EventType': [row['EventType']],
                        'PuckControlState': [row['PuckControlState']],
                        'EventPrimaryPlayerId': [row2['EventPrimaryPlayerId']],
                        'EventPrimaryPlayerName': [row2['EventPrimaryPlayerName']],
                        'EventPrimaryTeam': [row2['EventPrimaryTeam']]
                    })
                    result_df = temp_df


                i += 1  # Increment the counter for control events
                write_data = result_df.to_json(orient='records')
                producer.produce(Writetopic[0], key=msg.key().decode("utf-8"), value=write_data,callback=delivery_report)  # Send message to Kafka
                producer.poll(0)  # Trigger the delivery report callback
                producer.flush()  # Ensure all messages are sent before exiting
                write_data2 = json.dumps(row)
                producer3.produce(Writetopic3[0], key=msg.key().decode("utf-8"), value=write_data2,callback=delivery_report)  # Send message to Kafka
                producer3.poll(0)  # Trigger the delivery report callback
                producer3.flush()  # Ensure all messages are sent before exiting

                # Handle successful passes
            elif row['EventType'] == 'Pass' and row['PassResult'] == 'Successful':
                temp_df = pd.DataFrame({
                    'EventType': [row['EventType']],
                    'PuckControlState': [row['PuckControlState']],
                    'EventPrimaryPlayerId': [row['EventPrimaryPlayerId']],
                    'EventPrimaryPlayerName': [row['EventPrimaryPlayerName']],
                    'EventSecondaryPlayerId': [row['EventSecondaryPlayerId']],
                    # Successful pass, include secondary player
                    'EventSecondaryPlayerName': [row['EventSecondaryPlayerName']],
                    'EventPrimaryTeam': [row['EventPrimaryTeam']]
                })

                result_df = temp_df
                write_data = result_df.to_json(orient='records')
                success += 1  # Increment the counter for successful passes

                temp_df2 = pd.DataFrame({
                   'EventPrimaryPlayerId': [row['EventSecondaryPlayerId']],
                   'EventPrimaryPlayerName': [row['EventSecondaryPlayerName']],
                   'EventPrimaryTeam': [row['EventPrimaryTeam']]
                    })
                result_df2 = temp_df2
                write_data2 = result_df2.to_json(orient='records')
                producer2.produce(Writetopic2[0], key=msg.key().decode("utf-8"), value=write_data2,callback=delivery_report)  # Send message to Kafka
                producer2.poll(0)  # Trigger the delivery report callback
                producer2.flush()  # Ensure all messages are sent before exiting
                producer.produce(Writetopic[0], key=msg.key().decode("utf-8"), value=write_data,callback=delivery_report)  # Send message to Kafka
                producer.poll(0)  # Trigger the delivery report callback
                producer.flush()  # Ensure all messages are sent before exiting
                sleep(5)
                write_data3 = json.dumps(row)
                producer3.produce(Writetopic3[0], key=msg.key().decode("utf-8"), value=write_data3,callback=delivery_report)  # Send message to Kafka
                producer3.poll(0)  # Trigger the delivery report callback
                producer3.flush()  # Ensure all messages are sent before exiting

                # Handle failed passes
            elif row['EventType'] == 'Pass' and row['PassResult'] == 'Failed':
                temp_df = pd.DataFrame({
                    'EventType': [row['EventType']],
                    'PuckControlState': [row['PuckControlState']],
                    'EventPrimaryPlayerId': [row['EventPrimaryPlayerId']],
                    'EventPrimaryPlayerName': [row['EventPrimaryPlayerName']],
                    'EventSecondaryPlayerId': ['Null'],
                    'EventPrimaryTeam': [row['EventPrimaryTeam']]
                })
                fail = fail + 1
                write_data = result_df.to_json(orient='records')
                producer.produce(Writetopic[0], key=msg.key().decode("utf-8"), value=write_data,callback=delivery_report)  # Send message to Kafka
                producer.poll(0)  # Trigger the delivery report callback
                producer.flush()  # Ensure all messages are sent before exiting
                write_data2 = json.dumps(row)
                producer3.produce(Writetopic3[0], key=msg.key().decode("utf-8"), value=write_data2,callback=delivery_report)  # Send message to Kafka
                producer3.poll(0)  # Trigger the delivery report callback
                producer3.flush()  # Ensure all messages are sent before exiting

            #print(f'key: {msg.key().decode("utf-8")}, value: {msg.value().decode("utf-8")}')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(consumer, consumer2, Readtopic, Readtopic2, Writetopic, Writetopic2, Writetopic3)
