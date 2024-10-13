import pandas as pd
from confluent_kafka import Producer
from concurrent.futures import ThreadPoolExecutor
import time
import os

folder_path = os.getcwd() + "/data/CleanData"

# Kafka configuration
# 47.254.134.189
kafka_config = {
    'bootstrap.servers': 'kafka-1:19092',  # Update with your Kafka broker address
}

# Initialize the Kafka producer
producer = Producer(kafka_config)

def delivery_report(err, msg):
    """ Callback called once Kafka acknowledges the message """
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        #print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        pass

def read_and_send_messages(csv_file, topic):
    """ Read CSV and send messages to Kafka """
    print(folder_path)
    file_path = os.path.join(folder_path, csv_file)
    df = pd.read_csv(file_path)  # Read CSV file
    temp_df = pd.DataFrame({
        'EventPrimaryPlayerId': ['888052795'],
        'EventPrimaryPlayerName': ['#69 Lukas Radil'],
        'PlaymakerValue': ['27']
    })

    write_data = temp_df.to_json()

    for _, row in df.iterrows():
        message = row.to_json()  # Convert row to JSON string
        #print(f"Sending message: {message}, Key: {csv_file[5:11]}")
        producer.produce(topic, key=csv_file[5:11], value=message, callback=delivery_report)  # Send message to Kafka
        producer.poll(0)  # Trigger the delivery report callback
        producer.flush()  # Ensure all messages are sent before exiting

# Ensure all messages are sent before exiting

if __name__ == "__main__":
    # List of CSV files
    csv_files = ["2_Drittel_988_1001.csv"]
    kafka_topic = "Challenge2GameDataCSV"  # Kafka topic to send the messages to

    # Send messages with a time difference between starting each CSV file
    read_and_send_messages(csv_files[0], kafka_topic)