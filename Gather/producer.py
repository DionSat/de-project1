import urllib.request
from datetime import datetime
import json
import sys
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import os

if __name__ == '__main__':
    #f = urllib.request.urlopen("http://www.psudataeng.com:8000/getBreadCrumbData")
    #data = json.load(f)
    #with urllib.request.urlopen('http://www.psudataeng.com:8000/getBreadCrumbData') as f:
        #data = f.read().decode('utf-8')

    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by sending sensor data one by one.
    f = open("/home/dion/environments/output.json", "r")
    data = json.load(f)
    topic = "sensor-data"

    count = 0

    for i in data:
        count += 1
        producer.poll(10)
        event_trip_id = i["EVENT_NO_TRIP"]
        sensor_d = json.dumps(i)
        producer.produce(topic, sensor_d, event_trip_id, callback=delivery_callback)
        if count % 10000 == 0 and count != 0:
            # Clearing the Screen
            os.system('clear')
            producer.poll(10000)
            producer.flush()

    # Block until the messages are sent.
    # producer.poll(10000)
    # producer.flush()
