import requests
import urllib.request
from datetime import datetime
import json
import sys
import time
from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer
import os

if __name__ == '__main__':
    with urllib.request.urlopen("http://www.psudataeng.com:8000/getBreadCrumbData") as f:
        try:
            data = json.loads(f.read().decode('utf-8'))
        except:
            print("No Data")

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
    topic = "bread-crumbs"

    count = 0
    for i in data:
        count += 1
        sensor_d = json.dumps(i)
        producer.produce(topic, sensor_d, str(count), callback=delivery_callback)
        if count % 100000 == 0 and count != 0:
            # Clearing the Screen
            os.system('clear')
            producer.poll(10000)
            producer.flush()

    # Block until the messages are sent.
    # producer.poll(10000)
    # producer.flush()
    timestr = time.strftime("%Y%m%d-%H%M")
    log = open(f"Logs/sensor_{timestr}.log", "w")
    log.write(f"Breadcrumbs all produced. There were: {count} sensor readings")
    log.close()
