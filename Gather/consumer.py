#!/usr/bin/env python

import time
import sys
import json
import pandas as pd
import numpy as np
import data_helper
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
from datetime import datetime

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser["default"])
    config.update(config_parser["consumer"])

    # Create Consumer instance
    consumer = Consumer(config)

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        if args.reset:
            for p in partitions:
                p.offset = OFFSET_BEGINNING
            consumer.assign(partitions)

    # Subscribe to topic
    timestr = time.strftime("%Y%m%d-%H%M")
    topic = "bread-crumbs"
    consumer.subscribe([topic], on_assign=reset_offset)
    count = 0
    prev_count = count
    data = []
    #df = pd.DataFrame()
    # f = open(f'Records/output_{timestr}.json', 'a')
    log = open(f"Logs/messages_{timestr}.log", mode="w", encoding="utf-8")

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
                if prev_count < count:  # Update the dataframe if more data is added
                    if len(data) != 0:
                        df = pd.DataFrame(data)    # Create Dataframe from list of json objects
                        df = data_helper.create_dataframe(df)    # Create the new columns the dataframe
                        data_helper.data_assertions(df)    # Test the assertions on the dataframe
                        bread_df, trip_df = data_helper.data_splitter(df)    # Split the dataframe into two dataframes
                        #data_helper.drop_contraints()
                        data_helper.create_db(bread_df, trip_df)
                        #print(bread_df)
                        #print(trip_df)
                    prev_count = count
                    """with open(f"Logs/messages_{timestr}.log", mode="w", encoding="utf-8") as log:
                        log.write(f"Total numbers of kafta messages: {count}")
                        log.close()"""
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.
                data.append(json.loads(msg.value().decode("utf-8")))
                count += 1
                # print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                # topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
