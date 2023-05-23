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
from loguru import logger

if __name__ == "__main__":
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument("config_file", type=FileType("r"))
    parser.add_argument("--reset", action="store_true")
    parser.add_argument("-d", "--delete", action="store_true", help="Delete the tables in postgres")
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    #config_parser.read_file(args.config)
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
    topic = "stop-event"
    consumer.subscribe([topic], on_assign=reset_offset)
    count = 0
    prev_count = count
    data = []
    count_inserts = 0
    prev_stop = 0
    stop_count = 0

    # If delete table option
    if args.delete:
        data_helper.stop_delete_db()

    # Poll for new messages from Kafka and print them.
    try:
        logger.add(f"Logs/output_{timestr}.log")
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
                        df = data_helper.create_stop_dataframe(df)    # Create the new columns the dataframe
                        data_helper.stop_data_assertions(df)    # Test the assertions on the dataframe
                        stop_df = df
                        prev_stop = data_helper.db_stop_rowcount()
                        if data_helper.check_stop_table():
                            stop_count = data_helper.insert_stop_db(stop_df)
                        else:
                            stop_count = data_helper.create_stop_db(stop_df)
                    prev_count = count
                    stop_count = stop_count - prev_stop
                    logger.success(f"{stop_count} rows were inserted in the stop table.")
                    logger.success(f"Total consumed messages: {count}")
                    """with open(f"Logs/messages_{timestr}.log", mode="w", encoding="utf-8") as log:
                        log.write(f"{stop_count} rows were inserted in the stop table.")
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
