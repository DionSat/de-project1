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
from bs4 import BeautifulSoup
import re
import os

def process_data(soup):
    trip_html = soup.find_all("h2")
    table_html = soup.find_all("table")
    # headers_html = soup.find_all("th")
    data_dict ={}
    stop_list = []
    final_list =[]

    # Get the list of headers and an additional trip_id header
    table_soup = BeautifulSoup(str(table_html[0]), "html.parser")
    header_html = table_soup.find_all("tr")
    row_soup = BeautifulSoup(str(header_html[0]), "html.parser")
    header_html = row_soup.find_all("th")
    str_cells = str(header_html)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '\"',str_cells))
    header_res = json.loads(clean2)
    header_res.append("trip_id")

    # Get the list of trip IDs
    str_cells = str(trip_html)
    clean = re.compile('<.*?>')
    clean2 = (re.sub(clean, '\"',str_cells))
    trip_res = json.loads(clean2)
    trip_ids = []
    for id in trip_res:
        res = re.sub('\D', '', id)
        trip_ids.append(res)

    # Create a list of the data rows with the trip ID added to the column
    for idx, table in enumerate(soup.find_all("table")):
        table_soup = BeautifulSoup(str(table), "html.parser")
        row_html = table_soup.find_all("tr")
        for row in row_html:
            row_td = row.find_all('td')
            str_cells = str(row_td)
            clean = re.compile('<.*?>')
            clean2 = (re.sub(clean, '\"',str_cells))
            data_res = json.loads(clean2)
            if len(data_res) != 0:
                data_res.append(trip_ids[idx])
                stop_list.append(data_res)

    # Convert list of lists into list of json or dicts so it fits the previous data format of Breadcrumbs
    for row in stop_list:
        for i, data in enumerate(row):
            data_dict[header_res[i]] = data
        final_list.append(data_dict)
        data_dict = {}

    return final_list

if __name__ == '__main__':
    with urllib.request.urlopen("http://www.psudataeng.com:8000/getStopEvents") as f:
        try:
            soup = BeautifulSoup(f, 'lxml')
        except:
            print("No Data")

    data = process_data(soup)

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
    topic = "stop-event"

    count = 0
    for i in data:
        count += 1
        sensor_d = json.dumps(i)
        producer.produce(topic, sensor_d, str(count), callback=delivery_callback)
        if count % 50000 == 0 and count != 0:
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
