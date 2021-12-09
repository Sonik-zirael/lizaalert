import json
from json.decoder import JSONDecodeError
import logging
import ssl
import zipfile
import argparse
from datetime import datetime

import certifi
import geopy.geocoders
from elasticsearch import Elasticsearch
from geopy.geocoders import Nominatim
from kafka.consumer.group import KafkaConsumer

from variables import mappingsElastic

ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx


es = Elasticsearch()

parser = argparse.ArgumentParser(description='This part manages data loading')
parser.add_argument('--mode', type=str, required=True,
                        choices=("kafka", "archive"),
                        help='Specifies in which mode program will be launched. '
                        'Set "kafka" to make program parse data provided by kafka. '
                        'Set "archive" to make program parse data in pre-packed archive.')
args = parser.parse_args()

if (args.mode) == 'archive':
    print('Work with archive')
    indices = ['topics']
    # es.delete_by_query(index=indices, body={"query": {"match_all": {}}})    # this will clean all data in db
    # es.indices.delete(index=indices)    # this will drop index
    es.indices.create(index=indices, body=mappingsElastic)  # this will create index with mapping. Nesseccery to store coordinates as geo_point

    parsed_data_zip = zipfile.ZipFile(r"../parsed.zip", "r")

    topics = None
    with parsed_data_zip.open("data.json", "r") as read_file:
        topics = json.loads(read_file.read().decode('utf-8'))

    regions_coords = {}
    for topic in topics:
        geolocator = Nominatim(scheme='http', user_agent="lizaalert", timeout = 100)
        locations = topic["Location"]
        newLocation = None
        for location in locations:
            if location["value"]["type"] in ["область", "регион", "республика", "край", "автономный округ"]:
                newLocation = location["value"]["name"] + " " + location["value"]["type"]
        if newLocation is None:
            for location in locations:
                if location["value"]["type"] in ["село", "город", "г.", "поселок", "п."]:
                    newLocation = location["value"]["name"]

        if newLocation not in regions_coords:
            if newLocation is not None:
                coordinates = geolocator.geocode(newLocation)
                region = None
                if coordinates is not None:
                    locationList = coordinates.address.split(', ')
                    if locationList[-1] == "Россия":
                        for i in range(len(locationList) - 1, 0, -1):
                            if "федеральный округ" in locationList[i]:
                                region = locationList[i - 1]
                                break
                    elif locationList[-1] in ["Беларусь", "Україна", "Қазақстан"]:
                        region = newLocation + ", " + locationList[-1]
                        for i in range(len(locationList) - 1, 0, -1):
                            if "область" in locationList[i] or "район" in locationList[i] or "округ" in locationList[i]:
                                region = locationList[i] + ", " + locationList[-1]
                                break
                    regions_coords[newLocation] = {
                        "region": region,
                        "LocationCoordinates": f"{coordinates.latitude},{coordinates.longitude}"
                    }
        if regions_coords.get(newLocation) is not None:
            topic["LocationCoordinates"] = regions_coords[newLocation]["LocationCoordinates"]
            topic["ShortLocation"] = regions_coords[newLocation]["region"]
            print(regions_coords[newLocation]["region"])
        es.index(
            index=indices,
            document=topic
        )
else:
    print('Work with kafka')
    indices = ['pipeline']
    if es.indices.exists(index=indices):
        es.delete_by_query(index=indices, body={"query": {"match_all": {}}})    # this will clean all data in db
        es.indices.delete(index=indices)    # this will drop index
    es.indices.create(index=indices, body=mappingsElastic)  # this will create index with mapping. Nesseccery to store coordinates as geo_point
    topic_name = "parsed_data_1"
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['localhost:9092'], api_version=(0, 10), consumer_timeout_ms=1000,
                             group_id='load_data_group')
    is_end_flag = False
    iterations_without_change = 0
    regions_coords = {}
    while True:
        iterations_without_change += 1
        if is_end_flag:
            break
        for message in consumer:
            consumer.commit()
            iterations_without_change = 0
            message_string = message.value.decode('utf-8')
            try:
                message_data = json.loads(message_string)
                is_end_flag = (message_data == 'end')
                if is_end_flag:
                    break
            except JSONDecodeError as jde:
                logging.warning('Received message %s in topic %s which is not in json format',
                                message_string, topic_name)
            message_data = message_data[0]
            geolocator = Nominatim(scheme='http', user_agent="lizaalert", timeout = 100)
            locations = message_data["Location"]
            newLocation = None
            for location in locations:
                if location["value"]["type"] in ["область", "регион", "республика", "край", "автономный округ"]:
                    newLocation = location["value"]["name"] + " " + location["value"]["type"]
            if newLocation is None:
                for location in locations:
                    if location["value"]["type"] in ["село", "город", "г.", "поселок", "п."]:
                        newLocation = location["value"]["name"]

            if newLocation not in regions_coords:
                if newLocation is not None:
                    coordinates = geolocator.geocode(newLocation)
                    region = None
                    if coordinates is not None:
                        locationList = coordinates.address.split(', ')
                        if locationList[-1] == "Россия":
                            for i in range(len(locationList) - 1, 0, -1):
                                if "федеральный округ" in locationList[i]:
                                    region = locationList[i - 1]
                                    break
                        elif locationList[-1] in ["Беларусь", "Україна", "Қазақстан"]:
                            region = newLocation + ", " + locationList[-1]
                            for i in range(len(locationList) - 1, 0, -1):
                                if "область" in locationList[i] or "район" in locationList[i] or "округ" in locationList[i]:
                                    region = locationList[i] + ", " + locationList[-1]
                                    break
                        regions_coords[newLocation] = {
                            "region": region,
                            "LocationCoordinates": f"{coordinates.latitude},{coordinates.longitude}"
                        }
            if regions_coords.get(newLocation) is not None:
                message_data["LocationCoordinates"] = regions_coords[newLocation]["LocationCoordinates"]
                message_data["ShortLocation"] = regions_coords[newLocation]["region"]
                print(regions_coords[newLocation]["region"])
            es.index(
                index=indices,
                document=message_data
            )
            print(message_data)

    consumer.commit()
    consumer.close()

print('Done data loading')
