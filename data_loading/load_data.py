import json
import ssl
import zipfile
from datetime import datetime

import certifi
import geopy.geocoders
from elasticsearch import Elasticsearch
from geopy.geocoders import Nominatim

from variables import mappingsElastic

ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx


es = Elasticsearch()

indices = ['topics']
es.delete_by_query(index=indices, body={"query": {"match_all": {}}})    # this will clean all data in db
es.indices.delete(index=indices)    # this will drop index
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

print('Done data loading')
