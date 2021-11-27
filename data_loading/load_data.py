from datetime import datetime
import json
from json import decoder
from types import coroutine
import zipfile
from elasticsearch import Elasticsearch
from elasticsearch_dsl import GeoPoint
import certifi
import ssl
import geopy.geocoders
from geopy.geocoders import Nominatim

from var import mappingsElastic

ctx = ssl.create_default_context(cafile=certifi.where())
geopy.geocoders.options.default_ssl_context = ctx


es = Elasticsearch()

indices = ['topics']
es.delete_by_query(index=indices, body={"query": {"match_all": {}}}) # this will clean all data in db
es.indices.delete(index=indices) # this will drop index
es.indices.create(index=indices, body=mappingsElastic) # this will create index with mapping. Nesseccery to store coordinates as geo_point

parsed_data_zip = zipfile.ZipFile(r"parsed.zip", "r")

topics = None
with open("data.json", "r", encoding='utf-8') as read_file:
    topics = json.loads(read_file.read())


regions_coords = {}
for topic in topics:
    geolocator = Nominatim(scheme='http', user_agent="lizaalert", timeout = 100)
    locations = topic["Location"]
    newLocation = None
    for location in locations:
        if location["value"]["type"] in ["область", "регион", "республика"]:
            newLocation = location["value"]["name"] + " " + location["value"]["type"]
    if newLocation is None:
        for location in locations:
            if location["value"]["type"] in ["село", "город", "г.", "поселок", "п."]:
                newLocation = location["value"]["name"]
    topic["ShortLocation"] = newLocation
    print(newLocation)
    if newLocation in regions_coords:
        if newLocation is not None:
            coordinates = regions_coords[newLocation]
    else:
        if newLocation is not None:
            coordinates = geolocator.geocode(newLocation)
            regions_coords[newLocation] = coordinates
    if coordinates is not None:
        if coordinates.latitude > 40 and coordinates.longitude > 20 and coordinates.longitude < 180:
            topic["LocationCoordinates"] = f"{coordinates.latitude},{coordinates.longitude}"
    es.index(
        index='topics',
        document=topic
    )

print('Done data loading')
