from datetime import datetime
import json
from json import decoder
import zipfile
from elasticsearch import Elasticsearch

es = Elasticsearch()

parsed_data_zip = zipfile.ZipFile(r"parsed.zip", "r")

topics = None
with parsed_data_zip.open("okrug_parsed.json", "r") as read_file:
    topics = json.loads(read_file.read().decode("utf-8"))

for topic in topics:
    if isinstance(topic, list):
        print('aaaaaaaaaa')
        topic = topic[0]
    # print(topic)
    # es.index(
    #     index='topics',
    #     document=topic
    # )

print('Done data loading')
