import json
import zipfile
import os

from data_parsing.lizaalert.parser import parse_archive_json, parse_okrug_json

data = None
row_data_zip = zipfile.ZipFile(r"../spider.zip", "r")
parsed_data_zip = zipfile.ZipFile(r"../parsed.zip", "w")

with row_data_zip.open("result.json", "r") as read_file:
    data = json.loads(read_file.read().decode("utf-8"))
parse_okrug_json(data, batch_size=20)
combined = []
for fname in list(filter(lambda x: x.startswith("okrug_parsed_"), os.listdir("../temp"))):
    with open("../temp/{}".format(fname), "r") as read_file:
        combined += json.loads(read_file.read())
with parsed_data_zip.open("okrug_parsed.json", "w") as write_file:
    write_file.write(json.dumps(combined, ensure_ascii=False).encode("utf-8"))

with row_data_zip.open("result-2.json", "r") as read_file:
    data = json.loads(read_file.read().decode("utf-8"))
parse_archive_json(data, batch_size=20)
combined = []
for fname in list(filter(lambda x: x.startswith("archive_parsed_"), os.listdir("../temp"))):
    with open("../temp/{}".format(fname), "r") as read_file:
        combined += json.loads(read_file.read())
with parsed_data_zip.open("archive_parsed.json", "w") as write_file:
    write_file.write(json.dumps(combined, ensure_ascii=False).encode("utf-8"))
