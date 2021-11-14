import json
import zipfile

from data_parsing.lizaalert.parser import parse_archive_json, parse_okrug_json

data = None
row_data_zip = zipfile.ZipFile(r"../spider.zip", "r")
parsed_data_zip = zipfile.ZipFile(r"../parsed.zip", "w")

with row_data_zip.open("result.json", "r") as read_file:
    data = json.loads(read_file.read().decode("utf-8"))
js = parse_okrug_json(data)
with parsed_data_zip.open("okrug_parsed.json", "w") as write_file:
    write_file.write(json.dumps(js, ensure_ascii=False).encode("utf-8"))

with row_data_zip.open("result-2.json", "r") as read_file:
    data = json.loads(read_file.read().decode("utf-8"))
js = parse_archive_json(data)
with parsed_data_zip.open("archive_parsed.json", "w") as write_file:
    write_file.write(json.dumps(js, ensure_ascii=False).encode("utf-8"))
