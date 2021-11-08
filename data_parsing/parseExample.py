import json
import zipfile

from data_parsing.lizaalert.parser import parse_json

data = None
with zipfile.ZipFile(r"../spider.zip", "r").open("result.json", "r") as read_file:
    data = json.loads(read_file.read().decode("utf-8"))
js = parse_json(data)
with zipfile.ZipFile(r"../parsed.zip", "w").open("parsed.json", "w") as write_file:
    write_file.write(json.dumps(js, ensure_ascii=False).encode("utf-8"))
