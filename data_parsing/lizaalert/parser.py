import sys
import traceback
from datetime import datetime
from data_parsing.lizaalert.rules import *
from data_parsing.lizaalert.text_parser import *
import json
from joblib import Parallel, delayed
import logging
from pathlib import Path

logging.basicConfig(stream=sys.stdout, level=logging.WARNING)


def parallel_parsing(post, post_data, okrug=None, region=None):
    first_message = post_data['posts'][0]
    title = post_data['title']
    content_array = first_message['contents']
    if okrug and region and content_array:
        content_array[0] += ', фо ' + okrug + ', ' + region
    content = '\n'.join(content_array)
    content = remove_stuff(content)
    published = datetime.fromisoformat(first_message['timestamp']).replace(tzinfo=None)

    # Ищем дополнительную информацию о пропавшем
    matches = get_matches(ADD, " ".join(first_message['contents']))
    additional = None
    if len(matches) > 0:
        additional = list(set(map(lambda x: x.get('value'), matches)))

    # Статус: жив, погиб, не найден
    matches = get_match(STATUS, title)
    status = matches.get('value') if matches else None

    # Ищем когда пропал (где-то рядом с датой есть слова "пропал", "не вернулся" и т.д.)
    matches = get_match(DATE_MISSED, " ".join(first_message['contents']))
    missed = None
    if matches is not None and matches.get('day') is not None and matches.get('month') is not None:
        year = published.year if matches.get('year') is None else matches.get('year')
        hours = 0 if matches.get('hours') is None else matches.get('hours')
        minutes = 0 if matches.get('minutes') is None else matches.get('minutes')
        try:
            missed = datetime(year, matches.get('month'), matches.get('day'), hours, minutes)
        except:
            pass
    # Если дату пропажи не нашли, ищем все даты, которые есть в сообщении, и выбираем ту, которая ближе к
    # дате публикации
    if matches is None:
        matches = get_matches(DATE, " ".join(first_message['contents']))
        for match in matches:
            year = published.year if match.get('year') is None else match.get('year')
            hours = 0 if match.get('hours') is None else match.get('hours')
            minutes = 0 if match.get('minutes') is None else match.get('minutes')
            try:
                date = datetime(year, match.get('month'), match.get('day'), hours, minutes)
                if missed is None or published > date and published - date < published - missed:
                    missed = date
            except:
                pass

    found = None
    start = None
    for message in post_data['posts'][1:]:
        text = " ".join(message['contents'])
        # Ищем дату когда человека нашли
        if found is None:
            matches = get_match(STATUS, text)
            if matches is not None and matches.get('value') != 'не найден':
                found = datetime.fromisoformat(message['timestamp']).replace(tzinfo=None).isoformat()
        # Ищем дату начала поисков
        if start is None:
            matches = get_match(VOLUNTEER, text)
            if matches is not None:
                date = datetime.fromisoformat(message['timestamp']).replace(tzinfo=None).isoformat()
                start = date
    return {
        'URL': post,
        'Status': status,
        'Additional': additional,
        'MissedDate': None if missed is None else missed.isoformat(),
        'PublishedDate': published.isoformat(),
        'StartDate': start,
        'FoundDate': found,
        'Name': name(content),
        'Gender': gender(title),
        'Location': location(content),
        'Age': age(title, content),
        'Signs': signs(content)
    }


def parse_okrug_json(data, batch_size=1000, start_batch=0, batch_number=None):
    Path("../temp").mkdir(parents=True, exist_ok=True)
    process_data = []
    for okrug, okrug_data in data.items():
        for region, region_data in okrug_data.items():
            process_data += [d + (okrug, region) for d in region_data.items()]
    offset = start_batch * batch_size
    cur_batch = start_batch
    logging.info("Start parsing okrug")
    json_dict = []
    while offset < len(process_data) and (batch_number is None or cur_batch - start_batch < batch_number):
        # Параллелим вычисления внутри пакета и задействуем все cpu кроме одного
        try:
            json_dict = Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region in
                process_data[offset:offset + batch_size])
            with open("../temp/okrug_parsed_{}.json".format(cur_batch), "w") as write_file:
                write_file.write(json.dumps(json_dict, ensure_ascii=False))
        except Exception as e:
            logging.error("Okrug batch {} was not parsed: {}".format(cur_batch, e))
        offset += batch_size
        cur_batch += 1
        # Без распараллеливания
        # try:
        #     json_dict.append(parallel_parsing(*process_data[offset]))
        #     if (offset + 1) % batch_size == 0 or offset == len(process_data) - 1:
        #         with open("../temp/okrug_parsed_{}.json".format(cur_batch), "w") as write_file:
        #             write_file.write(json.dumps(json_dict, ensure_ascii=False))
        #         json_dict.clear()
        #         cur_batch += 1
        # except Exception as e:
        #     logging.error("Okrug batch {} was not parsed: {}".format(cur_batch, e))
        # offset += 1


def parse_archive_json(data, batch_size=1000, start_batch=0, batch_number=None):
    Path("../temp").mkdir(parents=True, exist_ok=True)
    process_data = []
    for section_1, section_data in data.items():
        for year, people_data in section_data.items():
            process_data += people_data.items()
    offset = start_batch * batch_size
    cur_batch = start_batch
    logging.info("Start parsing achieve")
    json_dict = []
    while offset < len(process_data) and (batch_number is None or cur_batch - start_batch < batch_number):
        # Параллелим вычисления внутри пакета и задействуем все cpu кроме одного
        try:
            json_dict = Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data) for post, post_data in
                process_data[offset:offset + batch_size])
            with open("../temp/archive_parsed_{}.json".format(cur_batch), "w") as write_file:
                write_file.write(json.dumps(json_dict, ensure_ascii=False))
        except Exception as e:
            logging.error("Archive batch {} was not parsed: {}".format(cur_batch, e))
        offset += batch_size
        cur_batch += 1
        # Без распараллеливания
        # try:
        #     json_dict.append(parallel_parsing(*process_data[offset]))
        #     if (offset + 1) % batch_size == 0 or offset == len(process_data) - 1:
        #         with open("../temp/archive_parsed_{}.json".format(cur_batch), "w") as write_file:
        #             write_file.write(json.dumps(json_dict, ensure_ascii=False))
        #         json_dict.clear()
        #         cur_batch += 1
        # except Exception as e:
        #     logging.error("Archive batch {} was not parsed: {}".format(cur_batch, e))
        # offset += 1
