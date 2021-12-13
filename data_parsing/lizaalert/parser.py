import os
import sys
from datetime import datetime

from kafka.producer.kafka import KafkaProducer
from lizaalert.rules import *
from lizaalert.text_parser import *
import json
import pyspark
from joblib import Parallel, delayed
import logging
from pathlib import Path


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
    posts = 0
    users = []
    for message in post_data['posts'][1:]:
        posts += 1
        users.append(message['author']['username'])
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
        'Signs': signs(content),
        'AnswersNumber': posts,
        'UniqueUsers': len(set(users))
    }


def parse_general(data: dict,
                  process_type: str,
                  producer,
                  batch_size: int = 1000,
                  start_batch: int = 0,
                  batch_number: int = None,
                  n_proc: int = -2,
                  result_dir: str = '../tmp',
                  regions_as_keys: bool = True,
                  parsed_common_prefix: str = '',
                  spark_context: pyspark.SparkContext = None):
    Path(result_dir).mkdir(parents=True, exist_ok=True)
    error_batch = False
    process_data = []
    json_dict = []
    cur_batch = -1
    offset = 0
    for okrug, okrug_data in data.items():
        for region, region_data in okrug_data.items():
            if not regions_as_keys:
                okrug = None
                region = None

            for post, post_data in region_data.items():
                if (offset + 1) % batch_size == 0:
                    error_batch = False
                    cur_batch += 1
                # if (batch_number == -1 and offset < start_batch * batch_size) or error_batch or \
                #         batch_number != -1 and cur_batch >= start_batch + batch_number:
                #     offset += 1
                #     print('1')
                #     continue

                print('2')

                if process_type == "consistent":
                    # Без распараллеливания
                    try:
                        json_dict = Parallel(n_jobs=n_proc)(
                            delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region
                            in process_data)
                        batch_file_name = parsed_common_prefix + str(cur_batch) + '.json'
                        batch_file_path = os.path.join(result_dir, batch_file_name)
                        with open(batch_file_path, "w", encoding='utf-8') as out_file:
                            json.dump(json_dict, out_file, ensure_ascii=False, indent=4)
                        print('Send message to data loading consistent')
                        producer.send('parsed_data_1', json.dumps(json_dict).encode('utf-8'))
                        producer.flush()
                    except Exception as e:
                        logging.error("Batch {} was not parsed: {}".format(cur_batch, e))
                        json_dict.clear()
                        error_batch = True

                elif process_type == "parallel":
                    # Параллелим вычисления внутри пакета средствами Python и задействуем все cpu кроме одного
                    process_data.append((post, post_data, okrug, region))
                    if len(process_data) == batch_size:
                        try:
                            print(n_proc)
                            json_dict = Parallel(n_jobs=n_proc)(
                                delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region
                                in process_data)
                            batch_file_name = parsed_common_prefix + str(cur_batch) + '.json'
                            batch_file_path = os.path.join(result_dir, batch_file_name)
                            with open(batch_file_path, "w", encoding='utf-8') as out_file:
                                json.dump(json_dict, out_file, ensure_ascii=False, indent=4)
                            print('Send message to data loading parallel')
                            producer.send('parsed_data_1', json.dumps(json_dict).encode('utf-8'))
                            producer.flush()
                        except Exception as e:
                            logging.error("Batch {} was not parsed: {}".format(cur_batch, e))
                            process_data.clear()
                        process_data.clear()

                elif process_type == "spark" and spark_context is not None:
                    # Распараллеливание средствами Spark
                    process_data.append((post, post_data))
                    if len(process_data) == batch_size:
                        try:
                            json_dict = spark_context\
                                .parallelize(process_data)\
                                .map(lambda x: parallel_parsing(x[0], x[1], okrug, region))\
                                .collect()

                            batch_file_name = parsed_common_prefix + str(cur_batch) + '.json'
                            batch_file_path = os.path.join(result_dir, batch_file_name)
                            with open(batch_file_path, "w", encoding='utf-8') as out_file:
                                json.dump(json_dict, out_file, ensure_ascii=False, indent=4)
                        except Exception as e:
                            logging.error("Batch {} was not parsed: {}".format(cur_batch, e))
                            process_data.clear()
                        process_data.clear()

                offset += 1

    if process_type == "consistent" and json_dict:
        # Оставшийся неполный пакет без распараллеливания
        with open("../temp/okrug_parsed_{}.json".format(cur_batch + 1), "w") as write_file:
            write_file.write(json.dumps(json_dict, ensure_ascii=False))
        json_dict.clear()

    elif process_type == "parallel" and process_data:
        # Оставшийся неполный пакет с распараллеливанием средствами Python
        try:
            json_dict = Parallel(n_jobs=n_proc)(
                delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region in
                process_data)
            batch_file_name = parsed_common_prefix + str(cur_batch) + '.json'
            batch_file_path = os.path.join(result_dir, batch_file_name)
            with open(batch_file_path, "w", encoding='utf-8') as out_file:
                json.dump(json_dict, out_file, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error("Batch {} was not parsed: {}".format(cur_batch + 1, e))
        process_data.clear()

    elif process_type == "spark" and spark_context is not None and process_data:
        # Оставшийся неполный пакет с распараллеливанием с помощью Spark
        try:
            json_dict = spark_context \
                .parallelize(process_data) \
                .map(lambda x: parallel_parsing(x[0], x[1], okrug, region)) \
                .collect()

            batch_file_name = parsed_common_prefix + str(cur_batch) + '.json'
            batch_file_path = os.path.join(result_dir, batch_file_name)
            with open(batch_file_path, "w", encoding='utf-8') as out_file:
                json.dump(json_dict, out_file, ensure_ascii=False, indent=4)
        except Exception as e:
            logging.error("Batch {} was not parsed: {}".format(cur_batch + 1, e))
        process_data.clear()

    pass


# Just deprecated function
def parse_okrug_json(data, batch_size=1000, start_batch=0, batch_number=None):
    Path("../temp").mkdir(parents=True, exist_ok=True)
    error_batch = False
    process_data = []
    json_dict = []
    cur_batch = -1
    offset = 0
    for okrug, okrug_data in data.items():
        for region, region_data in okrug_data.items():
            for post, post_data in region_data.items():
                if (offset + 1) % batch_size == 0:
                    error_batch = False
                    cur_batch += 1
                if offset < start_batch * batch_size or error_batch or \
                        batch_number is not None and cur_batch >= start_batch + batch_number:
                    offset += 1
                    continue

                # Параллелим вычисления внутри пакета и задействуем все cpu кроме одного
                process_data.append((post, post_data, okrug, region))
                if len(process_data) == batch_size:
                    try:
                        json_dict = Parallel(n_jobs=-2)(
                            delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region
                            in process_data)
                        with open("../temp/okrug_parsed_{}.json".format(cur_batch), "w") as write_file:
                            write_file.write(json.dumps(json_dict, ensure_ascii=False))
                    except Exception as e:
                        logging.error("Okrug batch {} was not parsed: {}".format(cur_batch, e))
                        process_data.clear()
                    process_data.clear()

                # Без распараллеливания
                # try:
                #     json_dict.append(parallel_parsing(post, post_data, okrug, region))
                #     if (offset + 1) % batch_size == 0:
                #         with open("../temp/okrug_parsed_{}.json".format(cur_batch), "w") as write_file:
                #             write_file.write(json.dumps(json_dict, ensure_ascii=False))
                #         json_dict.clear()
                # except Exception as e:
                #     logging.error("Okrug batch {} was not parsed: {}".format(cur_batch, e))
                #     json_dict.clear()
                #     error_batch = True

                offset += 1

    # Оставшийся неполный пакет с распараллеливанием
    if process_data:
        try:
            json_dict = Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data, okrug, region in
                process_data)
            with open("../temp/okrug_parsed_{}.json".format(cur_batch + 1), "w") as write_file:
                write_file.write(json.dumps(json_dict, ensure_ascii=False))
        except Exception as e:
            logging.error("Okrug batch {} was not parsed: {}".format(cur_batch + 1, e))
        process_data.clear()

    # Оставшийся неполный пакет без распараллеливания
    # if json_dict:
    #     with open("../temp/okrug_parsed_{}.json".format(cur_batch + 1), "w") as write_file:
    #         write_file.write(json.dumps(json_dict, ensure_ascii=False))
    #     json_dict.clear()


# Just deprecated function
def parse_archive_json(data, batch_size=1000, start_batch=0, batch_number=None):
    Path("../temp").mkdir(parents=True, exist_ok=True)
    error_batch = False
    process_data = []
    json_dict = []
    cur_batch = -1
    offset = 0
    for section_1, section_data in data.items():
        for year, people_data in section_data.items():
            for post, post_data in people_data.items():
                if (offset + 1) % batch_size == 0:
                    error_batch = False
                    cur_batch += 1
                if offset < start_batch * batch_size or error_batch or \
                        batch_number is not None and cur_batch >= start_batch + batch_number:
                    offset += 1
                    continue

                # Параллелим вычисления внутри пакета и задействуем все cpu кроме одного
                process_data.append((post, post_data))
                if len(process_data) == batch_size:
                    try:
                        json_dict = Parallel(n_jobs=-2)(
                            delayed(parallel_parsing)(post, post_data) for post, post_data in process_data)
                        with open("../temp/archive_parsed_{}.json".format(cur_batch), "w") as write_file:
                            write_file.write(json.dumps(json_dict, ensure_ascii=False))
                    except Exception as e:
                        logging.error("Archive batch {} was not parsed: {}".format(cur_batch, e))
                        process_data.clear()
                    process_data.clear()

                # Без распараллеливания
                # try:
                #     json_dict.append(parallel_parsing(post, post_data))
                #     if (offset + 1) % batch_size == 0:
                #         with open("../temp/archive_parsed_{}.json".format(cur_batch), "w") as write_file:
                #             write_file.write(json.dumps(json_dict, ensure_ascii=False))
                #         json_dict.clear()
                # except Exception as e:
                #     logging.error("Archive batch {} was not parsed: {}".format(cur_batch, e))
                #     json_dict.clear()
                #     error_batch = True

                offset += 1

    # Оставшийся неполный пакет с распараллеливанием
    if process_data:
        try:
            json_dict = Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data) for post, post_data in process_data)
            with open("../temp/archive_parsed_{}.json".format(cur_batch + 1), "w") as write_file:
                write_file.write(json.dumps(json_dict, ensure_ascii=False))
        except Exception as e:
            logging.error("Archive batch {} was not parsed: {}".format(cur_batch + 1, e))
        process_data.clear()

    # Оставшийся неполный пакет без распараллеливания
    # if json_dict:
    #     with open("../temp/archive_parsed_{}.json".format(cur_batch + 1), "w") as write_file:
    #         write_file.write(json.dumps(json_dict, ensure_ascii=False))
    #     json_dict.clear()
