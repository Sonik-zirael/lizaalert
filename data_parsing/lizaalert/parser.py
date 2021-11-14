import time
from datetime import datetime
from data_parsing.lizaalert.rules import *
from data_parsing.lizaalert.text_parser import *
import json
from joblib import Parallel, delayed
from tqdm import tqdm


def parallel_parsing(post, post_data, okrug=None, region=None):
    first_message = post_data['posts'][0]
    title = post_data['title']
    content_array = first_message['contents']
    if okrug and region:
        content_array[0] += ', фо ' + okrug + ', ' + region
    content = '\n'.join(content_array)
    content = remove_stuff(content)
    published = datetime.fromisoformat(first_message['timestamp']).replace(tzinfo=None).isoformat()

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
            missed = datetime(year, matches.get('month'), matches.get('day'), hours, minutes).isoformat()
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
                date = datetime(year, match.get('month'), match.get('day'), hours, minutes).isoformat()
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
        'MissedDate': missed,
        'PublishedDate': published,
        'StartDate': start,
        'FoundDate': found,
        'Name': name(content),
        'Gender': gender(title),
        'Location': location(content),
        'Age': age(title, content),
        'Signs': signs(content)
    }


def parse_okrug_json(data):
    json_dict = []
    for okrug, okrug_data in tqdm(data.items()):
        for region, region_data in tqdm(okrug_data.items()):
            # Параллелим и задействуем все cpu кроме одного
            json_dict += Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data, okrug, region) for post, post_data in region_data.items())
            # for post, post_data in region_data.items():
            #     json_dict.append(parallel_parsing(post, post_data, okrug, region))
    return json.loads(json.dumps(json_dict, ensure_ascii=False, default=str))


def parse_archive_json(data):
    json_dict = []
    for section_1, section_data in tqdm(data.items()):
        for year, people_data in tqdm(section_data.items()):
            # Параллелим и задействуем все cpu кроме одного
            json_dict += Parallel(n_jobs=-2)(
                delayed(parallel_parsing)(post, post_data) for post, post_data in people_data.items())
            # for post, post_data in people_data.items():
            #     json_dict.append(parallel_parsing(post, post_data))
    return json.loads(json.dumps(json_dict, ensure_ascii=False, default=str))
