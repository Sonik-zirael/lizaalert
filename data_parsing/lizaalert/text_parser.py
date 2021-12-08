from yargy.token import is_morph_token

from lizaalert.grammar import *
from yargy import Parser
from yargy.pipelines import (
    morph_pipeline
)
import re


def name(text):
    parser = Parser(name_grammar())
    match = parser.find(text)
    m = None
    if match:
        m = match.fact.as_json
    return m


def location(text):
    parser = Parser(location_grammar())

    res = []
    for match in parser.findall(text):
        m = match.fact.as_json

        correct_location = True
        for _, od in m.items():
            # expect to have type and name
            if len(od.keys()) != 2:
                correct_location = False
                break

        if correct_location and m not in res:
            res.append(m)
    return res


def age(title, content):
    parser = Parser(age_grammar())
    match = parser.find(title)
    age = None
    if match:
        age = match.fact.as_json

    if not age:
        match = parser.find(content)
        if match:
            age = match.fact.as_json

    return age


def lost_date(text):
    parser = Parser(lost_date_grammar())
    match = parser.find(text)
    m = None
    if match:
        m = match.fact.as_json

    return m


def signs(text):
    parser = Parser(signs_grammar())
    res = []
    for match in parser.findall(text):
        m = format_sign_result(match.fact.as_json)
        res.append(m)
    return res


def gender(title):
    parser = Parser(search_grammar())
    match = parser.find(title)
    gender_ = None
    if match:
        for s in match.tokens:
            for form in s.forms:
                g = form.grams.gender
                if g.male:
                    gender_ = 'мужчина'
                elif g.female:
                    gender_ = 'женщина'

    if not gender_:
        gender_ = gender_by_name(title)
    return gender_


def gender_by_name(title):
    parser = Parser(name_grammar())
    match = parser.find(title)
    gender_ = None
    count_male = 0
    count_female = 0
    if match:
        for s in match.tokens:
            if not is_morph_token(s) or not s.forms:
                continue

            for form in s.forms:
                g = form.grams.gender
                if g.male:
                    count_male += 1
                elif g.female:
                    count_female += 1

    if count_female > count_male:
        gender_ = 'женщина'
    elif count_male > count_female:
        gender_ = 'мужчина'

    return gender_


def format_sign_result(match):
    clothes = morph_pipeline(['был одет'])
    parser = Parser(clothes)
    if parser.find(match['characteristic']):
        match['characteristic'] = 'Одежда'

    values = []
    for s in match['values'].split(','):
        values.append(s.strip().replace('.', ''))
    match['values'] = values

    more_signs = morph_pipeline(['примета'])
    parser = Parser(more_signs)
    if parser.find(match['characteristic']):
        match['characteristic'] = 'Приметы'
        match = parse_signs(match)

    return match


def parse_signs(match):
    height_rule, body_type_rule, hairs_rule, eye_rule = detailed_signs_grammar()

    parsed_res = []
    not_parsed = []

    for v in match['values']:
        parser = Parser(height_rule)

        height = parser.find(v)
        if height:
            parsed_res.append(height.fact.as_json)

        parser = Parser(body_type_rule)
        body_type = parser.find(v)
        if body_type:
            parsed_res.append(body_type.fact.as_json)

        parser = Parser(hairs_rule)
        hairs = parser.find(v)
        if hairs:
            parsed_res.append(hairs.fact.as_json)

        parser = Parser(eye_rule)
        eye = parser.find(v)
        if eye:
            parsed_res.append(eye.fact.as_json)

        if not(height or body_type or hairs or eye) and v != '':
            not_parsed.append(v)

    if len(not_parsed) != 0:
        match['values'] = not_parsed
        parsed_res.append(match)

    return parsed_res


def remove_stuff(text):
    formatted_lines = []
    for line in text.split(sep='\n'):
        if re.search(r'-{3,}', line):
            break

        words_and_number = re.search(r'\w+', line)
        info_org = re.search(r'инфорг', line.lower())
        phone_number = re.search(r'\d{10}', line.lower())
        coord = re.search(r'координатор', line.lower())
        orient = re.search(r'ориентир', line.lower())
        sbor = re.search(r'сбор(\s)*:', line.lower())
        headquarters = re.search(r'штаб(\s)*:', line.lower())
        if not words_and_number or info_org or phone_number or coord or orient or sbor or headquarters:
            continue

        line = re.sub(r'\[[^\]]+\]', '', line)
        line = re.sub(r'МО', 'Московская область', line)
        line = re.sub(r'ЛО', 'Ленинградская область', line)
        formatted_lines.append(line)
    text = '\n'.join(formatted_lines)
    return text
