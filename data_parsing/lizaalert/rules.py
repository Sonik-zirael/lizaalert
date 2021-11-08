import json

from yargy import Parser, or_, and_, not_, rule
from yargy.interpretation import fact
from yargy.pipelines import morph_pipeline
from yargy.predicates import eq, gram, gte, lte, normalized, length_eq
from yargy.tokenizer import MorphTokenizer

TOKENIZER = MorphTokenizer()
DEAD = morph_pipeline(['погиб'])
ALIVE = morph_pipeline(['жив'])
FOUND = morph_pipeline(['найден'])
NOT_FOUND = morph_pipeline(['пропал', 'потерялся', 'не найден', 'поиск продолжается', 'стоп'])

Status = fact('Status', ['value'])

STATUS = rule(
    or_(
        DEAD.interpretation(Status.value.const('погиб')),
        ALIVE.interpretation(Status.value.const('жив')),
        NOT_FOUND.interpretation(Status.value.const('не найден')),
        FOUND.interpretation(Status.value.const('найден')),
    )
).interpretation(
    Status
)

Additional = fact('Fact', ['value'])

ADD = rule(
    or_(
        morph_pipeline(
            ['медпомощь', 'медицинская помощь', 'мед. помощь', 'проблемы со здоровьем', 'дезориентирован',
             'потеря памяти', 'склероз', 'деменция']).interpretation(
            Additional.value.const('нуждается в медпомощи')),
        morph_pipeline(['лес', 'грибы', 'пикник']).interpretation(Additional.value.const('пропал в лесу')),
        morph_pipeline(['с работы', 'на работу', 'после работы']).interpretation(
            Additional.value.const('не вернулся с работы')),
        morph_pipeline(['ссора', 'поругались', 'драка']).interpretation(
            Additional.value.const('пропал после конфликта')),
        morph_pipeline(
            ['озеро', 'море', 'рыбалка', 'плыл', 'судно', 'корабль', 'вода', 'лодка']).interpretation(
            Additional.value.const('пропал на воде')),
        morph_pipeline(['авиация', 'авиа', 'авиакоординатор', 'вертолёт']).interpretation(
            Additional.value.const('работает авиация'))
    )
).interpretation(
    Additional
)

Date = fact('Date', ['year', 'month', 'day', 'hours', 'minutes'])

MONTH = or_(
    morph_pipeline(['январь', 'янв']).interpretation(Date.month.const(1)),
    morph_pipeline(['февраль', 'фев']).interpretation(Date.month.const(2)),
    morph_pipeline(['март', 'мар']).interpretation(Date.month.const(3)),
    morph_pipeline(['апрель', 'апр']).interpretation(Date.month.const(4)),
    morph_pipeline(['май']).interpretation(Date.month.const(5)),
    morph_pipeline(['июнь', 'июн']).interpretation(Date.month.const(6)),
    morph_pipeline(['июль', 'июл']).interpretation(Date.month.const(7)),
    morph_pipeline(['август', 'авг']).interpretation(Date.month.const(8)),
    morph_pipeline(['сентябрь', 'сен']).interpretation(Date.month.const(9)),
    morph_pipeline(['октябрь', 'окт']).interpretation(Date.month.const(10)),
    morph_pipeline(['ноябрь', 'ноя']).interpretation(Date.month.const(11)),
    morph_pipeline(['декабрь', 'дек']).interpretation(Date.month.const(12)),
)

DAY = and_(
    gte(1),
    lte(31)
).interpretation(
    Date.day.custom(int)
)

YEAR_WORD = or_(
    rule('г', eq('.').optional()),
    rule(normalized('год'))
)

YEAR = and_(
    gte(1000),
    lte(2100)
).interpretation(
    Date.year.custom(int)
)

YEAR_SHORT = and_(
    length_eq(2),
    gte(0),
    lte(99)
).interpretation(
    Date.year.custom(lambda x: int(x) + (2000 if int(x) < 22 else 1900))
)

HOURS = and_(
    gte(0),
    lte(23)
).interpretation(
    Date.hours.custom(int)
)

MINUTES = and_(
    gte(0),
    lte(59)
).interpretation(
    Date.minutes.custom(int)
)

TIME = rule(
    morph_pipeline(['в', 'около', 'после', 'примерно в', 'приблизительно в', 'с']).optional(),
    HOURS,
    eq('часов').optional(),
    rule(or_(eq('.'), eq('-'), eq(':')).optional(), MINUTES, eq('минут').optional()).optional()
)

MISSED = morph_pipeline([
    'пропал',
    'ушёл',
    'уехал',
    'пошел',
    'вышел',
    'местонахождение',
    'местоположение',
    'не вернулся',
    'в последний раз',
    'нет информации',
    'связь',
    'неизвестно'
])

DATE = rule(
    TIME.optional(),
    or_(
        rule(
            DAY,
            or_(eq('.'), eq('-')),
            and_(gte(1), lte(12)).interpretation(Date.month.custom(int)),
            or_(eq('.'), eq('-')),
            or_(
                YEAR,
                YEAR_SHORT
            ),
            YEAR_WORD.optional()
        ),
        rule(
            DAY,
            MONTH,
            YEAR,
            YEAR_WORD.optional()
        ),
        rule(
            DAY,
            MONTH
        )
    ),
    TIME.optional(),
).interpretation(
    Date
)

DATE_MISSED = or_(
    rule(
        or_(MISSED, DEAD, ALIVE, FOUND),
        not_(gram('Abbr')).optional().repeatable(max=2),
        DATE
    ),
    rule(
        DATE,
        not_(gram('Abbr')).optional().repeatable(max=2),
        or_(MISSED, DEAD, ALIVE, FOUND)
    )
).interpretation(
    Date
)

start = fact('Start', ['value'])

VOLUNTEER = morph_pipeline(['сбор', 'выезд', 'поиск', 'штаб', 'на месте', 'дома', 'экипаж', 'работает']).interpretation(
    start)


def get_matches(rule, line):
    parser = Parser(rule)
    matches = parser.findall(line)
    js = []
    for match in matches:
        js.append(json.dumps(match.fact.as_json, ensure_ascii=False))
    return json.loads('[' + ", ".join(js) + ']')


def get_match(rule, line):
    parser = Parser(rule)
    match = parser.find(line)
    return json.loads(json.dumps(match.fact.as_json, ensure_ascii=False)) if match is not None else None
