from yargy import (
    and_, not_
)
from yargy.relations import gnc_relation
from yargy.predicates import (
    normalized,
    dictionary,
    gte, lte,
    length_eq, caseless,
    is_capitalized
)
from yargy.interpretation import (
    fact,
    attribute
)
from lizaalert.constants import *


def name_grammar():
    gnc = gnc_relation()  # согласование по gender, number и case (падежу, числу и роду)

    Name = fact(
        'Name',
        ['first', 'last', 'middle']
    )

    STOP_WORDS = or_(
        normalized('пропал'),
        normalized('жив'),
        normalized('найден'),
        normalized('погиб'),
    )
    NAME = rule(and_(RU_WORD, is_capitalized(), not_(STOP_WORDS)))
    FIRST = NAME \
        .interpretation(Name.first) \
        .match(gnc)

    SURN = and_(RU_WORD, is_capitalized(), not_(STOP_WORDS))
    MAIDEN_LAST = rule(eq('('), SURN, eq(')'))
    LAST = rule(SURN, MAIDEN_LAST.optional()) \
        .interpretation(Name.last)

    PATR = rule(and_(RU_WORD, is_capitalized()))
    PATH_NOT_RUSSIAN = rule(PATR, eq('-'), PATR)
    MIDDLE = or_(
        PATH_NOT_RUSSIAN,
        PATR
    ) \
        .interpretation(Name.middle) \
        .match(gnc)

    FULL_NAME = or_(
        rule(LAST, FIRST, MIDDLE.optional()),
        rule(FIRST, LAST, MIDDLE.optional())
    ) \
        .interpretation(Name)

    return FULL_NAME


def location_grammar():
    from natasha.grammars.addr import \
        COUNTRY, FED_OKRUG, RESPUBLIKA, \
        KRAI, OBLAST, AUTO_OKRUG, \
        GOROD, DEREVNYA, SELO, \
        POSELOK, STREET,PROSPEKT, \
        PROEZD, PEREULOK, PLOSHAD, \
        SHOSSE, NABEREG, BULVAR, \
        DOM, KORPUS, STROENIE, \
        OFIS, KVARTIRA, RAION, \
        AddrPart, SETTLEMENT_NAME, Settlement, DOT
    HOUTOR_WORDS = or_(
        rule(
            caseless('x'),
            DOT.optional()
        ),
        rule(normalized('xутор'))
    ).interpretation(
        Settlement.type.const('хутор')
    )

    HOUTOR_NAME = SETTLEMENT_NAME.interpretation(
        Settlement.name
    )

    HOUTOR = rule(
        HOUTOR_WORDS,
        HOUTOR_NAME
    ).interpretation(
        Settlement
    )
    ADDR_PART = or_(
        COUNTRY,
        FED_OKRUG,

        RESPUBLIKA,
        KRAI,
        OBLAST,
        AUTO_OKRUG,

        RAION,
        GOROD,
        DEREVNYA,
        SELO,
        POSELOK,
        HOUTOR,

        STREET,
        PROSPEKT,
        PROEZD,
        PEREULOK,
        PLOSHAD,
        SHOSSE,
        NABEREG,
        BULVAR,

        DOM,
        KORPUS,
        STROENIE,
        OFIS,
        KVARTIRA
    ).interpretation(
        AddrPart.value
    ).interpretation(
        AddrPart
    )
    return ADDR_PART


def age_grammar():
    Age = fact(
        'Age',
        ['years']
    )
    YEARS = and_(
        gte(0),
        lte(150)
    ) \
        .interpretation(Age.years.custom(int))

    AGE = rule(
        name_grammar(),
        ANY_WORD.repeatable().optional(),
        YEARS,
        normalized('лет').optional()
    ) \
        .interpretation(Age)

    return AGE


def lost_date_grammar():
    LostDate = fact(
        'LostDate',
        ['year', 'month', 'day']
    )
    MONTHS = {
        'январь',
        'февраль',
        'март',
        'апрель',
        'мая',
        'июнь',
        'июль',
        'август',
        'сентябрь',
        'октябрь',
        'ноябрь',
        'декабрь'
    }
    MONTH_NAME = dictionary(MONTHS) \
        .interpretation(LostDate.month.normalized())
    MONTH = and_(
        gte(1),
        lte(12)
    ) \
        .interpretation(LostDate.month.custom(int))
    DAY = and_(
        gte(1),
        lte(31)
    ) \
        .interpretation(LostDate.day.custom(int))

    YEAR_WORD = or_(
        rule('г', eq('.').optional()),
        rule(normalized('год'))
    )

    YEAR = and_(
        gte(1000),
        lte(2100)
    ) \
        .interpretation(LostDate.year.custom(int))

    YEAR_SHORT = and_(
        length_eq(2),
        gte(0),
        lte(99)
    ) \
        .interpretation(LostDate.year.custom(lambda _: 1900 + int(_)))
    LOST_WORD = or_(
        rule(normalized('ушел')),
        rule(normalized('ушёл')),
        rule(normalized('ехал')),
        rule(normalized('вышел')),
        rule(normalized('уехал')),
        rule(normalized('пропал')),
        rule(normalized('больница')),
        rule(normalized('лечебное'), normalized('учереждение')),
        rule(normalized('неизвестно')),
        rule(normalized('находится')),
        rule(normalized('дата'), normalized('пропажи')),
        rule(normalized('место'), normalized('пропажи')),
        rule(normalized('не'), normalized('вернулся')),
    )

    DATE = or_(
        rule(
            DAY,
            '.',
            MONTH,
            '.',
            or_(
                YEAR,
                YEAR_SHORT
            ),
            YEAR_WORD.optional()
        ),
        rule(
            DAY,
            MONTH_NAME,
            YEAR,
            YEAR_WORD.optional()
        ),
        rule(
            DAY,
            MONTH_NAME
        ),
        rule(
            MONTH_NAME,
            YEAR,
            YEAR_WORD.optional()
        ),
    )

    LOST_DATE = or_(
        rule(LOST_WORD, ANY_WORD.optional().repeatable(), DATE),
        rule(DATE, ANY_WORD.optional().repeatable(), LOST_WORD)
    ).interpretation(
        LostDate
    )
    return LOST_DATE


def signs_grammar():
    Sign = fact(
        'Sign',
        ['characteristic', attribute('values')]
    )

    STOP_CHARACTERISTIC_WORD = or_(
        normalized('пропажа'),
        normalized('задача'),
        normalized('инфорг')
    )
    CHARACTERISTIC = rule(and_(RU_WORD, not_(STOP_CHARACTERISTIC_WORD))) \
        .repeatable() \
        .interpretation(Sign.characteristic)

    VALUE_PUNCT = or_(
        rule(eq(','), rule(EOL).optional()),
        rule(eq('-'))
    )
    VALUE = or_(rule(RU_WORD), rule(INT), VALUE_PUNCT) \
        .repeatable() \
        .interpretation(Sign.values)
    VALUE_END = or_(EOL, eq('.'))

    SIGN = rule(CHARACTERISTIC, COLON, rule(EOL).optional(), VALUE, VALUE_END) \
        .interpretation(Sign)
    return SIGN


def search_grammar():
    gnc = gnc_relation()

    SEARCH_RESULT = or_(
        rule(normalized('жив')),
        rule(normalized('найден')),
        rule(normalized('пропал')),
        rule(normalized('погиб'))
    ) \
        .match(gnc)
    return SEARCH_RESULT


def detailed_signs_grammar():
    Sign = fact(
        'Sign',
        ['characteristic', attribute('values')]
    )

    height_rule = rule(
        normalized('рост').interpretation(Sign.characteristic.normalized().custom(str.capitalize)),
        or_(RU_WORD, INT).repeatable().interpretation(Sign.values)
    ).interpretation(Sign)
    body_type_rule = rule(
        rule(RU_WORD).repeatable().interpretation(Sign.values.normalized()),
        normalized('телосложения').interpretation(Sign.characteristic.normalized().custom(str.capitalize)),
    ).interpretation(Sign)
    hairs_rule = rule(
        normalized('волосы').interpretation(Sign.characteristic.custom(str.capitalize)),
        or_(RU_WORD, eq('-')).repeatable().interpretation(Sign.values)
    ).interpretation(Sign)
    eye_rule = rule(
        normalized('глаза').interpretation(Sign.characteristic.custom(str.capitalize)),
        or_(RU_WORD, eq('-')).repeatable().interpretation(Sign.values)
    ).interpretation(Sign)

    return height_rule, body_type_rule, hairs_rule, eye_rule
