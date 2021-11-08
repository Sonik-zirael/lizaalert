from yargy import (
    rule, or_
)
from yargy.predicates import (
    eq,
    type,
    gram
)


INT = type('INT')
RU_WORD = type('RU')
NOUN = gram('NOUN')
COLON = eq(':')
COMMA = eq(',')
PUNCT = type('PUNCT')
EOL = type('EOL')
ANY_WORD = rule(or_(RU_WORD, INT, PUNCT))
