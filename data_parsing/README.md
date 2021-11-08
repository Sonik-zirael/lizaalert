# Обработка данных

Директория, в которой содержится код для обработки данных с форума.

Для начала обработки выполнить команду в консоли _python parseExample.py_


### Структура parsed.json
```json
[
  {
    "URL": "https://lizaalert.org/forum/viewtopic.php?f=294&t=39587",
    "Status": "погиб",
    "Additional": [
      "нуждается в медпомощи"
    ],
    "MissedDate": "2020-09-12 00:00:00",
    "PublishedDate": "2020-09-16 19:51:30",
    "StartDate": null,
    "FoundDate": "2020-09-25 16:50:40",
    "Name": {
      "first": "Виктор",
      "last": "Максаков",
      "middle": "Николаевич"
    },
    "Gender": "мужчина",
    "Location": [
      {
        "value": {
          "name": "Сургодь",
          "type": "село"
        }
      },
      {
        "value": {
          "name": "Торбеевский",
          "type": "район"
        }
      },
      {
        "value": {
          "name": "Мордовия",
          "type": "республика"
        }
      },
      {
        "value": {
          "name": "Приволжский",
          "type": "федеральный округ"
        }
      }
    ],
    "Age": {
      "years": 59
    },
    "LostDate": {
      "year": 2020,
      "month": "сентябрь",
      "day": 12
    },
    "Signs": [
      [
        {
          "characteristic": "Рост",
          "values": "165"
        },
        {
          "characteristic": "Телосложение",
          "values": "худощавый"
        },
        {
          "characteristic": "Волосы",
          "values": "русые короткие"
        },
        {
          "characteristic": "Глаза",
          "values": "карие"
        }
      ],
      {
        "characteristic": "Одежда",
        "values": [
          "светло-зелёная в клетку рубашка с коротким рукавом",
          "синие джинсы",
          "серые носки",
          "чёрные резиновые тапочки",
          "синяя камуфлированная кепка"
        ]
      },
      [
        {
          "characteristic": "Приметы",
          "values": [
            "шрам на шее"
          ]
        }
      ]
    ]
  }
]
```

      
