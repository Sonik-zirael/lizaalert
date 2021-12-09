# Обработка данных

Директория, в которой содержится код для обработки данных с форума.

Для начала обработки выполнить команду в консоли _python parseExample.py_

### Аргументы скрипта
```
usage: parseExample.py [-h] -m {kafka,archive} [--archive ARCHIVE ARCHIVE]
                       [-p RESULT_DIR] [-d] [--batch_size BATCH_SIZE]
                       [--start_batch START_BATCH]
                       [--batches_to_process BATCHES_TO_PROCESS]
                       [--process_type {consistent,parallel,spark}]
                       [--parallel_processes PARALLEL_PROCESSES]

optional arguments:
  -h, --help            show this help message and exit
  -m {kafka,archive}, --mode {kafka,archive}
                        Specifies in which mode program will be launched. Set
                        "kafka" to make program parse data provided by kafka.
                        Set "archive" to make program parse data in pre-packed
                        archive.
  --archive ARCHIVE ARCHIVE
                        A pair of arguments: path to archive and json format.
                        Archive at the specified path must contain
                        "data.json". json format may be "old" or "new". "old"
                        format stays for data from lizaalert archive forum
                        section, "new" for actual lizaalert data. Examples: "
                        --archive archive_records.zip old" or "--archive
                        this_year_data.zip new". This argument is required if
                        "--mode" is set to "archive".
  -p RESULT_DIR, --result_dir RESULT_DIR
                        Path to a directory, where parsed results will be
                        stored. Must be a valid path to an existing directory
                        absolute or relative to the script directory. Default
                        is "../temp"
  -d, --debug           Activates debug mode with more verbose output.

Optimized processing parameters:
  Parameters of batch processing and multiprocessing. Ignored if "--mode"
  set to "kafka"

  --batch_size BATCH_SIZE
                        Number of records to process at once. Slightly impacts
                        memory usage. Real batch size used may be smaller for
                        final batch, if there's not enough records to form a
                        full batch. Default is 1.
  --start_batch START_BATCH
                        Offset from the beginning of data in batches. If
                        exceeds overall amount of batches, no data will be
                        processed. Default is 0.
  --batches_to_process BATCHES_TO_PROCESS
                        Number of batches to process. May be less than
                        specified value, if dataset has not enough entries to
                        form specified number of batches of batch_size.
                        Default is -1 for all batches.
  --process_type {consistent,parallel,spark}
                        Specifies in which mode program will be executed. Set
                        "consistent" to make program parse data consistently.
                        Set "parallel" to make program parse data in parallel
                        with python tools. Set "spark" to make program parse
                        data in parallel with spark. Default is "parallel".
  --parallel_processes PARALLEL_PROCESSES
                        Number of processes of parsing to launch in parallel.
                        Greatly impacts memory usage. Default is -2 for number
                        of available processors - 1
```

## Настройка Spark

1. Установка бинарников ([туториал для Windows](https://sparkbyexamples.com/spark/apache-spark-installation-on-windows/)).  
На Windows 10, Java 11.0.1, Python 3.8.10 при запуске Spark версии 3.2.0 (Oct 13 2021) была ошибка.  
На версии 3.1.2 (Jun 01 2021) все работает.  
2. Настроить переменные окружения для Hadoop и Spark (в туториале также описано)
3. Задать переменную окружения PYSPARK_PYTHON=python (иначе будет ошибка при выполнении скрипта)


## Структура parsed.json
```json
[
  {
    "URL": "https://lizaalert.org/forum/viewtopic.php?f=294&t=39587",
    "Status": "погиб",
    "Additional": [
      "нуждается в медпомощи"
    ],
    "MissedDate": "2020-11-16T00:00:00",
    "PublishedDate": "2020-11-17T06:42:11",
    "StartDate": "2020-11-17T06:43:47",
    "FoundDate": "2020-11-17T07:30:22",
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

      
