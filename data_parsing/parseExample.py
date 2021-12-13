import argparse
import io
import json
from json.decoder import JSONDecodeError
import logging
import os
from typing import Tuple
import zipfile

from kafka import KafkaConsumer
from kafka.producer.kafka import KafkaProducer
from pyspark.sql import SparkSession

from lizaalert.parser import parse_general

DEFAULT_JSON_DATA_FILE_NAME = 'data.json'
KAFKA_BATCH_SIZE = 1
KAFKA_N_PROC = 1
KAFKA_BATCHES_TO_PROCESS = -1
KAFKA_START_BATCH = 0
PROCESS_TYPE = "parallel"
ITERATIONS_NO_CHANGE_KAFKA_CONSUMER_LIFETIME = 10


def parse_arguments() -> argparse.Namespace:

    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode', type=str, required=True,
                        choices=("kafka", "archive"),
                        help='Specifies in which mode program will be launched. '
                        'Set "kafka" to make program parse data provided by kafka. '
                        'Set "archive" to make program parse data in pre-packed archive.')
    parser.add_argument('--archive', type=str, required=False, action='append', nargs=2,
                        help='A pair of arguments: path to archive and json format. '
                        f'Archive at the specified path must contain "{DEFAULT_JSON_DATA_FILE_NAME}". '
                        'json format may be "old" or "new". "old" format stays for '
                        'data from lizaalert archive forum section, "new" for actual '
                        'lizaalert data. Examples: "--archive archive_records.zip old" or '
                        '"--archive this_year_data.zip new". This argument is required if '
                        '"--mode" is set to "archive".')
    parser.add_argument('-p', '--result_dir', type=str, required=False, default='../temp',
                        help='Path to a directory, where parsed results will be stored. '
                        'Must be a valid path to an existing directory absolute or relative to '
                        'the script directory. Default is "../temp"')
    parser.add_argument('-d', '--debug', required=False, action='store_true',
                        help='Activates debug mode with more verbose output.')
    batch_multiproc_group = parser.add_argument_group(title='Optimized processing parameters',
                                                      description='Parameters of batch processing '
                                                      'and multiprocessing. Ignored if '
                                                      '"--mode" set to "kafka"')
    batch_multiproc_group.add_argument('--batch_size', type=int, required=False,
                                       default=KAFKA_BATCH_SIZE,
                                       help='Number of records to process at once. '
                                       'Slightly impacts memory usage. Real batch size used may '
                                       'be smaller for final batch, if there\'s not enough '
                                       'records to form a full batch. Default is 1.')
    batch_multiproc_group.add_argument('--start_batch', type=int, required=False,
                                       default=KAFKA_START_BATCH,
                                       help='Offset from the beginning of data in batches. '
                                       'If exceeds overall amount of batches, no data will be '
                                       'processed. Default is 0.')
    batch_multiproc_group.add_argument('--batches_to_process', type=int, required=False,
                                       default=KAFKA_BATCHES_TO_PROCESS,
                                       help='Number of batches to process. May be less than '
                                       'specified value, if dataset has not enough entries to '
                                       'form specified number of batches of batch_size. '
                                       'Default is -1 for all batches.')
    batch_multiproc_group.add_argument('--process_type', type=str, required=False,
                                       default="parallel",
                                       choices=("consistent", "parallel", "spark"),
                                       help='Specifies in which mode program will be executed. '
                                            'Set "consistent" to make program parse data consistently. '
                                            'Set "parallel" to make program parse data in parallel with python tools. '
                                            'Set "spark" to make program parse data in parallel with spark. '
                                            'Default is "parallel". ')
    batch_multiproc_group.add_argument('--parallel_processes', type=int, required=False, default=-2,
                                       help='Number of processes of parsing to launch in parallel.'
                                       ' Greatly impacts memory usage. '
                                       'Default is -2 for number of available processors - 1')
    namespace = parser.parse_args()

    args_ok, message = validate_arguments(namespace)
    if not args_ok:
        parser.error(message + ' Use "-h" parameter for help on usage.')

    return namespace


def validate_arguments(ns: argparse.Namespace) -> Tuple[bool, str]:
    if not os.path.isabs(ns.result_dir):
        script_directory = os.path.abspath(os.path.dirname(__file__))
        ns.result_dir = os.path.abspath(os.path.join(script_directory, ns.result_dir))
    if not (os.path.exists(ns.result_dir) and os.path.isdir(ns.result_dir)):
        return (False, f'result_dir must be existing directory, but {ns.result_dir} is not.')

    if ns.mode == 'archive' and not ns.archive:
        return (False, 'Mode "archive" requires path to archive to be specified via "--archive".')

    if ns.mode == 'kafka':
        ns.batch_size = KAFKA_BATCH_SIZE
        ns.start_batch = KAFKA_START_BATCH
        ns.batches_to_process = KAFKA_BATCHES_TO_PROCESS
        ns.parallel_processes = KAFKA_N_PROC
        ns.process_type = PROCESS_TYPE
    else:
        if ns.batch_size < 1:
            return (False, 'batch_size must be a 1 or more.')
        if ns.start_batch < 0:
            return (False, 'start_batch must be a non-negative integer.')
        if ns.batches_to_process != -1 and ns.batches_to_process < 0:
            return (False, 'batches_to_process must be 1 or more or -1.')
        if ns.parallel_processes < 1 and ns.parallel_processes != -2:
            return (False, 'parallel_processes must be 1 or more or -2.')
    return True, 'OK'


def combine(dir_with_jsons):
    combined = []
    for fname in list(filter(lambda x: x.startswith("okrug_parsed_"), os.listdir(dir_with_jsons))):
        with open("../temp/{}".format(fname), "r") as read_file:
            combined += json.loads(read_file.read())


ns = parse_arguments()
if ns.debug:
    logging.basicConfig(level=logging.DEBUG, format='[%(levelname)s] %(name)s %(filename)s: %(message)s')
else:
    logging.basicConfig(level=logging.WARNING, format='[%(levelname)s] %(filename)s: %(message)s')

sc = None
if ns.process_type == "spark":
    spark = SparkSession.builder\
        .master('local[*]') \
        .appName('LizaAlertSpark')\
        .getOrCreate()
    sc = spark.sparkContext

if ns.mode == 'archive':
    for archive_path, archive_data_format in ns.archive:
        if not (os.path.exists(archive_path) and
                os.path.isfile(archive_path) and
                zipfile.is_zipfile(archive_path)):
            raise FileNotFoundError(f'Path {archive_path} is incorrect or not an archive.')

        data = None
        with zipfile.ZipFile(archive_path, 'r') as archive_handler:
            if DEFAULT_JSON_DATA_FILE_NAME not in archive_handler.namelist():
                raise FileNotFoundError(f'Archive {archive_path} doesn\'t contain {DEFAULT_JSON_DATA_FILE_NAME}')

            with archive_handler.open(DEFAULT_JSON_DATA_FILE_NAME, 'r') as data_file_bytes:
                data = json.load(io.TextIOWrapper(data_file_bytes, encoding='utf-8'))

        archive_name_no_ext = os.path.splitext(os.path.split(archive_path)[1])[0]

        parse_general(process_type=ns.process_type,
                      spark_context=sc,
                      data=data,
                      batch_size=ns.batch_size,
                      start_batch=ns.start_batch,
                      batch_number=ns.batches_to_process,
                      n_proc=ns.parallel_processes,
                      result_dir=ns.result_dir,
                      regions_as_keys=(archive_data_format=='new'),
                      parsed_common_prefix=archive_name_no_ext+'_parsed_')

else:
    regions_topic = 'regions_topic'
    archive_topic = 'archive_topic'
    topic_name = 'archive_topic'
    consumer = KafkaConsumer(topic_name, auto_offset_reset='earliest',
                             bootstrap_servers=['kafka:9092'], api_version=(0, 10),
                            #  consumer_timeout_ms=1000,
                             group_id='parse_data_group',
                             request_timeout_ms=100000,
                             session_timeout_ms=99000,
                             max_poll_records=100,)
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'], api_version=(0, 10))
    is_end_flag = False
    iterations_without_change = 0
    while True:
        iterations_without_change += 1
        if is_end_flag or iterations_without_change > ITERATIONS_NO_CHANGE_KAFKA_CONSUMER_LIFETIME:
            print('bye')
            # break
        for message in consumer:
            print('Msg')
            # consumer.commit()
            iterations_without_change = 0
            message_string = message.value.decode('utf-8')
            try:
                message_data = json.loads(message_string)
                is_end_flag = (message_data == 'end')
                if is_end_flag:
                    break
            except JSONDecodeError as jde:
                logging.warning('Received message %s in topic %s which is not in json format',
                                message_string, topic_name)
            ns.start_batch += 1
            parse_general(process_type=ns.process_type,
                          spark_context=sc,
                          data=message_data,
                          producer=producer,
                          batch_size=ns.batch_size,
                          start_batch=ns.start_batch,
                          batch_number=ns.batches_to_process,
                          n_proc=ns.parallel_processes,
                          result_dir=ns.result_dir,
                          regions_as_keys=False,
                          parsed_common_prefix='kafka_parsed_')
    consumer.commit()
    consumer.close()

sc.stop()
