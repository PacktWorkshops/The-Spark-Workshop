import os
import time
from datetime import datetime
from typing import List, Tuple
from pyspark.sql import SparkSession
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.domain_objects import WarcRecord
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc


def fall_asleep(record: WarcRecord):
    current_uri: str = record.target_uri
    start_time = str(datetime.now())
    process_id = str(os.getpid())
    print('@@1 falling asleep in process {} at {} processing {}'.format(process_id, start_time, current_uri))
    time.sleep(5)
    end_time = str(datetime.now())
    print('@@2 awakening in process {} at {} processing {}'.format(process_id, end_time, current_uri))
    return process_id, current_uri


def trivial_filter(processid_uri: (int, str)) -> bool:
    new_process_id = str(os.getpid())
    timepoint = str(datetime.now())
    print('@@3 filter in process {} at {} processing {}'.format(new_process_id, timepoint, processid_uri[1]))
    return True


def quick_print(processid_uri: (int, str)) -> (int, int):
    new_process_id = str(os.getpid())
    timepoint = str(datetime.now())
    print('@@4 map2 in process {} at {} processing {}'.format(new_process_id, timepoint, processid_uri[1]))
    return processid_uri[0], new_process_id

if __name__ == "__main__":
    session: SparkSession = create_session(4, "Wave exploration")

    input_warc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"  # ToDo: Change path
    raw_records = extract_raw_records(input_warc, session)
    warc_records = raw_records \
        .flatMap(parse_raw_warc)

    process_ids_rdd = warc_records\
        .map(fall_asleep)\
        .filter(trivial_filter)\
        .map(quick_print)

    distinct_process_ids: List[Tuple[int, int]] = process_ids_rdd.distinct().collect()
    print(distinct_process_ids)
