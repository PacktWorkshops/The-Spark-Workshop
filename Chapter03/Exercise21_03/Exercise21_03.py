from pyspark.sql import SparkSession
from utilities01_py.helper_python import create_session
from utilities02_py.domain_objects import WarcRecord
from utilities02_py.helper_python import extract_raw_records, parse_raw_warc
import datetime
import time
import threading
import os

def fall_asleep(record: WarcRecord):
    current_uri: str = record.target_uri
    start_time = str(datetime.datetime.now())
    # thread_id = str(threading.currentThread().daemon.real)
    thread_id =  str(threading.currentThread().ident)
    print('@@1 falling asleep in thread ' + thread_id + ' at ' + start_time + ' accessing ' + current_uri)
    time.sleep(2)
    end_time = str(datetime.datetime.now())
    print('@@2 awakening in thread ' + thread_id + ' at ' + end_time + ' accessing ' + current_uri)
    return thread_id, current_uri

if __name__ == "__main__":
    # main method of Exercise4_01.py comes here
    input_warc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"  # ToDo: Change
    session: SparkSession = create_session(3, "Wave exploration")

    raw_records = extract_raw_records(input_warc, session)
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))


    thread_ids = warc_records.map(lambda record: fall_asleep(record))
    print(thread_ids.count())

# val threadIdsRDD: RDD[(Long, Long)] = warcRecords
# .map(record => {
#     val currentUri = record.targetURI
# val startTime = LocalDateTime.now()
# val threadId: Long = Thread.currentThread().getId
# println(s"@@1 falling asleep in thread $threadId at $startTime accessing $currentUri")
# Thread.sleep(2000)
# val endTime = LocalDateTime.now()
# println(s"@@2 awakening in thread $threadId at $endTime accessing $currentUri")
# (Thread.currentThread().getId, currentUri)
# })
# .filter(threadIdUri => {
#     val startTime = LocalDateTime.now()
# val threadId: Long = Thread.currentThread().getId
# println(s"@@3 filter in thread $threadId at $startTime accessing ${threadIdUri._2}")
# true
# })
# .map(threadIdUri => {
#     val startTime = LocalDateTime.now()
# val threadId: Long = Thread.currentThread().getId
# println(s"@@4 map2 in thread $threadId at $startTime accessing ${threadIdUri._2}")
# (threadIdUri._1, threadId)
# })
#
#
# val distinctThreads = threadIdsRDD
# .distinct()
# .collect()
# .toList
#
# println(distinctThreads)
