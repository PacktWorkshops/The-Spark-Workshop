from pyspark.sql import SparkSession
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, sample_warc_loc
import time
import sys

if __name__ == "__main__":
    threads = int(sys.argv[1])
    warc_loc = sample_warc_loc
    session: SparkSession = create_session(threads, "PySpark Design")

    raw_records = extract_raw_records(warc_loc, session)
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))
    print(warc_records.getNumPartitions())
    warc_records.cache()
    print(warc_records.count())
    time.sleep(60 * 10)
