from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import extract_raw_records, parse_raw_warc, sample_warc_loc
from pyspark.sql import SparkSession
import time
from time import gmtime, strftime


def heavy_computation(record):
    time.sleep(0.2)
    return record


if __name__ == "__main__":
    session: SparkSession = create_session(2, "Activity 2")
    raw_records = extract_raw_records(sample_warc_loc, session)
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))
    warc_records \
        .map(lambda record: heavy_computation(record)) \
        .foreach(lambda _: None)
    print(strftime("%Y-%m-%d %H:%M:%S", gmtime()))
