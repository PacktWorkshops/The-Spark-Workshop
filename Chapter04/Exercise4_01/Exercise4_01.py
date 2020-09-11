import time
from sys import argv
from pyspark import RDD
from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, parse_raw_wet

if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .master('local[{}]'.format(3)) \
        .appName('Caching & Eviction') \
        .getOrCreate()
    session.sparkContext.setLogLevel('DEBUG')

    input_loc_warc = argv[1]
    input_loc_wet = argv[2]

    raw_records_warc: RDD = extract_raw_records(input_loc_warc, session)
    warc_records: RDD = raw_records_warc \
        .flatMap(lambda record: parse_raw_warc(record))

    raw_records_wet: RDD = extract_raw_records(input_loc_wet, session)
    wet_records: RDD = raw_records_wet \
        .flatMap(lambda record: parse_raw_wet(record))

    warc_records.cache()
    wet_records.cache()

    uri_keyed_warc = warc_records.map(lambda record: (record.target_uri, record))
    uri_keyed_wet = wet_records.map(lambda record: (record.target_uri, record))
    joined = uri_keyed_warc.join(uri_keyed_wet)

    print(joined.count())
    time.sleep(10 * 60)
