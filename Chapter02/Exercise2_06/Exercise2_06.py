from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import sample_warc_loc, extract_raw_records, parse_raw_warc
from timeit import default_timer


def heavy_computation():
    list = [1] * 1000
    total_sum = 0
    for i in range(0, 1000):
        total_sum += sum(list)
    return total_sum


if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .appName('Different Tasks') \
        .getOrCreate()
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    raw_records = extract_raw_records(sample_warc_loc, session)
    warc_records = raw_records.flatMap(lambda record: parse_raw_warc(record))
    invoked_heavy_rdd = warc_records.map(lambda record: heavy_computation())

    start_time = default_timer()
    invoked_heavy_rdd.foreach(lambda record: None)
    end_time = default_timer()
    print(str(end_time - start_time))
