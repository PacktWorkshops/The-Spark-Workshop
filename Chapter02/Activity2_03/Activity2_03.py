from sys import argv
from pyspark.sql import SparkSession
from Chapter02.Activity2_02.Activity2_02 import extract_plaintext, detect_language
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc

#   ~/spark-2.4.6-bin-hadoop2.7/bin/spark-submit --master local[1] ~/IdeaProjects/The-Spark-Workshop/Chapter02/Activity2_03/Activity2_03.py ~/IdeaProjects/The-Spark-Workshop/resources/webcorpus/warc.sample
if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .appName('Crawl Tagger Estimation') \
        .getOrCreate()
    input_file = argv[1]
    warc_records = extract_raw_records(input_file, session) \
        .flatMap(lambda record: parse_raw_warc(record)) \
        .filter(lambda record: record.warc_type == 'response')

    plaintexts_rdd = warc_records.flatMap(extract_plaintext)
    tagged_texts_rdd = plaintexts_rdd.map(lambda record: (record[0], detect_language(record[1])))

    from timeit import default_timer
    start_time = default_timer()
    size = tagged_texts_rdd.count()
    end_time = default_timer()
    print('Time taken: ' + str(end_time - start_time))
