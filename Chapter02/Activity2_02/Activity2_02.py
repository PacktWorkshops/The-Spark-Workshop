from sys import argv
from bs4 import BeautifulSoup
from re import sub
from string import printable
import pycld2
from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from Chapter02.utilities02_py.domain_objects import WarcRecord


def extract_plaintext(record: WarcRecord):
    parser = BeautifulSoup(record.html_source, 'html.parser')
    plaintext = ' '.join(parser.stripped_strings)
    plaintext_stripped = sub('\\s+', ' ', plaintext)
    if plaintext_stripped is None or plaintext_stripped == '':
        return ()  # empty tuple
    else:
        return [(record.target_uri, plaintext_stripped)]


def detect_language(text: str):
    cleaned_text = ''.join(x for x in text if x in printable)
    _, _, details = pycld2.detect(cleaned_text)
    (languageName, languageCode, percent, score) = details[0]
    return (languageCode, str(score))


#   ~/spark-2.4.6-bin-hadoop2.7/bin/spark-submit --master local[2] ~/IdeaProjects/The-Spark-Workshop/Chapter02/Activity2_02/Activity2_02.py ~/IdeaProjects/The-Spark-Workshop/resources/webcorpus/warc.sample ~/Output_Act2_2_Py
if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .appName('Crawl Tagger') \
        .getOrCreate()
    input_file = argv[1]
    output_dir = argv[2]
    warc_records = extract_raw_records(input_file, session) \
        .flatMap(lambda record: parse_raw_warc(record)) \
        .filter(lambda record: record.warc_type == 'response')

    plaintexts_rdd = warc_records.flatMap(extract_plaintext)
    tagged_texts_rdd = plaintexts_rdd.map(lambda record: (record[0], detect_language(record[1])))

    tagged_texts_rdd.saveAsTextFile(output_dir)
