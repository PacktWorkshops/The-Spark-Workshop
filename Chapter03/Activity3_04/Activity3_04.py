import re
import sys
from string import printable
import pycld2 as cld2
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc


def tag_records(partition):
    for warc_record in partition:
        parser = BeautifulSoup(warc_record.html_source, 'html.parser')
        plaintext = ' '.join(map(lambda record: record.text, parser.find_all("p")))
        plaintext_stripped = re.sub('\\s+', ' ', plaintext.strip())
        if plaintext_stripped is None or plaintext_stripped == '':
            yield ()  # empty tuple
        else:
            cleaned_text = ''.join(x for x in plaintext_stripped if x in printable)
            _, _, details = cld2.detect(cleaned_text)
            (languageName, languageCode, percent, score) = details[0]
            yield warc_record.target_uri, languageName, score, plaintext_stripped


if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .appName('Improved Crawl Tagger') \
        .getOrCreate()
    args = sys.argv
    input_file = args[1]
    output_dir = args[2]
    warc_records = extract_raw_records(input_file, session) \
        .flatMap(lambda record: parse_raw_warc(record)) \
        .filter(lambda record: record.warc_type == 'response')

    tagged_texts_rdd = warc_records \
        .mapPartitions(tag_records) \
        .filter(lambda record: record != ())

    tagged_texts_rdd.saveAsTextFile(output_dir)
