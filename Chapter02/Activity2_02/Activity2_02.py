import re
import sys
from string import printable
import pycld2 as cld2
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from Chapter02.utilities02_py.domain_objects import WarcRecord


def extract_plaintext(record: WarcRecord):
    parser = BeautifulSoup(record.html_source, 'html.parser')
    plaintext = ' '.join(map(lambda record: record.text, parser.find_all("p")))
    plaintext_stripped = re.sub('\\s+', ' ', plaintext.strip())
    if plaintext_stripped is None or plaintext_stripped == '':
        return ()  # empty tuple
    else:
        return [(record.target_uri, plaintext_stripped)]


def detect_language(text: str):
    cleaned_text = ''.join(x for x in text if x in printable)  # https://github.com/mikemccand/chromium-compact-language-detector/issues/22
    _, _, details = cld2.detect(cleaned_text)  # Tuple of up to 3 detected languages
    (languageName, languageCode, percent, score) = details[0]  # percent is what percentage of the original text was detected
    # as this language and score is the confidence score for that language.
    return (languageName, score, text)


#  ~/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master local[3] ~/IdeaProjects/The-Spark-Workshop/Chapter02/Activity2_02/Activity2_02.py ~/CC-MAIN-20191013195541-20191013222541-00000.warc ~/Act2.2PyOutput
if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .appName('Crawl Tagger') \
        .getOrCreate()
    args = sys.argv
    input_file = args[1]
    output_dir = args[2]

    warc_records = extract_raw_records(input_file, session) \
        .flatMap(lambda record: parse_raw_warc(record)) \
        .filter(lambda record: record.warc_type == 'response')

    plaintexts_rdd = warc_records.flatMap(extract_plaintext)
    tagged_texts_rdd = plaintexts_rdd.map(lambda record: (record[0], detect_language(record[1])))

    tagged_texts_rdd.saveAsTextFile(output_dir)
