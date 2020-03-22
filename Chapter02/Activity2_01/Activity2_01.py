from typing import Dict
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc

from pyspark.sql import SparkSession


if __name__ == "__main__":
    # input = sample_warc_loc
    spark: SparkSession = SparkSession.builder \
        .appName('SubmitWithMaster') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    input = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"
    warc_records = extract_raw_records(input, spark).flatMap(lambda record: parse_raw_warc(record))

    print(warc_records.count())
    untagged = warc_records.filter(lambda record: record.language == '')
    print(untagged.count())

    language_map: Dict[str, int] = warc_records.filter(lambda rec: rec.language != '').map(lambda rec: rec.language).countByValue()
    sorted_language_map = [(key, language_map[key]) for key in sorted(language_map, key=language_map.get)]
    print(sorted_language_map)

    uz_records = warc_records.filter(lambda rec: rec.language != '' and rec.language == 'uz').map(lambda rec: rec.target_uri)
    print(uz_records.collect())

    wikipages = warc_records.filter(lambda rec: 'wikipedia' in rec.target_uri).map(lambda rec: rec.target_uri)
    print(wikipages.collect())
