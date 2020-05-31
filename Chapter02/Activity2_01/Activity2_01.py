

from pyspark.sql import SparkSession


if __name__ == "__main__":
    # input = sample_warc_loc
    spark: SparkSession = SparkSession.builder \
        .appName('Activity 2.1') \
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    from operator import add
    from collections import defaultdict
    from typing import Dict
    from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc

    input = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc"
    warc_records = extract_raw_records(input, spark).flatMap(lambda record: parse_raw_warc(record))

    # print(warc_records.count())


    keyed_by_language = warc_records.filter(lambda rec: rec.language != '').map(lambda rec: (rec.language, 1))
    language_map: Dict[str, int] = keyed_by_language.reduceByKey(add).collectAsMap()
    ## language_list = keyed_by_language.reduceByKey(add).collect()
    ## language_map: Dict[str, int] = defaultdict(int)
    ## for key, value in language_list:
    ## ...     language_map[key] += value
    ## language_map
    # warc_records.filter(lambda rec: rec.language != '').map(lambda rec: rec.language).countByValue()

    sorted_language_list = [(key, language_map[key]) for key in sorted(language_map, key=language_map.get)]
    sorted_language_list[0:10]  # a subset of 10 of the rarest languages
    sorted_language_list[len(sorted_language_list)-1]  # most frequent language

    uz_records = warc_records.filter(lambda rec: rec.language != '' and rec.language == 'uz').map(lambda rec: rec.target_uri)
    print(uz_records.collect())

    wikipages = warc_records.filter(lambda rec: 'wikipedia' in rec.target_uri).map(lambda rec: rec.target_uri)
    print(wikipages.collect())

    untagged = warc_records.filter(lambda record: record.language == '')
    print(untagged.count())
