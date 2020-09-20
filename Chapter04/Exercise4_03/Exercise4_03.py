import time
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import count, sum, col
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, sample_warc_loc

if __name__ == "__main__":
    session: SparkSession = create_session(3, "Query Plans")
    session.sparkContext.setLogLevel("TRACE")
    lang_tag_mapping = [('en','english'),('pt-pt','portugese'),('cs','czech'),('de','german'),('es','spanish'),('eu','basque'),('it','italian'),('hu','hungarian'),('pt-br','portugese'),('fr','french'),('en-US','english'),('zh-TW','chinese')]
    lang_tag_df = session.createDataFrame(lang_tag_mapping, ['tag', 'language'])
    session.createDataFrame(lang_tag_mapping).show()
    raw_records = extract_raw_records(sample_warc_loc, session)
    warc_records_rdd: RDD = raw_records.flatMap(parse_raw_warc)
    warc_records_df: DataFrame = warc_records_rdd.toDF()\
        .select(col('target_uri'), col('language'))\
        .filter(col('language') != '')

    aggregated = warc_records_df\
        .groupBy(col('language'))\
        .agg(count(col('target_uri')))\
        .withColumnRenamed('language', 'tag')

    joined_df = aggregated.join(lang_tag_df, ['tag'])

    joined_df.show()
    joined_df.explain(True)
    time.sleep(10 * 60)

# +-----+-----------------+---------+
# |  tag|count(target_uri)| language|
# +-----+-----------------+---------+
# |   en|                5|  english|
# |pt-pt|                1|portugese|
# |   cs|                1|    czech|
# |   de|                1|   german|
# |   es|                4|  spanish|
# |   eu|                1|   basque|
# |   it|                1|  italian|
# |   hu|                1|hungarian|
# |pt-br|                1|portugese|
# |   fr|                1|   french|
# |en-US|                6|  english|
# |zh-TW|                1|  chinese|
# +-----+-----------------+---------+


# == Physical Plan ==
# *(6) Project [tag#55, count(target_uri)#52L, language#1]
#               +- *(6) SortMergeJoin [tag#55], [tag#0], Inner
#                                      :- *(3) Sort [tag#55 ASC NULLS FIRST], false, 0
#                                                    :  +- Exchange hashpartitioning(tag#55, 200)
# :     +- *(2) HashAggregate(keys=[language#25], functions=[count(target_uri#29)], output=[tag#55, count(target_uri)#52L])
# :        +- Exchange hashpartitioning(language#25, 200)
# :           +- *(1) HashAggregate(keys=[language#25], functions=[partial_count(target_uri#29)], output=[language#25, count#71L])
# :              +- *(1) Project [language#25, target_uri#29]
#                                 :                 +- *(1) Filter (isnotnull(language#25) && NOT (language#25 = ))
# :                    +- Scan ExistingRDD[block_digest#15,concurrent_to#16,content_length#17L,content_type#18,date_s#19L,html_content_type#20,html_length#21L,html_source#22,info_id#23,ip#24,language#25,payload_digest#26,payload_type#27,record_id#28,target_uri#29,warc_type#30]
#                                          +- *(5) Sort [tag#0 ASC NULLS FIRST], false, 0
#                                                        +- Exchange hashpartitioning(tag#0, 200)
#                                                                                     +- *(4) Filter (NOT (tag#0 = ) && isnotnull(tag#0))
#                                                                                                          +- Scan ExistingRDD[tag#0,language#1]