from pyspark.sql import SparkSession
from Chapter02.utilities02_py.helper_python import sample_warc_loc, extract_raw_records, parse_raw_warc
from Chapter02.Exercise2_06.Exercise2_06 import heavy_computation

spark: SparkSession = SparkSession.builder \
    .appName('SubmitWithMaster') \
    .getOrCreate()

raw_records = extract_raw_records(sample_warc_loc, spark)
warc_records = raw_records.flatMap(parse_raw_warc)
invoked_heavy_rdd = warc_records.map(lambda record: heavy_computation())
print(invoked_heavy_rdd.collect())