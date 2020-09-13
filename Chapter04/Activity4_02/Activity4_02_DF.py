import time
from pyspark import RDD
from pyspark.sql import SparkSession, DataFrame
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, parse_raw_wet


if __name__ == "__main__":
    input_loc_warc = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Modify path
    input_loc_wet = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet'  # ToDo: Modify path
    session: SparkSession = create_session(3, 'Activity 4.2 DataFrame')

    warc_records: RDD = extract_raw_records(input_loc_warc, session).flatMap(lambda record: parse_raw_warc(record))
    wet_records: RDD = extract_raw_records(input_loc_wet, session).flatMap(lambda record: parse_raw_wet(record))

    from pyspark.sql.functions import col
    warc_records_df: DataFrame = warc_records.toDF().select(col('target_uri'), col('language'))
    wet_records_df: DataFrame = wet_records.toDF().select(col('target_uri'), col('plain_text'))

    joined_df = warc_records_df.join(wet_records_df, ['target_uri'])
    spanish_records = joined_df.filter(col('language') == 'es')

    time.sleep(10 * 60)  # For exploring WebUI
