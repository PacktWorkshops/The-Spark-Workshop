import time
from pyspark import RDD
from pyspark.sql import SparkSession
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, parse_raw_wet


if __name__ == "__main__":
    input_loc_warc = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Modify path
    input_loc_wet = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet'  # ToDo: Modify path
    session: SparkSession = create_session(3, 'Activity 4.2 RDD')

    warc_records: RDD = extract_raw_records(input_loc_warc, session).flatMap(lambda record: parse_raw_warc(record))
    wet_records: RDD = extract_raw_records(input_loc_wet, session).flatMap(lambda record: parse_raw_wet(record))

    pair_warc: RDD = warc_records.map(lambda warc: (warc.target_uri, warc.language))
    pair_wet: RDD = wet_records.map(lambda wet: (wet.target_uri, wet.plain_text))

    joined = pair_warc.join(pair_wet)
    spanish_records = joined.filter(lambda triple: triple[1][0] == 'es')

    print(spanish_records.count())  # 133
    time.sleep(10 * 60)  # For exploring WebUI
