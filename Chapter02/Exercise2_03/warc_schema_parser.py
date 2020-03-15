from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import sample_warc_loc, extract_raw_records, parse_raw_warc

if __name__ == "__main__":
    session = create_session(3, 'WARC Parser')
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    raw_records = extract_raw_records(sample_warc_loc, session)
    warc_records = raw_records.flatMap(lambda record: parse_raw_warc(record))

    warc_records.toDF().printSchema()
    print('Total # of records: ' + str(warc_records.count()))
