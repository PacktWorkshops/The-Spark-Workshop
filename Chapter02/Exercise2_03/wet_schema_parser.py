from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import sample_wet_loc, extract_raw_records, parse_raw_wet

if __name__ == "__main__":
    session = create_session(3, 'WET Parser')
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    raw_records = extract_raw_records(sample_wet_loc, session)
    wet_records = raw_records.flatMap(lambda record: parse_raw_wet(record))

    wet_records.toDF().printSchema()
    print('Total # of records: ' + str(wet_records.count()))

