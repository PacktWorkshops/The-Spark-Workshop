from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from globalp.python.packtg.helper_python_global import sample_warc_loc

if __name__ == "__main__":
    warc_loc = sample_warc_loc
    session = create_session(3, 'Submit Parser')

    raw_records = extract_raw_records(warc_loc, session)
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))

    warc_records.toDF().printSchema()
    print(warc_records.count())
