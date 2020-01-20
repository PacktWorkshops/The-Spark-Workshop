from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import extract_raw_records, parse_raw_wet
from globalp.python.packtg.helper_python_global import sample_wet_loc

if __name__ == "__main__":
    wet_loc = sample_wet_loc
    session = create_session(3, 'Schema Parsing WET')
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    raw_records = extract_raw_records(wet_loc, session)
    wet_records = raw_records \
        .flatMap(lambda record: parse_raw_wet(record))

    wet_records.toDF().printSchema()
    print('Total records: ' + str(wet_records.count()))

# text_records = wet_records \
#     .filter(lambda record: record.warc_type != "warcinfo") \
#     .toDF()
#
# text_records.printSchema()
# text_records.show(3)
#
# wiki_records = text_records.rdd.filter(lambda record: 'wikipedia' in record.target_uri)
# print(wiki_records.count())
# # 1
# wikipediaTexts = wiki_records.map(lambda record: record.plain_text)
# print(wikipediaTexts.first())
