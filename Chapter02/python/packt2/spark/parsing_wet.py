from Chapter01.python.packt1.helper_python import create_session
from Chapter02.python.packt2.helper_python import extract_raw_records, parse_raw_wet
from globalp.python.packtg.helper_python_global import sample_wet_loc


if __name__ == "__main__":

    input_loc_wet = sample_wet_loc

    session = create_session(3, 'Web Corpus Parsing (Wet)')
    wet_records = extract_raw_records(input_loc_wet, session) \
        .flatMap(lambda record: parse_raw_wet(record))

    text_records = wet_records \
        .filter(lambda record: record.warc_type != "warcinfo") \
        .toDF()

    text_records.printSchema()
# root
# |-- block_digest: string (nullable = true)
# |-- content_length: long (nullable = true)
# |-- content_type: string (nullable = true)
# |-- date_s: long (nullable = true)
# |-- plain_text: string (nullable = true)
# |-- record_id: string (nullable = true)
# |-- refers_to: string (nullable = true)
# |-- target_uri: string (nullable = true)
# |-- warc_type: string (nullable = true)

    text_records.show(3)
# +--------------------+--------------+------------+----------+--------------------+--------------------+--------------------+--------------------+----------+
# |        block_digest|content_length|content_type|    date_s|          plain_text|           record_id|           refers_to|          target_uri| warc_type|
# +--------------------+--------------+------------+----------+--------------------+--------------------+--------------------+--------------------+----------+
# |sha1:YTWQDG2P3CZ2...|          1096|  text/plain|1566077047|Encyclopedia | Wo...|<urn:uuid:55e694b...|<urn:uuid:48e1553...|http://www.bioref...|conversion|
# |sha1:NCXTAQVEHGWW...|          5476|  text/plain|1566076394|За нарушения прот...|<urn:uuid:9ee9c2c...|<urn:uuid:07a442a...|http://0-1.ru/?id...|conversion|
# |sha1:V5LHTS2HYPQH...|         30034|  text/plain|1566075776|Новости 0-50.ru |...|<urn:uuid:7c9ed59...|<urn:uuid:f4b62f8...|http://0-50.ru/ne...|conversion|
# +--------------------+--------------+------------+----------+--------------------+--------------------+--------------------+--------------------+----------+

    wiki_records = text_records.rdd.filter(lambda record: 'wikipedia' in record.target_uri)
    print(wiki_records.count())
    # 1
    wikipediaTexts = wiki_records.map(lambda record: record.plain_text)
    print(wikipediaTexts.first())
    # Encyclopedia | World Factbook | World Flags | Reference Tables | List of Lists Academic Disciplines | Historical Timeline .....
