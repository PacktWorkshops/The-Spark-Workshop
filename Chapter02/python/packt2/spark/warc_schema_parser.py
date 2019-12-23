from Chapter01.python.packt1.helper_python import create_session
from Chapter02.python.packt2.helper_python import extract_raw_records, parse_raw_warc
from globalp.python.packtg.helper_python_global import sample_warc_loc

if __name__ == "__main__":
    warc_loc = sample_warc_loc
    session = create_session(3, 'Schema Parsing WARC')

    raw_records = extract_raw_records(sample_warc_loc, session)
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))

    responses = warc_records\
        .filter(lambda record: record.warc_type == "response")\
        .toDF()

    responses.printSchema()
# root
# |-- block_digest: string (nullable = true)
# |-- concurrent_to: string (nullable = true)
# |-- content_length: long (nullable = true)
# |-- content_type: string (nullable = true)
# |-- date_s: long (nullable = true)
# |-- html_content_type: string (nullable = true)
# |-- html_length: long (nullable = true)
# |-- html_source: string (nullable = true)
# |-- info_id: string (nullable = true)
# |-- ip: string (nullable = true)
# |-- language: string (nullable = true)
# |-- payload_digest: string (nullable = true)
# |-- payload_type: string (nullable = true)
# |-- record_id: string (nullable = true)
# |-- target_uri: string (nullable = true)
# |-- warc_type: string (nullable = true)


    responses.show(3)
# +--------------------+--------------------+--------------+--------------------+----------+--------------------+-----------+--------------------+--------------------+--------------+--------+--------------------+--------------------+--------------------+--------------------+---------+
# |        block_digest|       concurrent_to|content_length|        content_type|    date_s|   html_content_type|html_length|         html_source|             info_id|            ip|language|      payload_digest|        payload_type|           record_id|          target_uri|warc_type|
# +--------------------+--------------------+--------------+--------------------+----------+--------------------+-----------+--------------------+--------------------+--------------+--------+--------------------+--------------------+--------------------+--------------------+---------+
# |sha1:AFZZNJ5YSPXI...|<urn:uuid:c26c8cc...|         44287|application/http;...|1566073942|text/html; charse...|      43365|<!DOCTYPE html PU...|<urn:uuid:47046f6...|104.27.160.112|      sr|sha1:P5LGYLYIECUM...|           text/html|<urn:uuid:dc550ee...|http://013info.rs...| response|
# |sha1:T4G5RWLKOGI2...|<urn:uuid:53bd2b4...|           652|application/http;...|1566077414|text/html;charset...|        287|<!DOCTYPE html PU...|<urn:uuid:47046f6...|203.107.32.173|        |sha1:6R4DUYQ7DRZS...|application/xhtml...|<urn:uuid:e6068d3...|http://016.kouyu1...| response|
# |sha1:D7R5CNGVF5MD...|<urn:uuid:9408588...|         13394|application/http;...|1566078548|text/html;charset...|      13048|<!DOCTYPE html> <...|<urn:uuid:47046f6...|47.100.201.254|        |sha1:IK4EFX2V7UB5...|           text/html|<urn:uuid:b4a806a...|http://01gydc.com...| response|
# +--------------------+--------------------+--------------+--------------------+----------+--------------------+-----------+--------------------+--------------------+--------------+--------+--------------------+--------------------+--------------------+--------------------+---------+

    english_records = responses.filter(responses.language == 'en')
    print(english_records.count())
    # 1
    print(english_records.select('html_source').first())
    # Row(html_source='<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML+RDFa 1.0//EN"   "http://www.w3.org/MarkUp/DTD/xhtml-rdfa-1....