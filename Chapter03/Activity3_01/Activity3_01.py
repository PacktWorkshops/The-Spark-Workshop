import time
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, parse_raw_wet

if __name__ == "__main__":
    input_loc_warc = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Modify path
    input_loc_wet = '/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet'  # ToDo: Modify path
    session = create_session(3, 'Activity 3.1')

    raw_records_warc = extract_raw_records(input_loc_warc, session)
    warc_records = raw_records_warc.flatMap(lambda record: parse_raw_warc(record))
    raw_records_wet = extract_raw_records(input_loc_wet, session)
    wet_records = raw_records_wet.flatMap(lambda record: parse_raw_wet(record))
    pair_warc = warc_records.map(lambda warc: (warc.target_uri, (warc.warc_type, warc.record_id, warc.content_type, warc.block_digest, warc.date_s, warc.content_length, warc.info_id, warc.concurrent_to, warc.ip, warc.payload_digest, warc.payload_type, warc.html_content_type, warc.language, warc.html_length, warc.html_source)))
    pair_wet = wet_records.map(lambda wet: (wet.target_uri, wet.plain_text))

    joined = pair_warc.join(pair_wet, numPartitions=7)

    print(joined.count())
    time.sleep(10 * 60)  # For exploring WebUI
