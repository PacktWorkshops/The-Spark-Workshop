from collections import defaultdict

from pyspark.rdd import RDD
import re
from Chapter01.python.packt1.helper_python import *
from pyspark.sql import SparkSession, DataFrame
from Chapter02.python.packt2.helper_python import extract_warc_records, Warc_Record
from globalp.python.packtg.helper_python_global import sample_warc_loc

if __name__ == "__main__":

#     return word,  # single element tuple
# else:
#     return ()  # empty tuple

    blank_line = "(?m:^(?=[\r\n]))"
    blank_line_regex = re.compile(blank_line)
    new_line = "(\\r?\\n)+"
    new_line_regex = re.compile(new_line)

    def extract_warc_meta_info(raw_meta: str) -> DefaultDict[str, str]:
        meta_entries = defaultdict(str)
        fields = new_line_regex.split(raw_meta)
        for field in fields:
            key_value = field.split(':')
            meta_key = key_value[0].strip()
            if len(meta_key) == 0:
                continue
            meta_entries[meta_key] = ':'.join(key_value[1:len(key_value)]).strip()
        return meta_entries

    def extract_response_meta(response_meta: str):
        fields = new_line_regex.split(response_meta)
        content_type = ''
        language = ''
        content_length = -1
        for field in fields:
            if field.startswith('Content-Type:'):
                content_type = field[14:].strip()
            elif field.startswith('Content-Language:'):
                language = field[17:].strip()
            elif field.startswith('Content-Length:'):
                content_length = int(field[15:].strip())
        return content_type, language, content_length

    def parse_raw_warc_record(text: str) -> Tuple[Warc_Record]:
        match = blank_line_regex.findall(text)
        if len(match) == 0:
            return ()
        else:
            match_iter = re.finditer(blank_line_regex, text)
            match_starts: List[int] = [m.end(0) for m in match_iter]
            doc_start = match_starts[0]  # start of record
            meta_boundary = match_starts[1]  # end of meta section
            response_boundary = match_starts[2]  # end of response meta section
            raw_meta_info = text[doc_start:meta_boundary].strip()
            meta_pairs: DefaultDict[str, str] = extract_warc_meta_info(raw_meta_info)
            response_meta = text[meta_boundary + 1: response_boundary].strip()
            response_meta_triple: Tuple[str, str, int] = extract_response_meta(response_meta)
            page_content = text[response_boundary + 1:].strip()
            page_content = re.sub(new_line_regex, ' ', page_content)
            return Warc_Record(meta_pairs, response_meta, page_content),

    session = create_session(3, 'web corpus')
    warc_records = extract_warc_records(sample_warc_loc, session)\
        .flatMap(lambda record: parse_raw_warc_record(record))

    res: Warc_Record = warc_records.take(3)[2]
    print(res.html_source)
