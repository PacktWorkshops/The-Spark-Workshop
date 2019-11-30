from typing import Dict, Tuple, DefaultDict

from pyspark.sql import SparkSession, DataFrame

class Warc_Record:
    def __init__(self, meta_pairs: DefaultDict[str, str], response_meta: Tuple[str, str, int], source_html: str):
        self.warc_type = meta_pairs.get("WARC-Type", "")
    # var dateMs = -1L
    # val recordID = metaPairs.getOrElse("WARC-Record-ID", "")
    # var contentLength = -1
    # val contentType = metaPairs.getOrElse("Content-Type", "")
    # val infoID = metaPairs.getOrElse("WARC-Warcinfo-ID", "")
    # val concurrentTo = metaPairs.getOrElse("WARC-Concurrent-To", "")
    # val ip = metaPairs.getOrElse("WARC-IP-Address", "")
    # val targetURI = metaPairs.getOrElse("WARC-Target-URI", "")
    # val payloadDigest = metaPairs.getOrElse("WARC-Payload-Digest", "")
    # val blockDigest = metaPairs.getOrElse("WARC-Block-Digest", "")
    # val payloadType = metaPairs.getOrElse("WARC-Identified-Payload-Type", "")
    #
    # try {
    # contentLength = metaPairs.getOrElse("Content-Length", "-1").toInt
    # dateMs = Instant.parse(metaPairs.getOrElse("WARC-Date", "+1000000000-12-31T23:59:59.999999999Z")).getEpochSecond

# class ProfileParser:
#     def __init__(self, filename, normalize=True):
#         self.filename = filename
#         self.normalize = normalize  # normalize units
        self.html_source = source_html

def extract_warc_records(warc_loc: str, session: SparkSession):
    hadoop_confi = {"textinputformat.record.delimiter": "WARC/1.0"}
    warc_records = session.sparkContext \
        .newAPIHadoopFile(path=warc_loc, inputFormatClass="org.apache.hadoop.mapreduce.lib.input.TextInputFormat", keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text", conf=hadoop_confi)
    return warc_records.map(lambda tuple: tuple[1])