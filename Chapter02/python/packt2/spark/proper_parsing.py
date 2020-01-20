from pyspark.rdd import RDD

from utilities01_py.helper_python import create_session
from globalp.python.packtg.helper_python_global import sample_wet_loc

if __name__ == "__main__":
    input_loc_wet: str = sample_wet_loc
    session = create_session(2, "Proper crawl parsing")
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    hadoop_conf = {"textinputformat.record.delimiter": "WARC/1.0"}
    input_format_name = 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
    record_pairs: RDD = session.sparkContext \
        .newAPIHadoopFile(path=input_loc_wet, inputFormatClass=input_format_name,
                          keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text",
                          conf=hadoop_conf)
    record_texts = record_pairs.map(lambda pair: pair[1].strip())

    for record in record_texts.take(5):
        print(record)
        print('---------------------------')
    print('###########################')
    print('Total records: ' + str(record_texts.count()))
