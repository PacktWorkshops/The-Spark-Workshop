from pyspark.rdd import RDD
from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import sample_wet_loc

if __name__ == "__main__":
    session = create_session(2, "Proper crawl parsing")
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages

    hadoop_conf = {"textinputformat.record.delimiter": "WARC/1.0"}
    input_format_name = 'org.apache.hadoop.mapreduce.lib.input.TextInputFormat'
    record_pairs: RDD = session.sparkContext \
        .newAPIHadoopFile(path=sample_wet_loc, inputFormatClass=input_format_name,
                          keyClass="org.apache.hadoop.io.LongWritable", valueClass="org.apache.hadoop.io.Text",
                          conf=hadoop_conf)
    record_texts = record_pairs.map(lambda pair: pair[1].strip())

    for record in record_texts.take(5):
        print(record)
        print('-' * 20)
    print('#' * 40)
    print('Total # of records: ' + str(record_texts.count()))
