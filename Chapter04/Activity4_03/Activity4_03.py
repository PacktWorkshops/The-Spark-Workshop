from sys import argv
from pyspark.ml.common import _java2py
from Chapter01.utilities01_py.helper_python import create_session
from Chapter02.utilities02_py.helper_python import sample_warc_loc, extract_raw_records, parse_raw_warc


#  ~/spark-2.4.6-bin-hadoop2.7/bin/spark-submit  --driver-class-path  ~/IdeaProjects/The-Spark-Workshop/target/packt-uber-jar.jar:/Users/a/.m2/repository/com/google/guava/guava/28.2-jre/guava-28.2-jre.jar:/Users/a/.m2/repository/org/apache/commons/commons-compress/1.20/commons-compress-1.20.jar ~/IdeaProjects/The-Spark-Workshop/Chapter04/Activity4_03/Activity4_03.py ~/Output_Act4_3
if __name__ == "__main__":
    output_dir = argv[1]
    session = create_session(3, 'WARC Parser')

    warc_records = extract_raw_records(sample_warc_loc, session) \
        .flatMap(lambda record: parse_raw_warc(record)) \
        .filter(lambda record: record.warc_type == 'response')

    plaintexts_rdd = warc_records.map(lambda record: record.html_source)
    java_rdd = session.sparkContext._jvm.SerDe.pythonToJava(plaintexts_rdd._jrdd, True)
    tagged_java_rdd = session.sparkContext._jvm.Activity4_03.Activity4_03.tagJavaRDD(java_rdd)
    tagged_python_rdd = _java2py(session.sparkContext, tagged_java_rdd)

    tagged_python_rdd.saveAsTextFile(output_dir)
