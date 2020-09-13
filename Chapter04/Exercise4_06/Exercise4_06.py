from pyspark.sql import SparkSession
from pyspark.ml.common import _java2py
from Chapter01.utilities01_py.helper_python import create_session

#  ~/spark-2.4.6-bin-hadoop2.7/bin/spark-submit --driver-class-path ~/IdeaProjects/The-Spark-Workshop/target/packt-uber-jar.jar ~/IdeaProjects/The-Spark-Workshop/Chapter04/Exercise4_06/Exercise4_06.py
if __name__ == "__main__":
    session: SparkSession = create_session(2, "PySpark <> JVM")
    session.sparkContext.setLogLevel('ERROR')
    python_rdd = session.sparkContext.range(0, 5)

    java_rdd = session.sparkContext._jvm.SerDe.pythonToJava(python_rdd._jrdd, True)
    mapped_java_rdd = session.sparkContext._jvm.Exercise4_06.ScalaObject.executeInScala(java_rdd)
    mapped_python_rdd = _java2py(session.sparkContext, mapped_java_rdd)
    print(mapped_python_rdd.collect())
