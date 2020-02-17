from typing import List
import sys

million_list: List[int] = []  # 8697472
for number in range(1, 1000001):
    million_list.append(-1)

print(sys.getsizeof(million_list))
print(million_list[0])



# PySpark equivalent to steps (1)-(5) simply consists in adding the Chapter02 module to the PYTHONPATH environment variable which can be accomplished in a terminal via
# export PYTHONPATH=$PYTHONPATH:/Users/UserName/IdeaProjects/The-Spark-Workshop
# This is necessary because spark_submit_parser.py is not fully self-contained, it depends on a number of helper functions from our workshop project such as Chapter02.python.packt2.helper_python.extract_raw_records


# ./spark-shell --jars /Users/user/IdeaProjects/The-Spark-Workshop/target/packt-uber-jar.jar


# Python:
# 6) In general, the packaging system for Python tends to be less sophisticated compared to JVM languages like Java or Scala. This turns out to be a blessing in this exercise, the PySpark equivalent to steps (1)-(5) simply consists in adding the Chapter02 module to the PYTHONPATH environment variable which can be accomplished in a terminal via
# export PYTHONPATH=$PYTHONPATH:/Users/UserName/IdeaProjects/The-Spark-Workshop
# This is necessary because spark_submit_parser.py is not fully self-contained, it depends on a number of helper functions from our workshop project such as Chapter02.python.packt2.helper_python.extract_raw_records
#
# The PySpark program contained in spark_submit_parser.py can now be executed outside of IntelliJ by calling spark-submit with just one argument, the full location of the file:
#     ~/spark-2.4.4-bin-hadoop2.7/bin/spark-submit /Users/UserName/IdeaProjects/The-Spark-Workshop/Chapter02/python/packt2/spark/spark_submit_parser.py
