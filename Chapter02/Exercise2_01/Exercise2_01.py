from pyspark.rdd import RDD
from pyspark.sql import SparkSession

from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import sample_wet_loc

if __name__ == "__main__":
    session: SparkSession = create_session(2, "Default parsing")
    session.sparkContext.setLogLevel('ERROR')  # skips INFO messages
    records: RDD = session.sparkContext.textFile(sample_wet_loc)
    for record in records.take(50):
        print(record)
        print('-' * 20)
    print('#' * 40)
    print('Total # of records: ' + str(records.count()))
