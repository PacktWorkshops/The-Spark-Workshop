from pyspark.rdd import RDD

from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import sample_wet_loc

if __name__ == "__main__":
    session = create_session(2, "Default crawl parsing")
    session.sparkContext.setLogLevel('ERROR')  # skips INFO messages
    records: RDD = session.sparkContext.textFile(sample_wet_loc)
    for record in records.take(50):
        print(record)
        print('---------------------------')
    print('###########################')
    print('Total # of records: ' + str(records.count()))
