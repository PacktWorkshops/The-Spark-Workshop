from pyspark.rdd import RDD

from utilities01_py.helper_python import create_session
from globalp.python.packtg.helper_python_global import sample_wet_loc

if __name__ == "__main__":
    input_loc_wet: str = sample_wet_loc
    session = create_session(2, "Default crawl parsing")
    session.sparkContext.setLogLevel('ERROR')  # avoids printing of info messages
    records: RDD = session.sparkContext.textFile(sample_wet_loc)
    for record in records.take(50):
        print(record)
        print('---------------------------')
    print('###########################')
    print('Total records: ' + str(records.count()))
