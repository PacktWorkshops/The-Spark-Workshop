from utilities01_py.helper_python import *
from pyspark.sql import DataFrame

if __name__ == "__main__":
    wet_location = os.path.join('..', '..', '..', '..', 'resources', 'wet_dataframe', '*.csv')
    warc_location = os.path.join('..', '..', '..', '..', 'resources', 'warc_dataframe', '*.csv')

    session = create_session(3, 'web corpus')
    # contains plain text plus meta info
    wet_df: DataFrame = session \
        .read \
        .option('header', True) \
        .csv(wet_location)

    wet_df.printSchema()
# root
# |-- warcType: string (nullable = true)
# |-- targetURI: string (nullable = true)
# |-- date: string (nullable = true)
# |-- recordID: string (nullable = true)
# |-- refersTo: string (nullable = true)
# |-- digest: string (nullable = true)
# |-- contentType: string (nullable = true)
# |-- contentLength: string (nullable = true)
# |-- plainText: string (nullable = true)

    for row in wet_df.take(5):
        print(row)

    #############################################################################
    # contains original HTTP response + header + lots of meta info

    warc_df: DataFrame = session \
        .read \
        .option('header', True) \
        .csv(warc_location)

    warc_df.printSchema()
# root
# |-- warcType: string (nullable = true)
# |-- date: string (nullable = true)
# |-- recordID: string (nullable = true)
# |-- contentLength: string (nullable = true)
# |-- contentType: string (nullable = true)
# |-- infoID: string (nullable = true)
# |-- concurrentTo: string (nullable = true)
# |-- ip: string (nullable = true)
# |-- targetURI: string (nullable = true)
# |-- payloadDigest: string (nullable = true)
# |-- blockDigest: string (nullable = true)
# |-- payloadType: string (nullable = true)
# |-- htmlContentType: string (nullable = true)
# |-- language: string (nullable = true)
# |-- htmlLength: string (nullable = true)
# |-- htmlSource: string (nullable = true)

    for row in warc_df.take(5):
        print(row)