import unittest
from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from globalp.python.packtg.helper_python_global import sample_warc_loc
from pyspark import RDD

from pyspark.sql.tests import ReusedSQLTestCase



class HeavyObject():
    def __init__(self, prefix):
        self.prefix = prefix

    def get_id(self):
        return id(self)


def map_function(record):
    # new_heavy_object = HeavyObject('MapPartition')
    # id = new_heavy_object.get_id()
    list = []
    for element in partition:
        list.append(23)
    return list

def partition_function(partition):
    # new_heavy_object = HeavyObject('MapPartition')
    # id = new_heavy_object.get_id()
    list = []
    for element in partition:
        list.append(23)
    return list


def normal_map(input_rdd):
    return input_rdd.map(lambda p: partition_function(p))

def map_partitions(input_rdd):
    return input_rdd.mapPartitions(lambda p: partition_function(p))


class Exercise18_03_Unit_Test(ReusedSQLTestCase):

    def test_map_vs_mappartitions(self):
        warc_loc = sample_warc_loc
        raw_records = extract_raw_records(warc_loc, self.spark)
        warc_records = raw_records \
            .flatMap(lambda record: parse_raw_warc(record))

        input_partitions = warc_records.getNumPartitions()
        numberOfRecords = warc_records.count()

        print('@@ Number of partitions: ' + str(input_partitions))


        ids_of_map = warc_records.map()

        ids_of_mappartitions = map_partitions(warc_records)
        print(ids_of_mappartitions.count())




if __name__ == '__main__':
    unittest.main()