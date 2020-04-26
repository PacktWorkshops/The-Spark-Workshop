import unittest
from typing import List
from pyspark.rdd import RDD
from pyspark.sql.tests import ReusedSQLTestCase
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc, sample_warc_loc
from Chapter03.utilities03_py.HeavyObject import HeavyObject


class Exercise3_03_Unit_Test(ReusedSQLTestCase):
    def test_perrecord_vs_perpartition(self):
        raw_records = extract_raw_records(sample_warc_loc, self.spark)
        warc_records = raw_records \
            .flatMap(lambda record: parse_raw_warc(record))

        def map_function(_):
            new_heavy_object = HeavyObject('map')
            objet_id = new_heavy_object.get_id()
            return objet_id

        def partition_function(partition):
            new_heavy_object = HeavyObject('mapPartition')
            object_id = new_heavy_object.get_id()
            for _ in partition:
                yield object_id

        ids_after_map: RDD = warc_records.map(map_function)
        unique_ids_map: List[int] = ids_after_map.distinct().collect()

        ids_after_mappartitions: RDD = warc_records.mapPartitions(partition_function)
        unique_ids_mappartitions: List[int] = ids_after_mappartitions.distinct().collect()

        print('@' * 50)
        number_of_records: int = warc_records.count()
        number_of_partitions: int = warc_records.getNumPartitions()
        print('@@ Number of records: {}'.format(number_of_records))
        print('@@ Number of partitions: {}'.format(number_of_partitions))
        self.assertGreater(len(unique_ids_map), len(unique_ids_mappartitions))
        self.assertGreaterEqual(number_of_partitions, len(unique_ids_mappartitions))
        print(unique_ids_map)
        print(unique_ids_mappartitions)


if __name__ == '__main__':
    unittest.main()
