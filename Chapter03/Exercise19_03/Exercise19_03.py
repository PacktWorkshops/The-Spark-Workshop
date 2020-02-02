from pyspark.sql import SparkSession
from utilities01_py.helper_python import create_session
from utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from globalp.python.packtg.helper_python_global import sample_warc_loc


class HeavyObject:
    def __init__(self, prefix):
        print(prefix + ' new heavy object created')
        self.prefix = prefix

    def get_id(self):
        return id(self)


def map_function(_):
    new_heavy_object = HeavyObject('MapRecord')
    object_id = new_heavy_object.get_id()
    return object_id


def partition_function(partition):
    new_heavy_object = HeavyObject('MapPartition')
    object_id = new_heavy_object.get_id()
    return iter([object_id for _ in partition])


if __name__ == "__main__":
    warc_loc = sample_warc_loc
    session: SparkSession = create_session(2, "PerRecordVsPerPartition")

    raw_records = extract_raw_records(warc_loc, session)
    warc_records = raw_records \
        .flatMap(lambda record: parse_raw_warc(record))

    input_partitions = warc_records.getNumPartitions()
    number_of_records = warc_records.count()

    ids_of_map = warc_records \
        .map(map_function) \
        .count()
    print('@' * 50)

    ids_of_mappartition = warc_records \
        .mapPartitions(partition_function) \
        .distinct() \
        .collect()
    print('@' * 50)

    print('@@ Number of partitions: ' + str(input_partitions))
    print('@@ Number of records: ' + str(number_of_records))
    # assert(ids_of_map == numberOfRecords)
    print('##' + str(ids_of_mappartition))
    assert (ids_of_mappartition.count() == input_partitions)
