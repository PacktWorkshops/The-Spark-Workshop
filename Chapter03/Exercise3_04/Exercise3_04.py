from typing import Tuple
from pyspark.sql import SparkSession
from py4j.java_gateway import JavaGateway, CallbackServerParameters, GatewayParameters, launch_gateway
from Chapter02.utilities02_py.helper_python import extract_raw_records, parse_raw_warc
from Chapter02.utilities02_py.domain_objects import WarcRecord
from Chapter03.utilities03_py.RetryListener import RetryListener


def parse_method(text: str) -> Tuple[WarcRecord]:
    parsed_raw_warc = parse_raw_warc(text)
    # crasher = 5 / 0  # ToDo: Uncomment
    # print(crasher)  # ToDo: Uncomment
    return parsed_raw_warc


if __name__ == "__main__":
    session: SparkSession = SparkSession.builder \
        .master('local[3, 3]') \
        .appName('Failure Exploration') \
        .getOrCreate()

    session.sparkContext._gateway.start_callback_server()
    java_process = launch_gateway()
    gateway = JavaGateway(gateway_parameters=GatewayParameters(port=java_process),
                          callback_server_parameters=CallbackServerParameters(port=0))
    session.sparkContext._gateway.start_callback_server()
    listener = RetryListener()
    session.sparkContext._jsc.sc().addSparkListener(listener)

    print(session.sparkContext.parallelize(range(100), 3).collect())

    input_warc = '/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc'  # ToDo: Change path
    raw_records = extract_raw_records(input_warc, session)
    warc_records = raw_records.flatMap(parse_method)
    print(warc_records.count())

    gateway.shutdown_callback_server()
