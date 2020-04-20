from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

sc = SparkContext("local[2]", "My Spark App")
ssc = StreamingContext(sc, 10)

kafka_params = {"bootstrap.servers": "localhost:9092",
                "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
                "group.id": "streaming_test",
                "auto.offset.reset": "largest",
                "enable.auto.commit": "False"}

topics = ['test']

kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, topics=topics, kafkaParams=kafka_params)

kafkaStream.count().pprint()
kafkaStream.pprint()

ssc.start()
ssc.awaitTermination()
