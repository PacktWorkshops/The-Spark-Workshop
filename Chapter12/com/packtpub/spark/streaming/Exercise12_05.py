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

topics = ['website_traffic']

kafkaStream = KafkaUtils.createDirectStream(ssc=ssc, topics=topics, kafkaParams=kafka_params)


# Function to Parse Raw Web Traffic
def parse(record):
    userid_and_actionid = record[1]
    parts = userid_and_actionid.split(",")
    user_id = parts[0]
    action_id = parts[1]
    return int(action_id), user_id

# parse the data into a clean format
parsed_web_traffic = kafkaStream.map(parse)

# Create action description lookup data
actions = [(1, "opened website"), (2, "clicked"), (3, "scrolled")]
webActions = sc.parallelize(actions)

# Function to Join web actions with their descriptions
def join(record):
    return record.join(webActions)

# join the data
enhanced = parsed_web_traffic.transform(join)
enhanced.pprint()

ssc.start()
ssc.awaitTermination()
