from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

animalsNormalized = [Row("fido", 1, 4, 1),
                     Row("annabelle", 2, 15, 2),
                     Row("fred", 3, 29, 1),
                     Row("fred", 4, 1, 1),
                     Row("gus", 5, 1, 4),
                     Row("daisy", 6, 2, 5)]

animalTypeLookup = [Row("dog", 1),
                    Row("cat", 2),
                    Row("bear", 3),
                    Row("parrot", 4),
                    Row("fish", 5),
                    Row("iguana", 6)]

animalColorLookup = [Row("brown", 1),
                     Row("white", 2),
                     Row("black", 3),
                     Row("gold", 4),
                     Row("green", 5),
                     Row("red", 6)]

petsRDD = sc.parallelize(animalsNormalized)
colorsRDD = sc.parallelize(animalColorLookup)
typesRDD = sc.parallelize(animalTypeLookup)

petsDF = spark.createDataFrame(petsRDD, ['nickname', 'type', 'age', 'color'])
colors = spark.createDataFrame(colorsRDD, ['color_name', 'color_id'])
types = spark.createDataFrame(typesRDD, ['type_name', 'type_id'])

petsWithColors = petsDF.join(colors, col("color") == col("color_id"), how="left")
petsWithColors.select("nickname", "color_name", "age").show()

petsWithColorAndType = petsWithColors.join(types, col("type") == col("type_id"), how="left")
petsWithColorAndType.select("nickname", "type_name", "age", "color_name").show()