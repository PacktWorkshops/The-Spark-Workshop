from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Create a Spark Session
spark = SparkSession \
    .builder \
    .appName("My Spark App") \
    .master("local[2]") \
    .getOrCreate()

# create spark contexts
sc = spark.sparkContext

my_previous_pets = [Row("fido", "dog", 4, "brown"),
                    Row("annabelle", "cat", 15, "white"),
                    Row("fred", "bear", 29, "brown"),
                    Row("daisy", "cat", 8, "black"),
                    Row("jerry", "cat", 1, "white"),
                    Row("fred", "parrot", 1, "brown"),
                    Row("gus", "fish", 1, "gold"),
                    Row("gus", "dog", 11, "black"),
                    Row("daisy", "iguana", 2, "green"),
                    Row("rufus", "dog", 10, "gold")]

# create RDDs
petsRDD = sc.parallelize(my_previous_pets)

# create data frames
petsDF = spark.createDataFrame(petsRDD, ['nickname', 'type', 'age', 'color'])
petsDF.registerTempTable('pets')

spark.sql("select nickname, age from pets where type = 'cat'").show()

# option 1: pure sql
spark.sql("select nickname as youngest_cat, "
          "min(age) as age "
          "from pets "
          "where type = 'cat' "
          "group by nickname "
          "order by age asc "
          "limit 1")\
    .show()

spark.sql("select nickname as oldest_cat, "
          "max(age) as age "
          "from pets "
          "where type = 'cat' "
          "group by nickname "
          "order by age desc "
          "limit 1")\
    .show()

# # option 2: functional
petsDF.where("type = 'cat'").sort("age").limit(1).show()
petsDF.where("type = 'cat'").sort(col("age").desc()).limit(1).show()
