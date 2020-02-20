spark = SparkSession \
   .builder \
   .appName("exercise_twentytwo") \
   .getOrCreate()

from pyspark.sql.types import DateType

registration_df = df.withColumn("registration_date", df.registration_dttm.cast(DateType())).select("id", "registration_dttm", "registration_date")

registration_df.show(3)
registration_df.printSchema()
