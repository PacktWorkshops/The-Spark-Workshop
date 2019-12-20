val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()


val adult_df = spark.read.format("csv")
   .option("sep", ",")
   .option("inferSchema", "true")
   .option("header", "false")
   .load("hdfs://…/adult/adult_data.csv")
   .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")
   .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")
   .withColumnRenamed("marital-status", "marital_status")
   .withColumnRenamed("hours-per-week", "hours_per_week")
   .withColumnRenamed("native-country", "native_country")
   .withColumnRenamed("class", "class_label")

case class Store(store_name: String, zipcode: Integer)

case class Adult(age: Integer, workclass: String, education: String, marital_status: String, occupation: String, relationship: String, race: String, sex: String, hours_per_week: Double, native_country: String, class_label: String)

val adult_ds = adult_df.as[Adult]


val Array(training_ds, testing_ds) = adult_ds.randomSplit(Array(0.7, 0.3), seed = 775)



case class Adult(age: Integer, workclass: String, education: String, marital_status: String, occupation: String, relationship: String, race: String, sex: String, hours_per_week: Double, native_country: String, class_label: String)

val adult_ds = adult_df.as[Adult]

val Array(training_ds, testing_ds) = adult_ds.randomSplit(Array(0.7, 0.3), seed = 775)

training_ds.count

training_ds.select("native_country").except(testing_ds.select("native_country")).show()

val excluded_countries = training_ds.select("native_country").except(testing_ds.select("native_country")).collect().map(x => x(0))

val new_training_ds = training_ds.filter(not($"native_country".isin(excluded_countries:_*)))

new_training_ds.count
