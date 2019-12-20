spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()


adult_cat_df = spark.read.format("csv") \
  .load("hdfs://…/adult/adult_data.csv"
       , sep = ","
       , inferSchema = "true"
       , header = "false") \
  .toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class") \
  .drop("fnlwgt", "education-num", "capital-gain", "capital-loss")


# 1 a. Remove whitespaces from every cell in the DataFrame
from pyspark.sql.functions import trim

trimmed_df = adult_cat_df \
  .withColumn("workclass", trim(adult_cat_df["workclass"])) \
  .withColumn("education", trim(adult_cat_df["education"])) \
  .withColumn("marital-status", trim(adult_cat_df["marital-status"])) \
  .withColumn("occupation", trim(adult_cat_df["occupation"])) \
  .withColumn("relationship", trim(adult_cat_df["relationship"])) \
  .withColumn("race", trim(adult_cat_df["race"])) \
  .withColumn("sex", trim(adult_cat_df["sex"])) \
  .withColumn("native-country", trim(adult_cat_df["native-country"])) \
  .withColumn("class", trim(adult_cat_df["class"]))

# 1 b. Drop duplicated rows
dups_dropped_df = trimmed_df.dropDuplicates()

# 1 c. Replace every cell that is "?" to null value
replaced_questions_df = dups_dropped_df.replace("?", None)

# 1 d. Remove rows that have less than 9 non-null values
clean_df = replaced_questions_df.dropna(thresh = 9)

# 2. Split the cleaned DataFrame into training and testing DataFrames
training, testing = clean_df.randomSplit([0.7, 0.3], seed = 535)

# 3.	Create a ML Pipeline with StringIndexers, OneHotEncoderEstimator, VectorAssembler along with the training and testing DataFrames.
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import OneHotEncoderEstimator

age_indexer = StringIndexer(
  inputCol = "age"
  , outputCol = "age_index"
  , handleInvalid = "keep"
)

workclass_indexer = StringIndexer(
  inputCol = "workclass"
  , outputCol = "workclass_index"
  , handleInvalid = "keep"
)

education_indexer = StringIndexer(
  inputCol = "education"
  , outputCol = "education_index"
  , handleInvalid = "keep"
)

marital_indexer = StringIndexer(
  inputCol = "marital-status"
  , outputCol = "marital-status_index"
  , handleInvalid = "keep"
)

occupation_indexer = StringIndexer(
  inputCol = "occupation"
  , outputCol = "occupation_index"
  , handleInvalid = "keep"
)

relationship_indexer = StringIndexer(
  inputCol = "relationship"
  , outputCol = "relationship_index"
  , handleInvalid = "keep"
)

race_indexer = StringIndexer(
  inputCol = "race"
  , outputCol = "race_index"
  , handleInvalid = "keep"
)

sex_indexer = StringIndexer(
  inputCol = "sex"
  , outputCol = "sex_index"
  , handleInvalid = "keep"
)

hours_indexer = StringIndexer(
  inputCol = "hours-per-week"
  , outputCol = "hours-per-week_index"
  , handleInvalid = "keep"
)

country_indexer = StringIndexer(
  inputCol = "native-country"
  , outputCol = "native-country_index"
  , handleInvalid = "keep"
)

class_indexer = StringIndexer(
  inputCol = "class"
  , outputCol = "class_index"
  , handleInvalid = "keep"
)

encoder = OneHotEncoderEstimator(
  inputCols = ["age_index", "workclass_index", "education_index", "marital-status_index", "occupation_index", "relationship_index", "race_index", "sex_index", "hours-per-week_index", "native-country_index", "class_index"]
  , outputCols = ["age_vec", "workclass_vec", "education_vec", "marital-status_vec", "occupation_vec", "relationship_vec", "race_vec", "sex_vec", "hours-per-week_vec", "native-country_vec", "class_vec"]
)

assembler = VectorAssembler(
  inputCols = ["age_vec", "workclass_vec", "education_vec", "marital-status_vec", "occupation_vec", "relationship_vec", "race_vec", "sex_vec", "hours-per-week_vec", "native-country_vec"]
  , outputCol = "features"
)

pipeline = Pipeline(stages = [age_indexer, workclass_indexer, education_indexer, marital_indexer, occupation_indexer, relationship_indexer, race_indexer, sex_indexer, hours_indexer, country_indexer, class_indexer, encoder, assembler])

model = pipeline.fit(training)

transformed = model.transform(testing)

transformed.select("age", "age_index", "age_vec", "education", "education_index", "education_vec", "class", "class_index", "class_vec").show(10, truncate=False)

# 4.

(transformed.write
  .format("parquet")
  .mode("overwrite")
  .partitionBy("sex")
  .save("hdfs://…/adult_ml_ready"))
