val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()


// 1 a. Remove whitespaces from every cell in the DataFrame
import org.apache.spark.sql.functions.trim

val trimmed_df = adult_cat_df
   .withColumn("workclass", trim(adult_cat_df("workclass")))
   .withColumn("education", trim(adult_cat_df("education")))
   .withColumn("marital-status", trim(adult_cat_df("marital-status")))
   .withColumn("occupation", trim(adult_cat_df("occupation")))
   .withColumn("relationship", trim(adult_cat_df("relationship")))
   .withColumn("race", trim(adult_cat_df("race")))
   .withColumn("sex", trim(adult_cat_df("sex")))
   .withColumn("native-country", trim(adult_cat_df("native-country")))
   .withColumn("class", trim(adult_cat_df("class")))

// 1 b. Drop duplicated rows
val dups_dropped_df = trimmed_df.dropDuplicates

// 1 c. Replace every cell that is "?" to null value
val replaced_questions_df = dups_dropped_df.na.replace("*", Map("?" -> null))

// 1 d. Remove rows that have less than 9 non-null values
val clean_df = replaced_questions_df.na.drop(minNonNulls = 9)

// 2. Split the cleaned DataFrame into training and testing DataFrames
val Array(training, testing) = clean_df.randomSplit(Array(0.7, 0.3), seed = 535)

// 3. Create a ML Pipeline with StringIndexers, OneHotEncoderEstimator, VectorAssembler along with the training and testing DataFrames
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.OneHotEncoderEstimator

val age_indexer = new StringIndexer()
 .setInputCol("age")
 .setOutputCol("age_index")
 .setHandleInvalid("keep")

val workclass_indexer = new StringIndexer()
 .setInputCol("workclass")
 .setOutputCol("workclass_index")
 .setHandleInvalid("keep")

val education_indexer = new StringIndexer()
 .setInputCol("education")
 .setOutputCol("education_index")

val marital_indexer = new StringIndexer()
 .setInputCol("marital-status")
 .setOutputCol("marital-status_index")
 .setHandleInvalid("keep")

val occupation_indexer = new StringIndexer()
 .setInputCol("occupation")
 .setOutputCol("occupation_index")
 .setHandleInvalid("keep")

val relationship_indexer = new StringIndexer()
 .setInputCol("relationship")
 .setOutputCol("relationship_index")
 .setHandleInvalid("keep")

val race_indexer = new StringIndexer()
 .setInputCol("race")
 .setOutputCol("race_index")
 .setHandleInvalid("keep")

val sex_indexer = new StringIndexer()
 .setInputCol("sex")
 .setOutputCol("sex_index")
 .setHandleInvalid("keep")

val hours_indexer = new StringIndexer()
 .setInputCol("hours-per-week")
 .setOutputCol("hours-per-week_index")
 .setHandleInvalid("keep")

val country_indexer = new StringIndexer()
 .setInputCol("native-country")
 .setOutputCol("native-country_index")
 .setHandleInvalid("keep")

val class_indexer = new StringIndexer()
 .setInputCol("class")
 .setOutputCol("class_index")
 .setHandleInvalid("keep")

val encoder = new OneHotEncoderEstimator()
 .setInputCols(Array("age_index", "workclass_index", "education_index", "marital-status_index", "occupation_index", "relationship_index", "race_index", "sex_index", "hours-per-week_index", "native-country_index", "class_index"))
 .setOutputCols(Array("age_vec", "workclass_vec", "education_vec", "marital-status_vec", "occupation_vec", "relationship_vec", "race_vec", "sex_vec", "hours-per-week_vec", "native-country_vec", "class_vec"))

val assembler = new VectorAssembler()
 .setInputCols(Array("age_vec", "workclass_vec", "education_vec", "marital-status_vec", "occupation_vec", "relationship_vec", "race_vec", "sex_vec", "hours-per-week_vec", "native-country_vec"))
 .setOutputCol("features")

val pipeline = new Pipeline()
 .setStages(Array(age_indexer, workclass_indexer, education_indexer, marital_indexer, occupation_indexer, relationship_indexer, race_indexer, sex_indexer, hours_indexer, country_indexer, class_indexer, encoder, assembler))

val model = pipeline.fit(training)

val transformed = model.transform(testing)

transformed.select("age", "age_index", "age_vec", "education", "education_index", "education_vec", "class", "class_index", "class_vec").show(10, truncate=false)

// 4.

transformed.write
  .format("parquet")
  .mode("overwrite")
  .partitionBy("sex")
  .save("hdfs://…/adult_ml_ready")
