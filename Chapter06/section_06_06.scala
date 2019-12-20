val spark = SparkSession
   .builder()
   .appName("some_app_name")
   .getOrCreate()


val employment_df = spark.read.format("csv")
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("hdfs://…/us_employment/aat1.csv")

val electricity_df = spark.read.format("csv")
  .option("sep", ",")
  .option("header", "true")
  .option("inferSchema", "true")
  .load("hdfs://…/electric/electricity-normalized.csv")

val adult_df = spark.read.format("csv")
  .option("sep", ",")
  .option("inferSchema", "true")
  .option("header", "false")
  .load("hdfs://…/adult/adult_data.csv").toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")


import org.apache.spark.ml.linalg.Vectors

val my_vector = Vectors.dense(4.0, 5.0, 0.0, 3.0)
println(my_vector)


import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("year", "population", "labor_force", "population_percent", "employed_total", "employed_percent", "agrictulture_ratio", "nonagriculture_ratio", "unemployed", "unemployed_percent", "not_in_labor"))
  .setOutputCol("features")

val assembled = assembler.transform(employment_df)

assembled.show(truncate=false)



import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Summarizer

val assembler = new VectorAssembler()
  .setInputCols(Array("year", "population", "labor_force", "population_percent", "employed_total", "employed_percent", "agrictulture_ratio", "nonagriculture_ratio", "unemployed", "unemployed_percent", "not_in_labor"))
  .setOutputCol("features")

val assembled = assembler.transform(employment_df)

val summarizer = Summarizer.metrics("max", "mean").summary(assembled("features"))

assembled.select(summarizer).show(truncate=false)



import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.linalg.Matrix

val assembler = new VectorAssembler()
  .setInputCols(Array("date", "day", "period", "nswprice", "nswdemand", "vicprice", "vicdemand", "transfer"))
  .setOutputCol("features")

val assembled = assembler.transform(electricity_df)

val coeff_df = Correlation.corr(assembled, "features", "spearman")

val Row(coeff_matrix: Matrix) = coeff_df.head

val matrix_rdd = spark.sparkContext.parallelize(coeff_matrix.rowIter.toSeq)
matrix_rdd.take(coeff_matrix.numRows).foreach(println)



import org.apache.spark.ml.feature.StringIndexer

val category _indexer = new StringIndexer()
  .setInputCol("category ")
  .setOutputCol("category _index")

val pipeline = new Pipeline()
  .setStages(Array(category_indexer, sales_range_indexer, prediction_indexer, assembler))

val model = pipeline.fit(df)

val transformed = model.transform(df)



import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.stat.ChiSquareTest

val age_indexer = new StringIndexer()
  .setInputCol("age")
  .setOutputCol("age_index")

val education_indexer = new StringIndexer()
  .setInputCol("education")
  .setOutputCol("education_index")

val class_indexer = new StringIndexer()
  .setInputCol("class")
  .setOutputCol("class_index")

val assembler = new VectorAssembler()
  .setInputCols(Array("age_index", "education_index"))
  .setOutputCol("features")

val pipeline = new Pipeline()
  .setStages(Array(education_indexer, age_indexer, class_indexer, assembler))

val model = pipeline.fit(adult_cat_df)

val transformed = model.transform(adult_cat_df)

// transformed.show()

val chi_test = ChiSquareTest.test(transformed, "features", "class_index")

chi_test.show(truncate=false)

val chi = chi_test.head
println(s"pValues: ${chi.getAs[Vector](0)}")
println(s"degreesOfFreedom: ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
println(s"statistics: ${chi.getAs[Vector](2)}")
