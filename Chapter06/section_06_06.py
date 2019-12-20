spark = SparkSession \
   .builder \
   .appName("some_app_name") \
   .getOrCreate()

employment_df = spark.read.format("csv").load("hdfs://…/us_employment/aat1.csv"
                     , sep = ","
                     , inferSchema = "true"
                     , header = "true")


electricity_df = spark.read.format("csv").load("hdfs://…/electric/electricity-normalized.csv"
                     , sep = ","
                     , inferSchema = "true"
                     , header = "true")


adult_df = spark.read.format("csv").load("hdfs://…/adult/adult_data.csv"
                 , sep = ","
                 , inferSchema = "true"
                 , header = "false").toDF("age", "workclass", "fnlwgt", "education", "education-num", "marital-status", "occupation", "relationship", "race", "sex", "capital-gain", "capital-loss", "hours-per-week", "native-country", "class")

from pyspark.ml.linalg import Vectors

my_vector = Vectors.dense([4.0, 5.0, 0.0, 3.0])
print(my_vector)

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(
  inputCols = ['year', 'population', 'labor_force', 'population_percent', 'employed_total', 'employed_percent', 'agrictulture_ratio', 'nonagriculture_ratio', 'unemployed', 'unemployed_percent', 'not_in_labor']
  , outputCol = "features"
)

output = assembler.transform(employment_df)

output.select("features").show(10, truncate=False)




from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Summarizer

assembler = VectorAssembler(
  inputCols = ['year', 'population', 'labor_force', 'population_percent', 'employed_total', 'employed_percent', 'agrictulture_ratio', 'nonagriculture_ratio', 'unemployed', 'unemployed_percent', 'not_in_labor']
  , outputCol = "features"
)

assembled = assembler.transform(employment_df)

summarizer = Summarizer.metrics("max", "mean").summary(assembled["features"])

assembled.select(summarizer).show(truncate=False)

assembled.select(Summarizer.variance(assembled.features)).show(truncate=False)



from pyspark.ml.feature import VectorAssembler
from pyspark.ml.stat import Correlation

assembler = VectorAssembler(
  inputCols = ["date", "day", "period", "nswprice", "nswdemand", "vicprice", "vicdemand", "transfer"]
  , outputCol = "features"
)

assembled = assembler.transform(electricity_df)

pearson_corr = Correlation.corr(assembled, "features")

corr_list = pearson_corr.head()[0].toArray().tolist()
pearson_corr_df = spark.createDataFrame(corr_list)
pearson_corr_df.show(truncate=False)



from pyspark.ml.feature import StringIndexer

category_indexer = StringIndexer(
  inputCol = "category"
  , outputCol = "category_index"
)


pipeline = Pipeline(stages = [category_indexer, sales_range_indexer, prediction_indexer, assembler])

model = pipeline.fit(df)

transformed = model.transform(df)



from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.stat import ChiSquareTest

age_indexer = StringIndexer(
  inputCol = "age"
  , outputCol = "age_index"
)

education_indexer = StringIndexer(
  inputCol = "education"
  , outputCol = "education_index"
)

class_indexer = StringIndexer(
  inputCol = "class"
  , outputCol = "class_index"
)

assembler = VectorAssembler(
  inputCols = ["age_index", "education_index"]
  , outputCol = "features"
)

pipeline = Pipeline(stages = [education_indexer, age_indexer, class_indexer, assembler])

model = pipeline.fit(adult_cat_df)

transformed = model.transform(adult_cat_df)

chi_test = ChiSquareTest.test(transformed, "features", "class_index")

chi_test.show(truncate=False)

chi = chi_test.head()
print("pValues: " + str(chi.pValues))
print("degreesOfFreedom: " + str(chi.degreesOfFreedom))
print("statistics: " + str(chi.statistics))
