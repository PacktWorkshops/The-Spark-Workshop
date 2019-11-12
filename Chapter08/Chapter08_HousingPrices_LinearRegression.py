#!/usr/bin/env python

# create a spark session
import pyspark
spark_context = pyspark.SparkContext()
spark_session = pyspark.sql.SparkSession(spark_context)

# load csv file into dataframe
df = spark_session.read.csv("../nhousing.csv", header=False, inferSchema=True)
df.show()

# rename the columns to be more descriptive
df = df.selectExpr("_c0 as crim", "_c1 as zn", "_c2 as indux",                    "_c3 as chas", "_c4 as nox", "_c5 as rm",                    "_c6 as age", "_c7 as dis", "_c8 as rad",                    "_c9 as tax", "_c10 as ptratio", "_c11 as b",                    "_c12 as lstat", "_c13 as price")
df.printSchema()   

# transform into a dataframe suitable for machine learning
from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler( inputCols =     ['crim', 'zn', 'indux', 'chas', 'nox', 'rm', 'age',      'dis', 'rad', 'tax', 'ptratio', 'b', 'lstat'],     outputCol = 'features' )
mldf = vectorAssembler.transform(df)
mldf = mldf.select(['features','price'])
mldf.show(5)

# Split the data into a training and test dataframe
train_df, test_df = mldf.randomSplit([0.8, 0.2],seed=5) 

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=1)
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features").show()

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=50)
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features")

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=100, regParam=1, elasticNetParam=1 )
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features")

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=50, regParam=0.2, elasticNetParam=0.85 )
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features")

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=100, regParam=0.2, elasticNetParam=0.85 )
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features")

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

# Split the data into a training and test dataframe
train_df, test_df = mldf.randomSplit([0.8, 0.2],seed=20) 

# Train user linear regression
from pyspark.ml.regression import LinearRegression
lr = LinearRegression(featuresCol='features',         labelCol='price', maxIter=100, regParam=0.2, elasticNetParam=0.85 )
lr_model = lr.fit(train_df)

# Do some predictions on the hold-out, test set
predictions = lr_model.transform(test_df)
predictions.select("prediction","price","features")

print("Training R Squared: %f" % lr_model.summary.r2)

from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator( predictionCol="prediction",     labelCol="price", metricName="r2" )
print("Test R Squared: %g" % evaluator.evaluate(predictions))

