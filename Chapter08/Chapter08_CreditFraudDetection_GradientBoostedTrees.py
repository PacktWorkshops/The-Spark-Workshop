#!/usr/bin/env python

# create a spark session
import pyspark
spark_context = pyspark.SparkContext()
spark_session = pyspark.sql.SparkSession(spark_context)

# load csv file into dataframe
df = spark_session.read.csv("creditcard.csv", header=True,                    sep=",", inferSchema=True)
df.show(1)

# Print the schema to get the column names
df.printSchema()

from pyspark.ml.feature import VectorAssembler
vectorAssembler = VectorAssembler( inputCols= [ "V1","V2","V3","V4","V5",
                                                "V6","V7","V8","V9","V10",
                                                "V11","V12","V13","V14","V15",
                                                "V16","V17","V18","V19","V20"],
                                    outputCol= "features")
vdf = vectorAssembler.transform( df )
vdf = vdf.select(["features","class"])
vdf.show(1)

# Split data into training and test dataframe
df_train, df_test = vdf.randomSplit([0.8, 0.2],seed=5)
df_train.show(1)

# Create a GBT model and train it...
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
gbt = GBTRegressor(featuresCol="features", labelCol="class", maxIter=1)
pipeline = Pipeline(stages=[gbt])
model = pipeline.fit(df_train)

# Make predictions.
predictions = model.transform(df_test)
predictions = predictions.select("prediction", "class")
predictions.show()

from pyspark.sql.functions import udf,col
from pyspark.sql.types import IntegerType
func = udf(lambda prediction : 1 if prediction>0.5 else 0, IntegerType())
npredictions = predictions.withColumn('predclass', func( col("prediction") ) )
npredictions.show()

# Select (prediction, true label) and compute test error
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(
    labelCol="class", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Compute an AUC score
score = [ row.prediction for row in predictions.select('prediction').collect() ]
ground_truth = [ int(row["class"] ) for row in predictions.select('class').collect() ]
from sklearn import metrics
fpr, tpr, thresholds = metrics.roc_curve(ground_truth, score, pos_label=1)
print("AUC Score=", metrics.auc(fpr, tpr))

num_not_fraud = npredictions.filter(     ( npredictions["class"] == 0 ) ).count()
num_not_fraud_correct = npredictions.filter(     ( npredictions["class"] == 0 ) & ( npredictions["predclass"] == 0 ) )     .count()
print( "number of not fraud = ", num_not_fraud, ", number correctly classified", num_not_fraud_correct,       ", accuracy = ", num_not_fraud_correct/num_not_fraud )

num_fraud = npredictions.filter(     ( npredictions["class"] == 1 ) ).count()
num_fraud_correct = npredictions.filter(     ( npredictions["class"] == 1 ) & ( npredictions["predclass"] == 1 ) )     .count()
print( "number of fraud = ", num_fraud, ", number correctly classified", num_fraud_correct,       ", accuracy = ", num_fraud_correct/num_fraud )

# Create a GBT model and train it...
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
gbt = GBTRegressor(featuresCol="features", labelCol="class", maxIter=10)
pipeline = Pipeline(stages=[gbt])
model = pipeline.fit(df_train)

# Make predictions.
predictions = model.transform(df_test)
predictions = predictions.select("prediction", "class")

# Select (prediction, true label) and compute test error
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(
    labelCol="class", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Compute an AUC score
score = [ row.prediction for row in predictions.select('prediction').collect() ]
ground_truth = [ int(row["class"] ) for row in predictions.select('class').collect() ]
from sklearn import metrics
fpr, tpr, thresholds = metrics.roc_curve(ground_truth, score, pos_label=1)
print("AUC Score=", metrics.auc(fpr, tpr))

# Split data into training and test dataframe
df_train, df_test = vdf.randomSplit([0.8, 0.2],seed=10)

# Create a GBT model and train it...
from pyspark.ml.regression import GBTRegressor
from pyspark.ml import Pipeline
gbt = GBTRegressor(featuresCol="features", labelCol="class", maxIter=10)
pipeline = Pipeline(stages=[gbt])
model = pipeline.fit(df_train)

# Make predictions.
predictions = model.transform(df_test)
predictions = predictions.select("prediction", "class")

# Select (prediction, true label) and compute test error
from pyspark.ml.evaluation import RegressionEvaluator
evaluator = RegressionEvaluator(
    labelCol="class", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

# Compute an AUC score
score = [ row.prediction for row in predictions.select('prediction').collect() ]
ground_truth = [ int(row["class"] ) for row in predictions.select('class').collect() ]
from sklearn import metrics
fpr, tpr, thresholds = metrics.roc_curve(ground_truth, score, pos_label=1)
print("AUC Score=", metrics.auc(fpr, tpr))

