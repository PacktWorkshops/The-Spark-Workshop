#!/usr/bin/env python

# create a spark session
import pyspark
spark_context = pyspark.SparkContext()
spark_session = pyspark.sql.SparkSession(spark_context)

# load csv file into dataframe
df = spark_session.read.csv("../ratings.csv", header=True,                    sep=",", inferSchema=True)
df.show()

# Print the schema to get the column names
df.printSchema()

# Split data into training and test dataframe
df_train, df_test = df.randomSplit([0.8, 0.2],seed=5)

# Train via collaborative filtering using ALS modeling
from pyspark.ml.recommendation import ALS, ALSModel
als = ALS(maxIter=1,           userCol="user_id", itemCol="book_id",           ratingCol="rating", coldStartStrategy="drop")
model = als.fit(df_train)

# Predict ratings on the test set
from pyspark.ml.evaluation import RegressionEvaluator
predictions = model.transform(df_test)
predictions.show()

# Evaluate the predictions using RMSE
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",        predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Train via collaborative filtering using ALS modeling
from pyspark.ml.recommendation import ALS, ALSModel
als = ALS(maxIter=2,           userCol="user_id", itemCol="book_id",           ratingCol="rating", coldStartStrategy="drop")
model = als.fit(df_train)

# Predict ratings on the test set
from pyspark.ml.evaluation import RegressionEvaluator
predictions = model.transform(df_test)
predictions.show()

# Evaluate the predictions using RMSE
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",        predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
userRecs.show()

# Load the books CSV
books_df = spark_session.read.csv("../books.csv", header=True,                    sep=",", inferSchema=True)

# Grab the first user's recommendations, and get the book names
recs = userRecs.first().recommendations

# List the details of the top 10 recommendations for this user
from pyspark.sql.functions import col
for pair in recs:
    book_id = pair.book_id
    rating = pair.rating
    title = books_df[ books_df.id == book_id ].select( col("title"))
    print( "rating=%1.1f, %s" % ( rating, title.first().title ) )

# Train via collaborative filtering using ALS modeling
from pyspark.ml.recommendation import ALS, ALSModel
als = ALS(maxIter=4,           userCol="user_id", itemCol="book_id",           ratingCol="rating", coldStartStrategy="drop")
model = als.fit(df_train)

# Predict ratings on the test set
from pyspark.ml.evaluation import RegressionEvaluator
predictions = model.transform(df_test)
predictions.show(10)

# Evaluate the predictions using RMSE
evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating",        predictionCol="prediction")
rmse = evaluator.evaluate(predictions)
print("Root-mean-square error = " + str(rmse))

# Generate top 10 movie recommendations for each user
userRecs = model.recommendForAllUsers(10)
userRecs.show(1)

# Grab the first user's recommendations, and get the book names
recs = userRecs.first().recommendations

# List the details of the top 10 recommendations for this user
from pyspark.sql.functions import col
for pair in recs:
    title = books_df[ books_df.id == pair.book_id ].select( col("title"))
    print( "rating=%1.1f, %s" % ( pair.rating, title.first().title ) )

