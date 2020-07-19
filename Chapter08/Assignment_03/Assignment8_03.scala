package com.packtpub.workshop.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.ml.regression.LinearRegression

import org.apache.spark.ml.evaluation.RegressionEvaluator

import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS



object Exercise8_03 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("book-recommendation")
      .master( "local[*]")
      .getOrCreate()
    

    val book_rating = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load("/Users/arushkharbanda/The-Spark-Workshop/Chapter08/Data/ratings.csv")
    .withColumnRenamed("user_id", "user").withColumnRenamed("book_id", "item");
    
    
    val splitArr=  book_rating.randomSplit(Array(0.7, 0.3), 12345)
    val train=splitArr(0)
    val test=splitArr(1)
    
    book_rating.show()
    
    
    val als = new ALS()
    .setRank(100)
    .setMaxIter(5)
    .setRegParam(0.09)
    .setColdStartStrategy("drop")
    .setNonnegative(true)
    
    val model = als.fit(train)
    

    val evaluator=new RegressionEvaluator().setMetricName("r2").setLabelCol("rating").setPredictionCol("prediction")
    
    val predictions=model.transform(test)
    evaluator.evaluate(predictions)
    
    model.recommendForAllUsers(1).show()
    
    
    

    val paramGrid = new ParamGridBuilder()
    .addGrid(als.regParam, Array(0.1, 0.09))
    .addGrid(als.maxIter, Array(10, 12)) 
    .addGrid(als.rank, Array(20,50,100)) 
    .build()

    val crossval =new CrossValidator()
    .setEstimator(als)
    .setEstimatorParamMaps(paramGrid)
                          .setEvaluator(evaluator)

    val cvModel = crossval.fit(train)


    
    val bestModel=cvModel.bestModel
    
    val predictions_val=bestModel.transform(test)
    print("R2 for the best model using ALS "+evaluator.evaluate(predictions_val))

    

  }
}