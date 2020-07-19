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



object Exercise8_01 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("housing-price-prediction-regression")
      .master( "local[*]")
      .getOrCreate()
    

    val housing = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", " ")
    .load("/Users/arushkharbanda/The-Spark-Workshop/Chapter08/Data/housing.csv")
    .withColumnRenamed("MV","label");
    
    housing.show()
    
    housing.describe().toDF().show()
    

    val vectorAssemblerFeatures = new VectorAssembler().setInputCols( Array("CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PT", "B", "LSTAT")).setOutputCol("features")
    val vectorAssemblerFeaturesAndLabel = new VectorAssembler().setInputCols( Array("CRIM", "ZN", "INDUS", "CHAS", "NOX", "RM", "AGE", "DIS", "RAD", "TAX", "PT", "B", "LSTAT","label")).setOutputCol("featuresAndLabel")

    val pipeline = new Pipeline().setStages(Array(vectorAssemblerFeatures,vectorAssemblerFeaturesAndLabel))

    val model = pipeline.fit(housing)

    val housing_transformed = model.transform(housing)
    
    housing_transformed.select("label", "features","featuresAndLabel").show()
    
    
    val correlationRow=Correlation.corr(housing_transformed, "featuresAndLabel").head()
    val correlationArray=correlationRow.get(0)
    printf(correlationArray.toString())
    
    
    val testResult = ChiSquareTest.test(housing_transformed, "features", "label")

    val resultRow=testResult.head()
    val pValues=resultRow.get(0)
    val degreesOfFreedom=resultRow.get(1)
    val statistics=resultRow.get(2)
    
    
    val splitArr=  housing_transformed.select("features","label").randomSplit(Array(0.7, 0.3), 12345)
    val train=splitArr(0)
    val test=splitArr(1)
    
    
    val lr =new  LinearRegression()
    .setMaxIter(100)
    .setRegParam(0.3)
    .setElasticNetParam(0.8)
    
    val lr_model = lr.fit(train)
    
    
    
    val prediction=lr_model.transform(test)
    val lr_evaluator_r2 = new  RegressionEvaluator()
                          .setPredictionCol("prediction")
                          .setLabelCol("label")
                          .setMetricName("r2")
    val lr_evaluator_rmse = new RegressionEvaluator()
                          .setPredictionCol("prediction")
                         .setLabelCol("label")
                          .setMetricName("rmse")
    println("Linear Regression R2 "+lr_evaluator_r2.evaluate(prediction))
    println("Linear Regression RMSE "+lr_evaluator_rmse.evaluate(prediction))
    
    
    

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01, 0.3)) 
      .addGrid(lr.maxIter, Array(10, 100, 500)) 
      .build()

    val crossval = new CrossValidator()
    .setEstimator(lr)
    .setEstimatorParamMaps(paramGrid)
    .setEvaluator(lr_evaluator_r2)
    .setNumFolds(4)
    
                         
    val cvModel = crossval.fit(train)
    
    val prediction_val = cvModel.transform(test)
    val selected = prediction_val.select("prediction","label")
    
    val bestModel=cvModel.bestModel
    println("Linear Regression R2 "+lr_evaluator_r2.evaluate(prediction_val))
    println("Linear Regression RMSE "+lr_evaluator_rmse.evaluate(prediction_val))


  }
}