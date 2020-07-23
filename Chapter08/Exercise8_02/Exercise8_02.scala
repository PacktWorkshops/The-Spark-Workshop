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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit

import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.feature.StringIndexer

import org.apache.spark.ml.feature.Word2Vec

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.classification.LogisticRegression

import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.tuning.ParamGridBuilder



object Exercise8_02 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("genre-classification")
      .master( "local[*]")
      .getOrCreate()
    

    var data = spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", ",")
    .load("/Users/arushkharbanda/The-Spark-Workshop/Chapter08/Data/wiki_movie_plots_deduped.csv")
    
    data=data.na.drop()
    data=data.filter("genre in ('drama', 'comedy','horror')")
    
    data.show()
    
    val total=data.count()
    
    
    
    val genre_clean=data.groupBy("Genre").count().sort("count").withColumnRenamed("Genre", "GenreAgg")
    
    genre_clean.show()
            
    val weights=genre_clean.withColumn("total",lit(total)).withColumn("ratio",col("total")/col("count"))
    
    
    
    val data_final=data.join(weights, weights("GenreAgg")===data("genre"))
    data_final.show()



    val tokenizer = new Tokenizer()
    .setInputCol("Plot")
    .setOutputCol("tokens")
  
    val w2v = new Word2Vec()
    .setVectorSize(300)
    .setMinCount(0) 
    .setInputCol("tokens")
    .setOutputCol("features")
    val indexer = new StringIndexer()
    .setInputCol("Genre")
    .setOutputCol("label")
    val doc2vec_pipeline = new Pipeline().setStages((Array(tokenizer,w2v,indexer)))
    
    
    val doc2vec_model = doc2vec_pipeline.fit(data_final)
    val doc2vecs_df = doc2vec_model.transform(data_final)
    doc2vecs_df.select("tokens", "features","label").show()
    
    
    
    val train_n_test = doc2vecs_df.randomSplit(Array(0.7, 0.3), seed=12345)
    val train= train_n_test(0)
    val test= train_n_test(1)
    val lr_classifier = new LogisticRegression()
    .setFamily("multinomial")
    .setWeightCol("ratio")
    
    val lr_classifier_pipeline = new Pipeline().setStages(Array(lr_classifier))
    val lr_predictions = lr_classifier_pipeline.fit(train).transform(test)
    
    val lr_model_evaluator =new  MulticlassClassificationEvaluator()
    .setLabelCol("label")
    .setPredictionCol("prediction")
    .setMetricName("accuracy")
    
        
     print("LogisticRegression accuracy "+lr_model_evaluator.evaluate(lr_predictions))
        
        
        
        



  }
}