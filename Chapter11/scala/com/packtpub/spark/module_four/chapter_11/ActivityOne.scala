package com.packtpub.spark.module_four.chapter_11

import org.apache.spark.sql.SparkSession

object ActivityOne {

  case class Saying(word: String)

  def main(args: Array[String]): Unit = {

    // Build a SparkSession in Local Mode
    val spark = SparkSession
      .builder()
      .master("local[2]")
      .appName("My Spark App")
      .enableHiveSupport()
      .getOrCreate()

    // Import Spark Implicits for using .as[] function
    import spark.implicits._

    // Create some data from a funny saying with repetitious words in it
    val saying = "You cannot end a sentence with because, because because is a conjunction."

    // split the words by space so that they form a sequence of words
    val wordsOfTheSaying = saying.split(" ")

    // Consume the data into spark by parallelizing it and converting to a DataFrame (and optionally, a DataSet)
    val sayingInSpark = spark.sparkContext.parallelize(wordsOfTheSaying)
      .toDF("word")
      .as[Saying]

    sayingInSpark.show()

    // Clean the data by removing unneeded punctuation (by mapping over the data)
    val cleanedSaying = sayingInSpark.map(record => {

      // Store the word for continued use
      val wordOfSaying = record.word

      // if this word contains a comma, exclamation mark, or period, remove it!
      if (wordOfSaying.contains(",") || wordOfSaying.contains("!") || wordOfSaying.contains(".")){
        val cleanedWord = wordOfSaying
                          .replace(",", "")
                          .replace("!", "")
                          .replace(".", "")

        // Return the cleaned up word
        Saying(cleanedWord)
      }
      else {

        // No punctuation found, return the word as-is
        Saying(wordOfSaying)
      }
    })

    cleanedSaying.show()

    // Analyze the cleaned up Saying by counting the occurrence of each word within
    val analysis = cleanedSaying
      .groupBy("word")
      .agg("word" -> "count")
      .orderBy($"count(word)".desc)

    // Let's see those results!
    analysis.show()
  }
}