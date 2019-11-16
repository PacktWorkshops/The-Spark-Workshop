package packt1.solutions

import org.apache.spark.ml.feature.{RegexTokenizer, Tokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import packt1.HelperScala.{createSession, novellaLocation, separator}

object ZipfValidationB {

  def main(args: Array[String]): Unit = {


    val tokenizer = new Tokenizer().setInputCol("sentences").setOutputCol("words");

//    val tokenizer0 = new RegexTokenizer()
//      .setGaps(false)
//      .setPattern("\\w+|\\p{Punct}")
//      .setInputCol("rawText")
//      .setOutputCol("tokens")


    val session: SparkSession = createSession(2, "Zipf Validation")
    import session.implicits._
    val lines: RDD[String] = session.sparkContext.textFile(novellaLocation)

    lines
      .flatMap(_.toLowerCase.split("\\s+")) // split on whitespace
      .toDF("sentences")

    val linesDf: DataFrame = lines.toDF("sentences")



    val tokenized = tokenizer.transform(linesDf)
      .map(row => row.getString(0))

    tokenized.collect().foreach(println(_))

//    val countsPerToken: RDD[(String, Int)] = lines
//      .flatMap(_.toLowerCase.split("\\s+")) // split on whitespace
//      .map(word => (word.replaceAll("(^[^a-z0-9]+|[^a-z0-9]+$)", ""), 1)) // removing punctuation
//      .reduceByKey((count1, count2) => count1 + count2, 2)
//
//    val sortedCountsToken = countsPerToken.map(_.swap) // swap is equivalent to `pair =>(pair._2, pair._1)`
//      .sortByKey(ascending=false)
//
//    sortedCountsToken.saveAsTextFile(s"Chapter01${separator}sortedcounts")
  }

}