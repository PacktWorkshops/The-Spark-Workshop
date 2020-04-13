package Activity1_03

import org.apache.spark.ml.feature.RegexTokenizer
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities01.HelperScala.{createSession, novellaLocation}

object Activity1_03 {

  def main(args: Array[String]): Unit = {
    val session: SparkSession = createSession(2, "Spark Tokenizer")
    val linesDf: DataFrame = session.read.text(novellaLocation).withColumnRenamed("value", "sentences")

//    val tokenizer = new RegexTokenizer()
//      .setPattern("(\\W+|\\p{Punct}|-)")
//      .setInputCol("sentences")
//      .setOutputCol("tokens")
//
//    val tokenized = tokenizer.transform(linesDf)
//    tokenized.take(40).foreach(println(_))

  }

}
