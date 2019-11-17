package packt1.solutions

import org.apache.spark.ml.feature.{RegexTokenizer}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import packt1.HelperScala.{createSession, novellaLocation}

object ZipfValidationB {

  def main(args: Array[String]): Unit = {

    // creating tokenizer as in org.apache.spark.ml.feature.TokenizerTestData.scala
    val tokenizer0 = new RegexTokenizer()
      .setGaps(false)
      .setPattern("\\w+|\\p{Punct}")
      .setInputCol("rawText") // "input" column name
      .setOutputCol("tokens") // "output" column name

    val session: SparkSession = createSession(2, "Zipf Validation")
    import session.implicits._

    val lines: RDD[String] = session.sparkContext.textFile(novellaLocation)
    val linesDf: DataFrame = lines // conversion to DataFrame
      .toDF("rawText")
    val tokenized = tokenizer0 // tokenizer application
      .transform(linesDf)

    tokenized.show(10) // printing a subset

    /*
    +--------------------+--------------------+
    |             rawText|              tokens|
    +--------------------+--------------------+
    |The Project Guten...|[the, project, gu...|
    |                    |                  []|
    |This eBook is for...|[this, ebook, is,...|
    |almost no restric...|[almost, no, rest...|
    |re-use it under t...|[re, -, use, it, ...|
    |with this eBook o...|[with, this, eboo...|
    |                    |                  []|
    |                    |                  []|
    |Title: Heart of D...|[title, :, heart,...|
    |                    |                  []|
    +--------------------+--------------------+
    */
  }

}