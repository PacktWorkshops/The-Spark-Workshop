package Activity1_02

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.{createSession, novellaLocation}

object Activity1_02 {

  def main(args: Array[String]): Unit = {

    val session: SparkSession = createSession(2, "Zipf Validation")
    val lines: RDD[String] = session.sparkContext.textFile(novellaLocation)

    val tokens: RDD[String] = lines
      .flatMap(_.trim().toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)

    val countsPerToken: RDD[(String, Int)] = tokens
      .map(token => (token, 1))
      .reduceByKey((count1, count2) => count1 + count2, 2)

    val sortedCountsToken = countsPerToken
      .map(_.swap) // swap is equivalent to `pair =>(pair._2, pair._1)`
      .sortByKey(ascending = false)

    sortedCountsToken.saveAsTextFile(s"zipfsorted")
  }

}
