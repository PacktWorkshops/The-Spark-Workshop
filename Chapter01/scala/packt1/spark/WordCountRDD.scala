package packt1.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt1.HelperScala

object WordCountRDD {

  def main(args: Array[String]) {

    val session: SparkSession = HelperScala.createSession(2, "Scala WordCount RDD")
    val lines: RDD[String] = session.sparkContext.textFile(HelperScala.novellaLocation)

    // Preprocessing & reducing the input lines:
    val words: RDD[String] = lines.flatMap(_.split("\\s+"))
    val tokenFrequ: RDD[(String, Int)] = words.map(word => (word.toLowerCase(), 1))
    val counts: RDD[(String, Int)] = tokenFrequ.reduceByKey(_ + _)

    // Materializing to local disk:
    counts
      .coalesce(2) // optional, without coalesce many tiny output files are generated
      .saveAsTextFile("./countsplits")

    session.stop()
  }
}