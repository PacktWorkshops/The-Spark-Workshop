package Exercise2_06

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities02.WarcRecord
import Utilities02.HelperScala.{sampleWarcLoc, extractRawRecords, parseRawWarc}

object Exercise2_06 {

  def heavyComputation(record: WarcRecord): Long = {
    val array = Array.fill(1000)(1)
    var totalSum = 0L
    for (_ <- 0 to 10000) {
      totalSum += array.sum
    }
    totalSum
  }

  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("Different Tasks")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    val rawRecords: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecords.flatMap(parseRawWarc)
    val invokeHeavyRDD = warcRecords.map(heavyComputation)

    session.time(invokeHeavyRDD.foreach(_ => Unit))

  }
}