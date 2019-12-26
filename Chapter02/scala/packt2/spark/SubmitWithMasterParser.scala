package packt2.spark

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{extractRawRecords, parseRawWarc}
import packt2.{HelperScala, WarcRecord}

object SubmitWithMasterParser {
  def heavyComputation(record: WarcRecord): Long = {
    val array = Array.fill(1000)(1)
    var totalSum = 0L
    for (_ <- 0 to 10000) {
      totalSum += array.sum
    }
    totalSum
  }

  def main(args: Array[String]): Unit = {
    val inputLocWarc = HelperScala.sampleWarcLoc
    implicit val session: SparkSession = SparkSession.builder
      .appName("SubmitWithMasterParser")
      .getOrCreate()
    session.sparkContext.setLogLevel("ERROR")

    val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc(_))
    session.time(warcRecords.map(heavyComputation).foreach(_ => Unit))
    
  }
}
