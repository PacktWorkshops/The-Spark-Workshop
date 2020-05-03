package Activity3_02

import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}
import Utilities02.WarcRecord
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Activity3_02 {

  def heavyComputation(record: WarcRecord): Unit = Thread.sleep(200L)

  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("Activity 2")
      .master("local[2]")
      .getOrCreate()

    val rawRecords: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc)

    println(warcRecords.count()) // 3001
    println(warcRecords.getNumPartitions) // 4

    session.time(warcRecords.map(heavyComputation).foreach(_ => Unit))

  }
}
