package Activity2_03

import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}
import Utilities02.WarcRecord
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Activity2_03 {

  def heavyComputation(record: WarcRecord): Unit = Thread.sleep(200L)

  def main(args: Array[String]): Unit = {
    val inputLocWarc = sampleWarcLoc
    implicit val session: SparkSession = SparkSession.builder
      .appName("Activity 2")
      .master("local[2]")
      .getOrCreate()


    val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc(_))


    println(warcRecords.count()) // 3001
    println(warcRecords.getNumPartitions) // 4

    session.time(warcRecords.map(heavyComputation).foreach(_ => Unit))

  }
}
