package Exercise3_04

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities02.WarcRecord
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import Utilities03.RetryListener

object Exercise3_04 {
  def main(args: Array[String]): Unit = {

    implicit val session: SparkSession = SparkSession.builder
      .master(s"local[3, 3]")
      .appName("Failure Exploration")
      .getOrCreate()

    val retryListener = new RetryListener()
    session.sparkContext.addSparkListener(retryListener)

    val inputWarc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc" // ToDo: Change path
    val rawRecords: RDD[Text] = extractRawRecords(inputWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(record => {
        val parsedRawWarc = parseRawWarc(record)
         // val crasher = 5 / 0 // ToDo: Uncomment
         // print(crasher) // ToDo: Uncomment
        parsedRawWarc
      })

    println(warcRecords.count())
  }

}
