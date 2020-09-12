package Chapter04.Activity4_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet}
import Utilities02.{WarcRecord, WetRecord}

object Activity4_02_DF {
  def main(args: Array[String]): Unit = {
    val inputLocWarc = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc" // ToDo: Modify Path
    val inputLocWet = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet" // ToDo: Modify Path
    implicit val session: SparkSession = createSession(3, "Activity 4.2 DataFrame")

    val warcRecords: RDD[WarcRecord] = extractRawRecords(inputLocWarc).flatMap(parseRawWarc)
    val wetRecords: RDD[WetRecord] = extractRawRecords(inputLocWet).flatMap(parseRawWet)

    import session.implicits._
    val warcRecordsDf: DataFrame = warcRecords.toDF.select("targetURI", "language")
    val wetRecordsDf: DataFrame = wetRecords.toDF().select("targetURI", "plainText")

    val joinedDf = warcRecordsDf.join(wetRecordsDf, Seq("targetURI"))
    val spanishRecords = joinedDf.filter('language === "es") // median shuffle read 32.7 MB / 22412

    println(spanishRecords.count())
    Thread.sleep(1000L * 60L * 100L) // For exploring WebUI
  }
}
