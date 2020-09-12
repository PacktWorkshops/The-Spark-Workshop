package Chapter04.Activity4_01

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet}
import Utilities02.{WarcRecord, WetRecord}

object Activity4_02_RDD {
  def main(args: Array[String]): Unit = {
    val inputLocWarc = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc" // ToDo: Modify Path
    val inputLocWet = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet" // ToDo: Modify Path
    implicit val session: SparkSession = createSession(3, "Activity 4.2 RDD")

    val warcRecords: RDD[WarcRecord] = extractRawRecords(inputLocWarc).flatMap(parseRawWarc)
    val wetRecords: RDD[WetRecord] = extractRawRecords(inputLocWet).flatMap(parseRawWet)

    val pairWarc: RDD[(String, Option[String])] = warcRecords.map(warc => (warc.targetURI, warc.language))
    val pairWet: RDD[(String, String)] = wetRecords.map(wet => (wet.targetURI, wet.plainText))

    val joined: RDD[(String, (Option[String], String))] = pairWarc.join(pairWet)
    val spanishRecords = joined.filter(_._2._1.contains("es"))

    println(spanishRecords.count()) // 133
    Thread.sleep(1000L * 60L * 100L) // For exploring WebUI
  }
}