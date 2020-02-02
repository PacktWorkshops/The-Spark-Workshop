package Activity1_03

import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet}
import Utilities02.{WarcRecord, WetRecord}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Activity1_03 extends App {

  val inputLocWarc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"
  val inputLocWet = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc.wet"
  implicit val session: SparkSession = createSession(2, "Activity 1")

  val rawRecordsWarc: RDD[Text] = extractRawRecords(inputLocWarc)
  val warcRecords: RDD[WarcRecord] = rawRecordsWarc
    .flatMap(parseRawWarc(_))

  val rawRecordsWet: RDD[Text] = extractRawRecords(inputLocWet)
  val wetRecords: RDD[WetRecord] = rawRecordsWet
    .flatMap(parseRawWet(_))

  val pairWarc = warcRecords
    .map(warc => (warc.targetURI, warc.language))
  val pairWet = wetRecords
    .map(wet => (wet.targetURI, wet.plainText))

  val joined = pairWarc
    .join(pairWet)

  println(joined.count())

  Thread.sleep(1000L * 60L * 100L) // For exploring WebUI

}
