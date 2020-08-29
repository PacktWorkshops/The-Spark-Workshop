package Activity3_01

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet}
import Utilities02.{WarcRecord, WetRecord}

object Activity3_01 extends App {
  val inputLocWarc = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc"  // ToDo: Modify Path
  val inputLocWet = "/Users/a/CC-MAIN-20191013195541-20191013222541-00000.warc.wet" // ToDo: Modify Path
  implicit val session: SparkSession = createSession(3, "Activity 1")

  val rawRecordsWarc: RDD[Text] = extractRawRecords(inputLocWarc)
  val warcRecords: RDD[WarcRecord] = rawRecordsWarc
    .flatMap(parseRawWarc)
  val rawRecordsWet: RDD[Text] = extractRawRecords(inputLocWet)
  val wetRecords: RDD[WetRecord] = rawRecordsWet
    .flatMap(parseRawWet)
  val pairWarc = warcRecords
    .map(warc => (warc.targetURI, (warc.warcType, warc.dateS, warc.recordID, warc.contentLength, warc.contentType, warc.infoID,
      warc.concurrentTo, warc.ip, warc.payloadDigest, warc.blockDigest, warc.payloadType, warc.htmlContentType, warc.language,
      warc.htmlLength, warc.htmlSource)))
  val pairWet = wetRecords
    .map(wet => (wet.targetURI, wet.plainText))

  val joined = pairWarc
    .join(pairWet)

  println(joined.count())
  Thread.sleep(1000L * 60L * 100L) // For exploring WebUI

}
