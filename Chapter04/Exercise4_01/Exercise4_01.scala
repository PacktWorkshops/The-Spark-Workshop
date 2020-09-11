package Exercise4_01

import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet, sampleWarcLoc, sampleWetLoc}
import Utilities02.{WarcRecord, WetRecord}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

object Exercise4_01 {

  def main(args: Array[String]): Unit = {
    implicit val session = createSession(3, "Caching & Eviction")
    session.sparkContext.setLogLevel("DEBUG")

    val rawRecordsWarc: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecordsWarc
      .flatMap(parseRawWarc)

    val rawRecordsWet: RDD[Text] = extractRawRecords(sampleWetLoc)
    val wetRecords: RDD[WetRecord] = rawRecordsWet
      .flatMap(parseRawWet)

    // Caching the datasets:
    warcRecords.cache()
    wetRecords.cache()

    val uriKeyedWarc = warcRecords.map(warc => (warc.targetURI, warc))
    val uriKeyedWet = wetRecords.map(wet => (wet.targetURI, wet))
    val joined = uriKeyedWarc.join(uriKeyedWet)

    println(joined.count())
    Thread.sleep(10L * 60L * 1000L)

  }

}