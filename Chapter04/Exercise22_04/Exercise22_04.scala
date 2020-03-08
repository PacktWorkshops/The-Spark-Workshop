package Exercise22_04

import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet, sampleWarcLoc, sampleWetLoc}
import Utilities02.{WarcRecord, WetRecord}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

object Exercise22_04 {

  def main(args: Array[String]): Unit = {
    implicit val session = createSession(3, "Caching & Eviction")
    session.sparkContext.setLogLevel("DEBUG")

    val inputLocWarc = sampleWarcLoc
    val inputLocWet = sampleWetLoc
    val rawRecordsWarc: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecordsWarc
      .flatMap(parseRawWarc)

    val rawRecordsWet: RDD[Text] = extractRawRecords(inputLocWet)
    val wetRecords: RDD[WetRecord] = rawRecordsWet
      .flatMap(parseRawWet)

    warcRecords.cache()
    wetRecords.cache()

    val uriKeyedWarc = warcRecords.map(warc => (warc.targetURI, warc))
    val uriKeyedWet = wetRecords.map(wet => (wet.targetURI, wet))
    val joined = uriKeyedWarc.join(uriKeyedWet)

    println(joined.count())
    println(joined.toDebugString)
    Thread.sleep(1000L * 10L)

  }

}
