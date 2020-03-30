package Activity4_02

import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, parseRawWet, sampleWarcLoc, sampleWetLoc}
import Utilities02.{WarcRecord, WetRecord}
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD

object Activity4_02 {

  def main(args: Array[String]): Unit = {
    implicit val session = createSession(3, "Better memory footprint")
    import session.implicits._
    session.sparkContext.setLogLevel("DEBUG")

    val rawRecordsWarc: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecordsWarc
      .flatMap(parseRawWarc)

    val rawRecordsWet: RDD[Text] = extractRawRecords(sampleWetLoc)
    val wetRecords: RDD[WetRecord] = rawRecordsWet
      .flatMap(parseRawWet)

    val warcRecordsDs =  warcRecords.toDS()
    val wetRecordsDs = wetRecords.toDS()

    // Caching the datasets:
    warcRecordsDs.cache()
    wetRecordsDs.cache()

    println(warcRecordsDs.count())
    println(wetRecordsDs.count())

    Thread.sleep(300L * 1000L)

  }

}
