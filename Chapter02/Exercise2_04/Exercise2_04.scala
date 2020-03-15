package Exercise2_04

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities02.WarcRecord
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{sampleWarcLoc, extractRawRecords, parseRawWarc}

/**
 * Code for parsing .warc files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object Exercise2_04 {

  def main(args: Array[String]) = {
    implicit val session: SparkSession = createSession(3, "Spark-Submit")
    session.sparkContext.setLogLevel("ERROR") // avoids printing of info messages

    val rawRecords: RDD[Text] = extractRawRecords(sampleWarcLoc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc)

    import session.implicits._
    warcRecords.toDF().printSchema()
    println(s"Total # of records: ${warcRecords.count()}")

  }
}

