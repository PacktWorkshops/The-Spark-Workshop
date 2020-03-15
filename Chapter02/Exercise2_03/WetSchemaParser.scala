package Exercise2_03

import Utilities02.WetRecord
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{sampleWetLoc, extractRawRecords, parseRawWet}

/**
 * Code for parsing .wet files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object WetSchemaParser {

  def main(args: Array[String]) = {

    implicit val session: SparkSession = createSession(3, "WET Parser")
    session.sparkContext.setLogLevel("ERROR") // avoids printing of info messages

    val rawRecords: RDD[Text] = extractRawRecords(sampleWetLoc)
    val wetRecords: RDD[WetRecord] = rawRecords
      .flatMap(parseRawWet)

    import session.implicits._
    wetRecords.toDF().printSchema()
    println(s"Total # of records: ${wetRecords.count()}")

  }
}