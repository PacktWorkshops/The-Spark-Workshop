package packt2.spark

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{extractRawRecords, parseRawWet}
import packt2.HelperScala
import packt2.WetRecord

/**
 * Code for parsing .wet files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object WetSchemaParser {

  def main(args: Array[String]) = {

    val inputLocWet = HelperScala.sampleWetLoc
    implicit val session: SparkSession = HelperScala.createSession(3, "WET Parser")
    session.sparkContext.setLogLevel("ERROR") // avoids printing of info messages

    val rawRecords: RDD[Text] = extractRawRecords(inputLocWet)
    val wetRecords: RDD[WetRecord] = rawRecords
      .flatMap(parseRawWet(_))

    import session.implicits._
    wetRecords.toDF().printSchema()
    println(s"Total records: ${wetRecords.count()}")

    /*
    val textRecords = wetRecords
      .filter(_.warcType != "warcinfo") // skip meta header info for file
      .toDF()

    textRecords.show(3)
    val wikipediaRecords = textRecords.as[WetRecord].filter(e => e.targetURI.contains("wikipedia"))
    print(wikipediaRecords.count())
    // 1
    val wikipediaTexts = wikipediaRecords.map(_.plainText)
    println(wikipediaTexts.first())
    */

  }
}
