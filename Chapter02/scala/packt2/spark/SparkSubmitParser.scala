package packt2.spark

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{extractRawRecords, parseRawWarc}
import packt2.{HelperScala, WarcRecord}

/**
 * Code for parsing .warc files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object SparkSubmitParser {

  def main(args: Array[String]) = {

    val inputLocWarc = HelperScala.sampleWarcLoc
    implicit val session: SparkSession = HelperScala.createSession(3, "Submit Parser")

    val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc(_))

    import org.apache.spark.sql._
    import session.implicits._
    val responses: DataFrame = warcRecords
      .filter(_.warcType == "response")
      .toDF()
    responses.printSchema()

    responses.show(3)

    val englishRecords = responses.filter($"language" === "en")
    println(englishRecords.count())

  }
}

