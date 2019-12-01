package packt2.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{parseRawWetRecord, extractRawRecords}
import packt2.HelperScala
import packt2.WetRecord

/**
 * Code for parsing .wet files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object ParsingWet {

  def main(args: Array[String]) = {

    val inputLocWet = HelperScala.sampleWetLoc

    implicit val session: SparkSession = HelperScala.createSession(2, "Corpus Parsing Wet")
    import org.apache.spark.sql._
    import session.implicits._

    val wetRecords: RDD[WetRecord] = extractRawRecords(inputLocWet)
      .flatMap(parseRawWetRecord(_))

     val textRecords = wetRecords
        .filter(_.warcType != "warcinfo") // skip meta header info for file
        .toDF()

    textRecords.printSchema()
    /*
root
 |-- warcType: string (nullable = true)
 |-- targetURI: string (nullable = true)
 |-- dateS: long (nullable = false)
 |-- recordID: string (nullable = true)
 |-- refersTo: string (nullable = true)
 |-- blockDigest: string (nullable = true)
 |-- contentType: string (nullable = true)
 |-- contentLength: integer (nullable = false)
 |-- plainText: string (nullable = true)
    */

    textRecords.show(3)
 /*
+----------+--------------------+----------+--------------------+--------------------+--------------------+-----------+-------------+--------------------+
|  warcType|           targetURI|     dateS|            recordID|            refersTo|         blockDigest|contentType|contentLength|           plainText|
+----------+--------------------+----------+--------------------+--------------------+--------------------+-----------+-------------+--------------------+
|conversion|http://www.bioref...|1566077047|<urn:uuid:55e694b...|<urn:uuid:48e1553...|sha1:YTWQDG2P3CZ2...| text/plain|         1096|Encyclopedia | Wo...|
|conversion|http://0-1.ru/?id...|1566076394|<urn:uuid:9ee9c2c...|<urn:uuid:07a442a...|sha1:NCXTAQVEHGWW...| text/plain|         5476|За нарушения прот...|
|conversion|http://0-50.ru/ne...|1566075776|<urn:uuid:7c9ed59...|<urn:uuid:f4b62f8...|sha1:V5LHTS2HYPQH...| text/plain|        30034|Новости 0-50.ru |...|
+----------+--------------------+----------+--------------------+--------------------+--------------------+-----------+-------------+--------------------+
  */

    val wikipediaRecords = textRecords.as[WetRecord].filter(e => e.targetURI.contains("wikipedia"))
    print(wikipediaRecords.count())
    // 1
    val wikipediaTexts = wikipediaRecords.map(_.plainText)
    println(wikipediaTexts.first())
    /*
      Encyclopedia | World Factbook | World Flags | Reference Tables | List of Lists Academic Disciplines |
    */
  }
}
