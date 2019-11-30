package packt2.spark

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{parseRawWetRecord, extractWarcRecords}
import packt2.HelperScala
import packt2.WetRecord

/**
 * Code for parsing .wet files of the WARC corpus
 *
 * @author Phil, https://github.com/g1thubhub
 */
object ParsingWet {

  def main(args: Array[String]) = {

    implicit val session: SparkSession = HelperScala.createSession(2, "Corpus Parsing Wet")
    import org.apache.spark.sql._
    import session.implicits._

    val inputLocationWet = HelperScala.sampleWetLoc
    val webpagesRDD: RDD[WetRecord] = extractWarcRecords(inputLocationWet)
      .flatMap(parseRawWetRecord(_))
      .filter(_.warcType != "warcinfo") // skip meta header info for file

    val webpagesDataset: Dataset[WetRecord] = webpagesRDD.toDS()
    webpagesDataset.printSchema()
    /*
    root
      |-- warcType: string (nullable = true)
      |-- targetURI: string (nullable = true)
      |-- date: date (nullable = true)
      |-- recordID: string (nullable = true)
      |-- refersTo: string (nullable = true)
      |-- digest: string (nullable = true)
      |-- contentType: string (nullable = true)
      |-- contentLength: integer (nullable = false)
      |-- plainText: string (nullable = true)
    */

    webpagesDataset.show(10)
    /*
      +----------+--------------------+----------+--------------------+--------------------+--------------------+-----------+-------------+------------------------------------+
      |  warcType|           targetURI|      date|            recordID|            refersTo|              digest|contentType|contentLength|                           plainText|
      +----------+--------------------+----------+--------------------+--------------------+--------------------+-----------+-------------+------------------------------------+
      |conversion|http://www.bioref...|1970-01-19|<urn:uuid:55e694b...|<urn:uuid:48e1553...|sha1:YTWQDG2P3CZ2...| text/plain|         1096|                Encyclopedia | Wo...|
      |conversion|http://0-1.ru/?id...|1970-01-19|<urn:uuid:9ee9c2c...|<urn:uuid:07a442a...|sha1:NCXTAQVEHGWW...| text/plain|         5476|                За нарушения прот...|
      |conversion|http://0-50.ru/ne...|1970-01-19|<urn:uuid:7c9ed59...|<urn:uuid:f4b62f8...|sha1:V5LHTS2HYPQH...| text/plain|        30034|                Новости 0-50.ru |...|
      |conversion|  http://012845.com/|1970-01-19|<urn:uuid:df9f9eb...|<urn:uuid:ae3a1fb...|sha1:F2CLZMAYAXLV...| text/plain|         9551|                Guangzhou Uni.& C...|
    */

    val wikipediaRecords = webpagesDataset.filter(e => e.targetURI.contains("wikipedia"))
    val wikipediaTexts = wikipediaRecords.map(_.plainText)
    println(wikipediaTexts.first())
    /*
      Encyclopedia | World Factbook | World Flags | Reference Tables | List of Lists
      Academic Disciplines | Historical Timeline | Themed Timelines | Biographies | How-Tos
      Sponsor by The Tattoo Collection
      Hyperglycemia
      Main Page | See live article | Alphabetical index
      Hyperglycemia
      Hyperglycemia is the condition of having an excessive amount of glucose circulating in the blood plasma. Etymology hyper- in Greek meaning "too much"; -glyc- in Greek meaning "sweet"; -emia meaning "of bl
   */

    webpagesDataset
      .write
      .format("csv")
      .option("header", true)
      .save("./wet_dataframe")

  }
}
