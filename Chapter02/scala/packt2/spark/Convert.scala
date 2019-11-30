package packt2.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import packt2.HelperScala.{extractWarcRecords, parseRawWarcRecord, parseRawWetRecord}
import packt2.{HelperScala, WarcRecord, WetRecord}


object Convert extends App {
  implicit val session: SparkSession = HelperScala.createSession(2, "Corpus Parsing Wet")
  import org.apache.spark.sql._
  import session.implicits._

  val inputLocationWet = ParsingWet.getClass.getResource("/spark/webcorpus/wet.sample").getPath
  val inputLocationWarc = ParsingWet.getClass.getResource("/spark/webcorpus/warc.sample").getPath

  val wetRDD: RDD[WetRecord] = extractWarcRecords(inputLocationWet)
    .flatMap(parseRawWetRecord(_))
    .filter(_.warcType != "warcinfo") // skip meta header info for file

  val wetDataset: Dataset[WetRecord] = wetRDD.toDS()
  wetDataset
    .write
    .format("csv")
    .option("header", true)
    .save("./resources/wet_dataframe")


  val warcRDD: RDD[WarcRecord] = extractWarcRecords(inputLocationWarc)
    .flatMap(parseRawWarcRecord(_))
    .filter(_.warcType == "response")

  val warcDataset: Dataset[WarcRecord] = warcRDD.toDS()
  warcDataset
    .write
    .format("csv")
    .option("header", true)
    .save("./resources/warc_dataframe")

}
