package Utilities02

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Constants and helper functions
 *
 * @author Phil, https://github.com/g1thubhub
 */
object HelperScala {
  val sampleWarcLoc = "/Users/a/IdeaProjects/The-Spark-Workshop/resources/webcorpus/warc.sample"
  val sampleWetLoc = "/Users/a/IdeaProjects/The-Spark-Workshop/resources/webcorpus/wet.sample"

  private val delimiterWarcWet = "WARC/1.0" // Wrong => Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost, executor driver): java.lang.OutOfMemoryError: Java heap space
  private val blankLine: Regex = "(?m:^(?=[\r\n]))".r
  private val newLine = "[\\n\\r]+"

  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  def extractRawRecords(warcLoc: String)(implicit session: SparkSession): RDD[Text] = {
    val hadoopConf = session.sparkContext.hadoopConfiguration
    hadoopConf.set("textinputformat.record.delimiter", delimiterWarcWet)

    val warcRecords: RDD[Text] = session
      .sparkContext
      .newAPIHadoopFile(warcLoc, classOf[TextInputFormat], classOf[LongWritable], classOf[Text], hadoopConf)
      .map(_._2)
    warcRecords
  }

  // helper function for extracting meta info
  def extractMetaInfo(rawMetaInfo: String): mutable.Map[String, String] = {
    val metaEntries = mutable.Map.empty[String, String]
    val fields = rawMetaInfo.split(newLine) // split string on newlines
    for (field <- fields) {
      val keyValue = field.split(":")
      metaEntries(keyValue(0).trim) = keyValue.slice(1, keyValue.length).mkString(":").trim
    }
    metaEntries
  }

  def extractResponseMeta(responseMeta: String): (String, Option[String], Int) = {
    val fields = responseMeta.split(newLine) // split string on newlines
    var contentType, language = ""
    var contentLength = -1
    for (field <- fields) {
      if (field.startsWith("Content-Type:")) {
        contentType = field.substring(14).trim
      }
      else if (field.startsWith("Content-Language:")) {
        language = field.substring(17).trim
      }
      else if (field.startsWith("Content-Length:")) {
        contentLength = field.substring(15).trim.toInt
      }
    }
    (contentType, if (language.isEmpty) None else Some(language), contentLength)
  }

  // parses raw WarcWet records into domain objects of type spark.WarcRecord
  def parseRawWarc(text: Text): Option[WarcRecord] = {
    val rawContent = text.toString
    val matches = blankLine.findAllMatchIn(rawContent.toString)
    if (matches.isEmpty) { // malformed record, skip
      None
    }
    else {
      val matchStarts: List[Int] = matches.map(_.end).toList // get end points of matches, only first two elements are relevant
      val docStart = matchStarts.head // start of record
      val metaBoundary = matchStarts(1) // end of meta section
      val rawMetaInfo = rawContent.substring(docStart, metaBoundary).trim
      val metaPairs = extractMetaInfo(rawMetaInfo)
      val responseBoundary = matchStarts(2) // end of response meta section
      val responseMeta = rawContent.substring(metaBoundary + 1, responseBoundary).trim
      val responseMetaTriple = extractResponseMeta(responseMeta)
      val pageContent = rawContent.substring(responseBoundary + 1).trim
        .replaceAll("(\\r?\\n)+", " ")
      Some(WarcRecord(metaPairs, responseMetaTriple, pageContent))
    }
  }

  // parses raw WarcWet records into domain objects of type spark.WarcRecord
  def parseRawWet(text: Text): Option[WetRecord] = {
    val rawContent = text.toString // key is a line number which is is useless
    val matches = blankLine.findAllMatchIn(rawContent)
    if (matches.isEmpty) { // malformed record, skip
      None
    }
    else {
      val matchStarts: List[Int] = matches.map(_.end).toList // get end points of matches, only first two elements are relevant
      val docStart = matchStarts.head // start of record
      val boundary = matchStarts(1) // end of meta section
      val rawMetaInfo = rawContent.substring(docStart, boundary).trim
      val metaPairs = extractMetaInfo(rawMetaInfo)
      val pageContent = rawContent.substring(boundary + 1).trim
        .replaceAll("(\\r?\\n)+", " ")
      Some(WetRecord(metaPairs, pageContent))
    }
  }

  def extractWarcDataframe(inputLocationWarc: String, session: SparkSession): DataFrame = {
      implicit val sessionI = session
      import session.implicits._

      val warcRecords: RDD[Text] = extractRawRecords(inputLocationWarc)
      warcRecords
        .flatMap(parseRawWarc(_))
        .filter(_.warcType == "response")
        .toDF()
  }

  def extractWetDataframe(inputLocationWarc: String, session: SparkSession): DataFrame = {
    implicit val sessionI = session
    import session.implicits._

    val warcRecords: RDD[Text] = extractRawRecords(inputLocationWarc)
    warcRecords
      .flatMap(parseRawWet(_))
      .filter(_.warcType != "warcinfo")
      .toDF()
  }

}
