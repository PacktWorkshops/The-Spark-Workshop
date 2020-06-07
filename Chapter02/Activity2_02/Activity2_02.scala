package Activity2_02

import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import Utilities02.WarcRecord
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.tika.langdetect.OptimaizeLangDetector
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.sax.BodyContentHandler

object Activity2_02 {

  def extractPlainText(record: WarcRecord): Option[String] = {
    val textHandler = new BodyContentHandler(-1) // -1 skips document length limit
    val metadata = new Metadata()
    val parser = new HtmlParser()
    val context = new ParseContext()
    val html = record.htmlSource
    if (html.nonEmpty) {
      parser.parse(IOUtils.toInputStream(html, "UTF-8"), textHandler, metadata, context)
      val extractedText = textHandler.toString.trim.replaceAll("\\s+", " ")
      if (extractedText.nonEmpty)
        Some(extractedText)
      else
        None
    }
    else
      None
  }

  def detectLanguage(text: String): (String, Float) = {
    val languageIdentifier = new OptimaizeLangDetector()
    languageIdentifier.loadModels()
    val detected = languageIdentifier.detect(text)
    (detected.getLanguage, detected.getRawScore)
  }

  //  ~/spark-2.4.5-bin-hadoop2.7/bin/spark-submit --master local[3] --class Activity2_02.Activity2_02 --driver-class-path /Users/a/.m2/repository/com/google/guava/guava/28.2-jre/guava-28.2-jre.jar:/Users/a/.m2/repository/org/apache/commons/commons-compress/1.20/commons-compress-1.20.jar target/packt-uber-jar.jar   ~/CC-MAIN-20191013195541-20191013222541-00000.warc ~/Act2.2Output
  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder
      .appName("Crawl Tagger")
      .getOrCreate()

    val input = args(0)
    val outputDir = args(1)
    val warcRecords: RDD[WarcRecord] = extractRawRecords(input)
      .flatMap(parseRawWarc)
      .filter(_.warcType == "response")

    val taggedTexts = warcRecords.flatMap(record => {
      val plainText = extractPlainText(record)
      if (plainText.isDefined)
        Some(record.targetURI, plainText.get)
      else
        None
    }).map { case (uri, text) =>
      val (language, confidence) = detectLanguage(text)
      (uri, language, confidence, text)
    }

    taggedTexts.saveAsTextFile(outputDir)

  }
}
