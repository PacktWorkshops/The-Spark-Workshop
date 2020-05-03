package Activity3_04

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

object Activity3_04 {

  def tagRecords(partition: Iterator[WarcRecord]): Iterator[(String, String, Float, String)] = {
    val languageIdentifier = new OptimaizeLangDetector()
    languageIdentifier.loadModels()
    val textHandler = new BodyContentHandler(-1) // -1 skips document length limit
    val metadata = new Metadata()
    val parser = new HtmlParser()
    val context = new ParseContext()

    partition.flatMap(record => {
      val html = record.htmlSource
      var extractedText = ""
      if (html.nonEmpty) {
        parser.parse(IOUtils.toInputStream(html, "UTF-8"), textHandler, metadata, context)
        extractedText = textHandler.toString.trim.replaceAll("\\s+", " ")
        val detected = languageIdentifier.detect(extractedText)
        Some(record.targetURI, detected.getLanguage, detected.getRawScore, extractedText)
      }
      else
        None
    })
  }

  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder
      .appName("Improved Crawl Tagger")
      .getOrCreate()

    val input = args(0)
    val outputDir = args(1)

    val warcRecords: RDD[WarcRecord] = extractRawRecords(input)
      .flatMap(parseRawWarc)
      .filter(_.warcType == "response")

    val taggedTexts = warcRecords.mapPartitions(tagRecords)
    taggedTexts.saveAsTextFile(outputDir)

  }
}
