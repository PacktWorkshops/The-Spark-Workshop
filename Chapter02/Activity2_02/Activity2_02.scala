package Activity2_02

import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}
import Utilities02.WarcRecord
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
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

  def main(args: Array[String]): Unit = {
    val inputLocWarc = sampleWarcLoc
    implicit val session = createSession(3, "Tagging crawls")

    val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
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

    taggedTexts.take(5).foreach(println(_))


  }
}
