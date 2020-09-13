package Activity4_03

import org.apache.spark.api.java.JavaRDD
import org.apache.commons.io.IOUtils
import org.apache.spark.rdd.RDD
import org.apache.tika.langdetect.OptimaizeLangDetector
import org.apache.tika.metadata.Metadata
import org.apache.tika.parser.ParseContext
import org.apache.tika.parser.html.HtmlParser
import org.apache.tika.sax.BodyContentHandler

object Activity4_03  {

  def tagRecords(partition: Iterator[String]): Iterator[String] = {
    val metadata = new Metadata()
    val parser = new HtmlParser()
    val context = new ParseContext()
    val languageIdentifier = new OptimaizeLangDetector()
    languageIdentifier.loadModels()

    partition.flatMap(record => {
      if (record.nonEmpty) {
        val textHandler = new BodyContentHandler(-1) // -1 skips document length limit
        parser.parse(IOUtils.toInputStream(record, "UTF-8"), textHandler, metadata, context)
        val extractedText = textHandler.toString.trim.replaceAll("\\s+", " ")
        val detected = languageIdentifier.detect(extractedText)
        Some(detected.getLanguage + " " +  detected.getRawScore.toString)
      }
      else
        None
    })
  }

  def tagJavaRDD(rdd: JavaRDD[String]): RDD[String] = {
    val scalaRDD: RDD[String] = rdd.rdd
    scalaRDD.mapPartitions(tagRecords)
  }
}
