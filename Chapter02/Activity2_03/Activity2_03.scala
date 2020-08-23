package Activity2_03

import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import Activity2_02.Activity2_02.{extractPlainText, detectLanguage}
import Utilities02.WarcRecord
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Activity2_03 {

  // ~/spark-2.4.6-bin-hadoop2.7/bin/spark-submit --master local[1] --class Activity2_03.Activity2_03 --driver-class-path /Users/a/.m2/repository/com/google/guava/guava/28.2-jre/guava-28.2-jre.jar:/Users/a/.m2/repository/org/apache/commons/commons-compress/1.20/commons-compress-1.20.jar ~/IdeaProjects/The-Spark-Workshop/target/packt-uber-jar.jar ~/IdeaProjects/The-Spark-Workshop/resources/webcorpus/warc.sample
  def main(args: Array[String]): Unit = {
    implicit val session = SparkSession.builder
      .appName("Crawl Tagger Estimation")
      .getOrCreate()

    val input = args(0)
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
      (uri, language, confidence)
    }

    session.time(taggedTexts.count())

  }
}
