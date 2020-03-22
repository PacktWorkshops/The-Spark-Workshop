package Activity2_01

import Utilities02.WarcRecord
import org.apache.spark.rdd.RDD
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala


object Activity2_01 extends App {

  implicit val spark: SparkSession = HelperScala.createSession(3, "Acti 1")
  spark.sparkContext.setLogLevel("ERROR")

  //  val input = sampleWarcLoc
  val input = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"
  val warcRecords: RDD[WarcRecord] = extractRawRecords(input)(spark).flatMap(parseRawWarc)

  println(warcRecords.count()) // 166.951

  val untagged = warcRecords.filter(_.language.isEmpty)
  println(untagged.count()) // 166951

  val languageMap: scala.collection.Map[String, Long] = warcRecords.flatMap(rec => rec.language).countByValue()
  val sortedLanguageMap = languageMap.toList.sortBy(_._2)
  println(sortedLanguageMap.take(15)) // List((en-IE,1), (ba,1), (in,1), (ta,1), (yue,1), (is,1), (fi-FI,1), (en-ZA,1), (en-EN,1), (eu,1), (en-TT,1), (rw,1), (uz,1), (en-WW,1), (iw,1))

  val uzRecords = warcRecords.filter(rec => rec.language.isDefined && rec.language.get == "uz")
  println(uzRecords.map(_.targetURI).collect().toList) // List(http://handicraftman.uz/uz/node/299)


  val wikipages = warcRecords.flatMap(rec => if (rec.targetURI.contains("wikipedia")) Some(rec.targetURI) else None)

  println(wikipages.collect().toList) //  https://ady.wikipedia.org/wiki/Template:Purge, https://ady.wikipedia.org/wiki/Template:Purge, https://ady.wikipedia.org/wiki/Template:Purge, https://als.m.wikipedia.org/wiki/Polen,
  //
  //    /*
  //
  //
  //     */
  //        /*
  //
  //      scala> warcRecords.count()
  //  res0: Long = 3001
  //  scala> warcRecords.flatMap(record => record.language).distinct().collect()
  //  res1: Array[String] = Array(eu, fr, cs, en, hu, de, pt-pt, pt-br, zh-TW, es, en-US, it)
  //  scala> warcRecords.filter(record => record.language.isDefined && record.language.get == "fr").count
  //  res2: Long = 1
  //  scala> warcRecords.filter(record => record.language.isDefined && record.language.get == "fr").map(_.targetURI).collect().toList
  //
  //
  //
  //
  //
  //      >>> warc_records.count()
  //  3001
  //  >>> warc_records.map(lambda record : record.language).filter(lambda lang: len(lang) > 0).distinct().collect()
  //  ['cs', 'es', 'pt-pt', 'en', 'fr', 'zh-TW', 'hu', 'en-US', 'pt-br', 'it', 'de', 'eu']
  //  >>> warc_records.filter(lambda record: len(record.language) > 0 and record.language == 'fr').count()
  //  1
  //  >>> warc_records.filter(lambda record: len(record.language) > 0 and record.language == 'fr').map(lambda record: record.target_uri).collect()
  //
  //       */
  //
  //


}
