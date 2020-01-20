package packt2.activity

import Utilities01.HelperScala
import Utilities02.WarcRecord
import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}


object Shell extends App {

  val inputLocWarc = "/Users/a/Downloads/CC-MAIN-20191013195541-20191013222541-00000.warc"
  implicit val session: SparkSession = HelperScala.createSession(3, "Submit Parser")

  val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
  val warcRecords: RDD[WarcRecord] = rawRecords
    .flatMap(parseRawWarc(_))

//  println(warcRecords.count()) // 166.951

//  val languages = warcRecords.flatMap(record => record.language).countByValue()
  // Map(zh-TW -> 8, it-IT -> 12, en-us -> 42, en-IE -> 1, und -> 4, ba -> 1, in -> 1, hr -> 8, ta -> 1, ka -> 2, ar -> 16, yue -> 1, fr -> 153, is -> 1, en-NZ -> 3, ua-UA -> 4, fi-FI -> 1, lv -> 2, en-ZA -> 1, en-EN -> 1, UTF-8 -> 2, eu -> 1, en-TT -> 1, EN -> 3, rw -> 1, uz -> 1, en-WW -> 1, en,en-ca -> 3, iw -> 1, id-ID -> 2, it-IT-x-lvariant-MO -> 1, uk -> 9, fr-FRA -> 1, ES -> 1, th-TH -> 1, en-INT -> 2, nl-NL -> 15, en-QA -> 1, ga -> 2, br -> 1, en-IN -> 1, da-DK -> 4, da-DK,da-DK -> 1, pt-PT -> 3, ru-RU -> 24, de-AT -> 6, de,en -> 1, sv-SE -> 1, pt -> 8, en-HK -> 1, zh_CN -> 2, ar-ae -> 1, cs -> 15, de-DE,de-DE -> 1, gl -> 2, : en -> 3, zh-HANS -> 1, sr -> 6, de-CH -> 5, zh-CN -> 23, us -> 2, el -> 12, it -> 69, sk-sk -> 4, ca -> 12, en-in -> 1, pt-BR -> 20, ja-JP -> 13, es-ES -> 10, vi -> 11, en-US,es-ES -> 1, es-VE -> 2, fr-be -> 1, as -> 1, mn-MN -> 2, nl -> 81, en-CZ -> 1, bg -> 4, EN-US -> 1, ko -> 9, en-PH -> 1, en-AU -> 12, mk -> 1, English -> 1, perl -> 1, dz -> 1, no_NO, no -> 2, english -> 1, et -> 8, de -> 266, en-gb -> 11, ha -> 3, nb -> 2, de-de -> 2, sl-si -> 1, nl-BE -> 2, en-CA -> 6, en-SE -> 1, it-it -> 1, ru -> 103, fr-FR -> 10, en,en-uk -> 1, th -> 8, id -> 2, zh-hant -> 1, sq -> 2, de-DE -> 34, ro-RO -> 1, sv -> 19, en-US-MOBILE -> 2, tr -> 6, da -> 16, de_DE -> 2, zh-tw -> 7, zh-CN,en-us -> 3, hr-HR -> 1, ko, ko -> 9, it-CH -> 2, en -> 1680, he -> 6, fr-CA -> 4, es-CO -> 1, Romanian -> 1, fr-BE -> 3, en-US,en-US -> 3, sk -> 10, fr-Fr -> 1, fr-FR, fr-CA -> 1, zh-HK -> 1, en-US -> 890, az -> 2, nl-NL,nl-NL -> 3, es -> 133, ca; -> 1, ca-ES -> 1, mne -> 1, hi -> 6, ci -> 2, zh-hans -> 5, sk-SK -> 1, mr -> 3, sk_SK -> 1, en- -> 1, sk_SK.utf8 -> 1, be -> 2, en-GB -> 47, es-es -> 3, pt- -> 1, pt-pt -> 1, fa-IR -> 1, en-au -> 1, pt-br -> 20, ZA -> 1, en,ro -> 1, ja -> 58, fr, de, nl, en -> 1, en,en-bs -> 1, en-be -> 1, fi -> 21, en-BW -> 1, ro -> 10, es-ar -> 1, en-US,nb-NO -> 1, lt -> 8, no -> 14, en-FI -> 1, zh-CN, zh-CN -> 2, km -> 1, ko-KR -> 18, kk -> 1, en,en-us -> 16, sl -> 2, co-ci -> 1, fa -> 9, zh -> 1, ms -> 3, de; -> 1, es-MX -> 4, hu -> 19, pl-PL -> 3, pl -> 31, vi-VN -> 9, tr-TR -> 5, sh -> 3)

//  val uzLanguage = warcRecords.filter(record => record.language.isDefined && record.language.get == "uz").map(_.targetURI).collect()
//  println(uzLanguage.toList)
//

val wikipages = warcRecords.filter(_.targetURI.contains("wikipedia")).map(_.targetURI)

  wikipages.collect().foreach(println(_))

  /*


   */
      /*

    scala> warcRecords.count()
res0: Long = 3001
scala> warcRecords.flatMap(record => record.language).distinct().collect()
res1: Array[String] = Array(eu, fr, cs, en, hu, de, pt-pt, pt-br, zh-TW, es, en-US, it)
scala> warcRecords.filter(record => record.language.isDefined && record.language.get == "fr").count
res2: Long = 1
scala> warcRecords.filter(record => record.language.isDefined && record.language.get == "fr").map(_.targetURI).collect().toList





    >>> warc_records.count()
3001
>>> warc_records.map(lambda record : record.language).filter(lambda lang: len(lang) > 0).distinct().collect()
['cs', 'es', 'pt-pt', 'en', 'fr', 'zh-TW', 'hu', 'en-US', 'pt-br', 'it', 'de', 'eu']
>>> warc_records.filter(lambda record: len(record.language) > 0 and record.language == 'fr').count()
1
>>> warc_records.filter(lambda record: len(record.language) > 0 and record.language == 'fr').map(lambda record: record.target_uri).collect()

     */





}
