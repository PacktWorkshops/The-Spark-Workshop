package Chapter04.Exercise4_03

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import Utilities02.WarcRecord
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc, sampleWarcLoc}


object Exercise4_03 {
  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = createSession(3, "Query Plans")
    session.sparkContext.setLogLevel("TRACE")
    import session.implicits._

    val langTagMapping = Seq[(String, String)](("en","english"),("pt-pt","portugese"),("cs","czech"),("de","german"),("es","spanish"), ("eu","basque"),("it","italian"),("hu","hungarian"),("pt-br","portugese"),("fr","french"),("en-US","english"),("zh-TW","chinese"))
    val langTagDF: DataFrame = langTagMapping.toDF("tag", "language")

    val warcRecordsRdd: RDD[WarcRecord] = extractRawRecords(sampleWarcLoc).flatMap(parseRawWarc)
    val warcRecordsDf: DataFrame = warcRecordsRdd.toDF()
      .select('targetURI, 'language)
      .filter('language.isNotNull)

    val aggregated = warcRecordsDf
      .groupBy('language)
      .agg(count('targetURI))
      .withColumnRenamed("language", "tag")

    val joinedDf: DataFrame = aggregated.join(langTagDF, Seq("tag"))

    joinedDf.show()
    joinedDf.explain(true)
    Thread.sleep(10L * 60L * 1000L)
  }
}

/*
+-----+----------------+---------+
|tag  |count(targetURI)|language |
+-----+----------------+---------+
|en   |5               |english  |
|pt-pt|1               |portugese|
|cs   |1               |czech    |
|de   |1               |german   |
|es   |4               |spanish  |
|eu   |1               |basque   |
|it   |1               |italian  |
|hu   |1               |hungarian|
|pt-br|1               |portugese|
|fr   |1               |french   |
|en-US|6               |english  |
|zh-TW|1               |chinese  |
+-----+----------------+---------+
 */

/*
== Physical Plan ==
*(2) Project [tag#67, count(targetURI)#64L, language#6]
+- *(2) BroadcastHashJoin [tag#67], [tag#5], Inner, BuildRight
   :- *(2) HashAggregate(keys=[language#39], functions=[count(targetURI#34)], output=[tag#67, count(targetURI)#64L])
   :  +- Exchange hashpartitioning(language#39, 200)
   :     +- *(1) HashAggregate(keys=[language#39], functions=[partial_count(targetURI#34)], output=[language#39, count#83L])
   :        +- *(1) Project [targetURI#34, language#39]
   :           +- *(1) Filter isnotnull(language#39)
   :              +- *(1) SerializeFromObject [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).warcType, true, false) AS warcType#26, assertnotnull(input[0, Utilities02.WarcRecord, true]).dateS AS dateS#27L, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).recordID, true, false) AS recordID#28, assertnotnull(input[0, Utilities02.WarcRecord, true]).contentLength AS contentLength#29, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).contentType, true, false) AS contentType#30, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).infoID, true, false) AS infoID#31, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).concurrentTo, true, false) AS concurrentTo#32, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).ip, true, false) AS ip#33, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).targetURI, true, false) AS targetURI#34, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).payloadDigest, true, false) AS payloadDigest#35, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).blockDigest, true, false) AS blockDigest#36, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).payloadType, true, false) AS payloadType#37, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).htmlContentType, true, false) AS htmlContentType#38, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, unwrapoption(ObjectType(class java.lang.String), assertnotnull(input[0, Utilities02.WarcRecord, true]).language), true, false) AS language#39, assertnotnull(input[0, Utilities02.WarcRecord, true]).htmlLength AS htmlLength#40, staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, assertnotnull(input[0, Utilities02.WarcRecord, true]).htmlSource, true, false) AS htmlSource#41]
   :                 +- Scan[obj#25]
   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
      +- LocalTableScan [tag#5, language#6]
 */
