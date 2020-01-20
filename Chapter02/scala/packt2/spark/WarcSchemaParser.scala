//package packt2.spark
//
//import org.apache.hadoop.io.Text
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import Utilities01.HelperScala.createSession
//import Utilities02.HelperScala.{sampleWarcLoc, extractRawRecords, parseRawWarc}
//
///**
// * Code for parsing .warc files of the WARC corpus
// *
// * @author Phil, https://github.com/g1thubhub
// */
//object WarcSchemaParser {
//
//  def main(args: Array[String]) = {
//
//    val inputLocWarc = sampleWarcLoc
//    implicit val session: SparkSession = createSession(3, "WARC Parser")
//    session.sparkContext.setLogLevel("ERROR")  // avoids printing of info messages
//
//    val rawRecords: RDD[Text] = extractRawRecords(inputLocWarc)
//    val warcRecords: RDD[WarcRecord] = rawRecords
//      .flatMap(parseRawWarc(_))
//  CURRENT TIME PRINT OUT
//    import session.implicits._
//    warcRecords.toDF().printSchema()
//    println(s"Total records: ${warcRecords.count()}")
//
//    /*
//    import org.apache.spark.sql._
//    val responses: DataFrame = warcRecords
//      .filter(_.warcType == "response")
//      .toDF()
//    responses.printSchema()
//
//    responses.show(3)
//
//    // small subset of overall English records which were explicitly marked in server response, see scratchpad.txt
//    val englishRecords = responses.filter($"language" === "en")
//    println(englishRecords.count())
//    // 1
//    println(englishRecords.map(_.getAs[String]("htmlSource")).first())
//  */
//  }
//}
