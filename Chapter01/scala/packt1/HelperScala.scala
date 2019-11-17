package packt1

import java.io.File

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.util.matching.Regex

/**
 * Constants and helper functions
 *
 * @author Phil, https://github.com/g1thubhub
 */
object HelperScala {

  val separator = File.separator

  val novellaLocation = s"resources${separator}mapreduce${separator}HoD.txt" // location of input for word count programs

  val delimiterWarcWet = "WARC/1.0" // Wrong => Exception in thread "main" org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 0.0 failed 1 times, most recent failure: Lost task 0.0 in stage 0.0 (TID 0, localhost, executor driver): java.lang.OutOfMemoryError: Java heap space
  val delimiterWarcWetBytes: Array[Byte] = delimiterWarcWet.getBytes()
  val blankLine: Regex = "(?m:^(?=[\r\n]))".r
  val newLine = "[\\n\\r]+"

  def createSession(numThreads: Int = 3, name: String = "Spark Application"): SparkSession = {
    val session: SparkSession = SparkSession.builder
      .master(s"local[$numThreads]") // program simulates a single executor with numThreads cores (one local JVM with numThreads threads)
      .appName(name)
      .getOrCreate()
    session
  }

  def getNeighbours(line: String): Array[(String, Int)] = {
    val tokens: Array[String] = line.split("\\s+")
    tokens.map(token => (token, tokens.length))
  }

  def calcAverage(wordStat: (String, (Int, Int))): (String, Double) = {
    val word = wordStat._1
    val count = wordStat._2._1
    val neighbours = wordStat._2._2
    val avg = neighbours.toDouble / count.toDouble
    (word, avg)
  }

  // helper function for extracting meta info
  def extractWetMetaInfo(rawMetaInfo: String) = {
    val metaEntries = mutable.Map.empty[String, String]
    val fields = rawMetaInfo.split(newLine) // split string on newlines
    for(field <- fields) {
      val keyValue = field.split(":")
      metaEntries(keyValue(0).trim) = keyValue.slice(1, keyValue.length).mkString(":").trim
    }
    metaEntries
  }

//  // parses raw WarcWet records into domain objects of type spark.WarcRecord
//  def parseRawWetRecord(keyValue: (LongWritable, Text)): Option[WarcWetRecord] = {
//    val rawContent = keyValue._2.toString // key is a line number which is is useless
//    val matches = blankLine.findAllMatchIn(rawContent)
//    if(matches.isEmpty) { // malformed record, skip
//      None
//    }
//    else {
//      val matchStarts =  matches.map(_.end).toList // get end points of matches, only first two elements are relevant
//      val docStart = matchStarts(0) // start of record
//      val boundary = matchStarts(1) // end of meta section
//      val rawMetaInfo = rawContent.substring(docStart, boundary).trim
//      val metaPairs = extractWetMetaInfo(rawMetaInfo)
//      val pageContent = rawContent.substring(boundary + 1).trim
//      Some(WarcWetRecord(metaPairs, pageContent))
//    }
//  }
//
//  // helper function for extracting meta info
//  def extractWarcMetaInfo(rawMetaInfo: String):  mutable.Map[String, String] = {
//    val metaEntries = mutable.Map.empty[String, String]
//    val fields = rawMetaInfo.split(newLine) // split string on newlines
//    for(field <- fields) {
//      val keyValue = field.split(":")
//      metaEntries(keyValue(0).trim) = keyValue.slice(1, keyValue.length).mkString(":").trim
//    }
//    metaEntries
//  }
//
//  def extractResponseMetaInfo(responseMeta: String): (String, Option[String], Int) = {
//    val metaEntries = mutable.Map.empty[String, String]
//    val fields = responseMeta.split(newLine) // split string on newlines
//    var contentType, language = ""
//    var contentLength = -1
//
//    for(field <- fields) {
//      if(field.startsWith("Content-Type:")) {
//        contentType = field.substring(14).trim
//      }
//      else if(field.startsWith("Content-Language:")) {
//        language = field.substring(17).trim
//
//      }
//      else if(field.startsWith("Content-Length:")) {
//        contentLength = field.substring(15).trim.toInt
//      }
//    }
//    (contentType, if(language.isEmpty) None else Some(language), contentLength)
//  }
//
//  // parses raw WarcWet records into domain objects of type spark.WarcRecord
//  def parseRawWarcRecord(keyValue: (LongWritable, Text)): Option[WarcRecord] = {
//    val rawContent = keyValue._2.toString // key is a line number which is is useless
//    val matches = blankLine.findAllMatchIn(rawContent)
//    if(matches.isEmpty) { // malformed record, skip
//      None
//    }
//    else {
//      val matchStarts =  matches.map(_.end).toList // get end points of matches, only first two elements are relevant
//      val docStart = matchStarts(0) // start of record
//      val metaBoundary = matchStarts(1) // end of meta section
//      val serverBoundary = matchStarts(2) // end of server meta section
//      val rawMetaInfo = rawContent.substring(docStart, metaBoundary).trim
//      val metaPairs = extractWarcMetaInfo(rawMetaInfo)
//      val responseMeta = rawContent.substring(metaBoundary + 1, serverBoundary).trim
//      val responseMetaTriple = extractResponseMetaInfo(responseMeta)
//      val pageContent = rawContent.substring(serverBoundary + 1).trim
//      Some(WarcRecord(metaPairs, responseMetaTriple, pageContent))
//    }
//  }

}