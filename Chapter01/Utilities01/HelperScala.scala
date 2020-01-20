package Utilities01

import java.io.File
import org.apache.spark.sql.SparkSession
import scala.util.matching.Regex

/**
 * Constants and helper functions
 *
 * @author Phil, https://github.com/g1thubhub
 */
object HelperScala {

  val separator = File.separator
  val novellaLocation = "/Users/a/IdeaProjects/The-Spark-Workshop/resources/HoD.txt" // ToDo: Specify

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

}