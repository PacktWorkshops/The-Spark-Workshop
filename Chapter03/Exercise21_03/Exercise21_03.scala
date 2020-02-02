package Exercise20_03

import java.time.LocalDateTime

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import Utilities01.HelperScala.createSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import Utilities02.WarcRecord

object Exercise20_03 {
  def main(args: Array[String]): Unit = {
    val inputWarc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc" // ToDo: Change
    implicit val session = createSession(3, "Wave exploration")

    val rawRecords: RDD[Text] = extractRawRecords(inputWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(parseRawWarc)

    val threadIdsRDD: RDD[(Long, Long)] = warcRecords
      .map(record => {
        val currentUri = record.targetURI
        val startTime = LocalDateTime.now()
        val threadId: Long = Thread.currentThread().getId
        println(s"@@1 falling asleep in thread $threadId at $startTime accessing $currentUri")
        Thread.sleep(500)
        val endTime = LocalDateTime.now()
        println(s"@@2 awakening in thread $threadId at $endTime accessing $currentUri")
        (Thread.currentThread().getId, currentUri)
      })
      .filter(threadIdUri => {
        val startTime = LocalDateTime.now()
        val threadId: Long = Thread.currentThread().getId
        println(s"@@3 filter in thread $threadId at $startTime accessing ${threadIdUri._2}")
        true
      })
      .map(threadIdUri => {
        val startTime = LocalDateTime.now()
        val currentThreadId: Long = Thread.currentThread().getId
        println(s"@@4 map2 in thread $currentThreadId at $startTime accessing ${threadIdUri._2}")
        (threadIdUri._1, currentThreadId)
      })


    val distinctThreads = threadIdsRDD
      .distinct()
      .collect()
      .toList

    println(distinctThreads)
  }
}
