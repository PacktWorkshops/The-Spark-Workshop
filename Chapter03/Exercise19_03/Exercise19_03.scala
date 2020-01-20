package Exercise19_03

import org.apache.hadoop.io.Text
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}
import org.apache.spark.sql.SparkSession
import Utilities02.HelperScala.{extractRawRecords, parseRawWarc}
import Utilities02.WarcRecord

class RetryListener extends SparkListener {
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskId: Long = taskStart.taskInfo.taskId
    val taskAttempt: Int = taskStart.taskInfo.attemptNumber
    val stageId = taskStart.stageId
    val stageAttemptId: Int = taskStart.stageAttemptId
    println(s"@@ Starting task $taskId attempt $taskAttempt under stage $stageId attempt $stageAttemptId")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskAttempt: Int = taskEnd.taskInfo.attemptNumber
    val stageAttempt = taskEnd.stageAttemptId
    val stageId = taskEnd.stageId
    val taskId = taskEnd.taskInfo.taskId
    val success = taskEnd.taskInfo.failed
    println(s"@@ Task $taskId / attempt $taskAttempt in stage $stageId / attempt $stageAttempt has failed? $success")
  }
}


object Exercise18_03 {
  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .master(s"local[3, 3]")
      .appName("Failure Exploration")
      .getOrCreate()

    val retryListener = new RetryListener()
    session.sparkContext.addSparkListener(retryListener)

    val inputWarc = "/Users/a/Desktop/Buch/CC-MAIN-20191013195541-20191013222541-00000.warc"
    val rawRecords: RDD[Text] = extractRawRecords(inputWarc)
    val warcRecords: RDD[WarcRecord] = rawRecords
      .flatMap(record => {
        val parsedRawWarc = parseRawWarc(record)
                val crasher = 5 / 0
                print(crasher)
        parsedRawWarc
      })

    println(warcRecords.count())
  }

}
