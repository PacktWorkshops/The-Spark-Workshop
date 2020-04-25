package Utilities03

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd, SparkListenerTaskStart}

class RetryListener extends SparkListener {
  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    val taskId: Long = taskStart.taskInfo.taskId
    val taskAttempt: Int = taskStart.taskInfo.attemptNumber
    val stageId = taskStart.stageId
    val stageAttemptId: Int = taskStart.stageAttemptId
    println(s"@@ Starting task $taskId | attempt $taskAttempt in stage $stageId | attempt $stageAttemptId")
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val taskAttempt: Int = taskEnd.taskInfo.attemptNumber
    val stageAttempt = taskEnd.stageAttemptId
    val stageId = taskEnd.stageId
    val taskId = taskEnd.taskInfo.taskId
    val success = taskEnd.taskInfo.failed
    println(s"@@ Ending task $taskId | attempt $taskAttempt in stage $stageId | attempt $stageAttempt | has failed? $success")
  }
}
