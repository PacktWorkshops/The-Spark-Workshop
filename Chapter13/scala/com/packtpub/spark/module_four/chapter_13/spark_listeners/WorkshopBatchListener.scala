package com.packtpub.spark.module_four.chapter_13.spark_listeners

import org.apache.spark.scheduler._
import org.joda.time.DateTime

class WorkshopBatchListener extends SparkListener{
  override def onApplicationStart(applicationStart: SparkListenerApplicationStart): Unit = {
    println("Application: " + applicationStart.appName + "(" + applicationStart.appId.getOrElse("no app id") + ") started!")
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    println("Application terminated at: " + new DateTime(applicationEnd.time))

  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageInfo = stageCompleted.stageInfo
    val duration = stageInfo.completionTime.get - stageInfo.submissionTime.get

    println("--------------- Stage " + stageInfo.stageId + " (" + stageInfo.name + ") Complete: ---------------" )
    println("Records Read: " + stageInfo.taskMetrics.inputMetrics.recordsRead)
    println("Records Written: " + stageInfo.taskMetrics.outputMetrics.recordsWritten)
    println("Number of Tasks Used: " + stageInfo.numTasks)
    println("Duration: " + duration/1000 + "s")
    println("-------------------------------------------------------------------------------\n")

  }

  override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted): Unit = super.onStageSubmitted(stageSubmitted)

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = super.onTaskStart(taskStart)

  override def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult): Unit = super.onTaskGettingResult(taskGettingResult)

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = super.onTaskEnd(taskEnd)

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = super.onJobStart(jobStart)

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {

  }

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate): Unit = super.onEnvironmentUpdate(environmentUpdate)

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = super.onBlockManagerAdded(blockManagerAdded)

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = super.onBlockManagerRemoved(blockManagerRemoved)

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = super.onUnpersistRDD(unpersistRDD)

  override def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = super.onExecutorMetricsUpdate(executorMetricsUpdate)

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = super.onExecutorAdded(executorAdded)

  override def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved): Unit = {
    println("Executor " + executorRemoved.executorId + " was removed for " + executorRemoved.reason + " at the time: " + new DateTime(executorRemoved.time))
  }

  override def onExecutorBlacklisted(executorBlacklisted: SparkListenerExecutorBlacklisted): Unit = super.onExecutorBlacklisted(executorBlacklisted)

  override def onExecutorBlacklistedForStage(executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage): Unit = super.onExecutorBlacklistedForStage(executorBlacklistedForStage)

  override def onNodeBlacklistedForStage(nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage): Unit = super.onNodeBlacklistedForStage(nodeBlacklistedForStage)

  override def onExecutorUnblacklisted(executorUnblacklisted: SparkListenerExecutorUnblacklisted): Unit = super.onExecutorUnblacklisted(executorUnblacklisted)

  override def onNodeBlacklisted(nodeBlacklisted: SparkListenerNodeBlacklisted): Unit = super.onNodeBlacklisted(nodeBlacklisted)

  override def onNodeUnblacklisted(nodeUnblacklisted: SparkListenerNodeUnblacklisted): Unit = super.onNodeUnblacklisted(nodeUnblacklisted)

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = super.onBlockUpdated(blockUpdated)

  override def onSpeculativeTaskSubmitted(speculativeTask: SparkListenerSpeculativeTaskSubmitted): Unit = super.onSpeculativeTaskSubmitted(speculativeTask)

  override def onOtherEvent(event: SparkListenerEvent): Unit = super.onOtherEvent(event)
}
