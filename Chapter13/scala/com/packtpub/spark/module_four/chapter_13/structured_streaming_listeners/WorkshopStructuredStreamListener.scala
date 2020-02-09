package com.packtpub.spark.module_four.chapter_13.structured_streaming_listeners

import org.apache.spark.sql.streaming.StreamingQueryListener

class WorkshopStructuredStreamListener extends StreamingQueryListener{
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    val id = event.id
    val name = event.name
    val runID = event.runId

    println("Query Started:" + name + " " + id + "/" + runID)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val progress = event.progress

  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    val id = event.id
    val exception = event.exception.getOrElse("No Exception Provided")
    val runID = event.runId

    println("Query Terminated: " + id + "/" + runID + " because of: " + exception)
  }
}