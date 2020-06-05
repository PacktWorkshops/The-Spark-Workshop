package com.packtpub.spark.module_four.chapter_13.streaming_listeners

import org.apache.spark.streaming.kafka010.OffsetRange
import org.apache.spark.streaming.scheduler._
import org.joda.time.DateTime

class WorkshopStreamListener extends StreamingListener{
  override def onStreamingStarted(streamingStarted: StreamingListenerStreamingStarted): Unit = {
    println("Streaming started at: " + new DateTime(streamingStarted.time))
  }

  override def onReceiverStarted(receiverStarted: StreamingListenerReceiverStarted): Unit = super.onReceiverStarted(receiverStarted)

  override def onReceiverError(receiverError: StreamingListenerReceiverError): Unit = super.onReceiverError(receiverError)

  override def onReceiverStopped(receiverStopped: StreamingListenerReceiverStopped): Unit = super.onReceiverStopped(receiverStopped)

  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted): Unit = super.onBatchSubmitted(batchSubmitted)

  override def onBatchStarted(batchStarted: StreamingListenerBatchStarted): Unit = {
    val batchTime = batchStarted.batchInfo.batchTime
    val numRecords = batchStarted.batchInfo.numRecords
    val startingDelay = batchStarted.batchInfo.schedulingDelay
    val numStreamInputs = batchStarted.batchInfo.streamIdToInputInfo.size

    println("--------------- Batch (" + batchTime + ") Start: ---------------------")
    println("Records Processed: " + numRecords)
    println("Streams Processed: " + numStreamInputs)
    println("Starting Delay: " + startingDelay.get + " ms")
    println("---------------------------------------------------------------------")
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted): Unit = {
    val batchTime = batchCompleted.batchInfo.batchTime
    val numRecords = batchCompleted.batchInfo.numRecords
    val processingTime = batchCompleted.batchInfo.processingDelay
    val startingDelay = batchCompleted.batchInfo.schedulingDelay
    val totalDelay = batchCompleted.batchInfo.totalDelay
    val numOutputs = batchCompleted.batchInfo.outputOperationInfos.size
    val numStreamInputs = batchCompleted.batchInfo.streamIdToInputInfo.size

    println("--------------- Batch (" + batchTime + ") Complete: ------------------")
    println("Records Processed: " + numRecords)
    println("Streams Processed: " + numStreamInputs)
    println("Outputs Processed: " + numOutputs)
    println("Starting Delay: " + startingDelay.get + " ms")
    println("Time to Process: " + processingTime.get + " ms")
    println("Total Time to Process (Delay + Processing Time): " + totalDelay.get + " ms")

    // Print summary statistics per stream / topic
    batchCompleted.batchInfo.streamIdToInputInfo.foreach(stream => {

      // Fetch Key and Value
      val streamID = stream._1
      val streamInfo = stream._2

      // Fetch High Level Information
      val inputStreamID = streamInfo.inputStreamId
      val numRecords = streamInfo.numRecords
      val description = streamInfo.metadataDescription.get
      val listOfOffsetsProcessed = streamInfo.metadata("offsets").asInstanceOf[List[OffsetRange]]
      val topic = listOfOffsetsProcessed.head.topic

      // Start Summary with High Level
      println("--------------- Topic " + inputStreamID + " (" + topic + ") Processed: ------------------")

      // if records received, print information, otherwise skip
      if (numRecords < 1){
        println("No records received from topic.")
      }
      else {
        println("Description: " + description)
        println("Records Processed: " + numRecords)

        listOfOffsetsProcessed.foreach(kafkaPartition => {
          val firstOffsetProcessed = kafkaPartition.fromOffset // fromOffset is inclusive
          val lastOffsetProcessed = kafkaPartition.untilOffset - 1 // untilOffset is exclusive
          val partitionNumber = kafkaPartition.partition
          val partitionNumRecords = kafkaPartition.count()

          // Complete Summary with Partition Level Details if records received
          if (partitionNumRecords < 1) {
            println("No records received from partition.")
          }
          else {
            println("Partition: " + partitionNumber)
            println("Offset Range Processed: " + firstOffsetProcessed + " -> " + lastOffsetProcessed)
            println("Records Processed: " + partitionNumRecords)
          }

        })
      }
    })
    println("------------------------------------------------------------")

  }

  override def onOutputOperationStarted(outputOperationStarted: StreamingListenerOutputOperationStarted): Unit = super.onOutputOperationStarted(outputOperationStarted)

  override def onOutputOperationCompleted(outputOperationCompleted: StreamingListenerOutputOperationCompleted): Unit = super.onOutputOperationCompleted(outputOperationCompleted)
}
